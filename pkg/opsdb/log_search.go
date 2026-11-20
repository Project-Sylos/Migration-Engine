// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// LogSearchQuery filters persisted migration logs.
type LogSearchQuery struct {
	TextExact string
	Action    string
	NodeID    string
	From      time.Time
	To        time.Time
	Limit     int
}

// SearchLogs returns newest-matching logs first. Exact text is a substring match on Message.
func (s *Store) SearchLogs(q LogSearchQuery) ([]LogRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	if q.NodeID != "" {
		return s.searchLogsByIndex(logNodeIndexPrefix(q.NodeID), q, limit)
	}
	if q.Action != "" {
		return s.searchLogsByIndex(logActionIndexPrefix(q.Action), q, limit)
	}
	return s.searchLogsScan(q, limit)
}

func (s *Store) searchLogsByIndex(prefix []byte, q LogSearchQuery, limit int) ([]LogRecord, error) {
	var out []LogRecord
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(append(append([]byte(nil), prefix...), 0xff)); it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
			key := it.Item().KeyCopy(nil)
			// idx:log:node:{id}:{nano}:{logID} or idx:log:action:{action}:{nano}:{logID}
			parts := bytes.Split(key, []byte(":"))
			if len(parts) < 2 {
				continue
			}
			logID := string(parts[len(parts)-1])
			item, err := txn.Get(logIDKey(logID))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var rec LogRecord
			if err := item.Value(func(val []byte) error {
				var decErr error
				rec, decErr = decodeLog(val)
				return decErr
			}); err != nil {
				return err
			}
			if !logMatches(rec, q) {
				continue
			}
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

func (s *Store) searchLogsScan(q LogSearchQuery, limit int) ([]LogRecord, error) {
	prefix := []byte("log:")
	var out []LogRecord
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(append(append([]byte(nil), prefix...), 0xff)); it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
			var rec LogRecord
			if err := it.Item().Value(func(val []byte) error {
				var decErr error
				rec, decErr = decodeLog(val)
				return decErr
			}); err != nil {
				return err
			}
			if !logMatches(rec, q) {
				continue
			}
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

func logMatches(rec LogRecord, q LogSearchQuery) bool {
	if !q.From.IsZero() && rec.At.Before(q.From) {
		return false
	}
	if !q.To.IsZero() && rec.At.After(q.To) {
		return false
	}
	if q.Action != "" && rec.Action != q.Action {
		return false
	}
	if q.NodeID != "" && rec.NodeID != q.NodeID {
		return false
	}
	if q.TextExact != "" && !strings.Contains(rec.Message, q.TextExact) {
		return false
	}
	return true
}
