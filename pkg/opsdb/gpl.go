// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

// GPLRecord is a sparse naming/compatibility issue for a SRC node.
type GPLRecord struct {
	SrcID        string `json:"src_id"`
	Status       string `json:"status"`
	ProposedName string `json:"proposed_name,omitempty"`
	IssuesJSON   string `json:"issues_json,omitempty"`
	UpdatedAt    int64  `json:"updated_at,omitempty"`
	DstAction    string `json:"dst_action,omitempty"`
}

func gplKey(srcID string) []byte {
	return []byte("gpl:" + srcID)
}

func gplStatusKey(status, srcID string) []byte {
	return []byte("gplst:" + status + ":" + srcID)
}

// PutGPL writes or replaces a GPL issue record.
func (s *Store) PutGPL(rec GPLRecord) error {
	if s == nil || rec.SrcID == "" {
		return nil
	}
	b, err := encode(rec)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Set(gplKey(rec.SrcID), b); err != nil {
			return err
		}
		if rec.Status != "" {
			return txn.Set(gplStatusKey(rec.Status, rec.SrcID), []byte{1})
		}
		return nil
	})
}

// BatchPutGPL writes many GPL records.
func (s *Store) BatchPutGPL(recs []GPLRecord) error {
	if s == nil || len(recs) == 0 {
		return nil
	}
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for i := range recs {
		rec := &recs[i]
		if rec.SrcID == "" {
			continue
		}
		b, err := encode(*rec)
		if err != nil {
			return err
		}
		if err := wb.Set(gplKey(rec.SrcID), b); err != nil {
			return err
		}
		if rec.Status != "" {
			if err := wb.Set(gplStatusKey(rec.Status, rec.SrcID), []byte{1}); err != nil {
				return err
			}
		}
	}
	return wb.Flush()
}

// GetGPL loads one GPL record.
func (s *Store) GetGPL(srcID string) (GPLRecord, bool, error) {
	var rec GPLRecord
	if s == nil || srcID == "" {
		return rec, false, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(gplKey(srcID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return decodeInto(val, &rec)
		})
	})
	return rec, rec.SrcID != "", err
}

// BatchGetGPL loads GPL records for src ids.
func (s *Store) BatchGetGPL(srcIDs []string) (map[string]GPLRecord, error) {
	out := make(map[string]GPLRecord, len(srcIDs))
	if s == nil || len(srcIDs) == 0 {
		return out, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		for _, id := range srcIDs {
			if id == "" {
				continue
			}
			item, err := txn.Get(gplKey(id))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var rec GPLRecord
			if err := item.Value(func(val []byte) error {
				return decodeInto(val, &rec)
			}); err != nil {
				return err
			}
			out[id] = rec
		}
		return nil
	})
	return out, err
}

// DeleteGPL removes a GPL record.
func (s *Store) DeleteGPL(srcID string) error {
	if s == nil || srcID == "" {
		return nil
	}
	prev, ok, err := s.GetGPL(srcID)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(gplKey(srcID)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if ok && prev.Status != "" {
			if err := txn.Delete(gplStatusKey(prev.Status, srcID)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}

// ListGPLByStatus returns src ids with the given status (via gplst: postfix index).
func (s *Store) ListGPLByStatus(status string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100_000
	}
	prefix := []byte("gplst:" + status + ":")
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			k := string(it.Item().Key())
			id := k[len(prefix):]
			if id == "" {
				continue
			}
			ids = append(ids, id)
			if len(ids) >= limit {
				return nil
			}
		}
		return nil
	})
	return ids, err
}

// GPLMatchesFilter reports whether the sparse issue matches a path-issue filter token.
func GPLMatchesFilter(rec GPLRecord, filter, category string) bool {
	if filter == "" && category == "" {
		return true
	}
	st := rec.Status
	switch filter {
	case "issues":
		if st != "pending" && st != "manual_review" {
			return false
		}
	case "manual":
		if st != "manual_review" {
			return false
		}
	case "accepted":
		if st != "accepted" {
			return false
		}
	case "rejected":
		return false // gpl_status ignored is on status overlay, not issue row
	case "none":
		return false
	case "":
		// category-only
	default:
		return false
	}
	if category != "" && rec.IssuesJSON != "" {
		if !strings.Contains(rec.IssuesJSON, category) {
			return false
		}
	}
	return true
}
