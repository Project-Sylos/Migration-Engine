// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

func catalogCursorKey(kind, side string) []byte {
	if kind == "map" {
		return []byte("cat:cursor:map")
	}
	return []byte("cat:cursor:node:" + side)
}

// PutCatalogCursor overwrites the durable ingest resume watermark (one key, never deleted).
func (s *Store) PutCatalogCursor(kind, side, id string) error {
	if s == nil || s.db == nil || id == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(catalogCursorKey(kind, side), []byte(id))
	})
}

// GetCatalogCursor returns the last persisted ingest watermark.
func (s *Store) GetCatalogCursor(kind, side string) (string, error) {
	if s == nil || s.db == nil {
		return "", nil
	}
	var id string
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(catalogCursorKey(kind, side))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			id = string(val)
			return nil
		})
	})
	return id, err
}

// ListNodeIDs pages operational node ids in key order after afterID (no deletes).
func (s *Store) ListNodeIDs(side, afterID string, limit int) ([]string, error) {
	prefix := nodePrefix(side)
	var start []byte
	if afterID != "" {
		start = nodeKey(side, afterID)
	}
	return s.listPrefixIDs(prefix, start, limit, func(key []byte) (string, bool) {
		if !bytes.HasPrefix(key, prefix) {
			return "", false
		}
		id := string(key[len(prefix):])
		return id, id != ""
	})
}

// ListMapSrcIDs pages map:src ids in key order after afterID (no deletes).
func (s *Store) ListMapSrcIDs(afterID string, limit int) ([]string, error) {
	prefix := []byte("map:src:")
	var start []byte
	if afterID != "" {
		start = mapSrcKey(afterID)
	}
	return s.listPrefixIDs(prefix, start, limit, func(key []byte) (string, bool) {
		if !bytes.HasPrefix(key, prefix) {
			return "", false
		}
		id := string(key[len(prefix):])
		return id, id != ""
	})
}

func (s *Store) listPrefixIDs(prefix, start []byte, limit int, parse func([]byte) (string, bool)) ([]string, error) {
	if limit <= 0 {
		limit = 10_000
	}
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		if len(start) > 0 {
			it.Seek(start)
			if it.Valid() && bytes.Equal(it.Item().Key(), start) {
				it.Next()
			}
		} else {
			it.Seek(prefix)
		}
		for ; it.ValidForPrefix(prefix) && len(ids) < limit; it.Next() {
			id, ok := parse(it.Item().KeyCopy(nil))
			if !ok {
				continue
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list prefix ids: %w", err)
	}
	return ids, nil
}
