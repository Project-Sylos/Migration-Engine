// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

// ListIDsByCopyStatus returns node ids with the given copy_status on side
// (via idx:copy:{side}:{status}:{id}).
func (s *Store) ListIDsByCopyStatus(side, status string, limit int) ([]string, error) {
	if s == nil || strings.TrimSpace(status) == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100_000
	}
	prefix := copyStatusIndexPrefix(side, status)
	var ids []string
	err := s.db.View(func(txn *badger.Txn) error {
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
