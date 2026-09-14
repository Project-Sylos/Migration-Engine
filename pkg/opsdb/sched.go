// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"

	badger "github.com/dgraph-io/badger/v4"
)

// ListSchedAtDepth pages pend: ids at side/phase/depth/type after afterID (cursor-only; no st:* filter).
func (s *Store) ListSchedAtDepth(side, phase string, depth int, nodeType, afterID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 500
	}
	nodeType = pendingNodeType(nodeType)
	prefix := pendingPrefix(side, phase, depth, nodeType)
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		if afterID != "" {
			startKey := pendingKey(side, phase, depth, nodeType, afterID)
			it.Seek(startKey)
			if it.Valid() {
				k := it.Item().KeyCopy(nil)
				if bytes.Equal(k, startKey) {
					it.Next()
				}
			}
		} else {
			it.Seek(prefix)
		}
		for ; it.ValidForPrefix(prefix) && len(ids) < limit; it.Next() {
			_, _, _, _, id, ok := parsePendingKey(it.Item().KeyCopy(nil))
			if !ok || id == "" {
				continue
			}
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}
