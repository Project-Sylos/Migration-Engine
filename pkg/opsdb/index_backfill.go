// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	metaTriIndexV1Key = "meta:tri_index_v1"
	metaTriIndexV1Val = "1"
)

// HasTriIndexV1 reports whether trigram secondary indexes were backfilled or written on insert.
func (s *Store) HasTriIndexV1() (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(metaTriIndexV1Key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			ok = string(val) == metaTriIndexV1Val
			return nil
		})
	})
	return ok, err
}

// EnsureTriIndexV1 rebuilds name/seg/tri secondary indexes when meta:tri_index_v1 is missing.
// New inserts already write trigrams inline; this only repairs stores created before idx:tri.
func (s *Store) EnsureTriIndexV1() error {
	if s == nil || s.db == nil {
		return nil
	}
	ok, err := s.HasTriIndexV1()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if err := s.RebuildSecondaryIndexes(); err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set([]byte(metaTriIndexV1Key), []byte(metaTriIndexV1Val))
	})
}

// RebuildSecondaryIndexes rewrites name/size/mtime/seg/tri indexes from all SRC+DST node records.
func (s *Store) RebuildSecondaryIndexes() error {
	if s == nil || s.db == nil {
		return nil
	}
	for _, side := range []string{SideSRC, SideDST} {
		if err := s.rebuildSideIndexes(side); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) rebuildSideIndexes(side string) error {
	const page = 2000
	after := ""
	for {
		ids, err := s.ListNodeIDs(side, after, page)
		if err != nil {
			return fmt.Errorf("opsdb rebuild list %s: %w", side, err)
		}
		if len(ids) == 0 {
			return nil
		}
		nodes, err := s.BatchGetNode(side, ids)
		if err != nil {
			return fmt.Errorf("opsdb rebuild get %s: %w", side, err)
		}
		wb := s.db.NewWriteBatch()
		for _, id := range ids {
			n, ok := nodes[id]
			if !ok || n.ID == "" {
				continue
			}
			if err := deleteNodeIndexes(wb, side, n); err != nil {
				wb.Cancel()
				return err
			}
			if err := setNodeIndexes(wb, side, n); err != nil {
				wb.Cancel()
				return err
			}
		}
		if err := wb.Flush(); err != nil {
			return fmt.Errorf("opsdb rebuild flush %s: %w", side, err)
		}
		after = ids[len(ids)-1]
		if len(ids) < page {
			return nil
		}
	}
}
