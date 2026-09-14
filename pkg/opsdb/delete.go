// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

// DeleteNode removes node metadata, status, parent child index, id map, and pending keys.
func (s *Store) DeleteNode(side, id string) error {
	if s == nil || id == "" {
		return nil
	}
	n, ok, err := s.GetNode(side, id)
	if err != nil {
		return err
	}
	var srcID, dstID string
	if side == SideDST {
		if m, ok, err := s.GetMapByDst(id); err != nil {
			return err
		} else if ok {
			srcID, dstID = m.SrcID, m.DstID
		}
	}
	return s.update(func(txn *badger.Txn) error {
		if ok {
			if err := deleteNodeIndexesTxn(txn, side, n); err != nil {
				return err
			}
		}
		if err := txn.Delete(nodeKey(side, id)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err := deletePhaseStatusTxn(txn, side, id); err != nil {
			return err
		}
		if ok && n.ParentID != "" {
			if err := txn.Delete(childKey(side, n.ParentID, id)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		if srcID != "" {
			if err := txn.Delete(mapSrcKey(srcID)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		if dstID != "" {
			if err := txn.Delete(mapDstKey(dstID)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return deletePendingForIDTxn(txn, side, id)
	})
}

func deletePendingForIDTxn(txn *badger.Txn, side, id string) error {
	prefix := []byte("pend:" + side + ":")
	suffix := ":" + id
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	var keys [][]byte
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		k := it.Item().KeyCopy(nil)
		if strings.HasSuffix(string(k), suffix) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
		_, phase, depth, nodeType, _, ok := parsePendingKey(k)
		if !ok {
			continue
		}
		if err := incrSchedCount(txn, side, phase, depth, nodeType, -1); err != nil {
			return err
		}
	}
	return nil
}
