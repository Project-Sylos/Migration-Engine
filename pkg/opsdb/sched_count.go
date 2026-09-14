// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

func readSchedCount(txn *badger.Txn, side, phase string, depth int, nodeType string) (int64, error) {
	item, err := txn.Get(schedCountKey(side, phase, depth, nodeType))
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n int64
	err = item.Value(func(val []byte) error {
		_, scanErr := fmt.Sscan(string(val), &n)
		return scanErr
	})
	return n, err
}

func writeSchedCount(txn *badger.Txn, side, phase string, depth int, nodeType string, n int64) error {
	k := schedCountKey(side, phase, depth, nodeType)
	if n <= 0 {
		err := txn.Delete(k)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}
	return txn.Set(k, fmt.Appendf(nil, "%d", n))
}

func incrSchedCount(txn *badger.Txn, side, phase string, depth int, nodeType string, delta int64) error {
	if delta == 0 {
		return nil
	}
	cur, err := readSchedCount(txn, side, phase, depth, nodeType)
	if err != nil {
		return err
	}
	return writeSchedCount(txn, side, phase, depth, nodeType, cur+delta)
}

// GetSchedCountAtDepth returns the delta-maintained pending count at side/phase/depth/type (O(1)).
func (s *Store) GetSchedCountAtDepth(side, phase string, depth int, nodeType string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	nodeType = pendingNodeType(nodeType)
	var n int64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		n, err = readSchedCount(txn, side, phase, depth, nodeType)
		return err
	})
	return n, err
}

// ApplySchedCountDeltas applies a flush-time SUM of schedcnt changes in one transaction.
func (s *Store) ApplySchedCountDeltas(deltas []SchedCountDelta) error {
	if s == nil || s.db == nil || len(deltas) == 0 {
		return nil
	}
	coalesced := map[string]int64{}
	meta := map[string]SchedCountDelta{}
	for _, d := range deltas {
		if d.Delta == 0 || d.Side == "" || d.Phase == "" {
			continue
		}
		nt := pendingNodeType(d.NodeType)
		k := fmt.Sprintf("%s\x00%s\x00%d\x00%s", d.Side, d.Phase, d.Depth, nt)
		coalesced[k] += d.Delta
		meta[k] = SchedCountDelta{Side: d.Side, Phase: d.Phase, Depth: d.Depth, NodeType: nt}
	}
	if len(coalesced) == 0 {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		for k, delta := range coalesced {
			if delta == 0 {
				continue
			}
			m := meta[k]
			if err := incrSchedCount(txn, m.Side, m.Phase, m.Depth, m.NodeType, delta); err != nil {
				return err
			}
		}
		return nil
	})
}
