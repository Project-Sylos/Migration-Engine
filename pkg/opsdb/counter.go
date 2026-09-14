// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

func readInt64(txn *badger.Txn, k []byte) (int64, error) {
	item, err := txn.Get(k)
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

func writeInt64(txn *badger.Txn, k []byte, n int64) error {
	if n == 0 {
		err := txn.Delete(k)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}
	return txn.Set(k, fmt.Appendf(nil, "%d", n))
}

func incrInt64(txn *badger.Txn, k []byte, delta int64) error {
	if delta == 0 {
		return nil
	}
	cur, err := readInt64(txn, k)
	if err != nil {
		return err
	}
	return writeInt64(txn, k, cur+delta)
}

// DepthStatRow is one per-depth counter for catalog/stats sync.
type DepthStatRow struct {
	Side  string
	Depth int
	Key   string
	Count int64
}

// DepthCounterDelta is one per-depth stat mutation.
type DepthCounterDelta struct {
	Side  string
	Depth int
	Key   string
	Delta int64
}

// ApplyReviewAndDepth applies review counters and per-depth counters in one transaction.
// Deltas are coalesced by key first so a large flush is O(unique keys), not O(rows).
func (s *Store) ApplyReviewAndDepth(reviewKeys []string, reviewDeltas []int64, depth []DepthCounterDelta) error {
	if s == nil || s.db == nil {
		return nil
	}
	reviewSum := map[string]int64{}
	for i, k := range reviewKeys {
		if k == "" || i >= len(reviewDeltas) || reviewDeltas[i] == 0 {
			continue
		}
		reviewSum[k] += reviewDeltas[i]
	}
	type depthKey struct {
		Side  string
		Depth int
		Key   string
	}
	depthSum := map[depthKey]int64{}
	maxSeen := map[string]int64{}
	for _, d := range depth {
		if d.Key == "" || d.Delta == 0 || d.Side == "" {
			continue
		}
		depthSum[depthKey{Side: d.Side, Depth: d.Depth, Key: d.Key}] += d.Delta
		if int64(d.Depth) > maxSeen[d.Side] {
			maxSeen[d.Side] = int64(d.Depth)
		}
	}
	if len(reviewSum) == 0 && len(depthSum) == 0 {
		return nil
	}
	type totKey struct {
		Side string
		Key  string
	}
	return s.update(func(txn *badger.Txn) error {
		for k, delta := range reviewSum {
			if delta == 0 {
				continue
			}
			if err := incrInt64(txn, statKey(k), delta); err != nil {
				return err
			}
		}
		totSum := map[totKey]int64{}
		for k, delta := range depthSum {
			if delta == 0 {
				continue
			}
			if err := incrInt64(txn, depthStatKey(k.Side, k.Depth, k.Key), delta); err != nil {
				return err
			}
			totSum[totKey{Side: k.Side, Key: k.Key}] += delta
		}
		for k, delta := range totSum {
			if delta == 0 {
				continue
			}
			if err := incrInt64(txn, depthTotKey(k.Side, k.Key), delta); err != nil {
				return err
			}
		}
		for side, want := range maxSeen {
			cur, err := readInt64(txn, depthMaxKey(side))
			if err != nil {
				return err
			}
			if want > cur {
				if err := writeInt64(txn, depthMaxKey(side), want); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// GetDepthStat returns the per-depth counter (O(1)).
func (s *Store) GetDepthStat(side string, depth int, key string) (int64, error) {
	if s == nil || s.db == nil || key == "" {
		return 0, nil
	}
	var n int64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		n, err = readInt64(txn, depthStatKey(side, depth, key))
		return err
	})
	return n, err
}

// GetDepthStatTotal returns the all-depths sum for key (O(1)).
func (s *Store) GetDepthStatTotal(side, key string) (int64, error) {
	if s == nil || s.db == nil || key == "" {
		return 0, nil
	}
	var n int64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		n, err = readInt64(txn, depthTotKey(side, key))
		return err
	})
	return n, err
}

// GetDepthMax returns the highest depth that has received a depth-stat delta.
func (s *Store) GetDepthMax(side string) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	var n int64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		n, err = readInt64(txn, depthMaxKey(side))
		return err
	})
	return int(n), err
}

// ListDepthStats lists per-depth counters for a Duck snapshot sync (phase boundary only).
func (s *Store) ListDepthStats() ([]DepthStatRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	prefix := []byte("stat:depth:")
	var out []DepthStatRow
	err := s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			side, depth, key, ok := parseDepthStatKey(item.Key())
			if !ok {
				continue
			}
			var n int64
			if err := item.Value(func(val []byte) error {
				_, err := fmt.Sscan(string(val), &n)
				return err
			}); err != nil {
				return err
			}
			out = append(out, DepthStatRow{Side: side, Depth: depth, Key: key, Count: n})
		}
		return nil
	})
	return out, err
}

func parseDepthStatKey(k []byte) (side string, depth int, key string, ok bool) {
	s := string(k)
	const p = "stat:depth:"
	if len(s) <= len(p) || s[:len(p)] != p {
		return "", 0, "", false
	}
	rest := s[len(p):]
	sideEnd := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == ':' {
			sideEnd = i
			break
		}
	}
	if sideEnd <= 0 {
		return "", 0, "", false
	}
	side = rest[:sideEnd]
	rest = rest[sideEnd+1:]
	depthEnd := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == ':' {
			depthEnd = i
			break
		}
	}
	if depthEnd <= 0 {
		return "", 0, "", false
	}
	if _, err := fmt.Sscan(rest[:depthEnd], &depth); err != nil {
		return "", 0, "", false
	}
	key = rest[depthEnd+1:]
	if key == "" {
		return "", 0, "", false
	}
	return side, depth, key, true
}
