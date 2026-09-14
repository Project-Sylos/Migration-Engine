// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

// writeNodeIndexes writes secondary catalog indexes for n. If prev is non-nil and
// differs, old index keys are deleted first (no delete-while-iterating a query).
func writeNodeIndexes(wb *badger.WriteBatch, side string, n NodeRecord, prev *NodeRecord) error {
	if n.ID == "" {
		return nil
	}
	if prev != nil && nodeIndexFieldsEqual(*prev, n) {
		return nil
	}
	if prev != nil {
		if err := deleteNodeIndexes(wb, side, *prev); err != nil {
			return err
		}
	}
	return setNodeIndexes(wb, side, n)
}

func nodeIndexFieldsEqual(a, b NodeRecord) bool {
	return a.Path == b.Path && a.Name == b.Name && a.DisplayPath == b.DisplayPath &&
		a.Size == b.Size && a.MTime == b.MTime
}

func setNodeIndexes(wb *badger.WriteBatch, side string, n NodeRecord) error {
	return applyNodeIndexes(side, n, wb.Set, nil)
}

func deleteNodeIndexes(wb *badger.WriteBatch, side string, n NodeRecord) error {
	return applyNodeIndexes(side, n, nil, wb.Delete)
}

func setNodeIndexesTxn(txn *badger.Txn, side string, n NodeRecord) error {
	return applyNodeIndexes(side, n, txn.Set, nil)
}

func deleteNodeIndexesTxn(txn *badger.Txn, side string, n NodeRecord) error {
	return applyNodeIndexes(side, n, nil, func(k []byte) error {
		err := txn.Delete(k)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

func applyNodeIndexes(side string, n NodeRecord, set func([]byte, []byte) error, del func([]byte) error) error {
	pathKey := PathIndexKey(side, n.Path)
	name := n.Name
	if name == "" {
		name = pathBase(n.Path)
	}
	keys := [][]byte{
		SizeIndexKey(side, n.Size, n.ID),
		MTimeIndexKey(side, ParseMTimeUnixNano(n.MTime), n.ID),
	}
	if name != "" {
		keys = append(keys, NameIndexKey(side, name, n.ID))
	}
	for _, tok := range PathSegments(n.Path, name) {
		keys = append(keys, SegIndexKey(side, tok, n.ID))
	}
	for _, g := range NodeTrigrams(n.Path, n.DisplayPath, name) {
		keys = append(keys, TriIndexKey(side, g, n.ID))
	}
	if set != nil {
		if err := set(pathKey, []byte(n.ID)); err != nil {
			return err
		}
		for _, k := range keys {
			if err := set(k, []byte{1}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := del(pathKey); err != nil {
		return err
	}
	for _, k := range keys {
		if err := del(k); err != nil {
			return err
		}
	}
	return nil
}

func pathBase(p string) string {
	p = NormalizeIndexPath(p)
	if p == "/" {
		return ""
	}
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return p
	}
	return p[i+1:]
}

func indexBucketDeltas(side string, n NodeRecord, sign int64) []string {
	keys := []string{
		SizeBucketKey(side, n.Size),
		MTimeBucketKey(side, ParseMTimeUnixNano(n.MTime)),
	}
	_ = sign
	return keys
}

// GetNodeIDByPath looks up a node id via idx:path.
func (s *Store) GetNodeIDByPath(side, p string) (string, error) {
	if s == nil || s.db == nil {
		return "", nil
	}
	var id string
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(PathIndexKey(side, p))
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

const defaultSubtreeScanLimit = 100_000

// ListSubtreeIDs streams node ids under root (exact + descendants) via idx:path.
func (s *Store) ListSubtreeIDs(side, root string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultSubtreeScanLimit
	}
	var ids []string
	after := ""
	for {
		chunk, next, done, err := s.ScanSubtreeIDs(side, root, after, limit-len(ids), SubtreeScanOpts{})
		if err != nil {
			return nil, err
		}
		ids = append(ids, chunk...)
		if done || len(ids) >= limit {
			break
		}
		after = next
	}
	return ids, nil
}

// SubtreeScanOpts configures idx:path prefix scans.
type SubtreeScanOpts struct {
	ExcludeRoot bool
}

// ScanSubtreeIDs pages node ids under root via idx:path lexical order.
// afterPath is the last path returned on the prior page ("" to start). nextAfter is the
// last path in this page for continuation. done is true when the subtree is exhausted.
func (s *Store) ScanSubtreeIDs(side, root, afterPath string, limit int, opts SubtreeScanOpts) (ids []string, nextAfter string, done bool, err error) {
	if s == nil || s.db == nil {
		return nil, "", true, nil
	}
	if limit <= 0 {
		limit = defaultSubtreeScanLimit
	}
	rootNorm := NormalizeIndexPath(root)
	sidePrefix := []byte(indexPathPrefix + side + ":")
	err = s.view(func(txn *badger.Txn) error {
		itOpts := badger.DefaultIteratorOptions
		itOpts.PrefetchValues = true
		it := txn.NewIterator(itOpts)
		defer it.Close()
		startKey := PathIndexPrefix(side, root)
		if afterPath != "" {
			startKey = PathIndexKey(side, afterPath)
		}
		for it.Seek(startKey); it.Valid(); it.Next() {
			k := it.Item().Key()
			if !bytes.HasPrefix(k, sidePrefix) {
				done = true
				return nil
			}
			pathPart := pathFromPathIndexKey(k, side)
			if pathPart == "" {
				continue
			}
			if !pathUnderRoot(pathPart, rootNorm) {
				if pathPart > rootNorm && !strings.HasPrefix(pathPart, rootNorm+"/") {
					done = true
					return nil
				}
				continue
			}
			if afterPath != "" && pathPart == afterPath {
				continue
			}
			if opts.ExcludeRoot && pathPart == rootNorm {
				continue
			}
			var id string
			if err := it.Item().Value(func(val []byte) error {
				id = string(val)
				return nil
			}); err != nil {
				return err
			}
			if id == "" {
				continue
			}
			ids = append(ids, id)
			nextAfter = pathPart
			if len(ids) >= limit {
				return nil
			}
		}
		done = true
		return nil
	})
	if err != nil {
		return nil, "", false, err
	}
	if len(ids) == 0 && afterPath == "" {
		done = true
	}
	return ids, nextAfter, done, nil
}

func pathFromPathIndexKey(key []byte, side string) string {
	prefix := indexPathPrefix + side + ":"
	s := string(key)
	if !strings.HasPrefix(s, prefix) {
		return ""
	}
	rest := s[len(prefix):]
	rest = strings.TrimSuffix(rest, "/")
	if rest == "" {
		return "/"
	}
	return rest
}

func pathUnderRoot(p, root string) bool {
	if root == "/" {
		return true
	}
	return p == root || strings.HasPrefix(p, root+"/")
}

// ScanSizeRange returns ids with size in [lo, hi] (inclusive).
func (s *Store) ScanSizeRange(side string, lo, hi int64, limit int) ([]string, error) {
	return s.scanU64Index(SizeIndexSidePrefix(side), SizeIndexBound(side, lo), SizeIndexBound(side, hi+1), limit)
}

// ScanMTimeRange returns ids with mtime nanos in [lo, hi] (inclusive).
func (s *Store) ScanMTimeRange(side string, lo, hi int64, limit int) ([]string, error) {
	return s.scanU64Index(MTimeIndexSidePrefix(side), MTimeIndexBound(side, lo), MTimeIndexBound(side, hi+1), limit)
}

func (s *Store) scanU64Index(sidePrefix, start, endExclusive []byte, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100_000
	}
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(start); it.Valid(); it.Next() {
			k := it.Item().Key()
			if !bytes.HasPrefix(k, sidePrefix) {
				break
			}
			if bytes.Compare(k, endExclusive) >= 0 {
				break
			}
			id, ok := parseIndexID(k)
			if !ok {
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

// ScanNamePrefix returns ids whose lowercased name starts with prefix.
func (s *Store) ScanNamePrefix(side, namePrefix string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100_000
	}
	prefix := NameIndexPrefix(side, namePrefix)
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			id, ok := parseIndexID(it.Item().Key())
			if !ok {
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

// ScanSegToken returns ids posting under a path/name segment token.
func (s *Store) ScanSegToken(side, token string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100_000
	}
	prefix := SegIndexPrefix(side, token)
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			id, ok := parseIndexID(it.Item().Key())
			if !ok {
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

// ScanTrigramIntersect returns ids that post under every trigram of needle (smallest-list-first intersect).
// Callers must exact-verify contains on name/path. Empty needle or no grams → nil, nil.
func (s *Store) ScanTrigramIntersect(side, needle string, limit int) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100_000
	}
	grams := ExtractTrigrams(needle)
	if len(grams) == 0 {
		return nil, nil
	}
	type posting struct {
		gram string
		ids  []string
	}
	lists := make([]posting, 0, len(grams))
	for _, g := range grams {
		ids, err := s.scanTriGram(side, g, limit)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return nil, nil
		}
		lists = append(lists, posting{gram: g, ids: ids})
	}
	// Smallest posting first for intersect.
	for i := 1; i < len(lists); i++ {
		j := i
		for j > 0 && len(lists[j-1].ids) > len(lists[j].ids) {
			lists[j-1], lists[j] = lists[j], lists[j-1]
			j--
		}
	}
	set := make(map[string]struct{}, len(lists[0].ids))
	for _, id := range lists[0].ids {
		set[id] = struct{}{}
	}
	for _, p := range lists[1:] {
		next := make(map[string]struct{}, len(set))
		for _, id := range p.ids {
			if _, ok := set[id]; ok {
				next[id] = struct{}{}
			}
		}
		set = next
		if len(set) == 0 {
			return nil, nil
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) scanTriGram(side, gram string, limit int) ([]string, error) {
	prefix := TriIndexPrefix(side, gram)
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			id, ok := parseIndexID(it.Item().Key())
			if !ok {
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

// EstimateSizeBucketSum estimates how many nodes fall in size buckets overlapping [lo, hi].
func (s *Store) EstimateSizeBucketSum(side string, lo, hi int64) (int64, error) {
	loB := sizeLog2Bucket(lo)
	hiB := sizeLog2Bucket(hi)
	if hiB < loB {
		loB, hiB = hiB, loB
	}
	var sum int64
	for b := loB; b <= hiB; b++ {
		n, err := s.GetStat(fmt.Sprintf("idx:size:%s:%d", side, b))
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}

// EstimateMTimeBucketSum estimates nodes in mtime month buckets overlapping [lo, hi] nanos.
func (s *Store) EstimateMTimeBucketSum(side string, lo, hi int64) (int64, error) {
	loM := mtimeMonthBucket(lo)
	hiM := mtimeMonthBucket(hi)
	if hiM < loM {
		loM, hiM = hiM, loM
	}
	var sum int64
	for m := loM; m <= hiM; m++ {
		n, err := s.GetStat(fmt.Sprintf("idx:mtime:%s:%d", side, m))
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}
