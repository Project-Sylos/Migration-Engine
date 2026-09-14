// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// Options configures Badger open behavior.
type Options struct {
	Dir string
}

// Store is the Badger operational database.
type Store struct {
	dir   string
	db    *badger.DB
	mu    sync.RWMutex
	seq   atomic.Uint64
	opSeq atomic.Uint64
}

// Open opens or creates a Badger store at dir.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("opsdb: dir required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("opsdb mkdir: %w", err)
	}
	bopts := badger.DefaultOptions(filepath.Clean(opts.Dir))
	bopts.Logger = nil
	bopts.SyncWrites = false
	bopts.MemTableSize = 128 << 20
	bopts.NumMemtables = 5
	bopts.NumLevelZeroTables = 10
	bopts.NumLevelZeroTablesStall = 30
	bopts.NumCompactors = 8
	bopts.ValueLogFileSize = 256 << 20
	bopts.IndexCacheSize = 64 << 20
	db, err := badger.Open(bopts)
	if err != nil {
		return nil, fmt.Errorf("opsdb open: %w", err)
	}
	s := &Store{dir: opts.Dir, db: db}
	if err := s.EnsureTriIndexV1(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("opsdb trigram index: %w", err)
	}
	return s, nil
}

// Close closes the Badger database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Sync persists durable state (Badger value log sync).
func (s *Store) Sync() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Sync()
}

// Dir returns the store directory.
func (s *Store) Dir() string {
	if s == nil {
		return ""
	}
	return s.dir
}

func (s *Store) update(fn func(txn *badger.Txn) error) error {
	return s.db.Update(fn)
}

func (s *Store) view(fn func(txn *badger.Txn) error) error {
	return s.db.View(fn)
}

func putNodeTxn(txn *badger.Txn, side string, n NodeRecord, payload []byte) error {
	if err := txn.Set(nodeKey(side, n.ID), payload); err != nil {
		return err
	}
	if n.ParentID != "" {
		if err := txn.Set(childKey(side, n.ParentID, n.ID), []byte{1}); err != nil {
			return err
		}
	}
	return setNodeIndexesTxn(txn, side, n)
}

// PutNode writes catalog metadata (create or replace) and secondary indexes.
func (s *Store) PutNode(side string, n NodeRecord) error {
	if n.ID == "" {
		return fmt.Errorf("opsdb PutNode: empty id")
	}
	b, err := encode(n)
	if err != nil {
		return err
	}
	prev, _, err := s.GetNode(side, n.ID)
	if err != nil {
		return err
	}
	err = s.update(func(txn *badger.Txn) error {
		if prev.ID != "" && !nodeIndexFieldsEqual(prev, n) {
			if err := deleteNodeIndexesTxn(txn, side, prev); err != nil {
				return err
			}
		}
		return putNodeTxn(txn, side, n, b)
	})
	if err != nil {
		return err
	}
	keys, deltas := appendIndexBucketDeltas(nil, nil, side, n, func() *NodeRecord {
		if prev.ID == "" {
			return nil
		}
		return &prev
	}())
	return s.ApplyReviewAndDepth(keys, deltas, nil)
}

// PutStatus overwrites the status overlay for id.
func (s *Store) PutStatus(side, id string, st StatusRecord) error {
	if id == "" {
		return fmt.Errorf("opsdb PutStatus: empty id")
	}
	return s.update(func(txn *badger.Txn) error {
		return writePhaseStatusTxn(txn, side, id, st)
	})
}

// BatchPutStatus writes status overlays for many ids in one transaction.
func (s *Store) BatchPutStatus(side string, puts map[string]StatusRecord) error {
	if len(puts) == 0 {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		for id, st := range puts {
			if id == "" {
				continue
			}
			if err := writePhaseStatusTxn(txn, side, id, st); err != nil {
				return err
			}
		}
		return nil
	})
}

// PutIDMap writes bidirectional map entries.
func (s *Store) PutIDMap(m IDMapRecord) error {
	if m.SrcID == "" || m.DstID == "" {
		return fmt.Errorf("opsdb PutIDMap: empty ids")
	}
	b, err := encode(m)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Set(mapSrcKey(m.SrcID), b); err != nil {
			return err
		}
		return txn.Set(mapDstKey(m.DstID), b)
	})
}

func pendingWrite(add, exists bool) (set, del bool, delta int64) {
	if add {
		if exists {
			return false, false, 0
		}
		return true, false, 1
	}
	if !exists {
		return false, false, 0
	}
	return false, true, -1
}

func applyPendingKeyTxn(txn *badger.Txn, side, phase string, depth int, nodeType, id string, add bool) (bool, error) {
	if id == "" {
		return false, nil
	}
	nodeType = pendingNodeType(nodeType)
	pk := pendingKey(side, phase, depth, nodeType, id)
	_, err := txn.Get(pk)
	exists := err == nil
	if err != nil && err != badger.ErrKeyNotFound {
		return false, err
	}
	set, del, _ := pendingWrite(add, exists)
	if set {
		return true, txn.Set(pk, []byte{1})
	}
	if del {
		return true, txn.Delete(pk)
	}
	return false, nil
}

func pendingSchedDelta(side, phase, nodeType string, depth int, add bool) SchedCountDelta {
	delta := int64(1)
	if !add {
		delta = -1
	}
	return SchedCountDelta{Side: side, Phase: phase, Depth: depth, NodeType: nodeType, Delta: delta}
}

func applyPendingDeltasTxn(txn *badger.Txn, side, id string, depth int, deltas []PendingDelta) ([]SchedCountDelta, error) {
	var sched []SchedCountDelta
	for _, d := range deltas {
		nt := pendingNodeType(d.NodeType)
		changed, err := applyPendingKeyTxn(txn, side, d.Phase, depth, nt, id, d.Add)
		if err != nil {
			return nil, err
		}
		if changed {
			sched = append(sched, pendingSchedDelta(side, d.Phase, nt, depth, d.Add))
		}
	}
	return sched, nil
}

func addPendingTxn(txn *badger.Txn, side, phase string, depth int, nodeType, id string) error {
	changed, err := applyPendingKeyTxn(txn, side, phase, depth, nodeType, id, true)
	if err != nil || !changed {
		return err
	}
	return incrSchedCount(txn, side, phase, depth, pendingNodeType(nodeType), 1)
}

func deletePendingTxn(txn *badger.Txn, side, phase string, depth int, nodeType, id string) error {
	changed, err := applyPendingKeyTxn(txn, side, phase, depth, nodeType, id, false)
	if err != nil || !changed {
		return err
	}
	return incrSchedCount(txn, side, phase, depth, pendingNodeType(nodeType), -1)
}

// PendingDelta is one frontier key to add or remove with a node+status write.
type PendingDelta struct {
	Phase      string
	NodeType   string
	Add        bool
	PendWasSet bool
	// RetainKey decrements schedcnt without deleting the pend key (fold tickets).
	RetainKey bool
}

// SchedCountDelta is a flush-time SUM for schedcnt:{side}:{phase}:{depth}:{type}.
type SchedCountDelta struct {
	Side     string
	Phase    string
	Depth    int
	NodeType string
	Delta    int64
}

// PutNodeStatusPending writes node, status, and pending keys in one transaction.
// It does not touch schedcnt; returned deltas are +1/-1 for keys that actually changed.
func (s *Store) PutNodeStatusPending(side string, n NodeRecord, st StatusRecord, depth int, deltas []PendingDelta) ([]SchedCountDelta, error) {
	if n.ID == "" {
		return nil, fmt.Errorf("opsdb PutNodeStatusPending: empty id")
	}
	nb, err := encode(n)
	if err != nil {
		return nil, err
	}
	var sched []SchedCountDelta
	err = s.update(func(txn *badger.Txn) error {
		sched = nil
		if err := putNodeTxn(txn, side, n, nb); err != nil {
			return err
		}
		if err := writePhaseStatusTxn(txn, side, n.ID, st); err != nil {
			return err
		}
		var err error
		sched, err = applyPendingDeltasTxn(txn, side, n.ID, depth, deltas)
		return err
	})
	return sched, err
}

// PutStatusPendingKeys overwrites st: and applies pending key deltas without schedcnt.
// Returns sched deltas for keys that actually changed.
func (s *Store) PutStatusPendingKeys(side, id string, st StatusRecord, depth int, deltas []PendingDelta) ([]SchedCountDelta, error) {
	if id == "" {
		return nil, fmt.Errorf("opsdb PutStatusPendingKeys: empty id")
	}
	var sched []SchedCountDelta
	err := s.update(func(txn *badger.Txn) error {
		sched = nil
		if err := writePhaseStatusTxn(txn, side, id, st); err != nil {
			return err
		}
		var err error
		sched, err = applyPendingDeltasTxn(txn, side, id, depth, deltas)
		return err
	})
	return sched, err
}

// AddPending adds id to a round frontier set (write once per round).
func (s *Store) AddPending(side, phase string, depth int, nodeType, id string) error {
	if id == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		return addPendingTxn(txn, side, phase, depth, nodeType, id)
	})
}

// DeletePending removes one frontier id at side/phase/depth/type.
func (s *Store) DeletePending(side, phase string, depth int, nodeType, id string) error {
	if id == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		return deletePendingTxn(txn, side, phase, depth, nodeType, id)
	})
}

func statusMatchesPendingPhase(st StatusRecord, phase, wantStatus string) bool {
	if wantStatus == "" {
		return true
	}
	switch phase {
	case PhaseCopy:
		cs := st.CopyStatus
		if cs == "" {
			cs = wantStatus
		}
		return cs == wantStatus
	case PhaseDel:
		ds := st.DeleteStatus
		if ds == "" {
			return false
		}
		return ds == wantStatus
	default:
		ts := st.TraversalStatus
		if ts == "" {
			ts = wantStatus
		}
		return ts == wantStatus
	}
}

// DropPendingPrefix removes all pending keys under side/phase/depth/type.
func (s *Store) DropPendingPrefix(side, phase string, depth int, nodeType string) error {
	nodeType = pendingNodeType(nodeType)
	prefix := pendingPrefix(side, phase, depth, nodeType)
	return s.update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return writeSchedCount(txn, side, phase, depth, nodeType, 0)
	})
}

// GetNode returns catalog metadata for id.
func (s *Store) GetNode(side, id string) (NodeRecord, bool, error) {
	var out NodeRecord
	found := false
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(side, id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			out, err = decodeNode(val)
			return err
		})
	})
	return out, found, err
}

// GetStatus returns status overlay for id.
func (s *Store) GetStatus(side, id string) (StatusRecord, bool, error) {
	var out StatusRecord
	found := false
	err := s.view(func(txn *badger.Txn) error {
		st, ok, err := mergedStatusTxn(txn, side, id)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		found = true
		out = st
		return nil
	})
	return out, found, err
}

// BatchGetStatus returns status for each id (missing ids omitted).
func (s *Store) BatchGetStatus(side string, ids []string) (map[string]StatusRecord, error) {
	out := make(map[string]StatusRecord, len(ids))
	err := s.view(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue
			}
			st, ok, err := mergedStatusTxn(txn, side, id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			out[id] = st
		}
		return nil
	})
	return out, err
}

// BatchGetNode returns catalog metadata for each id (missing ids omitted).
func (s *Store) BatchGetNode(side string, ids []string) (map[string]NodeRecord, error) {
	out := make(map[string]NodeRecord, len(ids))
	err := s.view(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue
			}
			item, err := txn.Get(nodeKey(side, id))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var n NodeRecord
			if err := item.Value(func(val []byte) error {
				n, err = decodeNode(val)
				return err
			}); err != nil {
				return err
			}
			out[id] = n
		}
		return nil
	})
	return out, err
}

// BatchGetNodeStatus loads node records and status overlays for ids in one view.
func (s *Store) BatchGetNodeStatus(side string, ids []string) (map[string]NodeRecord, map[string]StatusRecord, error) {
	nodes := make(map[string]NodeRecord, len(ids))
	sts := make(map[string]StatusRecord, len(ids))
	err := s.view(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue
			}
			item, err := txn.Get(nodeKey(side, id))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var n NodeRecord
			if err := item.Value(func(val []byte) error {
				n, err = decodeNode(val)
				return err
			}); err != nil {
				return err
			}
			nodes[id] = n
			st, ok, err := mergedStatusTxn(txn, side, id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			sts[id] = st
		}
		return nil
	})
	return nodes, sts, err
}

// WalkStatus iterates status keys in id order after afterID, returning merged status per id.
func (s *Store) WalkStatus(side, afterID string, reverse bool, fn func(id string, st StatusRecord) (bool, error)) error {
	if s == nil || s.db == nil || fn == nil {
		return nil
	}
	return s.view(func(txn *badger.Txn) error {
		travPrefix := statusPhasePrefix(side, PhaseTrav)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		it.Seek(travPrefix)
		hasTrav := it.ValidForPrefix(travPrefix)
		it.Close()
		if hasTrav {
			return walkStatusPhaseMergedTxn(txn, side, PhaseTrav, afterID, reverse, fn)
		}
		return walkLegacyStatusTxn(txn, side, afterID, reverse, fn)
	})
}

func walkStatusPhaseMergedTxn(txn *badger.Txn, side, phase, afterID string, reverse bool, fn func(id string, st StatusRecord) (bool, error)) error {
	prefix := statusPhasePrefix(side, phase)
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse
	it := txn.NewIterator(opts)
	defer it.Close()
	if reverse {
		if afterID != "" {
			k := statusPhaseKeyFor(side, phase, afterID)
			it.Seek(k)
			if it.Valid() && bytes.Equal(it.Item().Key(), k) {
				it.Next()
			}
		} else {
			it.Seek(append(append([]byte(nil), prefix...), 0xff))
		}
	} else if afterID != "" {
		k := statusPhaseKeyFor(side, phase, afterID)
		it.Seek(k)
		if it.Valid() && bytes.Equal(it.Item().Key(), k) {
			it.Next()
		}
	} else {
		it.Seek(prefix)
	}
	for ; it.ValidForPrefix(prefix); it.Next() {
		_, _, id, ok := parseStatusPhaseKey(it.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		st, ok, err := mergedStatusTxn(txn, side, id)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		cont, err := fn(id, st)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func walkLegacyStatusTxn(txn *badger.Txn, side, afterID string, reverse bool, fn func(id string, st StatusRecord) (bool, error)) error {
	prefix := statusPrefix(side)
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse
	it := txn.NewIterator(opts)
	defer it.Close()
	if reverse {
		if afterID != "" {
			k := statusKey(side, afterID)
			it.Seek(k)
			if it.Valid() && bytes.Equal(it.Item().Key(), k) {
				it.Next()
			}
		} else {
			it.Seek(append(append([]byte(nil), prefix...), 0xff))
		}
	} else if afterID != "" {
		k := statusKey(side, afterID)
		it.Seek(k)
		if it.Valid() && bytes.Equal(it.Item().Key(), k) {
			it.Next()
		}
	} else {
		it.Seek(prefix)
	}
	for ; it.ValidForPrefix(prefix); it.Next() {
		id, ok := parseStatusKey(it.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		st, ok, err := mergedStatusTxn(txn, side, id)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		cont, err := fn(id, st)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// GetMapBySrc returns dst id for src id.
func (s *Store) GetMapBySrc(srcID string) (IDMapRecord, bool, error) {
	var out IDMapRecord
	found := false
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(mapSrcKey(srcID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			out, err = decodeIDMap(val)
			return err
		})
	})
	return out, found, err
}

// GetMapByDst returns src id for dst id.
func (s *Store) GetMapByDst(dstID string) (IDMapRecord, bool, error) {
	var out IDMapRecord
	found := false
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(mapDstKey(dstID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			out, err = decodeIDMap(val)
			return err
		})
	})
	return out, found, err
}

// BatchGetMap returns SRC↔DST maps for ids (missing omitted). dstKey true looks up map:dst, false map:src.
func (s *Store) BatchGetMap(ids []string, dstKey bool) (map[string]IDMapRecord, error) {
	out := make(map[string]IDMapRecord, len(ids))
	err := s.view(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue
			}
			k := mapSrcKey(id)
			if dstKey {
				k = mapDstKey(id)
			}
			item, err := txn.Get(k)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var rec IDMapRecord
			if err := item.Value(func(val []byte) error {
				rec, err = decodeIDMap(val)
				return err
			}); err != nil {
				return err
			}
			out[id] = rec
		}
		return nil
	})
	return out, err
}

// BatchGetKids returns packed SRC/DST children for each parent id (missing omitted).
func (s *Store) BatchGetKids(side string, parentIDs []string) (map[string][]KidRecord, error) {
	out := make(map[string][]KidRecord, len(parentIDs))
	err := s.view(func(txn *badger.Txn) error {
		for _, parentID := range parentIDs {
			if parentID == "" {
				continue
			}
			item, err := txn.Get(kidsKey(side, parentID))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var kids []KidRecord
			if err := item.Value(func(val []byte) error {
				kids, err = decodeKids(val)
				return err
			}); err != nil {
				return err
			}
			if len(kids) > 0 {
				out[parentID] = kids
			}
		}
		return nil
	})
	return out, err
}

func collectChildren(it *badger.Iterator, side, parentID, afterID string, limit int) []string {
	prefix := childPrefix(side, parentID)
	if afterID != "" {
		start := childKey(side, parentID, afterID)
		it.Seek(start)
		if it.Valid() {
			k := it.Item().KeyCopy(nil)
			if bytes.Equal(k, start) {
				it.Next()
			}
		}
	} else {
		it.Seek(prefix)
	}
	var ids []string
	for ; it.ValidForPrefix(prefix) && len(ids) < limit; it.Next() {
		id, ok := parseChildKey(it.Item().KeyCopy(nil))
		if !ok || id == "" {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// ListChildren returns child node ids for parent_id in sort order, up to limit after afterID.
func (s *Store) ListChildren(side, parentID, afterID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		ids = collectChildren(it, side, parentID, afterID, limit)
		return nil
	})
	return ids, err
}

// ListChildrenMany returns child ids per parent in one view (key order, up to limit each).
func (s *Store) ListChildrenMany(side string, parentIDs []string, limit int) (map[string][]string, error) {
	if limit <= 0 {
		limit = 10000
	}
	out := make(map[string][]string, len(parentIDs))
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for _, parentID := range parentIDs {
			if parentID == "" {
				continue
			}
			ids := collectChildren(it, side, parentID, "", limit)
			if len(ids) > 0 {
				out[parentID] = ids
			}
		}
		return nil
	})
	return out, err
}

// ListRecentLogs returns the newest log records first.
func (s *Store) ListRecentLogs(limit int) ([]LogRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}
	prefix := []byte("log:")
	var out []LogRecord
	err := s.view(func(txn *badger.Txn) error {
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
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

// ListPending pages pending frontier ids at depth/type, optionally filtered by wantStatus on st:*.
func (s *Store) ListPending(side, phase string, depth int, nodeType, afterID, wantStatus string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 500
	}
	nodeType = pendingNodeType(nodeType)
	prefix := pendingPrefix(side, phase, depth, nodeType)
	var ids []string
	err := s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		startKey := prefix
		if afterID != "" {
			startKey = pendingKey(side, phase, depth, nodeType, afterID)
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
			if wantStatus != "" {
				st, ok, err := mergedStatusTxn(txn, side, id)
				if err != nil {
					return err
				}
				if !ok || !statusMatchesPendingPhase(st, phase, wantStatus) {
					continue
				}
			}
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}

// IncrStat adds delta to a review counter key.
func (s *Store) IncrStat(key string, delta int64) error {
	if key == "" || delta == 0 {
		return nil
	}
	return s.ApplyReviewAndDepth([]string{key}, []int64{delta}, nil)
}

// SetStat sets an absolute review counter value.
func (s *Store) SetStat(key string, value int64) error {
	if s == nil || s.db == nil || key == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		return writeInt64(txn, statKey(key), value)
	})
}

// GetStat reads a review counter.
func (s *Store) GetStat(key string) (int64, error) {
	var cur int64
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(statKey(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			_, err := fmt.Sscan(string(val), &cur)
			return err
		})
	})
	return cur, err
}

// AppendLog appends one log record.
func (s *Store) AppendLog(rec LogRecord) error {
	return s.AppendLogs([]LogRecord{rec})
}

// AppendLogs appends many append-only log records in one Badger WriteBatch flush.
func (s *Store) AppendLogs(recs []LogRecord) error {
	if s == nil || s.db == nil || len(recs) == 0 {
		return nil
	}
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for i := range recs {
		if err := writeSealLog(wb, &recs[i]); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// GetLogsByIDs loads log records by their id (task failure logs and general logs).
func (s *Store) GetLogsByIDs(ids []string) (map[string]LogRecord, error) {
	out := make(map[string]LogRecord)
	if s == nil || s.db == nil || len(ids) == 0 {
		return out, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			item, err := txn.Get(logIDKey(id))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}
			var rec LogRecord
			if err := item.Value(func(val []byte) error {
				rec, err = decodeLog(val)
				return err
			}); err != nil {
				return err
			}
			out[id] = rec
		}
		return nil
	})
	return out, err
}

// AppendDBOp appends one timing sample.
func (s *Store) AppendDBOp(rec DBOpRecord) error {
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	seq := s.opSeq.Add(1)
	b, err := encode(rec)
	if err != nil {
		return err
	}
	k := opKey(rec.At.UnixNano(), seq)
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(k, b)
	})
}

// ListDBOps returns recent timing samples (newest first). If opFilter is non-empty, only that op.
func (s *Store) ListDBOps(opFilter string, limit int) ([]DBOpRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	prefix := []byte("op:")
	var out []DBOpRecord
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(append(append([]byte(nil), prefix...), 0xff)); it.ValidForPrefix(prefix) && len(out) < limit; it.Next() {
			var rec DBOpRecord
			if err := it.Item().Value(func(val []byte) error {
				var decErr error
				rec, decErr = decodeDBOp(val)
				return decErr
			}); err != nil {
				return err
			}
			if opFilter != "" && rec.Op != opFilter {
				continue
			}
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

// AppendQueueStats stores metrics and updates latest pointer.
func (s *Store) AppendQueueStats(rec QueueStatsRecord) error {
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	b, err := encode(rec)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Set(qstatKey(rec.QueueKey, rec.Phase, rec.At.UnixNano()), b); err != nil {
			return err
		}
		return txn.Set(qstatLatestKey(rec.QueueKey, rec.Phase), b)
	})
}

// LatestQueueStats returns the latest metrics JSON for queue_key and phase.
func (s *Store) LatestQueueStats(queueKey, phase string) (*QueueStatsRecord, error) {
	if s == nil {
		return nil, nil
	}
	var rec QueueStatsRecord
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(qstatLatestKey(queueKey, phase))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var err error
			rec, err = decodeQueueStats(val)
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	if rec.QueueKey == "" && rec.MetricsJSON == "" {
		return nil, nil
	}
	return &rec, nil
}

// AllLatestQueueStats returns the latest metrics row per (queue_key, phase).
func (s *Store) AllLatestQueueStats() ([]QueueStatsRecord, error) {
	if s == nil {
		return nil, nil
	}
	prefix := []byte("qstat:latest:")
	var out []QueueStatsRecord
	err := s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			rec, err := decodeQueueStats(val)
			if err != nil {
				return err
			}
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

// AppendTaskError appends one task error record.
func (s *Store) AppendTaskError(rec TaskErrorRecord) error {
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	id := rec.NodeID
	if id == "" {
		id = fmt.Sprintf("%d", rec.At.UnixNano())
	}
	b, err := encode(rec)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(taskErrKey(rec.At.UnixNano(), id), b)
	})
}

// NextCatalogSeq returns a monotonic batch sequence number.
func (s *Store) NextCatalogSeq() uint64 {
	return s.seq.Add(1)
}
