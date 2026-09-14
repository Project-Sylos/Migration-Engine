// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

func parentIndexPath(path string) string {
	path = NormalizeIndexPath(path)
	if path == "" || path == "/" {
		return ""
	}
	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return "/"
	}
	return path[:idx]
}

// childOnPath returns the immediate child of ancestorPath that lies on the path to tip.
// Example: ancestor=/A, tip=/A/B/D -> /A/B.
func childOnPath(ancestorPath, tip string) string {
	tip = NormalizeIndexPath(tip)
	ancestorPath = NormalizeIndexPath(ancestorPath)
	if tip == "" || tip == "/" || ancestorPath == tip {
		return ""
	}
	var rest string
	if ancestorPath == "/" {
		rest = strings.TrimPrefix(tip, "/")
	} else {
		if tip != ancestorPath && !strings.HasPrefix(tip, ancestorPath+"/") {
			return ""
		}
		rest = strings.TrimPrefix(tip, ancestorPath+"/")
	}
	if rest == "" {
		return ""
	}
	seg, _, _ := strings.Cut(rest, "/")
	if ancestorPath == "/" {
		return "/" + seg
	}
	return ancestorPath + "/" + seg
}

func deleteEligibleForSkipLive(st StatusRecord) bool {
	if !copyStatusIsComplete(st.CopyStatus) {
		return false
	}
	ds := st.DeleteStatus
	return ds == "" || ds == deleteStatusFailed || deleteStatusIsPending(ds)
}

func delFrontierDelta(prev, next string, nt string) []PendingDelta {
	wasOn := deleteStatusOnFrontier(prev)
	nowOn := deleteStatusOnFrontier(next)
	if wasOn == nowOn {
		return nil
	}
	return []PendingDelta{{
		Phase: PhaseDel, NodeType: nt,
		Add: nowOn, PendWasSet: wasOn,
	}}
}

func (s *Store) writeDeleteStatus(id string, n NodeRecord, prev, next StatusRecord) error {
	nt := nodeTypeNT(n.Type)
	deltas := delFrontierDelta(prev.DeleteStatus, next.DeleteStatus, nt)
	var sched []SchedCountDelta
	err := s.update(func(txn *badger.Txn) error {
		b, err := encode(statusDelRecord(next))
		if err != nil {
			return err
		}
		// Always write st:del so SkippedDescendantCount can return to zero.
		if err := txn.Set(statusDelKey(SideSRC, id), b); err != nil {
			return err
		}
		var aerr error
		sched, aerr = applyPendingDeltasTxn(txn, SideSRC, id, n.Depth, deltas)
		return aerr
	})
	if err != nil {
		return err
	}
	return s.ApplySchedCountDeltas(sched)
}

func (s *Store) retargetSiblings(ancestorID, excludeChildID, fromStatus, toStatus string) error {
	childIDs, err := s.ListChildren(SideSRC, ancestorID, "", 100_000)
	if err != nil {
		return err
	}
	if len(childIDs) == 0 {
		return nil
	}
	nodes, err := s.BatchGetNode(SideSRC, childIDs)
	if err != nil {
		return err
	}
	stMap, err := s.BatchGetStatus(SideSRC, childIDs)
	if err != nil {
		return err
	}
	for _, id := range childIDs {
		if id == "" || id == excludeChildID {
			continue
		}
		st := stMap[id]
		if st.DeleteStatus != fromStatus {
			continue
		}
		n := nodes[id]
		next := st
		next.DeleteStatus = toStatus
		if err := s.writeDeleteStatus(id, n, st, next); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) walkDeleteSkipAncestors(tipPath string, delta int64, promote bool) error {
	if delta == 0 {
		return nil
	}
	paths := StrictAncestorPaths(tipPath)
	paths = append(paths, "/")
	for _, ancPath := range paths {
		if err := s.applyDeleteSkipAncestor(ancPath, tipPath, delta, promote); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyDeleteSkipAncestor(ancPath, tipPath string, delta int64, promote bool) error {
	ancID, err := s.GetNodeIDByPath(SideSRC, ancPath)
	if err != nil || ancID == "" {
		return err
	}
	n, ok, err := s.GetNode(SideSRC, ancID)
	if err != nil || !ok {
		return err
	}
	prev, ok, err := s.GetStatus(SideSRC, ancID)
	if err != nil || !ok {
		return err
	}
	next := prev
	next.SkippedDescendantCount = prev.SkippedDescendantCount + delta
	if next.SkippedDescendantCount < 0 {
		next.SkippedDescendantCount = 0
	}

	flippedToSkipped := false
	flippedToPending := false
	if n.Depth >= 1 {
		if promote && next.SkippedDescendantCount > 0 && deleteEligibleForSkipLive(prev) && prev.DeleteStatus != deleteStatusSkipped {
			next.DeleteStatus = deleteStatusSkipped
			flippedToSkipped = true
		}
		if !promote && prev.DeleteStatus == deleteStatusSkipped && next.SkippedDescendantCount == 0 {
			next.DeleteStatus = deleteStatusPendingExplicit
			flippedToPending = true
		}
	}

	if err := s.writeDeleteStatus(ancID, n, prev, next); err != nil {
		return err
	}

	childPath := childOnPath(ancPath, tipPath)
	var excludeChildID string
	if childPath != "" {
		excludeChildID, err = s.GetNodeIDByPath(SideSRC, childPath)
		if err != nil {
			return err
		}
	}
	if flippedToSkipped {
		return s.retargetSiblings(ancID, excludeChildID, deleteStatusPendingInherited, deleteStatusPendingExplicit)
	}
	if !flippedToPending {
		return nil
	}
	if err := s.retargetSiblings(ancID, excludeChildID, deleteStatusPendingExplicit, deleteStatusPendingInherited); err != nil {
		return err
	}
	if excludeChildID == "" {
		return nil
	}
	return s.rederiveDeleteRoot(excludeChildID)
}

func (s *Store) rederiveDeleteRoot(id string) error {
	n, ok, err := s.GetNode(SideSRC, id)
	if err != nil || !ok {
		return err
	}
	st, ok, err := s.GetStatus(SideSRC, id)
	if err != nil || !ok {
		return err
	}
	if !deleteStatusIsPending(st.DeleteStatus) {
		return nil
	}
	desired := deleteStatusPendingExplicit
	parentPath := parentIndexPath(n.Path)
	if parentPath != "" {
		parentID, err := s.GetNodeIDByPath(SideSRC, parentPath)
		if err != nil {
			return err
		}
		if parentID != "" {
			pst, ok, err := s.GetStatus(SideSRC, parentID)
			if err != nil {
				return err
			}
			if ok && deleteStatusIsPending(pst.DeleteStatus) {
				desired = deleteStatusPendingInherited
			}
		}
	}
	if st.DeleteStatus == desired {
		return nil
	}
	next := st
	next.DeleteStatus = desired
	return s.writeDeleteStatus(id, n, st, next)
}

// ApplyDeleteForestSkip marks rootPath and its subtree skipped, updates ancestor
// skipped-descendant counts, poisons eligible ancestors, and promotes orphaned siblings.
func (s *Store) ApplyDeleteForestSkip(rootPath string) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" || rootPath == "/" {
		return out, nil
	}
	var delta int64
	err := s.ApplySubtreeScan(SideSRC, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok || n.Depth < 1 {
				continue
			}
			prev := chunk.Status[id]
			if prev.DeleteStatus == deleteStatusSkipped {
				continue
			}
			if !deleteEligibleForSkipLive(prev) {
				continue
			}
			next := prev
			next.DeleteStatus = deleteStatusSkipped
			if err := s.writeDeleteStatus(id, n, prev, next); err != nil {
				return err
			}
			delta++
			s.noteMutation(&out, n)
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	if err := s.walkDeleteSkipAncestors(rootPath, delta, true); err != nil {
		return out, err
	}
	return out, nil
}

// ApplyDeleteForestUnskip restores rootPath and its skipped subtree to pending*,
// decrements ancestor skipped-descendant counts, and demotes siblings when ancestors recover.
func (s *Store) ApplyDeleteForestUnskip(rootPath string) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" || rootPath == "/" {
		return out, nil
	}
	var delta int64
	var rootID string
	err := s.ApplySubtreeScan(SideSRC, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok || n.Depth < 1 {
				continue
			}
			prev := chunk.Status[id]
			if prev.DeleteStatus != deleteStatusSkipped {
				continue
			}
			if !copyStatusIsComplete(prev.CopyStatus) {
				continue
			}
			next := prev
			next.DeleteStatus = deleteStatusPendingInherited
			if NormalizeIndexPath(n.Path) == rootPath {
				rootID = id
				next.DeleteStatus = deleteStatusPendingExplicit
			}
			if err := s.writeDeleteStatus(id, n, prev, next); err != nil {
				return err
			}
			delta++
			s.noteMutation(&out, n)
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	if err := s.walkDeleteSkipAncestors(rootPath, -delta, false); err != nil {
		return out, err
	}
	if rootID != "" {
		if err := s.rederiveDeleteRoot(rootID); err != nil {
			return out, err
		}
	}
	return out, nil
}
