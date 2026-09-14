// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
)

const (
	copyStatusPending            = "pending"
	copyStatusFailed             = "failed"
	copyStatusSuccessful         = "successful"
	copyStatusAlreadyExisted     = "already_existed"
	copyStatusExcludedExplicit   = "excluded_explicit"
	copyStatusExcludedInherited  = "excluded_inherited"
	deleteStatusDeleted          = "deleted"
	deleteStatusPendingExplicit  = "pending_explicit"
	deleteStatusPendingInherited = "pending_inherited"
	deleteStatusFailed           = "failed"
	deleteStatusSkipped          = "skipped"
)

// SubtreeMutationResult aggregates one subtree status mutation.
type SubtreeMutationResult struct {
	Affected      int64
	Folders       int64
	Files         int64
	SelectedBytes int64
}

func copyStatusIsPending(s string) bool {
	return s == "" || s == copyStatusPending
}

func copyStatusIsExcludedValue(s string) bool {
	return s == copyStatusExcludedExplicit || s == copyStatusExcludedInherited
}

func copyTypedStatKey(status, nodeType string) string {
	if status == "" {
		status = copyStatusPending
	}
	return "copy/" + status + "/" + nodeTypeNT(nodeType)
}

func copyFileBytesStatKey(status string) string {
	if status == "" {
		status = copyStatusPending
	}
	return "copy/" + status + "/file_bytes"
}

func copyStatusIsComplete(s string) bool {
	return s == copyStatusSuccessful || s == copyStatusAlreadyExisted
}

func deleteStatusIsPending(s string) bool {
	return s == deleteStatusPendingExplicit || s == deleteStatusPendingInherited
}

func deleteStatusOnFrontier(s string) bool {
	return s == deleteStatusPendingExplicit || s == deleteStatusFailed
}

func nodeTypeNT(t string) string {
	if t == NodeTypeFile {
		return NodeTypeFile
	}
	return NodeTypeFolder
}

func (s *Store) noteMutation(res *SubtreeMutationResult, n NodeRecord) {
	res.Affected++
	if n.Type == NodeTypeFolder {
		res.Folders++
	} else {
		res.Files++
		res.SelectedBytes += n.Size
	}
}

// PropagateCopyFailureUnderPath marks strict descendants with pending copy as failed.
func (s *Store) PropagateCopyFailureUnderPath(parentPath string) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	parentPath = NormalizeIndexPath(parentPath)
	if parentPath == "" || parentPath == "/" {
		return out, nil
	}
	err := s.ApplySubtreeScan(SideSRC, parentPath, SubtreeScanOpts{ExcludeRoot: true}, 0, func(chunk SubtreeChunk) error {
		writes := make([]SealStatusWrite, 0, len(chunk.IDs))
		for _, id := range chunk.IDs {
			prev := chunk.Status[id]
			if !copyStatusIsPending(prev.CopyStatus) {
				continue
			}
			n := chunk.Nodes[id]
			nt := nodeTypeNT(n.Type)
			writes = append(writes, SealStatusWrite{
				Side: SideSRC,
				ID:   id,
				Status: StatusRecord{
					TraversalStatus: prev.TraversalStatus,
					CopyStatus:      copyStatusFailed,
				},
				PrevStatus: prev,
				Depth:      n.Depth,
				Deltas: []PendingDelta{{
					Phase: PhaseCopy, NodeType: nt, Add: false, PendWasSet: copyStatusIsPending(prev.CopyStatus),
				}},
			})
			s.noteMutation(&out, n)
		}
		if len(writes) == 0 {
			return nil
		}
		if _, err := s.WriteSealBatch(nil, writes, nil, nil, nil); err != nil {
			return err
		}
		return nil
	})
	return out, err
}

// CascadeDeleteUnderPath marks strict descendants with pending* delete as deleted.
func (s *Store) CascadeDeleteUnderPath(rootPath string) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" || rootPath == "/" {
		return out, nil
	}
	err := s.ApplySubtreeScan(SideSRC, rootPath, SubtreeScanOpts{ExcludeRoot: true}, 0, func(chunk SubtreeChunk) error {
		writes := make([]SealStatusWrite, 0, len(chunk.IDs))
		for _, id := range chunk.IDs {
			prev := chunk.Status[id]
			if !copyStatusIsComplete(prev.CopyStatus) || !deleteStatusIsPending(prev.DeleteStatus) {
				continue
			}
			n := chunk.Nodes[id]
			nt := nodeTypeNT(n.Type)
			writes = append(writes, SealStatusWrite{
				Side: SideSRC,
				ID:   id,
				Status: StatusRecord{
					TraversalStatus:        prev.TraversalStatus,
					CopyStatus:             prev.CopyStatus,
					DeleteStatus:           deleteStatusDeleted,
					SkippedDescendantCount: prev.SkippedDescendantCount,
				},
				PrevStatus: prev,
				Depth:      n.Depth,
				Deltas: []PendingDelta{{
					Phase: PhaseDel, NodeType: nt, Add: false, PendWasSet: deleteStatusOnFrontier(prev.DeleteStatus),
				}},
			})
			s.noteMutation(&out, n)
		}
		if len(writes) == 0 {
			return nil
		}
		if _, err := s.WriteSealBatch(nil, writes, nil, nil, nil); err != nil {
			return err
		}
		return nil
	})
	return out, err
}

// ApplySubtreeCopyExclusion writes copy exclude/unexclude on sealed status and pend:copy
// over the path prefix. Root becomes excluded_explicit; descendants excluded_inherited.
// skip, if set, leaves a node unchanged (search except list).
func (s *Store) ApplySubtreeCopyExclusion(rootPath string, excluded bool, skip func(id, path, typ string) bool) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" {
		return out, nil
	}
	err := s.ApplySubtreeScan(SideSRC, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		return s.applyCopyExclusionChunk(rootPath, excluded, skip, chunk, &out)
	})
	return out, err
}

// ApplyNodeCopyExclusion writes copy exclude/unexclude for one SRC node (no descendants).
func (s *Store) ApplyNodeCopyExclusion(id string, excluded bool) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	if id == "" {
		return out, nil
	}
	nodes, err := s.BatchGetNode(SideSRC, []string{id})
	if err != nil {
		return out, err
	}
	n, ok := nodes[id]
	if !ok {
		return out, nil
	}
	sts, err := s.BatchGetStatus(SideSRC, []string{id})
	if err != nil {
		return out, err
	}
	chunk := SubtreeChunk{
		IDs:    []string{id},
		Nodes:  nodes,
		Status: sts,
		Paths:  map[string]string{id: n.Path},
	}
	err = s.applyCopyExclusionChunk(NormalizeIndexPath(n.Path), excluded, nil, chunk, &out)
	return out, err
}

func (s *Store) applyCopyExclusionChunk(rootPath string, excluded bool, skip func(id, path, typ string) bool, chunk SubtreeChunk, out *SubtreeMutationResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	depth := make([]DepthCounterDelta, 0, len(chunk.IDs)*2)
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		if skip != nil && skip(id, n.Path, n.Type) {
			continue
		}
		prev := chunk.Status[id]
		var nextCopy string
		if excluded {
			if !copyStatusIsPending(prev.CopyStatus) {
				continue
			}
			nextCopy = copyStatusExcludedInherited
			if NormalizeIndexPath(n.Path) == rootPath {
				nextCopy = copyStatusExcludedExplicit
			}
		} else {
			if !copyStatusIsExcludedValue(prev.CopyStatus) {
				continue
			}
			nextCopy = copyStatusPending
		}
		nt := nodeTypeNT(n.Type)
		prevCopy := prev.CopyStatus
		if prevCopy == "" {
			prevCopy = copyStatusPending
		}
		writes = append(writes, SealStatusWrite{
			Side: SideSRC,
			ID:   id,
			Status: StatusRecord{
				TraversalStatus:        prev.TraversalStatus,
				CopyStatus:             nextCopy,
				DeleteStatus:           prev.DeleteStatus,
				SkippedDescendantCount: prev.SkippedDescendantCount,
			},
			PrevStatus: prev,
			Depth:      n.Depth,
			Deltas: []PendingDelta{{
				Phase: PhaseCopy, NodeType: nt,
				Add: copyStatusIsPending(nextCopy), PendWasSet: copyStatusIsPending(prev.CopyStatus),
			}},
		})
		depth = append(depth,
			DepthCounterDelta{Side: SideSRC, Depth: n.Depth, Key: copyTypedStatKey(prevCopy, nt), Delta: -1},
			DepthCounterDelta{Side: SideSRC, Depth: n.Depth, Key: copyTypedStatKey(nextCopy, nt), Delta: 1},
		)
		if n.Type == NodeTypeFile && n.Size > 0 {
			depth = append(depth,
				DepthCounterDelta{Side: SideSRC, Depth: n.Depth, Key: copyFileBytesStatKey(prevCopy), Delta: -n.Size},
				DepthCounterDelta{Side: SideSRC, Depth: n.Depth, Key: copyFileBytesStatKey(nextCopy), Delta: n.Size},
			)
		}
		s.noteMutation(out, n)
	}
	if len(writes) == 0 {
		return nil
	}
	sched, err := s.WriteSealBatch(nil, writes, nil, nil, nil)
	if err != nil {
		return err
	}
	if err := s.ApplySchedCountDeltas(sched); err != nil {
		return err
	}
	if err := s.ApplyReviewAndDepth(nil, nil, depth); err != nil {
		return err
	}
	return nil
}
