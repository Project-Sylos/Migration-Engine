// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "fmt"

// ApplySubtreeCopyRetry marks (mark=true) failed→pending or unmarks (mark=false)
// pending→failed under rootPath via idx:path scan, retargeting pend:copy.
func (s *Store) ApplySubtreeCopyRetry(rootPath string, mark bool) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" {
		return out, nil
	}
	err := s.ApplySubtreeScan(SideSRC, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		return s.applyCopyRetryChunk(mark, chunk, &out)
	})
	return out, err
}

// ApplyNodeCopyRetry marks or unmarks copy retry for one SRC node.
func (s *Store) ApplyNodeCopyRetry(id string, mark bool) (SubtreeMutationResult, error) {
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
	if err := s.applyCopyRetryChunk(mark, chunk, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) applyCopyRetryChunk(mark bool, chunk SubtreeChunk, out *SubtreeMutationResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	depth := make([]DepthCounterDelta, 0, len(chunk.IDs)*2)
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		prev := chunk.Status[id]
		prevCopy := prev.CopyStatus
		if prevCopy == "" {
			prevCopy = copyStatusPending
		}
		var nextCopy string
		nextExcl := prev.ExclusionSource
		if mark {
			if prevCopy != copyStatusFailed {
				continue
			}
			nextCopy = copyStatusPending
			nextExcl = ExclusionRetryMarkCopy
		} else {
			if prevCopy != copyStatusPending || prev.ExclusionSource != ExclusionRetryMarkCopy {
				continue
			}
			nextCopy = copyStatusFailed
			nextExcl = ""
		}
		nt := nodeTypeNT(n.Type)
		writes = append(writes, SealStatusWrite{
			Side: SideSRC,
			ID:   id,
			Status: StatusRecord{
				TraversalStatus:        prev.TraversalStatus,
				CopyStatus:             nextCopy,
				DeleteStatus:           prev.DeleteStatus,
				SkippedDescendantCount: prev.SkippedDescendantCount,
				ExclusionSource:        nextExcl,
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
	return s.ApplyReviewAndDepth(nil, nil, depth)
}
