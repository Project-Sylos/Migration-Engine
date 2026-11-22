// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

// ApplyCopyRetryChunk marks (Mark) failed→pending or unmarks (!Mark) pending→failed
// for one chunk of SRC nodes, retargeting pend:copy.
func (s *Store) ApplyCopyRetryChunk(sc RetryScope, chunk SubtreeChunk, out *SubtreeMutationResult) error {
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
		if sc.Mark {
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
	sched, err := s.WriteSealBatch(nil, writes, nil, nil)
	if err != nil {
		return err
	}
	if err := s.ApplySchedCountDeltas(sched); err != nil {
		return err
	}
	return s.ApplyReviewAndDepth(nil, nil, depth)
}
