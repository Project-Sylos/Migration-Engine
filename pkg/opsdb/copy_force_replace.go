// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

// ApplyCopyForceReplaceChunk marks already_existed → pending (prefer source, Mark) or
// undoes pending → already_existed (keep destination, !Mark) for paired files in a
// chunk. Folders are skipped; only file nodes are enrolled.
func (s *Store) ApplyCopyForceReplaceChunk(sc RetryScope, chunk SubtreeChunk, out *SubtreeMutationResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs)*2)
	depth := make([]DepthCounterDelta, 0, len(chunk.IDs)*2)
	dstIDs := make([]string, 0, len(chunk.IDs))
	type peerPlan struct {
		srcID    string
		dstID    string
		nextSRC  string
		nextDST  string
		nextExcl string
		prev     StatusRecord
		n        NodeRecord
		prevCopy string
	}
	plans := make([]peerPlan, 0, len(chunk.IDs))
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		// Prefer-source / keep-destination is file-only (exists-on-both pairs).
		if n.Type != NodeTypeFile {
			continue
		}
		mapped, mapOK, err := s.GetMapBySrc(id)
		if err != nil {
			return err
		}
		if !mapOK || mapped.DstID == "" {
			continue
		}
		prev := chunk.Status[id]
		prevCopy := prev.CopyStatus
		if prevCopy == "" {
			prevCopy = copyStatusPending
		}
		var nextCopy, nextDST string
		nextExcl := prev.ExclusionSource
		if sc.Mark {
			if prevCopy != copyStatusAlreadyExisted {
				continue
			}
			nextCopy = copyStatusPending
			nextDST = copyStatusNeedsOverwrite
			nextExcl = ExclusionForceReplaceCopy
		} else {
			// Keep destination: pending → already_existed for paired overwrites
			// (force-replace mark, or mtime keep-src without a mark).
			if prevCopy != copyStatusPending {
				continue
			}
			nextCopy = copyStatusAlreadyExisted
			nextDST = copyStatusAlreadyExisted
			nextExcl = ""
		}
		plans = append(plans, peerPlan{
			srcID: id, dstID: mapped.DstID,
			nextSRC: nextCopy, nextDST: nextDST, nextExcl: nextExcl,
			prev: prev, n: n, prevCopy: prevCopy,
		})
		dstIDs = append(dstIDs, mapped.DstID)
	}
	dstStatus, err := s.BatchGetStatus(SideDST, dstIDs)
	if err != nil {
		return err
	}
	for _, p := range plans {
		nt := nodeTypeNT(p.n.Type)
		writes = append(writes, SealStatusWrite{
			Side: SideSRC,
			ID:   p.srcID,
			Status: StatusRecord{
				TraversalStatus:        p.prev.TraversalStatus,
				CopyStatus:             p.nextSRC,
				DeleteStatus:           p.prev.DeleteStatus,
				SkippedDescendantCount: p.prev.SkippedDescendantCount,
				ExclusionSource:        p.nextExcl,
			},
			PrevStatus: p.prev,
			Depth:      p.n.Depth,
			Deltas: []PendingDelta{{
				Phase: PhaseCopy, NodeType: nt,
				Add: copyStatusIsPending(p.nextSRC), PendWasSet: copyStatusIsPending(p.prev.CopyStatus),
			}},
		})
		dstPrev := dstStatus[p.dstID]
		writes = append(writes, SealStatusWrite{
			Side: SideDST,
			ID:   p.dstID,
			Status: StatusRecord{
				TraversalStatus: dstPrev.TraversalStatus,
				CopyStatus:      p.nextDST,
			},
			PrevStatus: dstPrev,
			Depth:      p.n.Depth,
		})
		depth = append(depth,
			DepthCounterDelta{Side: SideSRC, Depth: p.n.Depth, Key: copyTypedStatKey(p.prevCopy, nt), Delta: -1},
			DepthCounterDelta{Side: SideSRC, Depth: p.n.Depth, Key: copyTypedStatKey(p.nextSRC, nt), Delta: 1},
		)
		if p.n.Size > 0 {
			depth = append(depth,
				DepthCounterDelta{Side: SideSRC, Depth: p.n.Depth, Key: copyFileBytesStatKey(p.prevCopy), Delta: -p.n.Size},
				DepthCounterDelta{Side: SideSRC, Depth: p.n.Depth, Key: copyFileBytesStatKey(p.nextSRC), Delta: p.n.Size},
			)
		}
		s.noteMutation(out, p.n)
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
