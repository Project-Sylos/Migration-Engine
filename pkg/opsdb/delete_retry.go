// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "fmt"

func copyCompleteForDeleteLive(st StatusRecord) bool {
	cs := st.CopyStatus
	return cs == copyStatusSuccessful || cs == copyStatusAlreadyExisted
}

// SeedUnsetDeleteStatuses writes pending* for copy-complete depth>=1 nodes with empty
// delete_status, then normalizes explicit/inherited via the caller's NormalizeDeleteForestOps.
func (s *Store) SeedUnsetDeleteStatuses() (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	err := s.ApplySubtreeScan(SideSRC, "/", SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		writes := make([]SealStatusWrite, 0, len(chunk.IDs))
		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok || n.Depth < 1 {
				continue
			}
			prev := chunk.Status[id]
			if !copyCompleteForDeleteLive(prev) || prev.DeleteStatus != "" {
				continue
			}
			next := prev
			next.DeleteStatus = deleteStatusPendingInherited
			nt := nodeTypeNT(n.Type)
			writes = append(writes, SealStatusWrite{
				Side: SideSRC, ID: id, Status: next, PrevStatus: prev, Depth: n.Depth,
				Deltas: []PendingDelta{{
					Phase: PhaseDel, NodeType: nt,
					Add: deleteStatusOnFrontier(next.DeleteStatus), PendWasSet: false,
				}},
			})
			s.noteMutation(&out, n)
		}
		if len(writes) == 0 {
			return nil
		}
		sched, err := s.WriteSealBatch(nil, writes, nil, nil)
		if err != nil {
			return err
		}
		return s.ApplySchedCountDeltas(sched)
	})
	return out, err
}

// ApplyDeleteRetryChunk marks (Mark) failed→pending* or unmarks (!Mark) pending*→failed
// for one chunk. The scan root becomes pending_explicit; descendants pending_inherited.
func (s *Store) ApplyDeleteRetryChunk(sc RetryScope, chunk SubtreeChunk, out *SubtreeMutationResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		prev := chunk.Status[id]
		next := prev
		if sc.Mark {
			if prev.DeleteStatus != deleteStatusFailed && prev.DeleteStatus != "" {
				continue
			}
			if !copyCompleteForDeleteLive(prev) {
				continue
			}
			if NormalizeIndexPath(n.Path) == sc.Root {
				next.DeleteStatus = deleteStatusPendingExplicit
			} else {
				next.DeleteStatus = deleteStatusPendingInherited
			}
		} else {
			if !deleteStatusIsPending(prev.DeleteStatus) {
				continue
			}
			next.DeleteStatus = deleteStatusFailed
		}
		nt := nodeTypeNT(n.Type)
		writes = append(writes, SealStatusWrite{
			Side: SideSRC, ID: id, Status: next, PrevStatus: prev, Depth: n.Depth,
			Deltas: delFrontierDelta(prev.DeleteStatus, next.DeleteStatus, nt),
		})
		s.noteMutation(out, n)
	}
	if len(writes) == 0 {
		return nil
	}
	sched, err := s.WriteSealBatch(nil, writes, nil, nil)
	if err != nil {
		return err
	}
	return s.ApplySchedCountDeltas(sched)
}
