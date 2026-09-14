// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "fmt"

func copyCompleteForDeleteLive(st StatusRecord) bool {
	cs := st.CopyStatus
	return cs == copyStatusSuccessful || cs == copyStatusAlreadyExisted
}

// ApplySubtreeDeleteRetry marks (mark=true) failed→pending* or unmarks (mark=false)
// pending*→failed under rootPath. Root becomes pending_explicit; descendants pending_inherited.
func (s *Store) ApplySubtreeDeleteRetry(rootPath string, mark bool) (SubtreeMutationResult, error) {
	var out SubtreeMutationResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" {
		return out, nil
	}
	err := s.ApplySubtreeScan(SideSRC, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		return s.applyDeleteRetryChunk(rootPath, mark, chunk, &out)
	})
	return out, err
}

// ApplyNodeDeleteRetry marks or unmarks delete retry for one SRC node.
func (s *Store) ApplyNodeDeleteRetry(id string, mark bool) (SubtreeMutationResult, error) {
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
	if err := s.applyDeleteRetryChunk(NormalizeIndexPath(n.Path), mark, chunk, &out); err != nil {
		return out, err
	}
	return out, nil
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
		sched, err := s.WriteSealBatch(nil, writes, nil, nil, nil)
		if err != nil {
			return err
		}
		return s.ApplySchedCountDeltas(sched)
	})
	return out, err
}

func (s *Store) applyDeleteRetryChunk(rootPath string, mark bool, chunk SubtreeChunk, out *SubtreeMutationResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		prev := chunk.Status[id]
		next := prev
		if mark {
			if prev.DeleteStatus != deleteStatusFailed && prev.DeleteStatus != "" {
				continue
			}
			if !copyCompleteForDeleteLive(prev) {
				continue
			}
			if NormalizeIndexPath(n.Path) == rootPath {
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
	sched, err := s.WriteSealBatch(nil, writes, nil, nil, nil)
	if err != nil {
		return err
	}
	return s.ApplySchedCountDeltas(sched)
}
