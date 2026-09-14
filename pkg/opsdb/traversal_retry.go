// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
)

const (
	traversalStatusPending    = "pending"
	traversalStatusFailed     = "failed"
	traversalStatusExcluded   = "excluded"
	traversalStatusSuccessful = "successful"
	// ExclusionRetryFromExcluded marks a node re-queued from copy exclusion.
	ExclusionRetryFromExcluded = "retry_from_excluded"
	// ExclusionRetryParkCopy marks a failed node re-queued for discovery retry
	// (fromRetry / pending_retry accounting). copy/pending is not parked.
	ExclusionRetryParkCopy = "retry_park_copy_pending"
	// ExclusionRetryMarkCopy marks a copy-failed node the user queued for copy retry.
	ExclusionRetryMarkCopy = "retry_mark_copy"
)

// IsDiscoveryRetryMark reports whether ExclusionSource is a live discovery-retry mark.
func IsDiscoveryRetryMark(src string) bool {
	return src == ExclusionRetryFromExcluded || src == ExclusionRetryParkCopy
}

// IsCopyRetryMark reports whether ExclusionSource is a live copy-retry mark.
func IsCopyRetryMark(src string) bool {
	return src == ExclusionRetryMarkCopy
}

// TraversalRetryResult summarizes a live mark/unmark for discovery retry.
// Counts come only from the mutation/purge passes (no separate recount scan).
type TraversalRetryResult struct {
	Affected     int64
	FromExcluded int64
	FromFailed   int64
	WasFolder    bool
	// DstPurged is filled when a folder mark deletes strict DST descendants.
	DstPurged SubtreePurgeResult
}

// SubtreePurgeResult is accumulated while deleting a path prefix (one scan).
type SubtreePurgeResult struct {
	Affected         int64
	Folders          int64
	Files            int64
	SizeDst          int64
	TraversalFailed  int64
	TraversalPending int64
	Excluded         int64
}

func traversalRetryEligible(st StatusRecord) bool {
	return st.TraversalStatus == traversalStatusFailed || st.TraversalStatus == traversalStatusExcluded
}

func travFrontierDelta(prev, next StatusRecord, nt string, isFile bool) []PendingDelta {
	if isFile {
		return nil
	}
	wasOn := prev.TraversalStatus == traversalStatusPending
	nowOn := next.TraversalStatus == traversalStatusPending
	if wasOn == nowOn {
		return nil
	}
	return []PendingDelta{{
		Phase: PhaseTrav, NodeType: nt,
		Add: nowOn, PendWasSet: wasOn,
	}}
}

func (s *Store) applyTraversalRetryChunk(side string, mark bool, chunk SubtreeChunk, out *TraversalRetryResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		prev := chunk.Status[id]
		next := prev
		if mark {
			if !traversalRetryEligible(prev) {
				continue
			}
			if prev.TraversalStatus == traversalStatusExcluded {
				out.FromExcluded++
				next.CopyStatus = copyStatusPending
				next.ExclusionSource = ExclusionRetryFromExcluded
				next.DeterminingRuleID = ""
			} else {
				out.FromFailed++
				next.ExclusionSource = ExclusionRetryParkCopy
			}
			next.TraversalStatus = traversalStatusPending
		} else {
			if prev.TraversalStatus != traversalStatusPending {
				continue
			}
			if prev.ExclusionSource == ExclusionRetryFromExcluded {
				out.FromExcluded++
				next.TraversalStatus = traversalStatusExcluded
				next.CopyStatus = copyStatusExcludedExplicit
				next.ExclusionSource = ""
			} else {
				out.FromFailed++
				next.TraversalStatus = traversalStatusFailed
				next.ExclusionSource = ""
			}
		}
		isFile := n.Type == NodeTypeFile
		writes = append(writes, SealStatusWrite{
			Side:       side,
			ID:         id,
			Status:     next,
			PrevStatus: prev,
			Depth:      n.Depth,
			Deltas:     travFrontierDelta(prev, next, nodeTypeNT(n.Type), isFile),
		})
		out.Affected++
		if !isFile {
			out.WasFolder = true
		}
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

func (s *Store) applyTraversalRetryUnderPath(side, rootPath string, mark bool) (TraversalRetryResult, error) {
	var out TraversalRetryResult
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" {
		return out, nil
	}
	err := s.ApplySubtreeScan(side, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		return s.applyTraversalRetryChunk(side, mark, chunk, &out)
	})
	return out, err
}

func (s *Store) applyTraversalRetryNode(side, id string, mark bool) (TraversalRetryResult, error) {
	var out TraversalRetryResult
	nodes, err := s.BatchGetNode(side, []string{id})
	if err != nil {
		return out, err
	}
	n, ok := nodes[id]
	if !ok {
		return out, fmt.Errorf("opsdb: %s node %s not found", side, id)
	}
	sts, err := s.BatchGetStatus(side, []string{id})
	if err != nil {
		return out, err
	}
	chunk := SubtreeChunk{
		IDs:    []string{id},
		Nodes:  nodes,
		Status: sts,
		Paths:  map[string]string{id: n.Path},
	}
	if err := s.applyTraversalRetryChunk(side, mark, chunk, &out); err != nil {
		return out, err
	}
	out.WasFolder = n.Type != NodeTypeFile
	return out, nil
}

// PurgeStrictDescendants deletes all nodes under rootPath excluding the root itself.
// Stats are accumulated in the same path-prefix pass used to delete (no recount scan).
func (s *Store) PurgeStrictDescendants(side, rootPath string) (SubtreePurgeResult, error) {
	var out SubtreePurgeResult
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" {
		return out, nil
	}
	err := s.ApplySubtreeScan(side, rootPath, SubtreeScanOpts{ExcludeRoot: true}, 0, func(chunk SubtreeChunk) error {
		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok {
				continue
			}
			st := chunk.Status[id]
			switch n.Type {
			case NodeTypeFolder:
				out.Folders++
			case NodeTypeFile:
				out.Files++
				out.SizeDst += n.Size
			}
			switch st.TraversalStatus {
			case traversalStatusPending:
				out.TraversalPending++
			case traversalStatusFailed:
				out.TraversalFailed++
			}
			if copyStatusIsExcludedValue(st.CopyStatus) {
				out.Excluded++
			}
			if err := s.DeleteNode(side, id); err != nil {
				return err
			}
			out.Affected++
		}
		return nil
	})
	return out, err
}

// ApplyTraversalRetryMark sets SRC failed/excluded nodes under the marked path to
// traversal pending via idx:path prefix scan and enrolls folders on pend:trav.
// Folders also purge strict DST descendants and mark a failed DST-at-path when present.
func (s *Store) ApplyTraversalRetryMark(srcID string, dstAtPathID string) (TraversalRetryResult, error) {
	var out TraversalRetryResult
	if s == nil || srcID == "" {
		return out, fmt.Errorf("opsdb: src id required")
	}
	n, ok, err := s.GetNode(SideSRC, srcID)
	if err != nil {
		return out, err
	}
	if !ok {
		return out, fmt.Errorf("opsdb: SRC node %s not found", srcID)
	}
	out.WasFolder = n.Type != NodeTypeFile
	if out.WasFolder {
		out, err = s.applyTraversalRetryUnderPath(SideSRC, n.Path, true)
	} else {
		out, err = s.applyTraversalRetryNode(SideSRC, srcID, true)
	}
	if err != nil || out.Affected == 0 {
		return out, err
	}
	if !out.WasFolder {
		return out, nil
	}
	purged, err := s.PurgeStrictDescendants(SideDST, n.Path)
	if err != nil {
		return out, err
	}
	out.DstPurged = purged
	if dstAtPathID == "" {
		return out, nil
	}
	// Eligible failed DST at the same path only; successful DST is re-queued in retry mode.
	// Do not fold DST counts into Affected: review Failed / pending_retry are SRC-only.
	_, err = s.applyTraversalRetryNode(SideDST, dstAtPathID, true)
	return out, err
}

// ApplyTraversalRetryUnmark restores pending retry nodes under the path to failed,
// or to excluded when they were marked from an exclusion.
func (s *Store) ApplyTraversalRetryUnmark(srcID string, dstAtPathID string) (TraversalRetryResult, error) {
	var out TraversalRetryResult
	if s == nil || srcID == "" {
		return out, fmt.Errorf("opsdb: src id required")
	}
	n, ok, err := s.GetNode(SideSRC, srcID)
	if err != nil {
		return out, err
	}
	if !ok {
		return out, fmt.Errorf("opsdb: SRC node %s not found", srcID)
	}
	out.WasFolder = n.Type != NodeTypeFile
	if out.WasFolder {
		out, err = s.applyTraversalRetryUnderPath(SideSRC, n.Path, false)
	} else {
		out, err = s.applyTraversalRetryNode(SideSRC, srcID, false)
	}
	if err != nil || out.Affected == 0 {
		return out, err
	}
	if dstAtPathID == "" {
		return out, nil
	}
	_, err = s.applyTraversalRetryNode(SideDST, dstAtPathID, false)
	return out, err
}

// ApplyDSTTraversalRetry marks (mark=true) or unmarks (mark=false) a DST-only
// discovery-retry node with live Badger writes. Folders scan the path prefix.
func (s *Store) ApplyDSTTraversalRetry(dstID string, mark bool) (TraversalRetryResult, error) {
	var out TraversalRetryResult
	if s == nil || dstID == "" {
		return out, fmt.Errorf("opsdb: dst id required")
	}
	n, ok, err := s.GetNode(SideDST, dstID)
	if err != nil {
		return out, err
	}
	if !ok {
		return out, fmt.Errorf("opsdb: DST node %s not found", dstID)
	}
	out.WasFolder = n.Type != NodeTypeFile
	if out.WasFolder {
		return s.applyTraversalRetryUnderPath(SideDST, n.Path, mark)
	}
	return s.applyTraversalRetryNode(SideDST, dstID, mark)
}
