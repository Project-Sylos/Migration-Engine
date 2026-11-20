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
	// ExclusionRetryExcludedFromFailed marks a copy-excluded node whose pending
	// state came from a retry-marked failure, so unexcluding restores failed
	// instead of pending.
	ExclusionRetryExcludedFromFailed = "retry_excluded_from_failed"
	// ExclusionAckFailedTraversal marks a traversal-excluded node the user
	// acknowledged from failed (intentional ignore; not root-pick exclude).
	ExclusionAckFailedTraversal = "ack_failed_traversal"
	// ExclusionForceReplaceCopy marks already_existed → pending (prefer source overwrite).
	ExclusionForceReplaceCopy = "force_replace_copy"
)

// IsDiscoveryRetryMark reports whether ExclusionSource is a live discovery-retry mark.
func IsDiscoveryRetryMark(src string) bool {
	return src == ExclusionRetryFromExcluded ||
		src == ExclusionRetryParkCopy
}

// IsCopyRetryMark reports whether ExclusionSource is a live copy-retry mark.
func IsCopyRetryMark(src string) bool {
	return src == ExclusionRetryMarkCopy
}

// IsForceReplaceMark reports whether ExclusionSource is a prefer-source force mark.
func IsForceReplaceMark(src string) bool {
	return src == ExclusionForceReplaceCopy
}

// IsCopyExcludedFromFailed reports whether ExclusionSource marks a copy
// exclusion that originated from a retry-marked failure.
func IsCopyExcludedFromFailed(src string) bool {
	return src == ExclusionRetryExcludedFromFailed
}

// IsTraversalAckFromFailed reports whether ExclusionSource marks a traversal
// exclusion that originated from acknowledging a failed discovery item.
func IsTraversalAckFromFailed(src string) bool {
	return src == ExclusionAckFailedTraversal
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

// ApplyTraversalRetryChunk marks (Mark) failed/excluded nodes in a chunk to traversal pending,
// or unmarks (!Mark) pending retry nodes back to failed or excluded.
func (s *Store) ApplyTraversalRetryChunk(sc RetryScope, chunk SubtreeChunk, out *TraversalRetryResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		prev := chunk.Status[id]
		next := prev
		if sc.Mark {
			if !traversalRetryEligible(prev) {
				continue
			}
			if prev.TraversalStatus == traversalStatusExcluded {
				if IsTraversalAckFromFailed(prev.ExclusionSource) {
					// Acknowledged-from-failed: retry like a normal failure.
					out.FromFailed++
					next.ExclusionSource = ExclusionRetryParkCopy
					next.DeterminingRuleID = ""
				} else {
					out.FromExcluded++
					next.CopyStatus = copyStatusPending
					next.ExclusionSource = ExclusionRetryFromExcluded
					next.DeterminingRuleID = ""
				}
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
			Side:       sc.Side,
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
	sched, err := s.WriteSealBatch(nil, writes, nil, nil)
	if err != nil {
		return err
	}
	return s.ApplySchedCountDeltas(sched)
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

// ApplyTraversalRetry marks (mark=true) SRC failed/excluded nodes under the path to
// traversal pending via idx:path prefix scan and enrolls folders on pend:trav, or unmarks
// (mark=false) pending retry nodes back to failed, or to excluded when they were marked from
// an exclusion. Marking a folder also purges strict DST descendants. A failed DST-at-path is
// marked or unmarked alongside when present.
func (s *Store) ApplyTraversalRetry(srcID, dstAtPathID string, mark bool) (TraversalRetryResult, error) {
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
	out, err = applyRetryAt(s, SideSRC, n, mark, s.ApplyTraversalRetryChunk)
	if err != nil || out.Affected == 0 {
		return out, err
	}
	if mark {
		if !out.WasFolder {
			return out, nil
		}
		purged, err := s.PurgeStrictDescendants(SideDST, n.Path)
		if err != nil {
			return out, err
		}
		out.DstPurged = purged
	}
	if dstAtPathID == "" {
		return out, nil
	}
	// Eligible failed DST at the same path only; successful DST is re-queued in retry mode.
	// Do not fold DST counts into Affected: review Failed / pending_retry are SRC-only.
	_, err = ApplyNodeRetry(s, SideDST, dstAtPathID, mark, s.ApplyTraversalRetryChunk)
	return out, err
}

// TraversalAckResult summarizes acknowledge / unacknowledge of failed discovery items.
type TraversalAckResult struct {
	Affected  int64
	WasFolder bool
}

// ApplyTraversalAckChunk acknowledges (Mark) failed nodes in a chunk as intentionally ignored,
// or restores (!Mark) acknowledged-from-failed nodes to failed. It does not enroll pend:trav.
func (s *Store) ApplyTraversalAckChunk(sc RetryScope, chunk SubtreeChunk, out *TraversalAckResult) error {
	writes := make([]SealStatusWrite, 0, len(chunk.IDs))
	for _, id := range chunk.IDs {
		n, ok := chunk.Nodes[id]
		if !ok {
			continue
		}
		prev := chunk.Status[id]
		next := prev
		if sc.Mark {
			if prev.TraversalStatus != traversalStatusFailed {
				continue
			}
			next.TraversalStatus = traversalStatusExcluded
			next.ExclusionSource = ExclusionAckFailedTraversal
			next.DeterminingRuleID = ""
		} else {
			if prev.TraversalStatus != traversalStatusExcluded || !IsTraversalAckFromFailed(prev.ExclusionSource) {
				continue
			}
			next.TraversalStatus = traversalStatusFailed
			next.ExclusionSource = ""
			next.DeterminingRuleID = ""
		}
		isFile := n.Type == NodeTypeFile
		writes = append(writes, SealStatusWrite{
			Side:       sc.Side,
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
	sched, err := s.WriteSealBatch(nil, writes, nil, nil)
	if err != nil {
		return err
	}
	return s.ApplySchedCountDeltas(sched)
}
