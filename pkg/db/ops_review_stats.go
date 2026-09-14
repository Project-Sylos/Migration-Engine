// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "codeberg.org/Sylos/Migration-Engine/pkg/opsdb"

func addReviewDelta(dst []ReviewStatsDelta, key string, delta int64) []ReviewStatsDelta {
	if key == "" || delta == 0 {
		return dst
	}
	return append(dst, ReviewStatsDelta{Key: key, Delta: delta})
}

func discoveryReviewDeltas(table string, n *NodeState) []ReviewStatsDelta {
	if n == nil {
		return nil
	}
	trav := n.TraversalStatus
	if trav == "" {
		trav = n.Status
	}
	if trav == "" {
		trav = StatusPending
	}
	var out []ReviewStatsDelta
	if table == "SRC" {
		out = addReviewDelta(out, ReviewKeyForStatus("traversal", trav), 1)
	} else {
		if NormalizeQueueNodeType(n.Type) == NodeTypeFile && n.Size > 0 {
			out = addReviewDelta(out, ReviewKeySizeDst, n.Size)
		}
		return out
	}
	copySt := n.CopyStatus
	if copySt == "" {
		copySt = CopyStatusPending
	}
	out = addReviewDelta(out, ReviewKeyForStatus("copy", copySt), 1)
	if n.DeleteStatus != "" {
		out = addReviewDelta(out, ReviewKeyForStatus("delete", n.DeleteStatus), 1)
	}
	pending := CopyStatusIsPending(copySt)
	switch NormalizeQueueNodeType(n.Type) {
	case NodeTypeFolder:
		if pending {
			out = addReviewDelta(out, ReviewKeyFolders, 1)
		}
	case NodeTypeFile:
		if n.Size > 0 {
			out = addReviewDelta(out, ReviewKeySizeSrc, n.Size)
			if pending {
				out = addReviewDelta(out, ReviewKeySizeSelected, n.Size)
			}
		}
		if pending {
			out = addReviewDelta(out, ReviewKeyFiles, 1)
		}
	}
	return out
}

func statusEventReviewDeltas(table string, e StatusEvent, fromRetry bool) []ReviewStatsDelta {
	var out []ReviewStatsDelta
	prevTrav, curTrav, travChanged := eventStatusPair(e.TraversalStatus, e.PrevTraversalStatus, StatusPending)
	// Footer Failed / traversal buckets are SRC-only. DST still updates depth/frontier
	// stats so queues can complete; counting DST here double-counts merged paths.
	if table == "SRC" && travChanged && prevTrav != curTrav {
		// Retry-marked nodes live as traversal_status=pending but are counted under
		// pending_retry, not traversal/pending. Do not debit the initial-discovery bucket.
		if !(fromRetry && prevTrav == StatusPending) {
			out = addReviewDelta(out, ReviewKeyForStatus("traversal", prevTrav), -1)
		}
		out = addReviewDelta(out, ReviewKeyForStatus("traversal", curTrav), 1)
	}
	if table == "SRC" {
		prevCopy, curCopy, copyChanged := eventStatusPair(e.CopyStatus, e.PrevCopyStatus, CopyStatusPending)
		if copyChanged && prevCopy != curCopy {
			out = addReviewDelta(out, ReviewKeyForStatus("copy", prevCopy), -1)
			out = addReviewDelta(out, ReviewKeyForStatus("copy", curCopy), 1)
		}
		prevDel := e.PrevDeleteStatus
		curDel := e.DeleteStatus
		if curDel != "" {
			if prevDel == "" && !DeleteStatusIsPending(curDel) {
				prevDel = DeleteStatusPendingExplicit
			}
			if prevDel != curDel {
				out = addReviewDelta(out, ReviewKeyForStatus("delete", prevDel), -1)
				out = addReviewDelta(out, ReviewKeyForStatus("delete", curDel), 1)
			}
		}
		if e.Size > 0 {
			out = applySelectedSizeReviewDeltas(out, prevCopy, curCopy, e.PrevDeleteStatus, e.DeleteStatus, e.Size)
		}
		if copyChanged && e.NodeType != "" {
			out = applyPendingTypeReviewDeltas(out, prevCopy, curCopy, e.NodeType)
		}
		if copyChanged && e.ExclusionSource == opsdb.ExclusionRetryMarkCopy &&
			(curCopy == CopyStatusSuccessful || curCopy == CopyStatusAlreadyExisted || curCopy == CopyStatusFailed) {
			out = addReviewDelta(out, ReviewKeyCopyPendingRetry, -1)
		}
	}
	if table == "SRC" && fromRetry && travChanged && prevTrav == StatusPending &&
		(curTrav == StatusSuccessful || curTrav == StatusFailed) {
		out = addReviewDelta(out, ReviewKeyTraversalPendingRetry, -1)
	}
	return out
}

// eventStatusPair treats an empty current status as unchanged (same as seal merge).
func eventStatusPair(cur, prev, emptyPrev string) (string, string, bool) {
	if cur == "" {
		return "", "", false
	}
	if prev == "" {
		prev = emptyPrev
	}
	return prev, cur, true
}

func applySelectedSizeReviewDeltas(out []ReviewStatsDelta, prevCopy, curCopy, prevDel, curDel string, size int64) []ReviewStatsDelta {
	if size <= 0 {
		return out
	}
	prevDel, curDel = normalizeDeletePairForSelected(prevDel, curDel)
	copyOmitted := prevCopy == "" && curCopy == ""
	wasCopyPending, nowCopyPending := false, false
	prevComplete, curComplete := false, false
	if copyOmitted {
		prevComplete, curComplete = true, true
	} else {
		wasCopyPending = CopyStatusIsPending(prevCopy)
		nowCopyPending = CopyStatusIsPending(curCopy)
		prevComplete = CopyStatusIsComplete(prevCopy)
		curComplete = CopyStatusIsComplete(curCopy)
	}
	if wasCopyPending && !nowCopyPending {
		out = addReviewDelta(out, ReviewKeySizeSelected, -size)
	} else if !wasCopyPending && nowCopyPending {
		out = addReviewDelta(out, ReviewKeySizeSelected, size)
	}
	wasDeleteSelected := prevComplete && (prevDel == "" || DeleteStatusIsPending(prevDel))
	nowDeleteSelected := curComplete && (curDel == "" || DeleteStatusIsPending(curDel))
	if wasDeleteSelected && !nowDeleteSelected {
		out = addReviewDelta(out, ReviewKeySizeDeleteSelected, -size)
	} else if !wasDeleteSelected && nowDeleteSelected {
		out = addReviewDelta(out, ReviewKeySizeDeleteSelected, size)
	}
	prevDeleted := prevDel == DeleteStatusDeleted
	nowDeleted := curDel == DeleteStatusDeleted
	if prevDeleted && !nowDeleted {
		out = addReviewDelta(out, ReviewKeySizeSrc, size)
	} else if !prevDeleted && nowDeleted {
		out = addReviewDelta(out, ReviewKeySizeSrc, -size)
	}
	return out
}

func normalizeDeletePairForSelected(prevDel, curDel string) (string, string) {
	if prevDel == "" && curDel == "" {
		return DeleteStatusPendingExplicit, DeleteStatusPendingExplicit
	}
	if prevDel == "" {
		prevDel = DeleteStatusPendingExplicit
	}
	if curDel == "" {
		curDel = prevDel
	}
	return prevDel, curDel
}

func applyPendingTypeReviewDeltas(out []ReviewStatsDelta, prevCopy, curCopy, nodeType string) []ReviewStatsDelta {
	var key string
	switch NormalizeQueueNodeType(nodeType) {
	case NodeTypeFolder:
		key = ReviewKeyFolders
	case NodeTypeFile:
		key = ReviewKeyFiles
	default:
		return out
	}
	was := CopyStatusIsPending(prevCopy)
	now := CopyStatusIsPending(curCopy)
	if was && !now {
		return addReviewDelta(out, key, -1)
	}
	if !was && now {
		return addReviewDelta(out, key, 1)
	}
	return out
}

func eventDepthStatsDeltas(table string, e StatusEvent) []DepthStatsDelta {
	depth := e.Depth
	var out []DepthStatsDelta
	prevTrav, curTrav, travChanged := eventStatusPair(e.TraversalStatus, e.PrevTraversalStatus, StatusPending)
	if travChanged && prevTrav != curTrav {
		out = append(out,
			DepthStatsDelta{Table: table, Depth: depth, Key: StatsKey(StatsKindTraversal, prevTrav), Delta: -1},
			DepthStatsDelta{Table: table, Depth: depth, Key: StatsKey(StatsKindTraversal, curTrav), Delta: 1},
		)
	}
	if table != "SRC" {
		return out
	}
	nodeType := e.NodeType
	prevCopy, curCopy, copyChanged := eventStatusPair(e.CopyStatus, e.PrevCopyStatus, CopyStatusPending)
	if copyChanged && prevCopy != curCopy {
		out = append(out,
			DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyTyped(StatsKindCopy, prevCopy, nodeType), Delta: -1},
			DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyTyped(StatsKindCopy, curCopy, nodeType), Delta: 1},
		)
		if NormalizeQueueNodeType(nodeType) == NodeTypeFile && e.Size > 0 {
			out = append(out,
				DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyCopyFileBytes(prevCopy), Delta: -e.Size},
				DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyCopyFileBytes(curCopy), Delta: e.Size},
			)
		}
	}
	prevDel := e.PrevDeleteStatus
	curDel := e.DeleteStatus
	if curDel != "" {
		if prevDel == "" && !DeleteStatusIsPending(curDel) {
			prevDel = DeleteStatusPendingExplicit
		}
		if prevDel != curDel {
			out = append(out,
				DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyTyped(StatsKindDelete, prevDel, nodeType), Delta: -1},
				DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyTyped(StatsKindDelete, curDel, nodeType), Delta: 1},
			)
			if NormalizeQueueNodeType(nodeType) == NodeTypeFile && e.Size > 0 {
				if prevDel != "" {
					out = append(out, DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyDeleteFileBytes(prevDel), Delta: -e.Size})
				}
				out = append(out, DepthStatsDelta{Table: table, Depth: depth, Key: StatsKeyDeleteFileBytes(curDel), Delta: e.Size})
			}
		}
	}
	return out
}

func (db *DB) incrOpsReviewDeltas(deltas []ReviewStatsDelta) error {
	return db.applyOpsHotpathStats(deltas, nil)
}

func (db *DB) applyOpsHotpathStats(review []ReviewStatsDelta, depth []DepthStatsDelta) error {
	if db == nil || db.Ops() == nil {
		return nil
	}
	if len(review) == 0 && len(depth) == 0 {
		return nil
	}
	reviewKeys := make([]string, 0, len(review))
	reviewDeltas := make([]int64, 0, len(review))
	for _, d := range review {
		if d.Delta == 0 || d.Key == "" {
			continue
		}
		reviewKeys = append(reviewKeys, d.Key)
		reviewDeltas = append(reviewDeltas, d.Delta)
	}
	depthPut := make([]opsdb.DepthCounterDelta, 0, len(depth))
	for _, d := range depth {
		if d.Delta == 0 || d.Key == "" {
			continue
		}
		side := opsdb.SideSRC
		if d.Table == "DST" {
			side = opsdb.SideDST
		}
		depthPut = append(depthPut, opsdb.DepthCounterDelta{
			Side: side, Depth: d.Depth, Key: d.Key, Delta: d.Delta,
		})
	}
	return db.Ops().ApplyReviewAndDepth(reviewKeys, reviewDeltas, depthPut)
}
