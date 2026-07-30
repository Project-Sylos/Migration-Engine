// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

// QueryNodes provides review-phase node search/filter without exposing SQL to API.
func (m *Migration) QueryNodes(filter NodeQueryFilter) ([]db.NodeState, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseTraversalSuspended &&
		phase != PhaseCopying && phase != PhaseCopySuspended && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return nil, fmt.Errorf("query nodes is only available after traversal reaches review phase")
	}
	return m.store.queryNodes(filter)
}

func pathReviewResult(affected int64, deltas map[string]int64) PathReviewActionResult {
	if deltas == nil {
		deltas = make(map[string]int64)
	}
	return PathReviewActionResult{AffectedCount: affected, Deltas: deltas}
}

// SetNodeExcluded mutates review exclusions in engine-owned store.
func (m *Migration) SetNodeExcluded(queueType, nodeID string, excluded bool) (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("set node excluded requires awaiting-traversal-review phase")
	}
	n, deltas, err := m.store.setNodeExcluded(nodeID, excluded)
	if err != nil {
		return PathReviewActionResult{}, fmt.Errorf("set node excluded: %w", err)
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// BulkExclude applies exclusion over a query slice.
func (m *Migration) BulkExclude(filter NodeQueryFilter, excluded bool) (PathReviewActionResult, error) {
	nodes, err := m.QueryNodes(filter)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	var total int64
	merged := make(map[string]int64)
	for i := range nodes {
		if nodes[i].Excluded == excluded {
			continue
		}
		n, deltas, err := m.store.setNodeExcluded(nodes[i].ID, excluded)
		if err != nil {
			return pathReviewResult(total, merged), fmt.Errorf("bulk exclude %s: %w", nodes[i].ID, err)
		}
		total += n
		for k, d := range deltas {
			merged[k] += d
		}
	}
	m.refreshRuntimeState()
	return pathReviewResult(total, merged), nil
}

// GetPathReviewStatsForView projects stats for a specific review UI. The
// source-cleanup view uses eligible delete statuses even while the migration is
// technically still in copy review.
func (m *Migration) GetPathReviewStatsForView(view string) PathReviewStats {
	if m.DB == nil {
		return PathReviewStats{}
	}
	snap, err := stats.GetReviewStatsSnapshot(m.DB)
	if err != nil {
		return PathReviewStats{}
	}
	raw := ReviewStatsRawFromSnapshot(snap)
	phase := m.Phase()
	if phase == PhaseDeleteReview {
		if remainingSize, sizeErr := stats.GetRemainingSourceSizeAfterDelete(m.DB); sizeErr == nil {
			raw.SizeSrc = remainingSize
		}
	}
	// Selected size + folder/file counts: one phase-aware overlay (pending vs eligible).
	spec := reviewSelectedSpec(phase, view)
	if overlay, overlayErr := stats.OverlayReviewSelected(m.DB, spec); overlayErr == nil {
		raw.SizeSelected = overlay.SelectedBytes
		raw.Folders = overlay.Folders
		raw.Files = overlay.Files
	}
	// size_src / size_dst: O(1) snapshot; one-shot repair for older DBs.
	if raw.SizeSrc == 0 && raw.SizeDst == 0 {
		m.maybeBackfillFolderFileStats(&raw)
	}
	statsOut := raw.ToPathReviewStats(phase)
	if view == "source-cleanup" {
		if counts, countsErr := stats.GetEligibleDeleteStatusCounts(m.DB); countsErr == nil {
			statsOut.PendingCount = int(counts.Pending)
			statsOut.FailedCount = int(counts.Failed)
			statsOut.ExcludedCount = int(counts.Skipped)
			statsOut.SuccessfulCount = int(counts.Deleted)
			statsOut.PendingRetriesCount = 0
			if phase == PhaseDeleteReview {
				statsOut.PendingRetriesCount = int(counts.Pending)
			}
		}
	}
	return statsOut
}

// maybeBackfillFolderFileStats fills size_src/size_dst once when those snapshot keys are
// still zero but nodes already exist. Folder/file counts are live-overlaid via
// OverlayReviewSelected, so they are not backfilled here.
func (m *Migration) maybeBackfillFolderFileStats(raw *ReviewStatsRaw) bool {
	if m == nil || m.DB == nil || raw == nil {
		return false
	}
	if raw.SizeSrc != 0 || raw.SizeDst != 0 {
		return false
	}
	_, _, sizeSrc, sizeDst, err := review.CountFolderFileSizeTotals(m.DB)
	if err != nil || (sizeSrc == 0 && sizeDst == 0) {
		return false
	}
	deltas := make([]db.ReviewStatsDelta, 0, 2)
	if sizeSrc != 0 {
		deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeySizeSrc, Delta: sizeSrc})
	}
	if sizeDst != 0 {
		deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeySizeDst, Delta: sizeDst})
	}
	err = m.DB.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.ApplyReviewStatsDeltas(deltas)
		})
	})
	if err != nil {
		return false
	}
	if m.Phase() != PhaseDeleteReview {
		raw.SizeSrc = sizeSrc
	}
	raw.SizeDst = sizeDst
	return true
}

func (m *Migration) GetTraversalSummary() (TraversalSummary, error) {
	status, err := InspectMigrationStatus(m.DB)
	if err != nil {
		return TraversalSummary{}, err
	}
	srcExcluded, err := pull.CountExcluded(m.DB, "SRC")
	if err != nil {
		return TraversalSummary{}, err
	}
	dstExcluded, err := pull.CountExcluded(m.DB, "DST")
	if err != nil {
		return TraversalSummary{}, err
	}
	copyCounts, err := stats.GetCopyStatusCountsFromEvents(m.DB)
	if err != nil {
		return TraversalSummary{}, err
	}
	merged, err := review.GetMergedReviewStats(m.DB, review.ReviewFilter{})
	if err != nil {
		return TraversalSummary{}, err
	}
	total := merged.Folders + merged.Files
	var foldersRatio, filesRatio float64
	if total > 0 {
		foldersRatio = roundRatio(float64(merged.Folders)/float64(total), 2)
		filesRatio = roundRatio(float64(merged.Files)/float64(total), 2)
	}
	return TraversalSummary{
		SrcTotal:    status.SrcTotal,
		DstTotal:    status.DstTotal,
		SrcPending:  status.SrcPending,
		DstPending:  status.DstPending,
		SrcFailed:   status.SrcFailed,
		DstFailed:   status.DstFailed,
		SrcExcluded: srcExcluded,
		DstExcluded: dstExcluded,
		CopyStatusCounts: CopyStatusCounts{
			Pending:    int(copyCounts.Pending),
			Successful: int(copyCounts.Complete()),
			Failed:     int(copyCounts.Failed),
			Skipped:    int(copyCounts.Skipped),
		},
		FoldersCount:     merged.Folders,
		FilesCount:       merged.Files,
		ExcludedCount:    merged.Excluded,
		TotalFileSizeSrc: merged.SizeSrc,
		TotalFileSizeDst: merged.SizeDst,
		FoldersRatio:     foldersRatio,
		FilesRatio:       filesRatio,
	}, nil
}

// roundRatio rounds v to n decimal places (e.g. 2 for 0.00).
func roundRatio(v float64, n int) float64 {
	if n <= 0 {
		return v
	}
	pow := 1.0
	for i := 0; i < n; i++ {
		pow *= 10
	}
	return float64(int64(v*pow+0.5)) / pow
}

func (m *Migration) mutatePathReviewRetry(nodeID string, allowedPhases []string, errMsg string, op func(string) (int64, map[string]int64, error)) (PathReviewActionResult, error) {
	phase := m.Phase()
	ok := false
	for _, p := range allowedPhases {
		if phase == p {
			ok = true
			break
		}
	}
	if !ok {
		return PathReviewActionResult{}, fmt.Errorf("%s", errMsg)
	}
	n, deltas, err := op(nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// RetryMutationKind selects discovery, copy, or delete retry mark/unmark.
type RetryMutationKind string

const (
	RetryMutationDiscovery RetryMutationKind = "discovery"
	RetryMutationCopy      RetryMutationKind = "copy"
	RetryMutationDelete    RetryMutationKind = "delete"
)

// SetNodeRetryMark marks (mark=true) or clears (mark=false) a retry of the given kind.
func (m *Migration) SetNodeRetryMark(kind RetryMutationKind, nodeID string, mark bool) (PathReviewActionResult, error) {
	switch kind {
	case RetryMutationCopy:
		status := db.CopyStatusFailed
		if mark {
			status = db.CopyStatusPending
		}
		return m.mutatePathReviewRetry(nodeID,
			[]string{PhaseTraversalReview, PhaseCopying, PhaseCopyReview},
			"retry copy mutation requires review or copying phase",
			func(id string) (int64, map[string]int64, error) {
				return m.store.setNodeCopyStatus(id, status)
			})
	case RetryMutationDelete:
		status := db.DeleteStatusFailed
		if mark {
			status = db.DeleteStatusPending
		}
		return m.mutatePathReviewRetry(nodeID,
			[]string{PhaseCopyReview, PhaseDeleting, PhaseDeleteReview},
			"retry delete mutation requires delete review or deleting phase",
			func(id string) (int64, map[string]int64, error) {
				return m.store.setNodeDeleteStatus(id, status)
			})
	case RetryMutationDiscovery:
		op := m.store.unmarkNodeForRetryDiscovery
		if mark {
			op = m.store.markNodeForRetryDiscovery
		}
		return m.mutatePathReviewRetry(nodeID, []string{PhaseTraversalReview},
			"retry discovery mutation requires awaiting-traversal-review phase", op)
	default:
		return PathReviewActionResult{}, fmt.Errorf("unknown retry mutation kind %q", kind)
	}
}

// SkipNodeDelete opts a successfully copied SRC node out of source removal during cleanup planning.
func (m *Migration) SkipNodeDelete(nodeID string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("skip delete requires awaiting-copy-review phase")
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	if node == nil {
		return PathReviewActionResult{}, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if !db.CopyStatusIsComplete(node.CopyStatus) {
		return PathReviewActionResult{}, fmt.Errorf("skip delete requires copy complete (successful or already_existed)")
	}
	n, deltas, err := m.store.setNodeDeleteStatusWithPropagation(nodeID, db.DeleteStatusSkipped)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// UnskipNodeDelete re-includes a SRC node in source removal during cleanup planning.
func (m *Migration) UnskipNodeDelete(nodeID string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("unskip delete requires awaiting-copy-review phase")
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	if node == nil {
		return PathReviewActionResult{}, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if !db.CopyStatusIsComplete(node.CopyStatus) {
		return PathReviewActionResult{}, fmt.Errorf("unskip delete requires copy complete (successful or already_existed)")
	}
	if node.DeleteStatus == db.DeleteStatusDeleted {
		return PathReviewActionResult{}, fmt.Errorf("cannot unskip already deleted node")
	}
	n, deltas, err := m.store.setNodeDeleteStatusWithPropagation(nodeID, db.DeleteStatusPending)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// PrepareSourceCleanup aligns delete_status with the user's selected SRC nodes for source removal.
// When keepNodeIDs is non-empty, only those nodes are pending and others are skipped.
// When deselectedNodeIDs is non-empty (and keepNodeIDs is empty), those nodes are skipped and others pending.
// When both are empty, this is idempotent first-time init: unset delete_status becomes pending.
// Existing pending/skipped/failed/deleted are left alone so remounting source-cleanup does not
// wipe the user's skip plan (that previously inflated delete progress denominators).
func (m *Migration) PrepareSourceCleanup(keepNodeIDs, deselectedNodeIDs []string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("prepare source cleanup requires awaiting-copy-review phase")
	}
	useKeepList := len(keepNodeIDs) > 0
	keep := make(map[string]bool, len(keepNodeIDs))
	for _, id := range keepNodeIDs {
		if id != "" {
			keep[id] = true
		}
	}
	deselected := make(map[string]bool, len(deselectedNodeIDs))
	for _, id := range deselectedNodeIDs {
		if id != "" {
			deselected[id] = true
		}
	}
	initOnly := !useKeepList && len(deselected) == 0
	var total int64
	merged := make(map[string]int64)
	offset := 0
	const pageSize = 1000
	for {
		nodes, err := pull.ListSrcNodesByCopyStatus(m.DB, db.CopyStatusSuccessful, pageSize, offset)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		if len(nodes) == 0 {
			break
		}
		for _, node := range nodes {
			if node.Depth == 0 {
				continue // root is metadata-only; no delete_status event (null)
			}
			var target string
			switch {
			case initOnly:
				// Discovery often already seeded pending; never reset skipped/failed/deleted.
				if node.DeleteStatus != "" {
					continue
				}
				target = db.DeleteStatusPending
			case useKeepList:
				if keep[node.ID] {
					if node.DeleteStatus == db.DeleteStatusDeleted || node.DeleteStatus == db.DeleteStatusFailed {
						continue
					}
					target = db.DeleteStatusPending
				} else {
					if node.DeleteStatus == db.DeleteStatusDeleted {
						continue
					}
					target = db.DeleteStatusSkipped
				}
			default: // deselected list
				if deselected[node.ID] {
					if node.DeleteStatus == db.DeleteStatusDeleted {
						continue
					}
					target = db.DeleteStatusSkipped
				} else {
					if node.DeleteStatus == db.DeleteStatusDeleted || node.DeleteStatus == db.DeleteStatusFailed {
						continue
					}
					target = db.DeleteStatusPending
				}
			}
			if node.DeleteStatus == target {
				continue
			}
			n, deltas, err := m.store.setNodeDeleteStatus(node.ID, target)
			if err != nil {
				return pathReviewResult(total, merged), err
			}
			total += n
			for k, v := range deltas {
				merged[k] += v
			}
		}
		if len(nodes) < pageSize {
			break
		}
		offset += pageSize
	}
	m.refreshRuntimeState()
	return pathReviewResult(total, merged), nil
}

func (m *Migration) RetryAllFailed() (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("retry all failed requires awaiting-traversal-review phase")
	}
	srcFailed, err := m.store.queryNodes(NodeQueryFilter{Queue: "SRC", Status: db.StatusFailed, Limit: 100000})
	if err != nil {
		return PathReviewActionResult{}, err
	}
	for i := range srcFailed {
		if err := m.store.setNodeTraversalStatus(srcFailed[i].ID, db.StatusPending); err != nil {
			return pathReviewResult(int64(i), nil), err
		}
	}
	dstFailed, err := m.store.queryNodes(NodeQueryFilter{Queue: "DST", Status: db.StatusFailed, Limit: 100000})
	if err != nil {
		return PathReviewActionResult{}, err
	}
	for i := range dstFailed {
		if err := m.store.setNodeTraversalStatus(dstFailed[i].ID, db.StatusPending); err != nil {
			return pathReviewResult(int64(len(srcFailed)+i), nil), err
		}
	}
	total := int64(len(srcFailed) + len(dstFailed))
	deltas := make(map[string]int64)
	addReviewDelta(deltas, DeltaTraversalFailed, -total)
	addReviewDelta(deltas, DeltaTraversalPending, total)
	m.refreshRuntimeState()
	return pathReviewResult(total, deltas), nil
}

func (m *Migration) SetNodeExcludedWithPropagation(queueType, nodeID string, excluded bool) (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("exclusion propagation requires awaiting-traversal-review phase")
	}
	n, deltas, err := m.store.setNodeExcludedWithPropagation(nodeID, excluded)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) BulkExcludeWithPropagation(filter NodeQueryFilter, excluded bool) (PathReviewActionResult, error) {
	nodes, err := m.QueryNodes(filter)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	var total int64
	merged := make(map[string]int64)
	for i := range nodes {
		n, deltas, err := m.store.setNodeExcludedWithPropagation(nodes[i].ID, excluded)
		if err != nil {
			return pathReviewResult(total, merged), err
		}
		total += n
		for k, d := range deltas {
			merged[k] += d
		}
	}
	m.refreshRuntimeState()
	return pathReviewResult(total, merged), nil
}

func (m *Migration) ListChildrenDiffs(req ListChildrenDiffsRequest) (ListChildrenDiffsResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return ListChildrenDiffsResult{}, fmt.Errorf("diff listing requires review or later phase")
	}
	return m.store.listChildrenDiffs(req)
}

func (m *Migration) SearchPathReviewItems(req SearchRequest) (SearchResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return SearchResult{}, fmt.Errorf("search requires review or later phase")
	}
	if !review.ReviewFilterHasSearchPredicate(searchRequestToReviewFilter(req)) {
		return SearchResult{}, ErrSearchRequiresFilter
	}
	return m.store.searchPathReviewItems(req)
}

func (m *Migration) GetChildrenDiffsStats(path string, foldersOnly bool, includeDestinationOnly *bool) (DiffsStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return DiffsStats{}, fmt.Errorf("diff stats requires review or later phase")
	}
	return m.store.getChildrenDiffsStats(path, foldersOnly, includeDestinationOnly)
}

// GetSearchStats returns aggregate counts for the same filter as SearchPathReviewItems (query, path, status, foldersOnly).
func (m *Migration) GetSearchStats(req SearchRequest) (DiffsStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return DiffsStats{}, fmt.Errorf("search stats requires review or later phase")
	}
	return m.store.getSearchStats(req)
}

func (m *Migration) GetQueueMetrics() (QueueMetricsSnapshot, error) {
	if o := m.activeQueueObs.Load(); o != nil {
		if raw, ok := o.LastQueueMetricsForAPI(); ok && len(raw) > 0 {
			return queueMetricsSnapshotFromRawJSON(raw), nil
		}
		// A live observer exists but has not produced its first memory snapshot yet.
		// Never fall back to DuckDB on a live API request.
		return QueueMetricsSnapshot{Queues: make(map[string]map[string]any)}, nil
	}
	if m.IsLive() {
		// Narrow startup/teardown window before the observer pointer is installed:
		// return an empty memory snapshot rather than querying queue_stats.
		return QueueMetricsSnapshot{Queues: make(map[string]map[string]any)}, nil
	}
	return m.store.getQueueMetrics()
}

// PossibleStall reports whether any active queue watchdog recently detected a stall.
func (m *Migration) PossibleStall() bool {
	if o := m.activeQueueObs.Load(); o != nil {
		return o.AnyPossibleStall()
	}
	return false
}

func queueMetricsSnapshotFromRawJSON(raw map[string][]byte) QueueMetricsSnapshot {
	out := QueueMetricsSnapshot{Queues: make(map[string]map[string]any, len(raw))}
	for key, blob := range raw {
		var parsed map[string]any
		if err := json.Unmarshal(blob, &parsed); err != nil {
			parsed = map[string]any{"raw": string(blob)}
		}
		out.Queues[key] = parsed
	}
	return out
}

func (m *Migration) GetLogs(limit int, groupByLevel bool) (LogsProjection, error) {
	entries, err := m.GetRecentLogs(limit)
	if err != nil {
		return LogsProjection{}, err
	}
	out := LogsProjection{Entries: entries}
	if groupByLevel {
		out.ByLevel = make(map[string][]LogEntry)
		for i := range entries {
			level := entries[i].Level
			out.ByLevel[level] = append(out.ByLevel[level], entries[i])
		}
	}
	return out, nil
}
func (m *Migration) GetRuntimeStatus() RuntimeState {
	m.refreshRuntimeState()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtimeState
}

func (m *Migration) GetRecentLogs(limit int) ([]LogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	out, err := m.store.listRecentLogs(limit)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	inMemory := m.logRing.recent(limit)
	m.mu.RUnlock()
	if len(inMemory) == 0 {
		return out, nil
	}
	merged := make([]LogEntry, 0, len(inMemory)+len(out))
	merged = append(merged, inMemory...)
	merged = append(merged, out...)
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}
