// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// reviewOrLaterPhase reports phases where diff listing, search, and stats are available.
func reviewOrLaterPhase(phase string) bool {
	switch phase {
	case PhaseTraversalReview, PhaseCopying, PhaseCopyReview, PhaseDeleting, PhaseDeleteSuspended, PhaseDeleteReview:
		return true
	}
	return false
}

// QueryNodes provides review-phase node search/filter without exposing SQL to API.
func (m *Migration) QueryNodes(filter NodeQueryFilter) ([]db.NodeState, error) {
	phase := m.Phase()
	if !reviewOrLaterPhase(phase) && phase != PhaseTraversalSuspended && phase != PhaseCopySuspended {
		return nil, fmt.Errorf("query nodes is only available after traversal reaches review phase")
	}
	return m.store.queryNodes(filter)
}

// mergeDeltas adds src into dst.
func mergeDeltas(dst, src map[string]int64) {
	for k, d := range src {
		dst[k] += d
	}
}

func pathReviewResult(affected int64, deltas map[string]int64) PathReviewActionResult {
	if deltas == nil {
		deltas = make(map[string]int64)
	}
	return PathReviewActionResult{AffectedCount: affected, Deltas: deltas}
}

func copyExclusionPhase(phase string) bool {
	return phase == PhaseTraversalReview || phase == PhaseCopyReview
}

// GetPathReviewStatsForView projects stats for a specific review UI.
//
// Migration phase alone cannot distinguish Discover vs Copy Plan: both use
// awaiting-traversal-review. The view query param selects the axis:
//   - "" (default): traversal/copy/delete axis from m.Phase()
//   - "copy-plan": copy-pending axis while still in traversal review
//   - "source-cleanup": delete-pending axis while still in copy review
func (m *Migration) GetPathReviewStatsForView(view string) PathReviewStats {
	if m.DB == nil {
		return PathReviewStats{}
	}
	snap, err := stats.GetReviewStatsSnapshot(m.DB)
	if err != nil {
		return PathReviewStats{}
	}
	if snap.DeletePending < 0 {
		snap.DeletePending = 0
	}
	raw := ReviewStatsRawFromSnapshot(snap)
	phase := m.Phase()
	if phase == PhaseDeleteReview {
		raw.SizeSrc = snap.SizeSrc
	}
	spec := reviewSelectedSpec(phase, view)
	if spec.Kind == db.StatsKindDelete && snap.SizeDeleteSelected != 0 {
		raw.SizeSelected = snap.SizeDeleteSelected
	}
	if overlay, overlayErr := stats.OverlayReviewSelected(m.DB, spec); overlayErr == nil {
		raw.Folders = overlay.Folders
		raw.Files = overlay.Files
		raw.SizeSelected = overlay.SelectedBytes
	}

	switch view {
	case "copy-plan":
		// Engine phase is still awaiting-traversal-review; project copy axis for this UI.
		out := raw.ToPathReviewStats(PhaseCopyReview)
		out.PendingRetriesCount = 0
		// Pending matches Selected population (same overlay), not a separate counter.
		out.PendingCount = intPtr(out.FoldersCount + out.FilesCount)
		return out
	case "source-cleanup":
		out := raw.ToPathReviewStats(phase)
		out.FailedCount = int(snap.DeleteFailed)
		out.ExcludedCount = int(snap.DeleteSkipped)
		out.SuccessfulCount = int(snap.DeleteDeleted)
		out.PendingCount = intPtr(int(snap.DeletePending))
		out.PendingRetriesCount = 0
		return out
	}

	out := raw.ToPathReviewStats(phase)
	deletePhase := phase == PhaseDeleting || phase == PhaseDeleteSuspended || phase == PhaseDeleteReview
	if deletePhase {
		out.FailedCount = int(snap.DeleteFailed)
		out.ExcludedCount = int(snap.DeleteSkipped)
		out.SuccessfulCount = int(snap.DeleteDeleted)
		if phase == PhaseDeleteReview {
			out.PendingCount = nil
			out.PendingRetriesCount = int(snap.DeletePending)
		} else {
			out.PendingCount = intPtr(int(snap.DeletePending))
			out.PendingRetriesCount = 0
		}
	}
	return out
}

// PathReviewPendingRetryCount is the O(1) retry counter for the current phase
// (traversal pending-retry, leftover copy pending, or delete pending).
func (m *Migration) PathReviewPendingRetryCount() (int, error) {
	if m == nil || m.DB == nil {
		return 0, nil
	}
	snap, err := stats.GetReviewStatsSnapshot(m.DB)
	if err != nil {
		return 0, err
	}
	return ReviewStatsRawFromSnapshot(snap).ToPathReviewStats(m.Phase()).PendingRetriesCount, nil
}

func (m *Migration) GetTraversalSummary() (TraversalSummary, error) {
	status, err := InspectMigrationStatus(m.DB)
	if err != nil {
		return TraversalSummary{}, err
	}
	snap, err := stats.GetReviewStatsSnapshot(m.DB)
	if err != nil {
		return TraversalSummary{}, err
	}
	copyCounts, err := stats.GetCopyStatusCountsFromEvents(m.DB)
	if err != nil {
		return TraversalSummary{}, err
	}
	total := snap.Folders + snap.Files
	var foldersRatio, filesRatio float64
	if total > 0 {
		foldersRatio = roundRatio(float64(snap.Folders)/float64(total), 2)
		filesRatio = roundRatio(float64(snap.Files)/float64(total), 2)
	}
	return TraversalSummary{
		SrcTotal:    status.SrcTotal,
		DstTotal:    status.DstTotal,
		SrcPending:  status.SrcPending,
		DstPending:  status.DstPending,
		SrcFailed:   status.SrcFailed,
		DstFailed:   status.DstFailed,
		SrcExcluded: int(snap.Excluded),
		DstExcluded: 0,
		CopyStatusCounts: CopyStatusCounts{
			Pending:    int(copyCounts.Pending),
			Successful: int(copyCounts.Complete()),
			Failed:     int(copyCounts.Failed),
			Skipped:    int(copyCounts.Skipped),
		},
		FoldersCount:     int(snap.Folders),
		FilesCount:       int(snap.Files),
		ExcludedCount:    int(snap.Excluded),
		TotalFileSizeSrc: snap.SizeSrc,
		TotalFileSizeDst: snap.SizeDst,
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
	return m.withPathMutation(nodeID, func() (PathReviewActionResult, error) {
		n, deltas, err := op(nodeID)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		return pathReviewResult(n, deltas), nil
	})
}

// RetryMutationKind selects discovery, copy, or delete retry mark/unmark.
type RetryMutationKind string

const (
	RetryMutationDiscovery RetryMutationKind = "discovery"
	RetryMutationCopy      RetryMutationKind = "copy"
	RetryMutationDelete    RetryMutationKind = "delete"
	// RetryMutationForceReplace prefers source for exists-on-both (mark=true) or undoes it.
	// Folders cascade to already_existed descendants; the folder container stays matched.
	RetryMutationForceReplace RetryMutationKind = "force_replace"
	// RetryMutationTraversalAck marks failed discovery nodes as intentionally ignored (mark=true,
	// traversal_status = excluded) or restores acknowledged-from-failed nodes to failed. Does not enroll the discovery frontier.
	RetryMutationTraversalAck RetryMutationKind = "traversal_ack"
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
			status = db.DeleteStatusPendingExplicit
		}
		return m.mutatePathReviewRetry(nodeID,
			[]string{PhaseCopyReview, PhaseDeleting, PhaseDeleteReview},
			"retry delete mutation requires delete review or deleting phase",
			func(id string) (int64, map[string]int64, error) {
				return m.store.setNodeDeleteStatus(id, status)
			})
	case RetryMutationForceReplace:
		return m.mutatePathReviewRetry(nodeID,
			[]string{PhaseTraversalReview, PhaseCopying, PhaseCopyReview},
			"force-replace requires traversal review, copying, or copy review phase",
			func(id string) (int64, map[string]int64, error) {
				return m.store.setNodeCopyForceReplace(id, mark)
			})
	case RetryMutationTraversalAck:
		return m.mutatePathReviewRetry(nodeID, []string{PhaseTraversalReview},
			"traversal acknowledge requires awaiting-traversal-review",
			func(id string) (int64, map[string]int64, error) {
				return m.store.setTraversalFailedAcknowledged(id, mark)
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

// SetNodeDeleteSkipped opts a SRC node out of source removal (skipped=true: plan pending, or
// acknowledge failed on delete results) or re-includes it during cleanup planning (skipped=false).
func (m *Migration) SetNodeDeleteSkipped(nodeID string, skipped bool) (PathReviewActionResult, error) {
	allowed := []string{PhaseCopyReview, PhaseDeleteReview}
	verb := "skip delete"
	errMsg := "skip delete requires awaiting-copy-review or awaiting-delete-review"
	target := db.DeleteStatusSkipped
	if !skipped {
		allowed = []string{PhaseCopyReview}
		verb = "unskip delete"
		errMsg = "unskip delete requires awaiting-copy-review phase"
		target = db.DeleteStatusPendingExplicit
	}
	return m.mutatePathReviewRetry(nodeID, allowed, errMsg, func(id string) (int64, map[string]int64, error) {
		node, err := pull.GetNodeByID(m.DB, "SRC", id)
		if err != nil {
			return 0, nil, err
		}
		if node == nil {
			return 0, nil, fmt.Errorf("node %s not found in SRC", id)
		}
		if !db.CopyStatusIsComplete(node.CopyStatus) {
			return 0, nil, fmt.Errorf("%s requires copy complete (successful or already_existed)", verb)
		}
		if !skipped && node.DeleteStatus == db.DeleteStatusDeleted {
			return 0, nil, fmt.Errorf("cannot unskip already deleted node")
		}
		return m.store.setNodeDeleteStatusWithPropagation(id, target)
	})
}

// PrepareSourceCleanup aligns delete_status with the user's selected SRC nodes for source removal.
// When keepNodeIDs is non-empty, only those nodes are pending* and others are skipped.
// When deselectedNodeIDs is non-empty (and keepNodeIDs is empty), those nodes are skipped and others pending*.
// When both are empty, this is idempotent first-time init: unset delete_status becomes pending*, then
// the delete forest is normalized (explicit roots + inherited descendants). Existing skipped/failed/deleted
// are left alone so remounting source-cleanup does not wipe the user's skip plan.
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
	if initOnly {
		seeded, err := m.DB.SeedUnsetDeleteStatuses()
		if err != nil {
			return PathReviewActionResult{}, err
		}
		deltas := make(map[string]int64)
		if seeded.Affected > 0 {
			addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusPendingExplicit, seeded.Affected)
			addReviewDelta(deltas, DeltaFolders, seeded.Folders)
			addReviewDelta(deltas, DeltaFiles, seeded.Files)
			if seeded.SelectedBytes != 0 {
				addReviewDelta(deltas, DeltaSizeDeleteSelected, seeded.SelectedBytes)
				addReviewDelta(deltas, DeltaSizeSelected, seeded.SelectedBytes)
			}
			if err := m.store.persistReviewDeltas(deltas); err != nil {
				return PathReviewActionResult{}, fmt.Errorf("persist review deltas: %w", err)
			}
		}
		return pathReviewResult(seeded.Affected, deltas), nil
	}
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
			case useKeepList:
				if keep[node.ID] {
					if node.DeleteStatus == db.DeleteStatusDeleted || node.DeleteStatus == db.DeleteStatusFailed {
						continue
					}
					target = db.DeleteStatusPendingInherited
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
					target = db.DeleteStatusPendingInherited
				}
			}
			if node.DeleteStatus == target {
				continue
			}
			if db.DeleteStatusIsPending(node.DeleteStatus) && db.DeleteStatusIsPending(target) {
				continue
			}
			n, deltas, err := m.store.setNodeDeleteStatus(node.ID, target)
			if err != nil {
				return pathReviewResult(total, merged), err
			}
			total += n
			mergeDeltas(merged, deltas)
		}
		if len(nodes) < pageSize {
			break
		}
		offset += pageSize
	}
	if err := m.DB.NormalizeDeleteForestOps(); err != nil {
		return pathReviewResult(total, merged), fmt.Errorf("normalize delete forest: %w", err)
	}
	return pathReviewResult(total, merged), nil
}

// excludeNodePhaseAware picks traversal-ack, delete-skip, or copy-exclusion by phase/status.
func (m *Migration) excludeNodePhaseAware(nodeID string, excluded bool) (int64, map[string]int64, error) {
	phase := m.Phase()
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if phase == PhaseTraversalReview {
		if excluded && node.TraversalStatus == db.StatusFailed {
			return m.store.setTraversalFailedAcknowledged(nodeID, true)
		}
		if !excluded && opsdb.IsTraversalAckFromFailed(node.ExclusionSource) {
			return m.store.setTraversalFailedAcknowledged(nodeID, false)
		}
	}
	if phase == PhaseDeleteReview {
		if excluded && node.DeleteStatus == db.DeleteStatusFailed {
			return m.store.setNodeDeleteStatusWithPropagation(nodeID, db.DeleteStatusSkipped)
		}
		if !excluded && node.DeleteStatus == db.DeleteStatusSkipped {
			return m.store.setNodeDeleteStatusWithPropagation(nodeID, db.DeleteStatusPendingExplicit)
		}
		return 0, nil, nil
	}
	// Post-copy review: re-including an ordinary exclusion queues it as
	// retry-marked pending so the retry CTA and copyPendingRetry update.
	if phase == PhaseCopyReview && !excluded {
		return m.store.setNodeExcludedAsRetry(nodeID)
	}
	return m.store.setNodeExcluded(nodeID, excluded)
}

func (m *Migration) SetNodeExcludedWithPropagation(queueType, nodeID string, excluded bool) (PathReviewActionResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopyReview && phase != PhaseDeleteReview {
		return PathReviewActionResult{}, fmt.Errorf("exclusion propagation requires a review phase")
	}
	return m.withPathMutation(nodeID, func() (PathReviewActionResult, error) {
		n, deltas, err := m.excludeNodePhaseAware(nodeID, excluded)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		return pathReviewResult(n, deltas), nil
	})
}

func (m *Migration) BulkExcludeWithPropagation(filter NodeQueryFilter, excluded bool) (PathReviewActionResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopyReview && phase != PhaseDeleteReview {
		return PathReviewActionResult{}, fmt.Errorf("bulk exclusion requires a review phase")
	}
	return m.withBulkMutation(func() (PathReviewActionResult, error) {
		nodes, err := m.QueryNodes(filter)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		var total int64
		merged := make(map[string]int64)
		for i := range nodes {
			n, deltas, err := m.excludeNodePhaseAware(nodes[i].ID, excluded)
			if err != nil {
				return pathReviewResult(total, merged), err
			}
			total += n
			mergeDeltas(merged, deltas)
		}
		return pathReviewResult(total, merged), nil
	})
}

// ApplySearchExclusion excludes (excluded=true) pending SRC nodes matching the same predicate as
// path-review search (flat conditions and/or ruleset), minus exceptIDs, or restores pending for
// excluded matches (excluded=false). applicationID is required when excluding and ignored otherwise.
func (m *Migration) ApplySearchExclusion(req SearchRequest, excluded bool, exceptIDs []string, applicationID string) (PathReviewActionResult, error) {
	verb := "unexclusion"
	if excluded {
		verb = "exclusion"
	}
	if !copyExclusionPhase(m.Phase()) {
		return PathReviewActionResult{}, fmt.Errorf("search %s requires awaiting-traversal-review or awaiting-copy-review phase", verb)
	}
	if excluded && applicationID == "" {
		return PathReviewActionResult{}, fmt.Errorf("filter application id required")
	}
	return m.withBulkMutation(func() (PathReviewActionResult, error) {
		f, err := searchRequestToReviewFilter(req)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		if !review.ReviewFilterHasSearchPredicate(f) {
			return PathReviewActionResult{}, fmt.Errorf("search predicate required")
		}
		criteriaJSON, err := json.Marshal(req)
		if err != nil {
			return PathReviewActionResult{}, fmt.Errorf("marshal search criteria: %w", err)
		}
		n, deltas, err := m.store.applySearchExclusion(
			excluded, f, string(criteriaJSON), exceptIDs, applicationID, time.Now().UTC().UnixNano(),
		)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		return pathReviewResult(n, deltas), nil
	})
}

func (m *Migration) ListChildrenDiffs(req ListChildrenDiffsRequest) (ListChildrenDiffsResult, error) {
	if !reviewOrLaterPhase(m.Phase()) {
		return ListChildrenDiffsResult{}, fmt.Errorf("diff listing requires review or later phase")
	}
	return m.store.listChildrenDiffs(req)
}

func (m *Migration) SearchPathReviewItems(ctx context.Context, req SearchRequest) (SearchResult, error) {
	if !reviewOrLaterPhase(m.Phase()) {
		return SearchResult{}, fmt.Errorf("search requires review or later phase")
	}
	if _, err := searchRequestToReviewFilter(req); err != nil {
		return SearchResult{}, err
	}
	readCtx, cancel := m.BeginInteractiveRead(ctx)
	defer cancel()
	return m.store.searchPathReviewItems(readCtx, req)
}

func (m *Migration) GetChildrenDiffsStats(path string, foldersOnly bool, includeDestinationOnly *bool) (DiffsStats, error) {
	if !reviewOrLaterPhase(m.Phase()) {
		return DiffsStats{}, fmt.Errorf("diff stats requires review or later phase")
	}
	return m.store.getChildrenDiffsStats(path, foldersOnly, includeDestinationOnly)
}

// GetSearchStats returns aggregate counts for the same filter as SearchPathReviewItems (query, path, status, foldersOnly).
func (m *Migration) GetSearchStats(ctx context.Context, req SearchRequest) (DiffsStats, error) {
	if !reviewOrLaterPhase(m.Phase()) {
		return DiffsStats{}, fmt.Errorf("search stats requires review or later phase")
	}
	readCtx, cancel := m.BeginInteractiveRead(ctx)
	defer cancel()
	return m.store.getSearchStats(readCtx, req)
}

func (m *Migration) GetQueueMetrics() (QueueMetricsSnapshot, error) {
	if o := m.activeQueueObs.Load(); o != nil {
		if raw, ok := o.LastQueueMetricsForAPI(); ok && len(raw) > 0 {
			return queueMetricsSnapshotFromRawJSON(raw), nil
		}
		// A live observer exists but has not produced its first memory snapshot yet.
		// Never fall back to DB on a live API request.
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
	if m.IsLive() {
		return m.liveRecentLogs(limit), nil
	}
	return m.store.listRecentLogs(limit)
}

// SearchLogs queries the logs sidecar (exact text, time range, action, node).
func (m *Migration) SearchLogs(q opsdb.LogSearchQuery) ([]LogEntry, error) {
	return m.store.searchLogs(q)
}

func (m *Migration) liveRecentLogs(limit int) []LogEntry {
	var out []LogEntry
	if logservice.LS != nil {
		live := logservice.LS.Recent(limit)
		out = make([]LogEntry, 0, len(live))
		for _, e := range live {
			out = append(out, LogEntry{
				ID: e.ID, Timestamp: e.At, Level: e.Level, Message: e.Message,
				Entity: e.Entity, EntityID: e.EntityID, Queue: e.Queue,
				Action: e.Action, NodeID: e.NodeID,
			})
		}
	}
	m.mu.RLock()
	fromPhase := m.logRing.recent(limit)
	m.mu.RUnlock()
	if len(out) == 0 {
		return fromPhase
	}
	seen := make(map[string]struct{}, len(out))
	for _, e := range out {
		if e.ID != "" {
			seen[e.ID] = struct{}{}
		}
	}
	for _, e := range fromPhase {
		if len(out) >= limit {
			break
		}
		if e.ID != "" {
			if _, ok := seen[e.ID]; ok {
				continue
			}
			seen[e.ID] = struct{}{}
		}
		out = append(out, e)
	}
	return out
}
