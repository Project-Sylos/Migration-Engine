// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "codeberg.org/Sylos/Migration-Engine/pkg/db"

// RetrySweepOptions are manager-level knobs for retry sweep runs.
type RetrySweepOptions struct {
	WorkerCount   int
	MaxRetries    int
	LogAddress    string
	LogLevel      string
	SkipListener  bool
	MaxKnownDepth int
}

// CopyPhaseOptions are optional overrides for copy phase / copy retry runs. Zero values use last run config.
type CopyPhaseOptions struct {
	WorkerCount  int
	MaxRetries   int
	LogAddress   string
	LogLevel     string
	SkipListener bool
}

// StopResult reports stop/suspend state after a stop request.
type StopResult struct {
	MigrationID   string
	Phase         string
	RuntimeStatus RuntimeState
	Stopped       bool
	// SoftSuspendRequested is true when a live traversal/copy run was asked to soft-suspend (drain + persist); completion is asynchronous.
	SoftSuspendRequested bool
	// ForceStopped is true when Stop() grace expired and the run context was canceled to end a stuck run.
	ForceStopped bool
}

// DiffItem is a path review row comparing source and destination state.
type DiffItem struct {
	Path               string
	Name               string
	Depth              int
	Type               string
	SrcNodeID          string
	DstNodeID          string
	SrcTraversalStatus string
	DstTraversalStatus string
	CopyStatus         string
	DeleteStatus       string
	Excluded           bool
	MissingOnSource    bool
	MissingOnDest      bool
	Size               int64
	SrcFailureLogID    string
	SrcFailureMessage  string
	DstFailureLogID    string
	DstFailureMessage  string
}

type ListChildrenDiffsRequest struct {
	Path            string
	Limit           int
	Offset          int
	SortBy          string
	SortDirection   string
	FoldersOnly     bool
	TraversalStatus string
	CopyStatus      string
	// IncludeDestinationOnly when false hides destination-only rows. Nil means include (legacy default).
	IncludeDestinationOnly *bool
}

type ListChildrenDiffsResult struct {
	Items  []DiffItem
	Total  int
	Limit  int
	Offset int
}

// PathReviewSearchCondition mirrors API/UI search filters (field names lowercase in JSON).
type PathReviewSearchCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator,omitempty"`
	Value    any    `json:"value"`
}

type SearchRequest struct {
	Query         string
	Path          string // empty = global search over all review paths
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
	FoldersOnly   bool

	// Structured search. StatusSearchType + TraversalStatus + CopyStatus filter rows.
	Conditions       []PathReviewSearchCondition `json:"conditions,omitempty"`
	StatusSearchType string                      `json:"statusSearchType,omitempty"` // traversal, copy, both
	TraversalStatus  string                      `json:"traversalStatus,omitempty"`
	CopyStatus       string                      `json:"copyStatus,omitempty"`
	DeleteStatus     string                      `json:"deleteStatus,omitempty"`
	// IncludeDestinationOnly when false hides destination-only rows. Nil means include (legacy default).
	IncludeDestinationOnly *bool `json:"includeDestinationOnly,omitempty"`
}

type SearchResult struct {
	Items  []DiffItem
	Total  int
	Limit  int
	Offset int
}

type DiffsStats struct {
	Total           int
	Folders         int
	Files           int
	MissingOnSource int
	MissingOnDest   int
	Excluded        int
}

// Canonical delta keys for PathReviewActionResult.Deltas. Only keys that changed (non-zero) are included.
const (
	DeltaTraversalPending      = "traversalPending"
	DeltaTraversalPendingRetry = "traversalPendingRetry"
	DeltaTraversalFailed       = "traversalFailed"
	DeltaCopyPending           = "copyPending"
	DeltaCopyFailed            = "copyFailed"
	DeltaCopySuccessful        = "copySuccessful"
	DeltaDeletePending         = "deletePending"
	DeltaDeleteFailed          = "deleteFailed"
	DeltaDeleteDeleted         = "deleteDeleted"
	DeltaDeleteSkipped         = "deleteSkipped"
	DeltaExcluded              = "excluded"
	DeltaFolders               = "folders"
	DeltaFiles                 = "files"
	DeltaSizeSrc               = "sizeSrc"
	DeltaSizeDst               = "sizeDst"
)

// addReviewDelta sets deltas[key] = delta only when delta != 0, so the API omits unchanged counters.
func addReviewDelta(deltas map[string]int64, key string, delta int64) {
	if delta != 0 {
		deltas[key] = delta
	}
}

// deleteStatusReviewDeltaKey maps delete_status to the migration-layer review delta key.
// Statuses without a tracked review counter (e.g. skipped) return "".
func deleteStatusReviewDeltaKey(status string) string {
	switch status {
	case db.DeleteStatusPending:
		return DeltaDeletePending
	case db.DeleteStatusFailed:
		return DeltaDeleteFailed
	case db.DeleteStatusDeleted:
		return DeltaDeleteDeleted
	case db.DeleteStatusSkipped:
		return DeltaDeleteSkipped
	default:
		return ""
	}
}

// addReviewDeltaForDeleteStatus applies a signed delta for one delete_status bucket.
func addReviewDeltaForDeleteStatus(deltas map[string]int64, status string, delta int64) {
	if key := deleteStatusReviewDeltaKey(status); key != "" {
		addReviewDelta(deltas, key, delta)
	}
}

// addReviewDeltaForDeleteStatusTransition updates review counters when delete_status changes.
func addReviewDeltaForDeleteStatusTransition(deltas map[string]int64, from, to string) {
	addReviewDeltaForDeleteStatus(deltas, from, -1)
	addReviewDeltaForDeleteStatus(deltas, to, 1)
}

// PathReviewActionResult is the result of a path review mutation. Deltas holds per-status/category changes (only non-zero keys). UI applies them to the matching counter; phase determines which counters are shown.
type PathReviewActionResult struct {
	AffectedCount int64
	Deltas        map[string]int64
}

// PathReviewStats is the UI/API-facing review stats shape. pendingCount, failedCount, and pendingRetriesCount are phase-aware.
type PathReviewStats struct {
	PendingCount        int
	FailedCount         int
	ExcludedCount       int
	PendingRetriesCount int
	SuccessfulCount     int
	FoldersCount        int
	FilesCount          int
	FoldersRatio        float64
	FilesRatio          float64
	TotalFileSize       struct {
		Src int64
		Dst int64
	}
}

// ReviewStatsRawFromSnapshot converts the DB snapshot to the migration-layer raw stats (e.g. for seeding the in-memory cache).
func ReviewStatsRawFromSnapshot(s db.ReviewStatsSnapshot) ReviewStatsRaw {
	return ReviewStatsRaw{
		TraversalPending:      s.TraversalPending,
		TraversalPendingRetry: s.TraversalPendingRetry,
		TraversalFailed:       s.TraversalFailed,
		CopyPending:           s.CopyPending,
		CopyFailed:            s.CopyFailed,
		CopySuccessful:        s.CopySuccessful,
		DeletePending:         s.DeletePending,
		DeleteFailed:          s.DeleteFailed,
		Excluded:              s.Excluded,
		Folders:               s.Folders,
		Files:                 s.Files,
		SizeSrc:               s.SizeSrc,
		SizeDst:               s.SizeDst,
	}
}

// ReviewStatsRaw is the canonical persisted counters in the universal stats table (key -> count).
// Used for cache and delta updates; PathReviewStats is derived from this plus phase.
type ReviewStatsRaw struct {
	TraversalPending      int64
	TraversalPendingRetry int64
	TraversalFailed       int64
	CopyPending           int64
	CopyFailed            int64
	CopySuccessful        int64
	DeletePending         int64
	DeleteFailed          int64
	Excluded              int64
	Folders               int64
	Files                 int64
	SizeSrc               int64
	SizeDst               int64
}

// ToPathReviewStats projects raw stats into the API shape using phase.
func (r ReviewStatsRaw) ToPathReviewStats(phase string) PathReviewStats {
	var pendingCount, failedCount, pendingRetriesCount int64
	switch phase {
	case PhaseTraversing, PhaseTraversalSuspended, PhaseTraversalReview:
		// Traversing includes initial traversal and traversal retry sweep; same counters as review for API polls.
		pendingCount = r.CopyPending
		failedCount = r.TraversalFailed
		pendingRetriesCount = r.TraversalPendingRetry
	case PhaseCopying, PhaseCopySuspended, PhaseCopyReview:
		pendingCount = 0
		failedCount = r.CopyFailed
		pendingRetriesCount = r.CopyPending // copy phase: no separate retry counter
	case PhaseDeleting, PhaseDeleteSuspended, PhaseDeleteReview:
		pendingCount = r.DeletePending
		failedCount = r.DeleteFailed
		pendingRetriesCount = r.DeletePending // items marked pending for delete retry (mirrors copy review)
	default:
		pendingCount = r.CopyPending
		failedCount = r.TraversalFailed
		pendingRetriesCount = r.TraversalPendingRetry
	}
	total := r.Folders + r.Files
	var foldersRatio, filesRatio float64
	if total > 0 {
		foldersRatio = roundRatio(float64(r.Folders)/float64(total), 2)
		filesRatio = roundRatio(float64(r.Files)/float64(total), 2)
	}
	return PathReviewStats{
		PendingCount:        int(pendingCount),
		FailedCount:         int(failedCount),
		ExcludedCount:       int(r.Excluded),
		PendingRetriesCount: int(pendingRetriesCount),
		SuccessfulCount:     int(r.CopySuccessful),
		FoldersCount:        int(r.Folders),
		FilesCount:          int(r.Files),
		FoldersRatio:        foldersRatio,
		FilesRatio:          filesRatio,
		TotalFileSize:       struct{ Src, Dst int64 }{r.SizeSrc, r.SizeDst},
	}
}

type QueueMetricsSnapshot struct {
	Queues map[string]map[string]any
}

type LogsProjection struct {
	Entries []LogEntry
	ByLevel map[string][]LogEntry
}
