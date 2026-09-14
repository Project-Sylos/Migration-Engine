// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"errors"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

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
	DstSize            int64
	HasDstSize         bool
	SrcFailureLogID    string
	SrcFailureMessage  string
	DstFailureLogID    string
	DstFailureMessage  string
	// ResolvedDstName is the accepted/committed destination basename from path_events.
	// Empty when no remap was applied. Review identity (Path/Name) stays SRC-original.
	ResolvedDstName string
	// DisplayPath is the user-facing name path (e.g. /Reports/a.txt). Path stays id_path for nav.
	DisplayPath string
	// DstDisplayPath is the destination-side friendly path when it differs (rename leaf).
	DstDisplayPath string
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
	// AfterPath / AfterID keyset cursor within the folder (path ASC). When set, Offset is ignored for filtering.
	AfterPath string
	AfterID   string
	// IncludeDestinationOnly when false hides destination-only rows. Nil means include.
	IncludeDestinationOnly *bool
}

type ListChildrenDiffsResult struct {
	Items   []DiffItem
	Total   *int // nil when unknown (hasMore pagination)
	HasMore bool
	Limit   int
	Offset  int
}

// PathReviewSearchCondition mirrors API/UI search filters (field names lowercase in JSON).
type PathReviewSearchCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator,omitempty"`
	Value    any    `json:"value"`
}

type SearchRequest struct {
	Query string
	Path  string // empty = global search over all review paths
	// UnderPath scopes to a folder and descendants (not direct children). Empty or "/" = global.
	UnderPath     string
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
	AfterPath     string
	AfterID       string

	FoldersOnly bool

	// Structured search. StatusSearchType + TraversalStatus + CopyStatus filter rows.
	Conditions       []PathReviewSearchCondition `json:"conditions,omitempty"`
	StatusSearchType string                      `json:"statusSearchType,omitempty"` // traversal, copy, both
	TraversalStatus  string                      `json:"traversalStatus,omitempty"`
	CopyStatus       string                      `json:"copyStatus,omitempty"`
	DeleteStatus     string                      `json:"deleteStatus,omitempty"`
	// IncludeDestinationOnly when false hides destination-only rows. Nil means include.
	IncludeDestinationOnly *bool `json:"includeDestinationOnly,omitempty"`
	// Ruleset is an optional filter-rules predicate compiled into SRC search SQL.
	Ruleset *filter.Ruleset `json:"ruleset,omitempty"`
}

type SearchResult struct {
	Items []DiffItem
	// Total is nil when unknown (search hot path does not COUNT). Exact totals come from GetSearchStats.
	Total   *int `json:"total,omitempty"`
	HasMore bool `json:"hasMore"`
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
}

// ErrSearchRequiresFilter is returned when SearchPathReviewItems is called with no narrowing predicates.
var ErrSearchRequiresFilter = errors.New("search requires at least one filter")

type DiffsStats struct {
	Total           int
	Folders         int
	Files           int
	MissingOnSource int
	MissingOnDest   int
	Excluded        int
	// Truncated means the count stopped early (deadline); Total is a lower bound.
	Truncated bool
}

// Canonical delta keys for PathReviewActionResult.Deltas. Only keys that changed (non-zero) are included.
const (
	DeltaTraversalPending      = "traversalPending"
	DeltaTraversalPendingRetry = "traversalPendingRetry"
	DeltaTraversalFailed       = "traversalFailed"
	DeltaCopyPending           = "copyPending"
	DeltaCopyPendingRetry      = "copyPendingRetry"
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
	DeltaSizeSelected          = "sizeSelected"
	DeltaSizeDeleteSelected    = "sizeDeleteSelected"
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
	case db.DeleteStatusPendingExplicit, db.DeleteStatusPendingInherited:
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
// PendingCount is nil when the current review view has no Pending column (Discover / delete results);
// it is never zeroed to "hide" a live counter.
type PathReviewStats struct {
	PendingCount        *int
	FailedCount         int
	ExcludedCount       int
	PendingRetriesCount int
	SuccessfulCount     int
	FoldersCount        int
	FilesCount          int
	FoldersRatio        float64
	FilesRatio          float64
	TotalFileSize       struct {
		Src      int64
		Dst      int64
		Selected int64
	}
}

// ReviewStatsRawFromSnapshot converts the DB snapshot to the migration-layer raw stats (e.g. for seeding the in-memory cache).
func ReviewStatsRawFromSnapshot(s db.ReviewStatsSnapshot) ReviewStatsRaw {
	return ReviewStatsRaw{
		TraversalPending:      s.TraversalPending,
		TraversalPendingRetry: s.TraversalPendingRetry,
		TraversalFailed:       s.TraversalFailed,
		CopyPending:           s.CopyPending,
		CopyPendingRetry:      s.CopyPendingRetry,
		CopyFailed:            s.CopyFailed,
		CopySuccessful:        s.CopySuccessful,
		DeletePending:         s.DeletePending,
		DeleteFailed:          s.DeleteFailed,
		DeleteSkipped:         s.DeleteSkipped,
		Excluded:              s.Excluded,
		Folders:               s.Folders,
		Files:                 s.Files,
		SizeSrc:               s.SizeSrc,
		SizeDst:               s.SizeDst,
		SizeSelected:          s.SizeSelected,
	}
}

// ReviewStatsRaw is the canonical persisted counters in the universal stats table (key -> count).
// Used for cache and delta updates; PathReviewStats is derived from this plus phase.
type ReviewStatsRaw struct {
	TraversalPending      int64
	TraversalPendingRetry int64
	TraversalFailed       int64
	CopyPending           int64
	CopyPendingRetry      int64
	CopyFailed            int64
	CopySuccessful        int64
	DeletePending         int64
	DeleteFailed          int64
	DeleteSkipped         int64
	Excluded              int64
	Folders               int64
	Files                 int64
	SizeSrc               int64
	SizeDst               int64
	SizeSelected          int64
}

// ToPathReviewStats projects raw stats into the API shape using phase.
// PendingCount is for plan/preview and live runs only; Discover and post-review
// (copy results, delete results) omit it (nil) rather than zeroing live counters.
func (r ReviewStatsRaw) ToPathReviewStats(phase string) PathReviewStats {
	var pendingCount *int
	var failedCount, pendingRetriesCount int64
	switch phase {
	case PhaseTraversing, PhaseTraversalSuspended, PhaseTraversalReview:
		failedCount = clampNonNeg(r.TraversalFailed)
		pendingRetriesCount = clampNonNeg(r.TraversalPendingRetry)
	case PhaseCopying, PhaseCopySuspended, PhaseCopyFinalizing, PhaseCopyFinalizeFailed:
		retry := clampNonNeg(r.CopyPendingRetry)
		pendingCount = intPtr(int(clampNonNeg(r.CopyPending - retry)))
		failedCount = clampNonNeg(r.CopyFailed)
		pendingRetriesCount = retry
	case PhaseCopyReview:
		// Post-copy review: no Pending column; marked leftovers stay under Pending Retry.
		failedCount = clampNonNeg(r.CopyFailed)
		pendingRetriesCount = clampNonNeg(r.CopyPendingRetry)
	case PhaseDeleting, PhaseDeleteSuspended:
		pendingCount = intPtr(int(clampNonNeg(r.DeletePending)))
		failedCount = clampNonNeg(r.DeleteFailed)
	case PhaseDeleteReview:
		failedCount = clampNonNeg(r.DeleteFailed)
		pendingRetriesCount = clampNonNeg(r.DeletePending)
	default:
		failedCount = clampNonNeg(r.TraversalFailed)
		pendingRetriesCount = clampNonNeg(r.TraversalPendingRetry)
	}
	total := r.Folders + r.Files
	var foldersRatio, filesRatio float64
	if total > 0 {
		foldersRatio = roundRatio(float64(r.Folders)/float64(total), 2)
		filesRatio = roundRatio(float64(r.Files)/float64(total), 2)
	}
	return PathReviewStats{
		PendingCount:        pendingCount,
		FailedCount:         int(failedCount),
		ExcludedCount:       int(r.Excluded),
		PendingRetriesCount: int(pendingRetriesCount),
		SuccessfulCount:     int(r.CopySuccessful),
		FoldersCount:        int(r.Folders),
		FilesCount:          int(r.Files),
		FoldersRatio:        foldersRatio,
		FilesRatio:          filesRatio,
		TotalFileSize: struct{ Src, Dst, Selected int64 }{
			Src:      maxInt64(0, r.SizeSrc),
			Dst:      maxInt64(0, r.SizeDst),
			Selected: maxInt64(0, r.SizeSelected),
		},
	}
}

func intPtr(n int) *int { return &n }

func clampNonNeg(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type QueueMetricsSnapshot struct {
	Queues map[string]map[string]any
}

type LogsProjection struct {
	Entries []LogEntry
	ByLevel map[string][]LogEntry
}
