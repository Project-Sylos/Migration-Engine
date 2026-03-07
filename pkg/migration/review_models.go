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

// StopResult reports stop/suspend state after a stop request.
type StopResult struct {
	MigrationID   string
	Phase         Phase
	RuntimeStatus RuntimeState
	Stopped       bool
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
	Excluded           bool
	MissingOnSource    bool
	MissingOnDest      bool
	Size               int64
}

type ListChildrenDiffsRequest struct {
	Path          string
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
	FoldersOnly   bool
	Status        string
}

type ListChildrenDiffsResult struct {
	Items  []DiffItem
	Total  int
	Limit  int
	Offset int
}

type SearchRequest struct {
	Query         string
	Path          string // empty = global search over all review paths
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
	FoldersOnly   bool
	Status        string
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

// PathReviewActionResult is the result of a path review mutation. Deltas holds per-status/category changes (e.g. "pending": -1, "excluded": 1) so the UI can update each counter. AffectedCount is the total rows touched.
type PathReviewActionResult struct {
	AffectedCount int64
	Deltas        map[string]int64 // keys: "pending", "failed", "excluded", "pendingRetries", "folders", "files", "sizeSrc", "sizeDst"
}

// PathReviewStats is the UI/API-facing review stats shape. pendingCount, failedCount, and pendingRetriesCount are phase-aware.
type PathReviewStats struct {
	PendingCount       int
	FailedCount        int
	ExcludedCount      int
	PendingRetriesCount int
	FoldersCount       int
	FilesCount         int
	FoldersRatio       float64
	FilesRatio         float64
	TotalFileSize      struct {
		Src int64
		Dst int64
	}
}

// ReviewStatsRawFromSnapshot converts the DB snapshot to the migration-layer raw stats (e.g. for seeding the in-memory cache).
func ReviewStatsRawFromSnapshot(s db.ReviewStatsSnapshot) ReviewStatsRaw {
	return ReviewStatsRaw{
		TraversalPending:     s.TraversalPending,
		TraversalPendingRetry: s.TraversalPendingRetry,
		TraversalFailed:      s.TraversalFailed,
		CopyPending:          s.CopyPending,
		CopyFailed:           s.CopyFailed,
		Excluded:             s.Excluded,
		Folders:              s.Folders,
		Files:                s.Files,
		SizeSrc:              s.SizeSrc,
		SizeDst:              s.SizeDst,
	}
}

// ReviewStatsRaw is the canonical persisted counters in the universal stats table (key -> count).
// Used for cache and delta updates; PathReviewStats is derived from this plus phase.
type ReviewStatsRaw struct {
	TraversalPending     int64
	TraversalPendingRetry int64
	TraversalFailed      int64
	CopyPending          int64
	CopyFailed           int64
	Excluded             int64
	Folders              int64
	Files                int64
	SizeSrc              int64
	SizeDst              int64
}

// ToPathReviewStats projects raw stats into the API shape using phase: traversal review uses traversal pending/failed and pendingRetries; copy review/completed uses copy pending/failed.
func (r ReviewStatsRaw) ToPathReviewStats(phase Phase) PathReviewStats {
	var pendingCount, failedCount, pendingRetriesCount int64
	switch phase {
	case PhaseReview:
		pendingCount = r.TraversalPending
		failedCount = r.TraversalFailed
		pendingRetriesCount = r.TraversalPendingRetry
	case PhaseCopying, PhaseCompleted:
		pendingCount = r.CopyPending
		failedCount = r.CopyFailed
		pendingRetriesCount = r.CopyPending // copy phase: no separate retry counter
	default:
		pendingCount = r.TraversalPending
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
