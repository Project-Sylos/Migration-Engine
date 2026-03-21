// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

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
	Path          string
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
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

type QueueMetricsSnapshot struct {
	Queues map[string]map[string]any
}

type LogsProjection struct {
	Entries []LogEntry
	ByLevel map[string][]LogEntry
}
