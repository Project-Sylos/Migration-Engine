// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// GPL naming issues are stored in the sparse gpl_issues table; no path_events table exists.
const (
	GPLIssueStatusPending      = "pending"
	GPLIssueStatusManualReview = "manual_review" // no safe auto-suggestion; user must rename
	GPLIssueStatusAccepted     = "accepted"

	// DstActionRename marks Accept on an already_existed SRC node: rename DST in place.
	DstActionRename = "rename"
)

// ID map sources and statuses (append-only id_map table).
const (
	IDMapSourceDSTCompare  = "dst_compare"
	IDMapSourceManualRemap = "manual_remap"
	IDMapSourceRootSeed    = "root_seed"

	IDMapStatusActive = "active"
)

// GPLIssue is the sparse current naming-issue shape.
type GPLIssue struct {
	SrcID        string
	Status       string
	ProposedName string
	IssuesJSON   string
	UpdatedAt    int64
	DstAction    string // empty (create-on-copy) or DstActionRename
}

// IDMapEvent is one append-only row for id_map.
type IDMapEvent struct {
	SrcInternalID string
	DstInternalID string
	EventTime     int64
	Source        string
	Status        string
	Depth         int // BFS round when the map row was written (Duck ingest gate).
}

// GPLStatePayload is the compact JSON stored on src_nodes.gpl_state.
// Part holds segment-local findings; Path holds path-scoped findings.
// PathLen is the joined migration-relative path length after this node's leaf eval
// (parent PathLen + sep + leaf).
type GPLStatePayload struct {
	Valid   bool          `json:"valid"`
	Part    GPLScopeState `json:"part"`
	Path    GPLScopeState `json:"path"`
	PathLen int           `json:"path_len,omitempty"`
}

// GPLScopeState is part-local or path-scoped GPL findings.
type GPLScopeState struct {
	Valid         bool     `json:"valid"`
	Categories    []string `json:"categories,omitempty"`
	ProposedClean string   `json:"proposed_clean,omitempty"`
	Collision     bool     `json:"collision,omitempty"`
}