// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// Legacy path-event names are retained for API/test source compatibility. They
// map onto the sparse gpl_issues table; no path_events table is created.
const (
	PathEventCategoryGPLClean    = "gpl_clean"
	PathEventCategoryManualRemap = "manual_remap"

	PathEventStatusPending      = "pending"
	PathEventStatusCollision    = "collision"
	PathEventStatusManualReview = "manual_review" // no safe auto-suggestion; user must rename
	PathEventStatusAccepted     = "accepted"
	PathEventStatusCommitted    = "committed"
	PathEventStatusReverted     = "reverted"

	GPLIssueStatusPending      = PathEventStatusPending
	GPLIssueStatusManualReview = PathEventStatusManualReview
	GPLIssueStatusAccepted     = PathEventStatusAccepted

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

// PathEvent is the legacy input shape bridged to GPLIssue.
type PathEvent struct {
	ID           string
	EventTime    int64
	Category     string
	ProposedPath string
	Status       string
	GPLIssues    string // serialized issue log; preserved across overrides
}

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
}

// GPLStatePayload is the compact JSON stored on src_nodes.gpl_state.
// Part holds segment-local findings; Path holds path-scoped findings; Parts is the
// effective migration-relative segment list for AddPart / ValidatePath cascades.
type GPLStatePayload struct {
	Valid bool          `json:"valid"`
	Part  GPLScopeState `json:"part"`
	Path  GPLScopeState `json:"path"`
	Parts []string      `json:"parts,omitempty"`

	// Flat fields retained for reading older gpl_state rows written before part/path split.
	Collision     bool     `json:"collision,omitempty"`
	Categories    []string `json:"categories,omitempty"`
	ProposedClean string   `json:"proposed_clean,omitempty"`
}

// GPLScopeState is part-local or path-scoped GPL findings.
type GPLScopeState struct {
	Valid         bool     `json:"valid"`
	Categories    []string `json:"categories,omitempty"`
	ProposedClean string   `json:"proposed_clean,omitempty"`
	Collision     bool     `json:"collision,omitempty"`
}

// EffectiveProposedClean returns the part-level proposed clean, falling back to legacy flat field.
func (p GPLStatePayload) EffectiveProposedClean() string {
	if p.Part.ProposedClean != "" {
		return p.Part.ProposedClean
	}
	return p.ProposedClean
}

// OverallValid reports whether both scopes are valid (legacy flat Valid if scopes empty).
func (p GPLStatePayload) OverallValid() bool {
	if len(p.Part.Categories) == 0 && len(p.Path.Categories) == 0 && !p.Part.Collision && p.ProposedClean == "" && len(p.Categories) > 0 {
		return p.Valid
	}
	return p.Part.Valid && p.Path.Valid && !p.Part.Collision
}
