// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"strings"
)

// NormalizeRootRelativePath returns a root-relative path with no "//".
// Root is "/"; children are "/name", "/name/child". Collapses any "//" to "/".
// Used for display/open paths only — not as a join key.
func NormalizeRootRelativePath(path string) string {
	if path == "" {
		return "/"
	}
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}
	if path != "/" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// NormalizeQueueNodeType coerces a node type to the structural values stored in queue tables.
// Cloud browse tags (team_folder, shared_folder, …) must not be persisted on src_nodes/dst_nodes.
func NormalizeQueueNodeType(typ string) string {
	if typ == NodeTypeFile {
		return NodeTypeFile
	}
	return NodeTypeFolder
}

// NormalizeSubtreeRootPathForPropagation normalizes an id_path so subtree updates match src_nodes.path
// (root-relative slashes, trim trailing slash except "/").
func NormalizeSubtreeRootPathForPropagation(path string) string {
	p := NormalizeRootRelativePath(path)
	if p != "/" && strings.HasSuffix(p, "/") {
		p = strings.TrimRight(p, "/")
	}
	return p
}

// IDPathSubtreePredicateSQL is a SQL fragment matching root id_path and descendants.
// Use with starts_with so UUID segments are not LIKE-wildcarded. rootCol is typically "n.path".
// For root "/", use path LIKE '/%' (or path <> '/' for strict descendants).
func IDPathSubtreePredicateSQL(rootCol string, includeRoot bool, rootEqParam, rootPrefixParam string) string {
	if includeRoot {
		return `(` + rootCol + ` = ` + rootEqParam + ` OR starts_with(` + rootCol + `, ` + rootPrefixParam + `))`
	}
	return `starts_with(` + rootCol + `, ` + rootPrefixParam + `)`
}

// NodeState is the in-memory representation of a row in src_nodes or dst_nodes.
// ID is a UUID v5 (MintNodeID). SRC↔DST pairing uses id_map; parent/child uses parent_id.
// Path/parent_path store immutable id ancestry chains (id/id/id), not display names.
// Name is the mutable basename used for UI, GPL leaf checks, and FS create/rename.
// DisplayPath is the write-once root-relative name path for filter rules (/ → /name → /a/b).
type NodeState struct {
	ID              string // UUID v5 internal id (MintNodeID)
	ServiceID       string // FS handle (cloud native id, or local path)
	ParentID        string // Parent's internal id
	ParentServiceID string
	Path            string // Immutable id_path ancestry key
	ParentPath      string // Parent id_path
	Name            string // Mutable display basename
	DisplayPath     string // Write-once discovery display path (filter rules); never updated on rename
	Type            string // "folder" or "file"
	Size            int64
	MTime           string
	Depth           int
	TraversalStatus string // pending, successful, failed, not_on_src (dst)
	CopyStatus      string // pending, in_progress, successful, failed (src)
	DeleteStatus    string // pending, deleted, failed (src)
	GPLStatus       string // pending, successful, failed (path-scoped cascade)
	Excluded        bool
	Errors          string // JSON placeholder for log refs
	Status          string // Alias for TraversalStatus (used by queue taskToNodeState)
	SrcID           string // Optional: corresponding SRC node id (DST seeding / compare)
	GPLState        string // Compact JSON (SRC only); empty for DST
	// IncludeOnly is JSON []string of child service IDs allowed when traversing this SRC folder.
	// Empty means unrestricted (list and keep all children).
	IncludeOnly string
	// ExclusionSource is sealed provenance (manual / filter id / discovery-retry marks).
	ExclusionSource string
}

// RuleEvaluationEvent is one append-only row for rule_evaluation_events.
type RuleEvaluationEvent struct {
	NodeID      string
	RulesetID   string
	RuleID      string
	Result      string // passed | excluded | engine_error
	EvalPhase   string // pre | post
	Label       string
	EvaluatedAt int64
}

// NodeMeta is a subset of NodeState for batch lookups.
type NodeMeta struct {
	ID              string
	Depth           int
	Type            string
	TraversalStatus string
	CopyStatus      string
	DeleteStatus    string
}

// InsertOperation represents a single node insert in a batch.
type InsertOperation struct {
	QueueType string // "SRC" or "DST"
	Level     int    // depth
	Status    string // initial traversal_status
	State     *NodeState
}

// FetchResult is one row from a keyset list (id + full state).
// DstParentServiceID is populated by ListNodesCopyKeyset via id_map → dst_nodes.
type FetchResult struct {
	Key                   string
	State                 *NodeState
	DstParentServiceID    string // DST parent's ServiceID from id_map join (copy pull only)
	DstParentNodeID       string // DST parent's internal id from id_map
	ResolvedDstPath       string // Effective destination path/segment from path_events (copy pull)
	DstMappedID           string // Current dst_internal_id from id_map for this src id (copy pull)
	ParentGPLState        string // Parent src_nodes.gpl_state (GPL cascade pull)
	SrcParentDeleteStatus string // SRC parent delete_status at pull (copy; avoids GetNodeByPath at complete)
}

// StatusEvent is one append-only row for src_status_events or dst_status_events.
type StatusEvent struct {
	ID                string
	TraversalStatus   string // nullable in DB
	CopyStatus        string // src only; empty for dst
	DeleteStatus      string // src only; empty for dst
	GPLStatus         string // path-scoped cascade; empty means "unchanged" for arg_max filters
	EventTime         int64
	Depth             int
	ErrorLogID        string // links to logs.id when this event records a task failure
	ExclusionSource   string // manual or filter application id; empty clears provenance
	DeterminingRuleID string
	ErrorLogMessage   string // transient: full log line written to logs.message at seal flush
	ErrorLogDetail    string // transient: bare error written to logs.detail at seal flush
	ErrorLogQueue     string // transient: logs.queue at seal flush
	// PrevTraversalStatus and PrevCopyStatus carry the status that was current before this event.
	// Set at enqueue time (task already has the loaded state); used by the seal buffer to compute
	// per-depth level-stat deltas without re-querying the events table.
	PrevTraversalStatus string
	PrevCopyStatus      string
	PrevDeleteStatus    string
	PrevGPLStatus       string
	// Size and NodeType are transient (not persisted on status_events). When set on SRC
	// events, seal flush applies size_selected / size_delete_selected and folders/files
	// deltas so Path Review Selected stays durable mid-round.
	Size     int64
	NodeType string
}

// TaskErrorRecord is one buffered row for task_errors (queue_type, phase, node_id, message, attempts, path).
type TaskErrorRecord struct {
	QueueType string
	Phase     string
	NodeID    string
	Message   string
	Attempts  int
	Path      string
}

func discoveryDepthStatsDeltas(table string, nodes []*NodeState) []DepthStatsDelta {
	out := make([]DepthStatsDelta, 0, len(nodes)*3)
	for _, n := range nodes {
		if n == nil {
			continue
		}
		trav := n.TraversalStatus
		if trav == "" {
			trav = StatusPending
		}
		out = append(out, DepthStatsDelta{Table: table, Depth: n.Depth, Key: StatsKey(StatsKindTraversal, trav), Delta: 1})
		if table != "SRC" {
			continue
		}
		copySt := n.CopyStatus
		if copySt == "" {
			copySt = CopyStatusPending
		}
		out = append(out, DepthStatsDelta{Table: table, Depth: n.Depth, Key: StatsKeyTyped(StatsKindCopy, copySt, n.Type), Delta: 1})
		if NormalizeQueueNodeType(n.Type) == NodeTypeFile && n.Size > 0 {
			out = append(out, DepthStatsDelta{Table: table, Depth: n.Depth, Key: StatsKeyCopyFileBytes(copySt), Delta: n.Size})
		}
		if n.DeleteStatus != "" {
			out = append(out, DepthStatsDelta{Table: table, Depth: n.Depth, Key: StatsKeyTyped(StatsKindDelete, n.DeleteStatus, n.Type), Delta: 1})
		}
	}
	return out
}

// NodeInsertName returns the display basename to store in the name column.
// Providers sometimes report a full path (or nothing) as the display name, so the
// stored value is always reduced to the leaf segment, falling back to the node path.
func NodeInsertName(name, path string) string {
	if base := NormalizeNodeBasename(name); base != "" && base != "/" {
		return base
	}
	// path is an id_path; do not treat UUID segments as display names.
	_ = path
	return ""
}

// NodeInsertPathFields returns normalized path columns for node table inserts.
// Depth-0 rows keep parent_path as stored (typically "" for roots). Deeper rows normalize parent_path.
func NodeInsertPathFields(path, parentPath string, depth int) (normPath, normParentPath string) {
	normPath = NormalizeRootRelativePath(path)
	if depth == 0 {
		normParentPath = parentPath
	} else {
		normParentPath = NormalizeRootRelativePath(parentPath)
	}
	return normPath, normParentPath
}
