// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "time"

// NodeRecord is immutable catalog metadata stored in Badger.
type NodeRecord struct {
	ID              string `json:"id"`
	ServiceID       string `json:"service_id,omitempty"`
	ParentID        string `json:"parent_id,omitempty"`
	ParentServiceID string `json:"parent_service_id,omitempty"`
	Path            string `json:"path,omitempty"`
	ParentPath      string `json:"parent_path,omitempty"`
	Name            string `json:"name,omitempty"`
	DisplayPath     string `json:"display_path,omitempty"`
	Type            string `json:"type,omitempty"`
	Size            int64  `json:"size,omitempty"`
	MTime           string `json:"mtime,omitempty"`
	Depth           int    `json:"depth,omitempty"`
	IncludeOnly     string `json:"include_only,omitempty"`
	GPLState        string `json:"gpl_state,omitempty"`
}

// StatusRecord is the hot mutable overlay for a node.
type StatusRecord struct {
	TraversalStatus          string `json:"traversal_status,omitempty"`
	CopyStatus               string `json:"copy_status,omitempty"`
	DeleteStatus             string `json:"delete_status,omitempty"`
	SkippedDescendantCount   int64  `json:"skipped_descendant_count,omitempty"`
	GPLStatus                string `json:"gpl_status,omitempty"`
	IncludeOnly              string `json:"include_only,omitempty"`
	ExclusionSource          string `json:"exclusion_source,omitempty"`
	DeterminingRuleID        string `json:"determining_rule_id,omitempty"`
	ErrorLogID               string `json:"error_log_id,omitempty"`
	// ChildSize is the sum of file bytes under this folder (identity, not selected).
	ChildSize                int64  `json:"child_size,omitempty"`
	XferOffset               int64  `json:"xfer_offset,omitempty"`
	XferSrcSize              int64  `json:"xfer_src_size,omitempty"`
	XferSrcMTime             string `json:"xfer_src_mtime,omitempty"`
	XferDstRef               string `json:"xfer_dst_ref,omitempty"`
	XferResumeToken          string `json:"xfer_resume_token,omitempty"`
}

// DisplayBytes is the size shown for a node. Folders use identity child_size; files use their own size.
func DisplayBytes(typ string, own, child int64) int64 {
	if typ == NodeTypeFile {
		return own
	}
	return child
}

// KidRecord is one slim child snapshot packed under kids:{side}:{parent}.
type KidRecord struct {
	ID              string `json:"id"`
	ServiceID       string `json:"service_id,omitempty"`
	ParentServiceID string `json:"parent_service_id,omitempty"`
	Path            string `json:"path,omitempty"`
	ParentPath      string `json:"parent_path,omitempty"`
	Name            string `json:"name,omitempty"`
	Type            string `json:"type,omitempty"`
	Size            int64  `json:"size,omitempty"`
	MTime           string `json:"mtime,omitempty"`
	Depth           int    `json:"depth,omitempty"`
	TraversalStatus string `json:"traversal_status,omitempty"`
	CopyStatus      string `json:"copy_status,omitempty"`
	DeleteStatus    string `json:"delete_status,omitempty"`
	ChildSize       int64  `json:"child_size,omitempty"`
}

// IDMapRecord is an SRC↔DST pairing.
type IDMapRecord struct {
	SrcID  string `json:"src_id"`
	DstID  string `json:"dst_id"`
	Source string `json:"source,omitempty"`
	Status string `json:"status,omitempty"`
}

// LogRecord is one append-only log row.
type LogRecord struct {
	ID        string    `json:"id"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Detail    string    `json:"detail,omitempty"`
	Component string    `json:"component,omitempty"`
	Entity    string    `json:"entity,omitempty"`
	EntityID  string    `json:"entity_id,omitempty"`
	Queue     string    `json:"queue,omitempty"`
	At        time.Time `json:"at"`
}

// DBOpRecord is one timing sample.
type DBOpRecord struct {
	Op         string    `json:"op"`
	SQL        string    `json:"sql,omitempty"`
	Rows       int64     `json:"rows,omitempty"`
	DurationNs int64     `json:"duration_ns"`
	Err        string    `json:"err,omitempty"`
	At         time.Time `json:"at"`
}

// QueueStatsRecord is durable queue metrics JSON.
type QueueStatsRecord struct {
	QueueKey    string    `json:"queue_key"`
	Phase       string    `json:"phase"`
	MetricsJSON string    `json:"metrics_json"`
	At          time.Time `json:"at"`
}

// TaskErrorRecord is one task failure.
type TaskErrorRecord struct {
	QueueType string    `json:"queue_type"`
	Phase     string    `json:"phase"`
	NodeID    string    `json:"node_id"`
	Message   string    `json:"message"`
	Attempts  int       `json:"attempts"`
	Path      string    `json:"path,omitempty"`
	At        time.Time `json:"at"`
}

// CatalogBatch is one sealed round diff for Duck ingest.
type CatalogBatch struct {
	Side   string
	Round  int
	Phase  string
	Nodes  []NodeRecord
	IDMaps []IDMapRecord
	Seq    uint64
}
