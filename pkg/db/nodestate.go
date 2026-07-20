// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"strings"
	"time"
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

// NormalizeSubtreeRootPathForPropagation normalizes a failed folder path so subtree updates match src_nodes.path
// (root-relative slashes, trim trailing slash except "/").
func NormalizeSubtreeRootPathForPropagation(path string) string {
	p := NormalizeRootRelativePath(path)
	if p != "/" && strings.HasSuffix(p, "/") {
		p = strings.TrimRight(p, "/")
	}
	return p
}

// NodeState is the in-memory representation of a row in src_nodes or dst_nodes.
// ID is a UUID v5 (MintNodeID). SRC↔DST pairing uses id_map; parent/child uses parent_id.
// Path/parent_path are display and open-path fields only.
type NodeState struct {
	ID               string // UUID v5 internal id (MintNodeID)
	ServiceID        string // FS handle (cloud native id, or local path)
	ParentID         string // Parent's internal id
	ParentServiceID  string
	Path             string // Display / open path (immutable after insert)
	ParentPath       string // Display parent path
	Name             string // Display name (for task/UI)
	Type             string // "folder" or "file"
	Size             int64
	MTime            string
	Depth            int
	TraversalStatus  string // pending, successful, failed, not_on_src (dst)
	CopyStatus       string // pending, in_progress, successful, failed (src)
	DeleteStatus     string // pending, deleted, failed (src)
	GPLStatus        string // pending, successful, failed (path-scoped cascade)
	Excluded         bool
	Errors           string // JSON placeholder for log refs
	Status           string // Alias for TraversalStatus (used by queue taskToNodeState)
	SrcID            string // Optional: corresponding SRC node id (DST seeding / compare)
	GPLState         string // Compact JSON (SRC only); empty for DST
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
	QueueType string   // "SRC" or "DST"
	Level     int      // depth
	Status    string   // initial traversal_status
	State     *NodeState
}

// FetchResult is one row from a keyset list (id + full state).
// DstParentServiceID is populated by ListNodesCopyKeyset via id_map → dst_nodes.
type FetchResult struct {
	Key                string
	State              *NodeState
	DstParentServiceID string // DST parent's ServiceID from id_map join (copy pull only)
	DstParentNodeID    string // DST parent's internal id from id_map
	ResolvedDstPath    string // Effective destination path/segment from path_events (copy pull)
	DstMappedID        string // Current dst_internal_id from id_map for this src id (copy pull)
	ParentGPLState     string // Parent src_nodes.gpl_state (GPL cascade pull)
}

// WriteOperation is an operation that can be buffered and flushed via the writer.
type WriteOperation interface {
	flush(w *Writer) error
}

// StatusEvent is one append-only row for src_status_events or dst_status_events.
type StatusEvent struct {
	ID               string
	TraversalStatus  string // nullable in DB
	CopyStatus       string // src only; empty for dst
	DeleteStatus     string // src only; empty for dst
	GPLStatus        string // path-scoped cascade; empty means "unchanged" for arg_max filters
	EventTime        int64
	Depth            int
	ErrorLogID       string // links to logs.id when this event records a task failure
	ErrorLogMessage  string // transient: full log line written to logs.message at seal flush
	ErrorLogDetail   string // transient: bare error written to logs.detail at seal flush
	ErrorLogQueue    string // transient: logs.queue at seal flush
	// PrevTraversalStatus and PrevCopyStatus carry the status that was current before this event.
	// Set at enqueue time (task already has the loaded state); used by the seal buffer to compute
	// per-depth level-stat deltas without re-querying the events table.
	PrevTraversalStatus string
	PrevCopyStatus      string
	PrevDeleteStatus    string
	PrevGPLStatus       string
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

// StatusUpdateOperation represents a traversal status transition (e.g. pending → successful).
type StatusUpdateOperation struct {
	QueueType string
	Level     int
	OldStatus string
	NewStatus string
	NodeID    string
}

func (o *StatusUpdateOperation) flush(w *Writer) error {
	// Status updates are applied via cache + SealLevel; no staging write.
	return nil
}

// BatchInsertOperation is a batch of node inserts.
type BatchInsertOperation struct {
	Operations []InsertOperation
}

func (o *BatchInsertOperation) flush(w *Writer) error {
	if len(o.Operations) == 0 {
		return nil
	}
	srcNodes := make([]*NodeState, 0)
	dstNodes := make([]*NodeState, 0)
	eventTime := time.Now().UnixNano()
	for _, op := range o.Operations {
		if op.State == nil {
			continue
		}
		s := op.State
		if s.TraversalStatus == "" {
			s.TraversalStatus = op.Status
		}
		if s.Status == "" {
			s.Status = s.TraversalStatus
		}
		table := op.QueueType
		if table == "DST" {
			dstNodes = append(dstNodes, s)
		} else {
			srcNodes = append(srcNodes, s)
		}
	}
	if len(srcNodes) > 0 {
		if err := w.AppenderInsert(tableSrcNodes, srcNodes); err != nil {
			return err
		}
		for _, s := range srcNodes {
			ev := &StatusEvent{ID: s.ID, TraversalStatus: s.TraversalStatus, CopyStatus: s.CopyStatus, DeleteStatus: s.DeleteStatus, EventTime: eventTime, Depth: s.Depth}
			if err := w.InsertStatusEvent("SRC", ev); err != nil {
				return err
			}
		}
	}
	if len(dstNodes) > 0 {
		if err := w.AppenderInsert(tableDstNodes, dstNodes); err != nil {
			return err
		}
		for _, s := range dstNodes {
			ev := &StatusEvent{ID: s.ID, TraversalStatus: s.TraversalStatus, EventTime: eventTime, Depth: s.Depth}
			if err := w.InsertStatusEvent("DST", ev); err != nil {
				return err
			}
		}
	}
	return nil
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
