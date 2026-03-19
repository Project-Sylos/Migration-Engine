// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// NormalizeRootRelativePath returns a root-relative path with no "//" so SRC/DST path_hash joins match.
// Root is "/"; children are "/name", "/name/child". Collapses any "//" to "/".
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

// NodeState is the in-memory representation of a row in src_nodes or dst_nodes.
// Path and parent_path are the join keys between SRC and DST.
type NodeState struct {
	ID               string // Internal ULID-like id (deterministic from queueType, nodeType, path)
	ServiceID        string // FS id
	ParentID         string // Parent's internal id
	ParentServiceID  string
	Path             string // Join key with other table
	ParentPath       string // Join key for children
	Name             string // Display name (for task/UI)
	Type             string // "folder" or "file"
	Size             int64
	MTime            string
	Depth            int
	TraversalStatus  string // pending, successful, failed, not_on_src (dst)
	CopyStatus       string // pending, in_progress, successful, failed (src)
	Excluded         bool
	Errors           string // JSON placeholder for log refs
	Status           string // Alias for TraversalStatus (used by queue taskToNodeState)
	SrcID            string // Optional: corresponding SRC node id (join is by path; used during seeding for DST root)
}

// NodeMeta is a subset of NodeState for batch lookups.
type NodeMeta struct {
	ID              string
	Depth           int
	Type            string
	TraversalStatus string
	CopyStatus      string
}

// InsertOperation represents a single node insert in a batch.
type InsertOperation struct {
	QueueType string   // "SRC" or "DST"
	Level     int      // depth
	Status    string   // initial traversal_status
	State     *NodeState
}

// FetchResult is one row from a keyset list (id + full state).
// DstParentServiceID is populated by ListNodesCopyKeyset when joining dst_nodes on parent path_hash.
type FetchResult struct {
	Key                string
	State              *NodeState
	DstParentServiceID string // DST parent's ServiceID from path_hash join (copy pull only)
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
	EventTime        int64
	Depth            int
	// PrevTraversalStatus and PrevCopyStatus carry the status that was current before this event.
	// Set at enqueue time (task already has the loaded state); used by the seal buffer to compute
	// per-depth level-stat deltas without re-querying the events table.
	PrevTraversalStatus string
	PrevCopyStatus      string
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
			ev := &StatusEvent{ID: s.ID, TraversalStatus: s.TraversalStatus, CopyStatus: s.CopyStatus, EventTime: eventTime, Depth: s.Depth}
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

// PathHash returns a deterministic 32-char hex hash of path for use as an index key.
func PathHash(path string) string {
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:16])
}

// PathHashForJoin returns PathHash(NormalizeRootRelativePath(path)). Use when storing or querying path_hash
// so that "" and "/" hash to the same value and SRC/DST root joins match.
func PathHashForJoin(path string) string {
	return PathHash(NormalizeRootRelativePath(path))
}

// DeterministicNodeID returns a stable id from (queueType, nodeType, path) for race-safe deduplication.
func DeterministicNodeID(queueType, nodeType, path string) string {
	h := sha256.New()
	h.Write([]byte(queueType))
	h.Write([]byte("\x00"))
	h.Write([]byte(nodeType))
	h.Write([]byte("\x00"))
	h.Write([]byte(path))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16]) // 32 hex chars
}
