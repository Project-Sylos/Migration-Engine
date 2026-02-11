// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"crypto/sha256"
	"encoding/hex"
)

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
type FetchResult struct {
	Key   string
	State *NodeState
}

// WriteOperation is an operation that can be buffered and flushed via the writer.
type WriteOperation interface {
	flush(w *Writer) error
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
	if o.NodeID == "" {
		return nil
	}
	table := o.QueueType
	if table != "SRC" && table != "DST" {
		table = "SRC"
	}
	return w.AppendStatusStaging(table, o.NodeID, o.OldStatus, o.NewStatus)
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
	}
	if len(dstNodes) > 0 {
		if err := w.AppenderInsert(tableDstNodes, dstNodes); err != nil {
			return err
		}
	}
	return nil
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
