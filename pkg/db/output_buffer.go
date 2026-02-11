// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"sync"
	"time"
)

// OutputBuffer buffers write operations and flushes them via the single writer.
type OutputBuffer struct {
	db        *DB
	batchSize int
	interval  time.Duration
	mu        sync.Mutex
	// Pending ops
	statusUpdates  []statusUpdate
	copyUpdates    []copyUpdate
	insertBatches  []InsertOperation
	writeOps       []WriteOperation
	nodeDeletions  []nodeDeletion
	onFlush        func(nodeIDs []string)
	flushScheduled bool
	stopCh         chan struct{}
}

type statusUpdate struct {
	table  string
	level  int
	oldSt  string
	newSt  string
	nodeID string
}

type copyUpdate struct {
	level    int
	nodeType string
	oldSt    string
	newSt    string
	nodeID   string
}

type nodeDeletion struct {
	table  string
	nodeID string
	level  int
	status string
}

// NewOutputBuffer creates a new output buffer. Flush uses db.RunUpdateWriterTx (staging appends, appender inserts, deletions).
func NewOutputBuffer(db *DB, batchSize int, interval time.Duration) *OutputBuffer {
	ob := &OutputBuffer{
		db:        db,
		batchSize: batchSize,
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
	go ob.flushLoop()
	return ob
}

func (ob *OutputBuffer) flushLoop() {
	ticker := time.NewTicker(ob.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ob.stopCh:
			return
		case <-ticker.C:
			ob.Flush()
		}
	}
}

// AddStatusUpdate buffers a traversal status update (staging + stats on flush).
func (ob *OutputBuffer) AddStatusUpdate(table string, level int, oldStatus, newStatus, nodeID string) {
	ob.mu.Lock()
	ob.statusUpdates = append(ob.statusUpdates, statusUpdate{table: table, level: level, oldSt: oldStatus, newSt: newStatus, nodeID: nodeID})
	ob.mu.Unlock()
	ob.maybeScheduleFlush()
}

// AddCopyStatusUpdate buffers a copy status update (staging + stats on flush).
func (ob *OutputBuffer) AddCopyStatusUpdate(table string, level int, nodeType, oldStatus, nodeID, newStatus string) {
	ob.mu.Lock()
	ob.copyUpdates = append(ob.copyUpdates, copyUpdate{level: level, nodeType: nodeType, oldSt: oldStatus, newSt: newStatus, nodeID: nodeID})
	ob.mu.Unlock()
	ob.maybeScheduleFlush()
}

// AddCreateNode buffers a single node insert (DST create during copy).
func (ob *OutputBuffer) AddCreateNode(table string, level int, status string, state *NodeState) {
	ob.mu.Lock()
	ob.insertBatches = append(ob.insertBatches, InsertOperation{QueueType: table, Level: level, Status: status, State: state})
	ob.mu.Unlock()
	ob.maybeScheduleFlush()
}

// AddMultiple buffers multiple write operations (status updates + batch inserts).
func (ob *OutputBuffer) AddMultiple(ops []WriteOperation) {
	if len(ops) == 0 {
		return
	}
	ob.mu.Lock()
	ob.writeOps = append(ob.writeOps, ops...)
	ob.mu.Unlock()
	ob.maybeScheduleFlush()
}

// AddNodeDeletion buffers a node delete (retry DST cleanup).
func (ob *OutputBuffer) AddNodeDeletion(table, nodeID string, level int, status string) {
	ob.mu.Lock()
	ob.nodeDeletions = append(ob.nodeDeletions, nodeDeletion{table: table, nodeID: nodeID, level: level, status: status})
	ob.mu.Unlock()
	ob.maybeScheduleFlush()
}

func (ob *OutputBuffer) maybeScheduleFlush() {
	ob.mu.Lock()
	n := len(ob.statusUpdates) + len(ob.copyUpdates) + len(ob.insertBatches) + len(ob.writeOps) + len(ob.nodeDeletions)
	ob.mu.Unlock()
	if n >= ob.batchSize {
		ob.Flush()
	}
}

// Flush runs appender path for staging + node inserts (RunAppenderTx with flush), then RunUpdateWriterTx for node deletions only.
// Staging is coalesced to one row per node (last update wins). Order in one Tx: src_staging → dst_staging → src_nodes → dst_nodes.
func (ob *OutputBuffer) Flush() {
	ob.mu.Lock()
	statusUpdates := ob.statusUpdates
	copyUpdates := ob.copyUpdates
	insertBatches := ob.insertBatches
	writeOps := ob.writeOps
	nodeDeletions := ob.nodeDeletions
	ob.statusUpdates = nil
	ob.copyUpdates = nil
	ob.insertBatches = nil
	ob.writeOps = nil
	ob.nodeDeletions = nil
	ob.mu.Unlock()

	if len(statusUpdates) == 0 && len(copyUpdates) == 0 && len(insertBatches) == 0 && len(writeOps) == 0 && len(nodeDeletions) == 0 {
		return
	}

	// Coalesce staging: one row per node for src_staging (traversal + copy) and dst_staging (traversal).
	type srcStagingRow struct{ traversal, copy string }
	srcStaging := make(map[string]srcStagingRow)
	dstStaging := make(map[string]string)
	var flushedIDs []string

	for _, u := range statusUpdates {
		if u.nodeID == "" {
			continue
		}
		flushedIDs = append(flushedIDs, u.nodeID)
		if u.table == "DST" {
			dstStaging[u.nodeID] = u.newSt
		} else {
			r := srcStaging[u.nodeID]
			r.traversal = u.newSt
			srcStaging[u.nodeID] = r
		}
	}
	for _, u := range copyUpdates {
		flushedIDs = append(flushedIDs, u.nodeID)
		r := srcStaging[u.nodeID]
		r.copy = u.newSt
		srcStaging[u.nodeID] = r
	}
	for _, op := range writeOps {
		if su, ok := op.(*StatusUpdateOperation); ok {
			if su.NodeID != "" {
				flushedIDs = append(flushedIDs, su.NodeID)
				if su.QueueType == "DST" {
					dstStaging[su.NodeID] = su.NewStatus
				} else {
					r := srcStaging[su.NodeID]
					r.traversal = su.NewStatus
					srcStaging[su.NodeID] = r
				}
			}
		}
	}

	// Collect node inserts from insertBatches and BatchInsertOperation in writeOps.
	var srcNodes, dstNodes []*NodeState
	for _, op := range insertBatches {
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
		if op.QueueType == "DST" {
			dstNodes = append(dstNodes, s)
		} else {
			srcNodes = append(srcNodes, s)
		}
		flushedIDs = append(flushedIDs, s.ID)
	}
	for _, op := range writeOps {
		if bi, ok := op.(*BatchInsertOperation); ok {
			for _, o := range bi.Operations {
				if o.State == nil {
					continue
				}
				s := o.State
				if s.TraversalStatus == "" {
					s.TraversalStatus = o.Status
				}
				if s.Status == "" {
					s.Status = s.TraversalStatus
				}
				if o.QueueType == "DST" {
					dstNodes = append(dstNodes, s)
				} else {
					srcNodes = append(srcNodes, s)
				}
				flushedIDs = append(flushedIDs, s.ID)
			}
		}
	}

	hasStagingOrInserts := len(srcStaging) > 0 || len(dstStaging) > 0 || len(srcNodes) > 0 || len(dstNodes) > 0
	if hasStagingOrInserts {
		err := ob.db.RunAppenderTx(func(aw *AppenderWriter) error {
			for nodeID, r := range srcStaging {
				if err := aw.srcStaging.AppendRow(nodeID, r.traversal, r.copy); err != nil {
					return err
				}
			}
			for nodeID, newTraversal := range dstStaging {
				if err := aw.dstStaging.AppendRow(nodeID, newTraversal); err != nil {
					return err
				}
			}
			for _, n := range srcNodes {
				if err := aw.AppendNode("SRC", n); err != nil {
					return err
				}
			}
			for _, n := range dstNodes {
				if err := aw.AppendNode("DST", n); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			// Re-queue or log; for now we drop on error to avoid blocking
			return
		}
	}

	if len(nodeDeletions) > 0 {
		err := ob.db.RunUpdateWriterTx(func(w *Writer) error {
			for _, d := range nodeDeletions {
				if err := w.DeleteNode(d.table, d.nodeID); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return
		}
	}

	if ob.onFlush != nil {
		ob.onFlush(flushedIDs)
	}
}

// SetOnFlush sets the callback invoked after a successful flush (e.g. to remove leased keys).
func (ob *OutputBuffer) SetOnFlush(fn func(nodeIDs []string)) {
	ob.mu.Lock()
	ob.onFlush = fn
	ob.mu.Unlock()
}
