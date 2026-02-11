// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"sync"
	"time"
)

const defaultBatchSize = 10000
const defaultFlushInterval = 3 * time.Second

// srcStagingRow holds coalesced traversal and copy status for a node in src_staging.
type srcStagingRow struct {
	traversal string
	copy      string
}

// stagingBuffer buffers status updates for a staging table. Flushes to appender; never checkpoints.
type stagingBuffer struct {
	table      string // "SRC" or "DST"
	db         *DB
	batchSize  int
	interval   time.Duration
	mu         sync.Mutex
	srcRows    map[string]srcStagingRow // nodeID -> row (src_staging has traversal + copy)
	dstRows    map[string]string        // nodeID -> newTraversalStatus (dst_staging)
	stopCh     chan struct{}
	onFlush    func(nodeIDs []string)
}

func newStagingBuffer(db *DB, table string) *stagingBuffer {
	sb := &stagingBuffer{
		table:     table,
		db:        db,
		batchSize: defaultBatchSize,
		interval:  defaultFlushInterval,
		stopCh:    make(chan struct{}),
	}
	if table == "SRC" {
		sb.srcRows = make(map[string]srcStagingRow)
	} else {
		sb.dstRows = make(map[string]string)
	}
	go sb.flushLoop()
	return sb
}

func (sb *stagingBuffer) flushLoop() {
	ticker := time.NewTicker(sb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-sb.stopCh:
			return
		case <-ticker.C:
			sb.Flush()
		}
	}
}

func (sb *stagingBuffer) addTraversal(nodeID, newStatus string) {
	if nodeID == "" {
		return
	}
	sb.mu.Lock()
	if sb.table == "SRC" {
		r := sb.srcRows[nodeID]
		r.traversal = newStatus
		sb.srcRows[nodeID] = r
	} else {
		sb.dstRows[nodeID] = newStatus
	}
	n := len(sb.srcRows) + len(sb.dstRows)
	sb.mu.Unlock()
	if n >= sb.batchSize {
		sb.Flush()
	}
}

func (sb *stagingBuffer) addCopy(nodeID, newStatus string) {
	if nodeID == "" || sb.table != "SRC" {
		return
	}
	sb.mu.Lock()
	r := sb.srcRows[nodeID]
	r.copy = newStatus
	sb.srcRows[nodeID] = r
	n := len(sb.srcRows)
	sb.mu.Unlock()
	if n >= sb.batchSize {
		sb.Flush()
	}
}

func (sb *stagingBuffer) Flush() {
	sb.mu.Lock()
	var srcRows map[string]srcStagingRow
	var dstRows map[string]string
	if sb.table == "SRC" {
		srcRows = sb.srcRows
		sb.srcRows = make(map[string]srcStagingRow)
	} else {
		dstRows = sb.dstRows
		sb.dstRows = make(map[string]string)
	}
	sb.mu.Unlock()

	var flushedIDs []string
	queueType := sb.table
	if len(srcRows) > 0 || len(dstRows) > 0 {
		for id := range srcRows {
			flushedIDs = append(flushedIDs, id)
		}
		for id := range dstRows {
			flushedIDs = append(flushedIDs, id)
		}
		err := sb.db.runStagingFlush(queueType, srcRows, dstRows)
		if err != nil {
			return
		}
	}
	if sb.onFlush != nil && len(flushedIDs) > 0 {
		sb.onFlush(flushedIDs)
	}
}

func (sb *stagingBuffer) SetOnFlush(fn func(nodeIDs []string)) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.onFlush = fn
}

func (sb *stagingBuffer) Stop() {
	close(sb.stopCh)
}

// nodesBuffer buffers node inserts for src_nodes or dst_nodes. Flushes to appender; never checkpoints.
type nodesBuffer struct {
	table     string // "SRC" or "DST"
	db        *DB
	batchSize int
	interval  time.Duration
	mu        sync.Mutex
	nodes     []*NodeState
	stopCh    chan struct{}
	onFlush   func(nodeIDs []string)
}

func newNodesBuffer(db *DB, table string) *nodesBuffer {
	nb := &nodesBuffer{
		table:     table,
		db:        db,
		batchSize: defaultBatchSize,
		interval:  defaultFlushInterval,
		nodes:     make([]*NodeState, 0, 256),
		stopCh:    make(chan struct{}),
	}
	go nb.flushLoop()
	return nb
}

func (nb *nodesBuffer) flushLoop() {
	ticker := time.NewTicker(nb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-nb.stopCh:
			return
		case <-ticker.C:
			nb.Flush()
		}
	}
}

func (nb *nodesBuffer) Add(n *NodeState, status string) {
	if n == nil {
		return
	}
	if n.TraversalStatus == "" {
		n.TraversalStatus = status
	}
	if n.Status == "" {
		n.Status = n.TraversalStatus
	}
	nb.mu.Lock()
	nb.nodes = append(nb.nodes, n)
	count := len(nb.nodes)
	nb.mu.Unlock()
	if count >= nb.batchSize {
		nb.Flush()
	}
}

func (nb *nodesBuffer) AddBatch(nodes []*NodeState, status string) {
	if len(nodes) == 0 {
		return
	}
	nb.mu.Lock()
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.TraversalStatus == "" {
			n.TraversalStatus = status
		}
		if n.Status == "" {
			n.Status = n.TraversalStatus
		}
		nb.nodes = append(nb.nodes, n)
	}
	count := len(nb.nodes)
	nb.mu.Unlock()
	if count >= nb.batchSize {
		nb.Flush()
	}
}

func (nb *nodesBuffer) Flush() {
	nb.mu.Lock()
	nodes := nb.nodes
	nb.nodes = make([]*NodeState, 0, cap(nb.nodes))
	nb.mu.Unlock()

	if len(nodes) == 0 {
		return
	}
	flushedIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		flushedIDs = append(flushedIDs, n.ID)
	}
	err := nb.db.runNodesFlush(nb.table, nodes)
	if err != nil {
		return
	}
	if nb.onFlush != nil {
		nb.onFlush(flushedIDs)
	}
}

func (nb *nodesBuffer) SetOnFlush(fn func(nodeIDs []string)) {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	nb.onFlush = fn
}

func (nb *nodesBuffer) Stop() {
	close(nb.stopCh)
}
