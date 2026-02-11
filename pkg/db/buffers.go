// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const defaultBatchSize = 100_000
const defaultFlushInterval = 30 * time.Second

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

func (sb *stagingBuffer) addTraversal(nodeID, newTraversal string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if sb.table == "SRC" {
		r := sb.srcRows[nodeID]
		r.traversal = newTraversal
		sb.srcRows[nodeID] = r
	} else {
		sb.dstRows[nodeID] = newTraversal
	}
}

func (sb *stagingBuffer) addCopy(nodeID, newCopyStatus string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if sb.table != "SRC" {
		return
	}
	r := sb.srcRows[nodeID]
	r.copy = newCopyStatus
	sb.srcRows[nodeID] = r
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

	if len(srcRows) == 0 && len(dstRows) == 0 {
		return
	}
	if err := sb.db.runStagingFlush(sb.table, srcRows, dstRows); err != nil {
		return
	}
	if sb.onFlush != nil {
		ids := make([]string, 0, len(srcRows)+len(dstRows))
		for k := range srcRows {
			ids = append(ids, k)
		}
		for k := range dstRows {
			ids = append(ids, k)
		}
		sb.onFlush(ids)
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

// nodesBuffer buffers node inserts for src_nodes or dst_nodes.
type nodesBuffer struct {
	table   string // "SRC" or "DST"
	db      *DB
	nodes   []*NodeState
	mu      sync.Mutex
	stopCh  chan struct{}
	onFlush func(nodeIDs []string)
}

func newNodesBuffer(db *DB, table string) *nodesBuffer {
	nb := &nodesBuffer{
		table:  table,
		db:     db,
		nodes:  make([]*NodeState, 0, 256),
		stopCh: make(chan struct{}),
	}
	go nb.flushLoop()
	return nb
}

func (nb *nodesBuffer) flushLoop() {
	ticker := time.NewTicker(defaultFlushInterval)
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
	nb.AddBatch([]*NodeState{n}, status)
}

func (nb *nodesBuffer) AddBatch(nodes []*NodeState, _ string) {
	if len(nodes) == 0 {
		return
	}
	nb.mu.Lock()
	nb.nodes = append(nb.nodes, nodes...)
	count := len(nb.nodes)
	nb.mu.Unlock()
	if count >= defaultBatchSize {
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

	// Instrumentation: verify live count after flush (single conn, no cross-connection test)
	tbl := tableName(nb.table)
	if pullConn, err := nb.db.GetDBForPulls(nb.table); err == nil {
		var n int
		if qerr := pullConn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+tbl).Scan(&n); qerr == nil {
			fmt.Printf("LIVE COUNT AFTER FLUSH (%s): %d (flushed %d)\n", tbl, n, len(flushedIDs))
		}
	}
	fmt.Printf("Flushing %s items: %d\n", nb.table, len(flushedIDs))
}

func (nb *nodesBuffer) SetOnFlush(fn func(nodeIDs []string)) {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	nb.onFlush = fn
}

func (nb *nodesBuffer) Stop() {
	close(nb.stopCh)
}
