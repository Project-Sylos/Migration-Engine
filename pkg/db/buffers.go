// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"time"
)

const defaultBatchSize = 50_000
const defaultFlushInterval = 30 * time.Second
const backPressureHardCapMultiple = 2 // block writes when buffer reaches 2x batch size

// srcStagingRow holds coalesced traversal and copy status for a node in src_staging.
type srcStagingRow struct {
	traversal string
	copy      string
}

// BufferKind identifies what the buffer stores (staging status vs node inserts).
type bufferKind int

const (
	bufferKindStaging bufferKind = iota
	bufferKindNodes
)

// writeBuffer is the unified buffer for staging and nodes. Only the data shape and write path differ.
type writeBuffer struct {
	table     string
	db        *DB
	kind      bufferKind
	batchSize int
	interval  time.Duration
	mu        sync.Mutex
	cond      *sync.Cond
	flushing  bool
	slots     chan struct{}
	stopCh    chan struct{}
	onFlush   func(nodeIDs []string)

	// kind-specific storage
	srcRows   map[string]srcStagingRow
	dstRows   map[string]string
	nodes     []*NodeState
}

func newWriteBuffer(db *DB, table string, kind bufferKind) *writeBuffer {
	hardCap := defaultBatchSize * backPressureHardCapMultiple
	wb := &writeBuffer{
		table:     table,
		db:        db,
		kind:      kind,
		batchSize: defaultBatchSize,
		interval:  defaultFlushInterval,
		slots:     make(chan struct{}, hardCap),
		stopCh:    make(chan struct{}),
	}
	wb.cond = sync.NewCond(&wb.mu)
	for i := 0; i < hardCap; i++ {
		wb.slots <- struct{}{}
	}
	if kind == bufferKindStaging {
		if table == "SRC" {
			wb.srcRows = make(map[string]srcStagingRow)
		} else {
			wb.dstRows = make(map[string]string)
		}
	} else {
		wb.nodes = make([]*NodeState, 0, 256)
	}
	go wb.flushLoop()
	return wb
}

func (wb *writeBuffer) flushLoop() {
	ticker := time.NewTicker(wb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-wb.stopCh:
			return
		case <-ticker.C:
			wb.Flush()
		}
	}
}

func (wb *writeBuffer) count() int {
	if wb.kind == bufferKindStaging {
		if wb.table == "SRC" {
			return len(wb.srcRows)
		}
		return len(wb.dstRows)
	}
	return len(wb.nodes)
}

func (wb *writeBuffer) getAndClearIfReady(force bool) (batch any, n int) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	for wb.flushing {
		wb.cond.Wait()
	}
	n = wb.count()
	if n == 0 || (!force && n < wb.batchSize) {
		return nil, 0
	}
	if wb.kind == bufferKindStaging {
		var src map[string]srcStagingRow
		var dst map[string]string
		if wb.table == "SRC" {
			src = wb.srcRows
			wb.srcRows = make(map[string]srcStagingRow)
		} else {
			dst = wb.dstRows
			wb.dstRows = make(map[string]string)
		}
		wb.flushing = true
		wb.cond.Broadcast()
		return stagingBatch{src: src, dst: dst}, n
	}
	nodes := wb.nodes
	wb.nodes = make([]*NodeState, 0, cap(wb.nodes))
	wb.flushing = true
	wb.cond.Broadcast()
	return nodes, n
}

func (wb *writeBuffer) setFlushingDone() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.flushing = false
	wb.cond.Broadcast()
}

func (wb *writeBuffer) runFlush(batch any, n int) []string {
	if wb.kind == bufferKindStaging {
		sb := batch.(stagingBatch)
		err := wb.db.runStagingFlush(wb.table, sb.src, sb.dst)
		if err != nil {
			fmt.Println("error running staging flush", err)
			return nil
		}
		ids := make([]string, 0, n)
		for k := range sb.src {
			ids = append(ids, k)
		}
		for k := range sb.dst {
			ids = append(ids, k)
		}
		return ids
	}
	nodes := batch.([]*NodeState)
	err := wb.db.runNodesFlush(wb.table, nodes)
	if err != nil {
		fmt.Println("error running nodes flush", err)
		return nil
	}
	ids := make([]string, 0, len(nodes))
	for _, nd := range nodes {
		ids = append(ids, nd.ID)
	}
	return ids
}

type stagingBatch struct {
	src map[string]srcStagingRow
	dst map[string]string
}

func (wb *writeBuffer) Flush() {
	batch, n := wb.getAndClearIfReady(false)
	if n == 0 {
		return
	}
	defer wb.setFlushingDone()
	ids := wb.runFlush(batch, n)
	if wb.onFlush != nil {
		wb.onFlush(ids)
	}
	for i := 0; i < n; i++ {
		wb.slots <- struct{}{}
	}
}

func (wb *writeBuffer) ForceFlush() {
	batch, n := wb.getAndClearIfReady(true)
	if n == 0 {
		return
	}
	defer wb.setFlushingDone()
	ids := wb.runFlush(batch, n)
	if wb.onFlush != nil {
		wb.onFlush(ids)
	}
	for i := 0; i < n; i++ {
		wb.slots <- struct{}{}
	}
}

func (wb *writeBuffer) SetOnFlush(fn func(nodeIDs []string)) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.onFlush = fn
}

func (wb *writeBuffer) Stop() {
	close(wb.stopCh)
}

func (wb *writeBuffer) checkFlushAfterAdd(count int) {
	if count >= wb.batchSize {
		wb.Flush()
	}
}

// --- staging add methods ---

func (wb *writeBuffer) addTraversal(nodeID string, depth int, oldTraversal, newTraversal string) {
	hardCap := wb.batchSize * backPressureHardCapMultiple
	<-wb.slots
	wb.mu.Lock()
	for (wb.table == "SRC" && len(wb.srcRows) >= hardCap) || (wb.table == "DST" && len(wb.dstRows) >= hardCap) {
		wb.cond.Wait()
	}
	var count int
	var wasNew bool
	if wb.table == "SRC" {
		r := wb.srcRows[nodeID]
		existed := r.traversal != "" || r.copy != ""
		effectiveOld := oldTraversal
		if existed && r.traversal != "" {
			effectiveOld = r.traversal
		}
		r.traversal = newTraversal
		wb.srcRows[nodeID] = r
		count = len(wb.srcRows)
		wasNew = !existed
		wb.mu.Unlock()
		wb.db.statsDeltas.addTraversalDelta("SRC", depth, effectiveOld, newTraversal)
	} else {
		prev, existed := wb.dstRows[nodeID]
		effectiveOld := oldTraversal
		if existed && prev != "" {
			effectiveOld = prev
		}
		wb.dstRows[nodeID] = newTraversal
		count = len(wb.dstRows)
		wasNew = !existed
		wb.mu.Unlock()
		wb.db.statsDeltas.addTraversalDelta("DST", depth, effectiveOld, newTraversal)
	}
	if !wasNew {
		wb.slots <- struct{}{}
	}
	wb.checkFlushAfterAdd(count)
}

func (wb *writeBuffer) addCopy(nodeID string, depth int, oldCopy, newCopy string) {
	if wb.table != "SRC" {
		return
	}
	hardCap := wb.batchSize * backPressureHardCapMultiple
	<-wb.slots
	wb.mu.Lock()
	for len(wb.srcRows) >= hardCap {
		wb.cond.Wait()
	}
	r := wb.srcRows[nodeID]
	existed := r.traversal != "" || r.copy != ""
	effectiveOld := oldCopy
	if existed && r.copy != "" {
		effectiveOld = r.copy
	}
	r.copy = newCopy
	wb.srcRows[nodeID] = r
	count := len(wb.srcRows)
	wb.mu.Unlock()
	wb.db.statsDeltas.addCopyDelta(depth, effectiveOld, newCopy)
	if existed {
		wb.slots <- struct{}{}
	}
	wb.checkFlushAfterAdd(count)
}

// --- nodes add methods ---

func (wb *writeBuffer) addNode(n *NodeState, _ string) {
	wb.addNodeBatch([]*NodeState{n})
}

func (wb *writeBuffer) addNodeBatch(nodes []*NodeState) {
	if len(nodes) == 0 {
		return
	}
	hardCap := defaultBatchSize * backPressureHardCapMultiple
	for i := 0; i < len(nodes); i++ {
		<-wb.slots
	}
	wb.mu.Lock()
	for len(wb.nodes) >= hardCap {
		wb.cond.Wait()
	}
	wb.nodes = append(wb.nodes, nodes...)
	count := len(wb.nodes)
	wb.mu.Unlock()
	wb.checkFlushAfterAdd(count)
}
