// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// FlushTrigger indicates why a flush was triggered. Used for adaptive tuning.
type FlushTrigger int

const (
	TriggerForce FlushTrigger = iota // Explicit flush (pull, advance, pause, stop)
	TriggerTimer                     // Time-based flush from flushLoop
	TriggerSize                      // Buffer reached batchSize in Add/AddMultiple
)

const (
	maxFlushInterval = 60 * time.Second
	maxBatchSize     = 20_000
)

// WriteOperation represents a buffered database write operation.
// All operations must implement Execute() to perform the actual DB write.
type WriteOperation interface {
	Execute(tx *bolt.Tx) error
}

// StatusUpdateOperation represents a node status transition (e.g., pending → successful).
type StatusUpdateOperation struct {
	QueueType string
	Level     int
	OldStatus string
	NewStatus string
	NodeID    string // ULID of the node
}

// Execute performs the status update within a transaction.
func (op *StatusUpdateOperation) Execute(tx *bolt.Tx) error {
	return UpdateNodeStatusInTxByID(tx, op.QueueType, op.Level, op.OldStatus, op.NewStatus, []byte(op.NodeID))
}

// BatchInsertOperation represents a batch of child node insertions.
type BatchInsertOperation struct {
	Operations []InsertOperation
}

// Execute performs the batch insert within a transaction.
func (op *BatchInsertOperation) Execute(tx *bolt.Tx) error {
	return BatchInsertNodesInTx(tx, op.Operations)
}

// CopyStatusOperation represents a copy status update (for copy queue).
// It updates both the node metadata and moves the node between copy status buckets.
type CopyStatusOperation struct {
	QueueType     string
	Level         int
	NodeType      string // "file" or "folder" (for stats delta without bucket lookup)
	OldCopyStatus string // Old copy status (for bucket transition)
	NewCopyStatus string
	NodeID        string // ULID of the node
}

// Execute performs the copy status update within a transaction.
// Updates node metadata and moves node between copy status buckets.
func (op *CopyStatusOperation) Execute(tx *bolt.Tx) error {
	nodeID := []byte(op.NodeID)

	// Only SRC nodes have copy status
	if op.QueueType != BucketSrc {
		return fmt.Errorf("copy status only applies to SRC nodes")
	}

	nodesBucket := GetNodesBucket(tx, op.QueueType, op.Level)
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
	}

	nodeData := nodesBucket.Get(nodeID)
	if nodeData == nil {
		return fmt.Errorf("node not found: %s", op.NodeID)
	}

	ns, err := DeserializeNodeState(nodeData)
	if err != nil {
		return fmt.Errorf("failed to deserialize node state: %w", err)
	}

	ns.CopyStatus = op.NewCopyStatus

	updatedData, err := ns.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	if err := nodesBucket.Put(nodeID, updatedData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	// Update copy status bucket membership if status actually changed
	// Use op.OldCopyStatus for bucket operations (what we know the node was in)
	// but check oldStatus != op.NewCopyStatus to avoid unnecessary updates
	if op.OldCopyStatus != "" && op.OldCopyStatus != op.NewCopyStatus {
		// Use op.NodeType for bucket routing (set at queue time to avoid bucket lookup)
		nodeType := NodeTypeFile
		if op.NodeType == "folder" {
			nodeType = NodeTypeFolder
		}

		// Delete from old copy status bucket
		oldBucket := GetCopyStatusBucket(tx, op.Level, nodeType, op.OldCopyStatus)
		if oldBucket != nil {
			if err := oldBucket.Delete(nodeID); err != nil {
				fmt.Printf("[Buffer Execute] ERROR: Failed to delete from old bucket: %v\n", err)
			}
		} else {
			fmt.Printf("[Buffer Execute] WARNING: Old bucket not found for level=%d, nodeType=%s, status=%s\n",
				op.Level, nodeType, op.OldCopyStatus)
		}

		// Add to new copy status bucket
		newBucket, err := GetOrCreateCopyStatusBucket(tx, op.Level, nodeType, op.NewCopyStatus)
		if err != nil {
			return fmt.Errorf("failed to get new copy status bucket: %w", err)
		}

		if err := newBucket.Put(nodeID, []byte{}); err != nil {
			return fmt.Errorf("failed to add to new copy status bucket: %w", err)
		}

		// Update copy status-lookup index (no node type needed in lookup)
		if err := UpdateCopyStatusLookup(tx, op.Level, nodeID, op.NewCopyStatus); err != nil {
			return fmt.Errorf("failed to update copy status-lookup: %w", err)
		}
	}

	return nil
}

// SetCompletedCountOperation sets the queue's total completed count in the stats bucket.
// The value is the current in-memory counter from the queue (success + final-failure count).
type SetCompletedCountOperation struct {
	QueueType string
	Value     int64
}

// Execute writes the completed count to the stats bucket.
func (op *SetCompletedCountOperation) Execute(tx *bolt.Tx) error {
	return SetCompletedCountInTx(tx, op.QueueType, op.Value)
}

// ExclusionUpdateOperation represents an exclusion state update for a node.
type ExclusionUpdateOperation struct {
	QueueType         string
	Level             int    // Level shard containing the node
	NodeID            string // ULID of the node
	InheritedExcluded bool
}

// Execute performs the exclusion state update within a transaction.
func (op *ExclusionUpdateOperation) Execute(tx *bolt.Tx) error {
	nodeID := []byte(op.NodeID)

	nodesBucket := GetNodesBucket(tx, op.QueueType, op.Level)
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
	}

	nodeData := nodesBucket.Get(nodeID)
	if nodeData == nil {
		return fmt.Errorf("node not found: %s", op.NodeID)
	}

	ns, err := DeserializeNodeState(nodeData)
	if err != nil {
		return fmt.Errorf("failed to deserialize node state: %w", err)
	}

	ns.InheritedExcluded = op.InheritedExcluded

	updatedData, err := ns.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	if err := nodesBucket.Put(nodeID, updatedData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	return nil
}

// ExclusionHoldingRemoveOperation represents removal of a ULID from exclusion-holding bucket.
type ExclusionHoldingRemoveOperation struct {
	QueueType string
	NodeID    string // ULID of the node
}

// Execute performs the removal from exclusion-holding bucket within a transaction.
func (op *ExclusionHoldingRemoveOperation) Execute(tx *bolt.Tx) error {
	holdingBucket := GetExclusionHoldingBucket(tx, op.QueueType)
	if holdingBucket == nil {
		return nil // Bucket doesn't exist, nothing to remove
	}

	return holdingBucket.Delete([]byte(op.NodeID))
}

// ExclusionHoldingAddOperation represents addition of a ULID to exclusion-holding bucket.
type ExclusionHoldingAddOperation struct {
	QueueType string
	NodeID    string // ULID of the node
	Depth     int
}

// Execute performs the addition to exclusion-holding bucket within a transaction.
func (op *ExclusionHoldingAddOperation) Execute(tx *bolt.Tx) error {
	holdingBucket, err := GetOrCreateExclusionHoldingBucket(tx, op.QueueType)
	if err != nil {
		return err
	}

	// Encode depth level as 8 bytes, big-endian
	depthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(depthBytes, uint64(op.Depth))

	return holdingBucket.Put([]byte(op.NodeID), depthBytes)
}

// NodeDeletionOperation represents deletion of a node from all relevant buckets.
type NodeDeletionOperation struct {
	QueueType string
	NodeID    string // ULID of the node
	Level     int
	Status    string // Current status before deletion (to determine which status bucket to update)
}

// Execute performs the node deletion within a transaction.
// Deletes from: nodes bucket, status bucket, status-lookup bucket, and removes from parent's children list.
func (op *NodeDeletionOperation) Execute(tx *bolt.Tx) error {
	nodeID := []byte(op.NodeID)

	// Get node state to determine parent ID
	nodesBucket := GetNodesBucket(tx, op.QueueType, op.Level)
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
	}

	nodeData := nodesBucket.Get(nodeID)
	if nodeData == nil {
		// Node already deleted, skip
		return nil
	}

	// Deserialize to get parent ID
	ns, err := DeserializeNodeState(nodeData)
	if err != nil {
		return fmt.Errorf("failed to deserialize node state: %w", err)
	}

	// 1. Delete from nodes bucket
	if err := nodesBucket.Delete(nodeID); err != nil {
		return fmt.Errorf("failed to delete from nodes bucket: %w", err)
	}

	// 2. Delete from status bucket
	statusBucket := GetStatusBucket(tx, op.QueueType, op.Level, op.Status)
	if statusBucket != nil {
		statusBucket.Delete(nodeID) // Ignore errors - node may not be in this bucket
	}

	// 3. Delete from status-lookup bucket
	lookupBucket := GetStatusLookupBucket(tx, op.QueueType, op.Level)
	if lookupBucket != nil {
		lookupBucket.Delete(nodeID) // Ignore errors
	}

	// 4. Remove from parent's children list (parent is at level op.Level-1)
	if ns.ParentID != "" {
		parentLevel := op.Level - 1
		if parentLevel < 0 {
			parentLevel = 0
		}
		childrenBucket := GetChildrenBucket(tx, op.QueueType, parentLevel)
		if childrenBucket != nil {
			parentID := []byte(ns.ParentID)
			childrenData := childrenBucket.Get(parentID)
			if childrenData != nil {
				var children []string
				if err := json.Unmarshal(childrenData, &children); err == nil {
					// Remove this child's ULID
					filtered := make([]string, 0, len(children))
					for _, c := range children {
						if c != op.NodeID {
							filtered = append(filtered, c)
						}
					}

					// Save updated list
					if len(filtered) > 0 {
						updatedData, err := json.Marshal(filtered)
						if err == nil {
							childrenBucket.Put(parentID, updatedData)
						}
					} else {
						// No children left, remove entry
						childrenBucket.Delete(parentID)
					}
				}
			}
		}
	}

	return nil
}

// LookupMappingOperation represents creation of a bidirectional lookup mapping between SRC and DST nodes at a given level.
type LookupMappingOperation struct {
	Level int    // Level shard for src-to-dst and dst-to-src buckets
	SrcID string // ULID of the SRC node
	DstID string // ULID of the DST node
}

// Execute performs the lookup mapping creation within a transaction.
func (op *LookupMappingOperation) Execute(tx *bolt.Tx) error {
	if op.SrcID == "" || op.DstID == "" {
		return nil // Skip if either ID is empty
	}

	// Store DST→SRC mapping
	dstToSrcBucket, err := GetOrCreateDstToSrcBucket(tx, op.Level)
	if err != nil {
		return fmt.Errorf("failed to get dst-to-src bucket: %w", err)
	}
	if err := dstToSrcBucket.Put([]byte(op.DstID), []byte(op.SrcID)); err != nil {
		return fmt.Errorf("failed to store dst-to-src mapping: %w", err)
	}

	// Store SRC→DST mapping
	srcToDstBucket, err := GetOrCreateSrcToDstBucket(tx, op.Level)
	if err != nil {
		return fmt.Errorf("failed to get src-to-dst bucket: %w", err)
	}
	if err := srcToDstBucket.Put([]byte(op.SrcID), []byte(op.DstID)); err != nil {
		return fmt.Errorf("failed to store src-to-dst mapping: %w", err)
	}

	return nil
}

// OutputBuffer batches write operations for efficient database writes.
// It supports three flush triggers: forced, size threshold, and time-based.
// Adaptive tuning: after timer-triggered flushes the interval doubles (cap 60s);
// after size-triggered flushes batchSize increases by 50% (cap 100K).
// Backpressure: workers block when buffer reaches maxSize (2x batchSize) and resume when it drains.
type OutputBuffer struct {
	db          *DB
	mu          sync.Mutex
	cond        *sync.Cond   // Condition variable for backpressure signaling
	operations  []WriteOperation
	batchSize   int
	maxSize     int          // Backpressure threshold (2 * batchSize)
	resumeSize  int          // Resume threshold (batchSize)
	flushInterval time.Duration // Current time-based flush interval (adaptive, cap 60s)
	stopChan    chan struct{}
	wg          sync.WaitGroup
	paused      bool
	stopOnce    sync.Once // Ensures Stop() is idempotent
	onFlush     func([]string)
	// getCompletedCount is called at flush time to push the queue's current completed count into the batch.
	getCompletedCount func() (queueType string, value int64)
}

// NewOutputBuffer creates a new output buffer that will flush every N operations or every interval.
// Backpressure is applied when buffer reaches 2x batchSize; workers resume when it drains to batchSize.
// Interval and batchSize adapt over time (timer: double interval up to 60s; size: +50% batch up to 100K).
func NewOutputBuffer(db *DB, batchSize int, flushInterval time.Duration) *OutputBuffer {
	ob := &OutputBuffer{
		db:             db,
		operations:     make([]WriteOperation, 0, batchSize),
		batchSize:      batchSize,
		maxSize:        2 * batchSize, // Backpressure threshold
		resumeSize:     batchSize,     // Resume threshold
		flushInterval:  flushInterval,
		stopChan:       make(chan struct{}),
		paused:         false,
	}
	ob.cond = sync.NewCond(&ob.mu)

	ob.wg.Add(1)
	go ob.flushLoop()

	return ob
}

// SetOnFlush registers a callback invoked after a successful flush with the list of flushed node IDs.
// The callback runs on the flush caller's goroutine.
func (ob *OutputBuffer) SetOnFlush(handler func([]string)) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.onFlush = handler
}

// SetOnCompletedCountGetter registers a getter that returns (queueType, completedCount) for this queue.
// When Flush runs, the getter is called (without holding the buffer lock) and the value is written to the stats bucket.
func (ob *OutputBuffer) SetOnCompletedCountGetter(getter func() (queueType string, value int64)) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.getCompletedCount = getter
}

// AddStatusUpdate adds a status update operation to the buffer.
func (ob *OutputBuffer) AddStatusUpdate(queueType string, level int, oldStatus, newStatus, nodeID string) {
	op := &StatusUpdateOperation{
		QueueType: queueType,
		Level:     level,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		NodeID:    nodeID,
	}
	ob.Add(op)
}

// AddBatchInsert adds a batch insert operation to the buffer.
func (ob *OutputBuffer) AddBatchInsert(operations []InsertOperation) {
	if len(operations) == 0 {
		return
	}
	op := &BatchInsertOperation{
		Operations: operations,
	}
	ob.Add(op)
}

// AddCreateNode adds a single node insert operation to the buffer.
// This is a convenience method for inserting a single node without needing to create a batch.
func (ob *OutputBuffer) AddCreateNode(queueType string, level int, status string, nodeState *NodeState) {
	if nodeState == nil {
		return
	}
	op := &BatchInsertOperation{
		Operations: []InsertOperation{
			{
				QueueType: queueType,
				Level:     level,
				Status:    status,
				State:     nodeState,
			},
		},
	}
	ob.Add(op)
}

// AddCopyStatusUpdate adds a copy status update operation to the buffer.
// nodeType is "file" or "folder" so stats deltas can be computed without bucket lookup.
func (ob *OutputBuffer) AddCopyStatusUpdate(queueType string, level int, nodeType, oldCopyStatus, nodeID, newCopyStatus string) {
	op := &CopyStatusOperation{
		QueueType:     queueType,
		Level:         level,
		NodeType:      nodeType,
		OldCopyStatus: oldCopyStatus,
		NodeID:        nodeID,
		NewCopyStatus: newCopyStatus,
	}
	ob.Add(op)
}

// AddExclusionUpdate adds an exclusion state update operation to the buffer.
func (ob *OutputBuffer) AddExclusionUpdate(queueType string, level int, nodeID string, inheritedExcluded bool) {
	op := &ExclusionUpdateOperation{
		QueueType:         queueType,
		Level:             level,
		NodeID:            nodeID,
		InheritedExcluded: inheritedExcluded,
	}
	ob.Add(op)
}

// AddExclusionHoldingRemove adds a removal from exclusion-holding bucket operation to the buffer.
func (ob *OutputBuffer) AddExclusionHoldingRemove(queueType string, nodeID string) {
	op := &ExclusionHoldingRemoveOperation{
		QueueType: queueType,
		NodeID:    nodeID,
	}
	ob.Add(op)
}

// AddExclusionHoldingAdd adds an addition to exclusion-holding bucket operation to the buffer.
func (ob *OutputBuffer) AddExclusionHoldingAdd(queueType string, nodeID string, depth int) {
	op := &ExclusionHoldingAddOperation{
		QueueType: queueType,
		NodeID:    nodeID,
		Depth:     depth,
	}
	ob.Add(op)
}

// AddNodeDeletion adds a node deletion operation to the buffer.
func (ob *OutputBuffer) AddNodeDeletion(queueType string, nodeID string, level int, status string) {
	op := &NodeDeletionOperation{
		QueueType: queueType,
		NodeID:    nodeID,
		Level:     level,
		Status:    status,
	}
	ob.Add(op)
}

// AddLookupMapping adds a bidirectional lookup mapping operation to the buffer.
func (ob *OutputBuffer) AddLookupMapping(level int, srcID, dstID string) {
	if srcID == "" || dstID == "" {
		return // Skip if either ID is empty
	}
	op := &LookupMappingOperation{
		Level: level,
		SrcID: srcID,
		DstID: dstID,
	}
	ob.Add(op)
}

// AddMultiple adds multiple operations to the buffer atomically.
// This prevents flushes from happening between related operations (e.g., status update + batch insert).
// Blocks if buffer is at or above maxSize (backpressure) until buffer is drained.
// All operations are added before checking if a flush is needed.
func (ob *OutputBuffer) AddMultiple(ops []WriteOperation) {
	if len(ops) == 0 {
		return
	}

	ob.mu.Lock()
	// Block if buffer is at or above maxSize (backpressure)
	for len(ob.operations) >= ob.maxSize {
		ob.cond.Wait()
	}
	// Add all operations at once
	ob.operations = append(ob.operations, ops...)
	shouldFlush := len(ob.operations) >= ob.batchSize
	ob.mu.Unlock()

	if shouldFlush {
		ob.flushWithTrigger(TriggerSize)
	}
}

// Add adds a write operation to the buffer. If batch size is reached, it triggers a flush.
// Blocks if buffer is at or above maxSize (backpressure) until buffer is drained.
// No deduplication happens here - that's done per-bucket during flush.
func (ob *OutputBuffer) Add(op WriteOperation) {
	ob.mu.Lock()
	// Block if buffer is at or above maxSize (backpressure)
	for len(ob.operations) >= ob.maxSize {
		ob.cond.Wait()
	}
	ob.operations = append(ob.operations, op)
	shouldFlush := len(ob.operations) >= ob.batchSize
	ob.mu.Unlock()

	if shouldFlush {
		ob.flushWithTrigger(TriggerSize)
	}
}

// Flush writes all buffered operations to BoltDB in a single transaction (force trigger; no adaptation).
func (ob *OutputBuffer) Flush() []string {
	return ob.flushWithTrigger(TriggerForce)
}

// flushWithTrigger performs the flush and applies adaptive tuning based on trigger (Timer or Size).
// Force trigger does not change interval or batch size.
// Operations are executed in the order they were added to the buffer.
// This is synchronous and blocks until the flush completes.
// Holds the lock during the entire transaction to prevent other goroutines
// from adding operations to the buffer while the transaction is executing.
// Returns the list of node IDs that had completion-affecting writes flushed.
func (ob *OutputBuffer) flushWithTrigger(trigger FlushTrigger) []string {
	// Get completed-count op without holding ob.mu so the getter can acquire the queue lock (avoids deadlock).
	ob.mu.Lock()
	getter := ob.getCompletedCount
	ob.mu.Unlock()
	var completedOp WriteOperation
	if getter != nil {
		queueType, value := getter()
		completedOp = &SetCompletedCountOperation{QueueType: queueType, Value: value}
	}

	ob.mu.Lock()

	if len(ob.operations) == 0 {
		ob.mu.Unlock()
		return nil
	}

	// Take snapshot and clear buffer
	batch := make([]WriteOperation, len(ob.operations))
	copy(batch, ob.operations)
	ob.operations = make([]WriteOperation, 0, ob.batchSize)

	// Prepend completed count so it's written with this batch
	if completedOp != nil {
		batch = append([]WriteOperation{completedOp}, batch...)
	}

	// Wake any workers blocked by backpressure (buffer is now empty, below resumeSize)
	ob.cond.Broadcast()

	handler := ob.onFlush

	// Flushing operations to BoltDB
	var flushedIDs []string
	err := ob.db.Update(func(tx *bolt.Tx) error {
		// Ensure stats bucket exists
		if _, err := getStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		// Compute stats deltas BEFORE executing writes (check what exists first)
		statsDeltas := computeStatsDeltas(tx, batch)
		flushedIDs = computeFlushedNodeIDs(tx, batch)

		// Execute all operations in order
		for i, op := range batch {
			if err := op.Execute(tx); err != nil {
				opType := fmt.Sprintf("%T", op)
				return fmt.Errorf("failed to execute operation %d of %d (type: %s): %w", i+1, len(batch), opType, err)
			}
		}

		// Apply all stats updates in one batch
		for bucketPathStr, delta := range statsDeltas {
			// Convert string path back to []string for UpdateBucketStatsInTx
			bucketPath := strings.Split(bucketPathStr, "/")
			if err := UpdateBucketStatsInTx(tx, bucketPath, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", bucketPathStr, err)
			}
		}

		return nil
	})

	if err != nil {
		// Log error with details
		fmt.Printf("ERROR flushing output buffer (%d operations): %v\n", len(batch), err)
		// Re-add operations to buffer for retry
		ob.operations = append(ob.operations, batch...)
		ob.mu.Unlock()
		return nil
	}

	// Adaptive tuning after successful flush (only when we actually flushed ops)
	switch trigger {
	case TriggerTimer:
		ob.maybeIncreaseTimerInterval()
	case TriggerSize:
		ob.maybeIncreaseBatchSize()
	}

	ob.mu.Unlock()

	if len(flushedIDs) > 0 && handler != nil {
		handler(flushedIDs)
	}

	return flushedIDs
}

// getFlushInterval returns the current time-based flush interval (caller must not hold ob.mu).
func (ob *OutputBuffer) getFlushInterval() time.Duration {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return ob.flushInterval
}

// maybeIncreaseTimerInterval doubles the flush interval, cap 60s. Call with ob.mu held (e.g. from flushWithTrigger).
func (ob *OutputBuffer) maybeIncreaseTimerInterval() {
	newInterval := ob.flushInterval * 2
	if newInterval > maxFlushInterval {
		newInterval = maxFlushInterval
	}
	if newInterval > ob.flushInterval {
		ob.flushInterval = newInterval
	}
}

// maybeIncreaseBatchSize increases batch size by 50%, cap 100K; updates maxSize and resumeSize. Call with ob.mu held.
func (ob *OutputBuffer) maybeIncreaseBatchSize() {
	newBatch := ob.batchSize * 3 / 2
	if newBatch > maxBatchSize {
		newBatch = maxBatchSize
	}
	if newBatch > ob.batchSize {
		ob.batchSize = newBatch
		ob.maxSize = 2 * ob.batchSize
		ob.resumeSize = ob.batchSize
	}
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
// Uses time.After(flushInterval) so the interval can adapt (timer-triggered flushes double it, cap 60s).
func (ob *OutputBuffer) flushLoop() {
	defer ob.wg.Done()

	for {
		interval := ob.getFlushInterval()
		select {
		case <-time.After(interval):
			ob.mu.Lock()
			paused := ob.paused
			ob.mu.Unlock()
			if !paused {
				ob.flushWithTrigger(TriggerTimer)
			}
		case <-ob.stopChan:
			ob.Flush() // Final flush before stopping (force, no adaptation)
			return
		}
	}
}

// Pause pauses the buffer (stops time-based flushing).
// Force-flushes before pausing to ensure state is persisted.
func (ob *OutputBuffer) Pause() {
	ob.Flush() // Force flush before pausing
	ob.mu.Lock()
	ob.paused = true
	ob.mu.Unlock()
}

// Resume resumes the buffer (resumes time-based flushing).
func (ob *OutputBuffer) Resume() {
	ob.mu.Lock()
	ob.paused = false
	ob.mu.Unlock()
}

// computeStatsDeltas analyzes all operations and computes stats deltas in batch.
// Returns a map of bucket path (as string) -> delta count.
// Groups operations by type and computes deltas efficiently.
func computeStatsDeltas(tx *bolt.Tx, operations []WriteOperation) map[string]int64 {
	deltas := make(map[string]int64)

	// Collect all status updates first - group by old status and new status
	oldStatusCounts := make(map[string]int64) // "queueType/level/status" -> count
	newStatusCounts := make(map[string]int64) // "queueType/level/status" -> count
	// Deduplicate status updates within the current batch to prevent stats overcount
	seenOldStatus := make(map[string]struct{}) // "queueType/level/status/nodeID"
	seenNewStatus := make(map[string]struct{}) // "queueType/level/status/nodeID"

	// Track lookup mappings that will be created by BatchInsertOperations in this batch
	// to avoid double-counting in LookupMappingOperation
	batchInsertMappings := make(map[string]bool) // "srcID:dstID" -> true
	seenLookup     := make(map[string]struct{})  // "srcID:dstID" for in-batch dedupe
	seenNodeDel    := make(map[string]struct{})   // "queueType/level/status/nodeID" and "queueType/nodeID" for in-batch dedupe

	// First pass: process BatchInsertOperations and collect their mappings
	for _, op := range operations {
		if batchInsert, ok := op.(*BatchInsertOperation); ok {
			// Compute deltas for batch insert (includes lookup mapping tracking)
			insertDeltas := computeBatchInsertStatsDeltas(tx, batchInsert.Operations)
			for path, delta := range insertDeltas {
				deltas[path] += delta
			}

			// Track which mappings will be created by this batch insert
			for _, insertOp := range batchInsert.Operations {
				if insertOp.QueueType == "DST" && insertOp.State != nil && insertOp.State.SrcID != "" {
					mappingKey := fmt.Sprintf("%s:%s", insertOp.State.SrcID, insertOp.State.ID)
					batchInsertMappings[mappingKey] = true
				}
			}
		}
	}

	// Second pass: process other operations from op fields only (no bucket lookups)
	for _, op := range operations {
		switch v := op.(type) {
		case *StatusUpdateOperation:
			nodeIDStr := v.NodeID
			oldKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.OldStatus)
			newKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.NewStatus)
			oldStatusNodeKey := fmt.Sprintf("%s/%s", oldKey, nodeIDStr)
			newStatusNodeKey := fmt.Sprintf("%s/%s", newKey, nodeIDStr)
			if _, seen := seenOldStatus[oldStatusNodeKey]; !seen {
				oldStatusCounts[oldKey]++
				seenOldStatus[oldStatusNodeKey] = struct{}{}
			}
			if _, seen := seenNewStatus[newStatusNodeKey]; !seen {
				newStatusCounts[newKey]++
				seenNewStatus[newStatusNodeKey] = struct{}{}
			}

		case *BatchInsertOperation:
			// Already processed in first pass, skip

		case *SetCompletedCountOperation:
			// Value is written directly in Execute(); no delta

		case *CopyStatusOperation:
			// Derive deltas from op fields only (NodeType set at queue time); dedupe by (key, nodeID)
			if v.OldCopyStatus != "" && v.OldCopyStatus != v.NewCopyStatus && v.NodeType != "" {
				nodeType := v.NodeType
				if nodeType != "folder" {
					nodeType = "file"
				}
				oldKey := fmt.Sprintf("SRC/%d/copy/%s/%s", v.Level, nodeType, v.OldCopyStatus)
				newKey := fmt.Sprintf("SRC/%d/copy/%s/%s", v.Level, nodeType, v.NewCopyStatus)
				oldCopyNodeKey := oldKey + "/" + v.NodeID
				newCopyNodeKey := newKey + "/" + v.NodeID
				if _, seen := seenOldStatus[oldCopyNodeKey]; !seen {
					oldStatusCounts[oldKey]++
					seenOldStatus[oldCopyNodeKey] = struct{}{}
				}
				if _, seen := seenNewStatus[newCopyNodeKey]; !seen {
					newStatusCounts[newKey]++
					seenNewStatus[newCopyNodeKey] = struct{}{}
				}
			}

		case *ExclusionUpdateOperation:
			// Exclusion updates don't change bucket counts, just node metadata
			// No stats updates needed

		case *ExclusionHoldingRemoveOperation, *ExclusionHoldingAddOperation:
			// Exclusion-holding bucket operations don't affect stats
			// No stats updates needed

		case *LookupMappingOperation:
			mappingKey := fmt.Sprintf("%s:%s", v.SrcID, v.DstID)
			if batchInsertMappings[mappingKey] {
				continue
			}
			if _, seen := seenLookup[mappingKey]; seen {
				continue
			}
			seenLookup[mappingKey] = struct{}{}
			srcToDstPathStr := strings.Join(GetSrcToDstBucketPath(v.Level), "/")
			dstToSrcPathStr := strings.Join(GetDstToSrcBucketPath(v.Level), "/")
			deltas[srcToDstPathStr]++
			deltas[dstToSrcPathStr]++

		case *NodeDeletionOperation:
			statusNodeKey := fmt.Sprintf("%s/%d/%s/%s", v.QueueType, v.Level, v.Status, v.NodeID)
			if _, seen := seenNodeDel[statusNodeKey]; !seen {
				seenNodeDel[statusNodeKey] = struct{}{}
				statusKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.Status)
				oldStatusCounts[statusKey]++
			}
			nodesNodeKey := v.QueueType + "/" + v.NodeID
			if _, seen := seenNodeDel[nodesNodeKey]; !seen {
				seenNodeDel[nodesNodeKey] = struct{}{}
				nodesPath := strings.Join(GetNodesBucketPath(v.QueueType, v.Level), "/")
				deltas[nodesPath]--
			}
		}
	}

	// Convert status update counts to bucket paths and apply deltas
	// Handle both traversal status (3 parts: queueType/level/status) and copy status (5 parts: SRC/level/copy/nodeType/status)
	for statusKey, count := range oldStatusCounts {
		parts := strings.Split(statusKey, "/")
		if len(parts) == 3 {
			// Traversal status bucket: queueType/level/status
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			status := parts[2]
			path := strings.Join(GetStatusBucketPath(queueType, level, status), "/")
			deltas[path] -= count // Subtract from old status
		} else if len(parts) == 5 && parts[2] == "copy" {
			// Copy status bucket: SRC/level/copy/nodeType/status
			level, _ := strconv.Atoi(parts[1])
			nodeType := parts[3]
			status := parts[4]
			path := strings.Join(GetCopyStatusBucketPath(level, nodeType, status), "/")
			deltas[path] -= count // Subtract from old status
		}
	}

	for statusKey, count := range newStatusCounts {
		parts := strings.Split(statusKey, "/")
		if len(parts) == 3 {
			// Traversal status bucket: queueType/level/status
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			status := parts[2]
			path := strings.Join(GetStatusBucketPath(queueType, level, status), "/")
			deltas[path] += count // Add to new status
		} else if len(parts) == 5 && parts[2] == "copy" {
			// Copy status bucket: SRC/level/copy/nodeType/status
			level, _ := strconv.Atoi(parts[1])
			nodeType := parts[3]
			status := parts[4]
			path := strings.Join(GetCopyStatusBucketPath(level, nodeType, status), "/")
			deltas[path] += count // Add to new status
		}
	}

	return deltas
}

// computeFlushedNodeIDs returns the list of node IDs that have completion-affecting
// writes in the current batch. These IDs are safe to release from the leased set
// once the batch is successfully flushed.
func computeFlushedNodeIDs(tx *bolt.Tx, operations []WriteOperation) []string {
	seen := make(map[string]struct{})
	var flushedIDs []string

	for _, op := range operations {
		switch v := op.(type) {
		case *StatusUpdateOperation:
			if v.NodeID == "" {
				continue
			}
			nodeID := []byte(v.NodeID)
			oldBucket := GetStatusBucket(tx, v.QueueType, v.Level, v.OldStatus)
			if oldBucket != nil && oldBucket.Get(nodeID) != nil {
				if _, exists := seen[v.NodeID]; !exists {
					seen[v.NodeID] = struct{}{}
					flushedIDs = append(flushedIDs, v.NodeID)
				}
			}

		case *CopyStatusOperation:
			if v.NodeID == "" {
				continue
			}
			nodesBucket := GetNodesBucket(tx, v.QueueType, v.Level)
			if nodesBucket != nil && nodesBucket.Get([]byte(v.NodeID)) != nil {
				if _, exists := seen[v.NodeID]; !exists {
					seen[v.NodeID] = struct{}{}
					flushedIDs = append(flushedIDs, v.NodeID)
				}
			}
		}
	}

	return flushedIDs
}

// Stop gracefully stops the output buffer and flushes remaining operations.
// Uses a timeout to prevent indefinite blocking if the flush loop is stuck.
// This method is idempotent - it can be called multiple times safely.
func (ob *OutputBuffer) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.stopChan)

		// Wait for flush loop to finish, but with a timeout to prevent hanging
		done := make(chan struct{}, 1)
		go func() {
			ob.wg.Wait()
			done <- struct{}{}
		}()

		select {
		case <-done:
			// Flush loop completed successfully
		case <-time.After(2 * time.Second):
			// Timeout - flush loop may be stuck or slow
			// Continue anyway to prevent blocking the entire shutdown
		}
	})
}
