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

// CopyStatusOperation represents a copy status update (for future copy queue).
type CopyStatusOperation struct {
	QueueType     string
	Level         int
	Status        string
	NodeID        string // ULID of the node
	NewCopyStatus string
}

// Execute performs the copy status update within a transaction.
func (op *CopyStatusOperation) Execute(tx *bolt.Tx) error {
	nodeID := []byte(op.NodeID)

	nodesBucket := GetNodesBucket(tx, op.QueueType)
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
	ns.CopyNeeded = (op.NewCopyStatus == CopyStatusPending)

	updatedData, err := ns.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	if err := nodesBucket.Put(nodeID, updatedData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	return nil
}

// ExclusionUpdateOperation represents an exclusion state update for a node.
type ExclusionUpdateOperation struct {
	QueueType         string
	NodeID            string // ULID of the node
	InheritedExcluded bool
}

// Execute performs the exclusion state update within a transaction.
func (op *ExclusionUpdateOperation) Execute(tx *bolt.Tx) error {
	nodeID := []byte(op.NodeID)

	nodesBucket := GetNodesBucket(tx, op.QueueType)
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
	nodesBucket := GetNodesBucket(tx, op.QueueType)
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

	// 4. Remove from parent's children list
	if ns.ParentID != "" {
		childrenBucket := GetChildrenBucket(tx, op.QueueType)
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

// OutputBuffer batches write operations for efficient database writes.
// It supports three flush triggers: forced, size threshold, and time-based.
type OutputBuffer struct {
	db          *DB
	mu          sync.Mutex
	operations  []WriteOperation
	batchSize   int
	flushTicker *time.Ticker
	stopChan    chan struct{}
	wg          sync.WaitGroup
	paused      bool
}

// NewOutputBuffer creates a new output buffer that will flush every N operations or every interval.
func NewOutputBuffer(db *DB, batchSize int, flushInterval time.Duration) *OutputBuffer {
	ob := &OutputBuffer{
		db:          db,
		operations:  make([]WriteOperation, 0, batchSize),
		batchSize:   batchSize,
		flushTicker: time.NewTicker(flushInterval),
		stopChan:    make(chan struct{}),
		paused:      false,
	}

	ob.wg.Add(1)
	go ob.flushLoop()

	return ob
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

// AddCopyStatusUpdate adds a copy status update operation to the buffer.
func (ob *OutputBuffer) AddCopyStatusUpdate(queueType string, level int, status, nodeID, newCopyStatus string) {
	op := &CopyStatusOperation{
		QueueType:     queueType,
		Level:         level,
		Status:        status,
		NodeID:        nodeID,
		NewCopyStatus: newCopyStatus,
	}
	ob.Add(op)
}

// AddExclusionUpdate adds an exclusion state update operation to the buffer.
func (ob *OutputBuffer) AddExclusionUpdate(queueType string, nodeID string, inheritedExcluded bool) {
	op := &ExclusionUpdateOperation{
		QueueType:         queueType,
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

// Add adds a write operation to the buffer. If batch size is reached, it triggers a flush.
// No deduplication happens here - that's done per-bucket during flush.
func (ob *OutputBuffer) Add(op WriteOperation) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	ob.operations = append(ob.operations, op)
	shouldFlush := len(ob.operations) >= ob.batchSize

	if shouldFlush {
		ob.mu.Unlock()
		ob.Flush()
		ob.mu.Lock()
	}
}

// Flush writes all buffered operations to BoltDB in a single transaction.
// Operations are executed in the order they were added to the buffer.
// This is synchronous and blocks until the flush completes.
// Holds the lock during the entire transaction to prevent other goroutines
// from adding operations to the buffer while the transaction is executing.
// This ensures atomicity: either all operations in the snapshot are written, or none are.
func (ob *OutputBuffer) Flush() {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if len(ob.operations) == 0 {
		return
	}

	// Take snapshot and clear buffer
	batch := make([]WriteOperation, len(ob.operations))
	copy(batch, ob.operations)
	ob.operations = make([]WriteOperation, 0, ob.batchSize)

	// Execute all operations in a single transaction
	// Lock is held during transaction to prevent concurrent additions to buffer
	err := ob.db.Update(func(tx *bolt.Tx) error {
		// Ensure stats bucket exists
		if _, err := getStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		// Compute stats deltas BEFORE executing writes (check what exists first)
		statsDeltas := computeStatsDeltas(tx, batch)

		// Execute all operations in order
		for i, op := range batch {
			if err := op.Execute(tx); err != nil {
				opType := fmt.Sprintf("%T", op)
				return fmt.Errorf("failed to execute operation %d of %d (type: %s): %w", i+1, len(batch), opType, err)
			}
		}

		// Apply all stats updates in one batch
		for bucketPathStr, delta := range statsDeltas {
			// Convert string path back to []string for updateBucketStats
			bucketPath := strings.Split(bucketPathStr, "/")
			if err := updateBucketStats(tx, bucketPath, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", bucketPathStr, err)
			}
		}

		return nil
	})

	if err != nil {
		// Log error with details
		fmt.Printf("ERROR flushing output buffer (%d operations): %v\n", len(batch), err)
		// Re-add operations to buffer for retry
		ob.mu.Lock()
		ob.operations = append(ob.operations, batch...)
		ob.mu.Unlock()
	}
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
func (ob *OutputBuffer) flushLoop() {
	defer ob.wg.Done()

	for {
		select {
		case <-ob.flushTicker.C:
			ob.mu.Lock()
			paused := ob.paused
			ob.mu.Unlock()
			if !paused {
				ob.Flush()
			}
		case <-ob.stopChan:
			ob.flushTicker.Stop()
			ob.Flush() // Final flush before stopping
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

	for _, op := range operations {
		switch v := op.(type) {
		case *StatusUpdateOperation:
			nodeID := []byte(v.NodeID)

			// Check if old status bucket has this entry
			oldBucket := GetStatusBucket(tx, v.QueueType, v.Level, v.OldStatus)
			if oldBucket != nil && oldBucket.Get(nodeID) != nil {
				oldKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.OldStatus)
				oldStatusCounts[oldKey]++
			}

			// Check if new status bucket already has this entry
			newBucket := GetStatusBucket(tx, v.QueueType, v.Level, v.NewStatus)
			if newBucket == nil || newBucket.Get(nodeID) == nil {
				newKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.NewStatus)
				newStatusCounts[newKey]++
			}

		case *BatchInsertOperation:
			// Batch insert: compute deltas for all inserts
			insertDeltas := computeBatchInsertStatsDeltas(tx, v.Operations)
			for path, delta := range insertDeltas {
				deltas[path] += delta
			}

		case *CopyStatusOperation:
			// Copy status updates don't change bucket counts, just node metadata
			// No stats updates needed

		case *ExclusionUpdateOperation:
			// Exclusion updates don't change bucket counts, just node metadata
			// No stats updates needed

		case *ExclusionHoldingRemoveOperation, *ExclusionHoldingAddOperation:
			// Exclusion-holding bucket operations don't affect stats
			// No stats updates needed

		case *NodeDeletionOperation:
			// Node deletion: subtract from status bucket and nodes bucket
			nodeID := []byte(v.NodeID)

			// Check if status bucket has this entry
			statusBucket := GetStatusBucket(tx, v.QueueType, v.Level, v.Status)
			if statusBucket != nil && statusBucket.Get(nodeID) != nil {
				statusKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.Status)
				oldStatusCounts[statusKey]++
			}

			// Always subtract from nodes bucket if node exists
			nodesBucket := GetNodesBucket(tx, v.QueueType)
			if nodesBucket != nil && nodesBucket.Get(nodeID) != nil {
				nodesPath := strings.Join(GetNodesBucketPath(v.QueueType), "/")
				deltas[nodesPath]--
			}
		}
	}

	// Convert status update counts to bucket paths and apply deltas
	for statusKey, count := range oldStatusCounts {
		parts := strings.Split(statusKey, "/")
		if len(parts) == 3 {
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			status := parts[2]
			path := strings.Join(GetStatusBucketPath(queueType, level, status), "/")
			deltas[path] -= count // Subtract from old status
		}
	}

	for statusKey, count := range newStatusCounts {
		parts := strings.Split(statusKey, "/")
		if len(parts) == 3 {
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			status := parts[2]
			path := strings.Join(GetStatusBucketPath(queueType, level, status), "/")
			deltas[path] += count // Add to new status
		}
	}

	return deltas
}

// Stop gracefully stops the output buffer and flushes remaining operations.
// Uses a timeout to prevent indefinite blocking if the flush loop is stuck.
func (ob *OutputBuffer) Stop() {
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
}
