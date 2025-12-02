// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// WriteOperation represents a buffered database write operation.
// All operations must implement Execute() to perform the actual DB write,
// and Key() to enable duplicate detection and coalescing.
type WriteOperation interface {
	Execute(tx *bolt.Tx) error
	Key() string // Returns a unique key for duplicate detection (typically path hash)
}

// StatusUpdateOperation represents a node status transition (e.g., pending → successful).
type StatusUpdateOperation struct {
	QueueType string
	Level     int
	OldStatus string
	NewStatus string
	Path      string
}

// Key returns the path hash as the unique key for this operation.
func (op *StatusUpdateOperation) Key() string {
	return HashPath(op.Path)
}

// Execute performs the status update within a transaction.
func (op *StatusUpdateOperation) Execute(tx *bolt.Tx) error {
	return UpdateNodeStatusInTx(tx, op.QueueType, op.Level, op.OldStatus, op.NewStatus, op.Path)
}

// BatchInsertOperation represents a batch of child node insertions.
type BatchInsertOperation struct {
	Operations []InsertOperation
}

// Key returns a composite key for batch operations (not used for coalescing, but required by interface).
func (op *BatchInsertOperation) Key() string {
	// Batch operations are not coalesced by key - they're merged by path hash during flush
	return fmt.Sprintf("batch_%d", len(op.Operations))
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
	Path          string
	NewCopyStatus string
}

// Key returns the path hash as the unique key for this operation.
func (op *CopyStatusOperation) Key() string {
	return HashPath(op.Path)
}

// Execute performs the copy status update within a transaction.
func (op *CopyStatusOperation) Execute(tx *bolt.Tx) error {
	pathHash := []byte(HashPath(op.Path))
	nodesBucket := GetNodesBucket(tx, op.QueueType)
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
	}

	nodeData := nodesBucket.Get(pathHash)
	if nodeData == nil {
		return fmt.Errorf("node not found: %s", op.Path)
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

	if err := nodesBucket.Put(pathHash, updatedData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
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
func (ob *OutputBuffer) AddStatusUpdate(queueType string, level int, oldStatus, newStatus, path string) {
	op := &StatusUpdateOperation{
		QueueType: queueType,
		Level:     level,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Path:      path,
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
func (ob *OutputBuffer) AddCopyStatusUpdate(queueType string, level int, status, path, newCopyStatus string) {
	op := &CopyStatusOperation{
		QueueType:     queueType,
		Level:         level,
		Status:        status,
		Path:          path,
		NewCopyStatus: newCopyStatus,
	}
	ob.Add(op)
}

// Add adds a write operation to the buffer. If batch size is reached, it triggers a flush.
func (ob *OutputBuffer) Add(op WriteOperation) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// Coalesce duplicates: for status updates and copy status, last write wins
	key := op.Key()
	if _, ok := op.(*StatusUpdateOperation); ok {
		// Remove any existing status update for the same path
		for i := len(ob.operations) - 1; i >= 0; i-- {
			if existing, ok := ob.operations[i].(*StatusUpdateOperation); ok && existing.Key() == key {
				ob.operations = append(ob.operations[:i], ob.operations[i+1:]...)
				break
			}
		}
	} else if _, ok := op.(*CopyStatusOperation); ok {
		// Remove any existing copy status update for the same path
		for i := len(ob.operations) - 1; i >= 0; i-- {
			if existing, ok := ob.operations[i].(*CopyStatusOperation); ok && existing.Key() == key {
				ob.operations = append(ob.operations[:i], ob.operations[i+1:]...)
				break
			}
		}
	} else if batchOp, ok := op.(*BatchInsertOperation); ok {
		// For batch inserts, merge by path hash within the batch
		ob.mergeBatchInsert(batchOp)
		return // mergeBatchInsert handles adding to operations
	}

	ob.operations = append(ob.operations, op)
	shouldFlush := len(ob.operations) >= ob.batchSize

	if shouldFlush {
		ob.mu.Unlock()
		ob.Flush()
		ob.mu.Lock()
	}
}

// mergeBatchInsert merges a new batch insert operation with existing batch inserts,
// coalescing duplicate path hashes (last insert wins).
func (ob *OutputBuffer) mergeBatchInsert(newBatch *BatchInsertOperation) {
	// Build a map of path hashes from existing batch inserts
	existingPaths := make(map[string]*InsertOperation)
	for _, existingOp := range ob.operations {
		if batch, ok := existingOp.(*BatchInsertOperation); ok {
			for _, insertOp := range batch.Operations {
				pathHash := HashPath(insertOp.State.Path)
				existingPaths[pathHash] = &insertOp
			}
		}
	}

	// Merge new batch: overwrite existing paths, add new ones
	for _, newOp := range newBatch.Operations {
		pathHash := HashPath(newOp.State.Path)
		existingPaths[pathHash] = &newOp
	}

	// Remove all existing batch insert operations
	filtered := make([]WriteOperation, 0, len(ob.operations))
	for _, op := range ob.operations {
		if _, ok := op.(*BatchInsertOperation); !ok {
			filtered = append(filtered, op)
		}
	}
	ob.operations = filtered

	// Create merged batch insert with all unique operations
	mergedOps := make([]InsertOperation, 0, len(existingPaths))
	for _, op := range existingPaths {
		mergedOps = append(mergedOps, *op)
	}

	if len(mergedOps) > 0 {
		mergedBatch := &BatchInsertOperation{
			Operations: mergedOps,
		}
		ob.operations = append(ob.operations, mergedBatch)
	}

	shouldFlush := len(ob.operations) >= ob.batchSize
	if shouldFlush {
		ob.mu.Unlock()
		ob.Flush()
		ob.mu.Lock()
	}
}

// Flush writes all buffered operations to BoltDB in a single transaction.
// This is synchronous and blocks until the flush completes.
func (ob *OutputBuffer) Flush() {
	ob.mu.Lock()
	if len(ob.operations) == 0 {
		ob.mu.Unlock()
		return
	}

	// Take snapshot and clear buffer
	batch := make([]WriteOperation, len(ob.operations))
	copy(batch, ob.operations)
	ob.operations = make([]WriteOperation, 0, ob.batchSize)
	ob.mu.Unlock()

	// Execute all operations in a single transaction
	err := ob.db.Update(func(tx *bolt.Tx) error {
		// Ensure stats bucket exists
		if _, err := getStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		// Compute stats deltas BEFORE executing writes (check what exists first)
		statsDeltas := computeStatsDeltas(tx, batch)

		// Execute all data operations
		for _, op := range batch {
			if err := op.Execute(tx); err != nil {
				return fmt.Errorf("failed to execute operation: %w", err)
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
		// Log error but don't fail - operations will be retried on next flush
		fmt.Printf("Error flushing output buffer: %v\n", err)
		// Re-add operations to buffer for retry (only if not a critical error)
		// For now, we'll let them be lost on error - in production might want retry logic
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
			// Check if old status bucket has this entry
			pathHash := []byte(HashPath(v.Path))
			oldBucket := GetStatusBucket(tx, v.QueueType, v.Level, v.OldStatus)
			if oldBucket != nil && oldBucket.Get(pathHash) != nil {
				oldKey := fmt.Sprintf("%s/%d/%s", v.QueueType, v.Level, v.OldStatus)
				oldStatusCounts[oldKey]++
			}

			// Check if new status bucket already has this entry
			newBucket := GetStatusBucket(tx, v.QueueType, v.Level, v.NewStatus)
			if newBucket == nil || newBucket.Get(pathHash) == nil {
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
