// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// LogBuffer is a lightweight buffer specifically for log entries.
// It batches log inserts for improved performance while keeping the implementation simple.
type LogBuffer struct {
	db          *DB
	mu          sync.Mutex
	entries     []LogEntry
	batchSize   int
	flushTicker *time.Ticker
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// NewLogBuffer creates a new log buffer that will flush every N entries or every interval.
func NewLogBuffer(db *DB, batchSize int, flushInterval time.Duration) *LogBuffer {
	lb := &LogBuffer{
		db:          db,
		entries:     make([]LogEntry, 0, batchSize),
		batchSize:   batchSize,
		flushTicker: time.NewTicker(flushInterval),
		stopChan:    make(chan struct{}),
	}

	lb.wg.Add(1)
	go lb.flushLoop()

	return lb
}

// Add adds a log entry to the buffer. If batch size is reached, it triggers a flush.
func (lb *LogBuffer) Add(entry LogEntry) {
	lb.mu.Lock()
	lb.entries = append(lb.entries, entry)
	shouldFlush := len(lb.entries) >= lb.batchSize
	lb.mu.Unlock()

	if shouldFlush {
		lb.Flush()
	}
}

// Flush writes all buffered entries to BoltDB in a single transaction.
func (lb *LogBuffer) Flush() {
	lb.mu.Lock()
	if len(lb.entries) == 0 {
		lb.mu.Unlock()
		return
	}

	// Take snapshot and clear buffer
	batch := lb.entries
	lb.entries = make([]LogEntry, 0, lb.batchSize)
	lb.mu.Unlock()

	// Execute batch insert to BoltDB in a single transaction
	err := lb.db.Update(func(tx *bolt.Tx) error {
		for _, entry := range batch {
			// Get or create the level-specific bucket
			levelBucket, err := GetOrCreateLogLevelBucket(tx, entry.Level)
			if err != nil {
				return fmt.Errorf("failed to get log level bucket: %w", err)
			}

			// Serialize entry
			value, err := SerializeLogEntry(entry)
			if err != nil {
				return fmt.Errorf("failed to serialize log entry: %w", err)
			}

			// Write to BoltDB using entry ID as key
			if err := levelBucket.Put([]byte(entry.ID), value); err != nil {
				return fmt.Errorf("failed to set log entry: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		// Silently fail - logs are not critical
		fmt.Printf("Error flushing log buffer: %v\n", err)
	}
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
func (lb *LogBuffer) flushLoop() {
	defer lb.wg.Done()

	for {
		select {
		case <-lb.flushTicker.C:
			lb.Flush()
		case <-lb.stopChan:
			lb.flushTicker.Stop()
			lb.Flush() // Final flush before stopping
			return
		}
	}
}

// Stop gracefully stops the log buffer and flushes remaining entries.
func (lb *LogBuffer) Stop() {
	close(lb.stopChan)
	lb.wg.Wait()
}

// WriteItem represents a single database write operation.
type WriteItem struct {
	// Type of operation: "insert", "update_status"
	Type string

	// For insert operations
	QueueType string
	Level     int
	Status    string // The status to use (e.g., "pending")
	State     *NodeState

	// For status update operations
	OldStatus string
	NewStatus string
	Path      string
}

// WriteItems performs bulk database writes in a single transaction.
// This simplifies the queue completion workflow by allowing all writes to be batched together.
func WriteItems(db *DB, items []WriteItem) error {
	if len(items) == 0 {
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
		for _, item := range items {
			switch item.Type {
			case "insert":
				if item.State == nil {
					continue
				}

				// Ensure NodeState has the status field populated
				if item.State.TraversalStatus == "" {
					item.State.TraversalStatus = item.Status
				}

				pathHash := []byte(HashPath(item.State.Path))
				parentHash := []byte(HashPath(item.State.ParentPath))

				// 1. Insert into nodes bucket
				nodesBucket := GetNodesBucket(tx, item.QueueType)
				if nodesBucket == nil {
					return fmt.Errorf("nodes bucket not found for %s", item.QueueType)
				}

				nodeData, err := item.State.Serialize()
				if err != nil {
					return fmt.Errorf("failed to serialize node state: %w", err)
				}

				if err := nodesBucket.Put(pathHash, nodeData); err != nil {
					return fmt.Errorf("failed to insert node: %w", err)
				}

				// 2. Add to status bucket
				statusBucket, err := GetOrCreateStatusBucket(tx, item.QueueType, item.Level, item.Status)
				if err != nil {
					return fmt.Errorf("failed to get status bucket: %w", err)
				}

				if err := statusBucket.Put(pathHash, []byte{}); err != nil {
					return fmt.Errorf("failed to add to status bucket: %w", err)
				}

				// 3. Update children index
				if item.State.ParentPath != "" {
					childrenBucket := GetChildrenBucket(tx, item.QueueType)
					if childrenBucket == nil {
						return fmt.Errorf("children bucket not found for %s", item.QueueType)
					}

					// Get existing children list
					var children []string
					childrenData := childrenBucket.Get(parentHash)
					if childrenData != nil {
						if err := DeserializeStringSlice(childrenData, &children); err != nil {
							return fmt.Errorf("failed to unmarshal children list: %w", err)
						}
					}

					// Add this child's hash if not already present
					childHash := HashPath(item.State.Path)
					found := false
					for _, c := range children {
						if c == childHash {
							found = true
							break
						}
					}

					if !found {
						children = append(children, childHash)
						childrenData, err := SerializeStringSlice(children)
						if err != nil {
							return fmt.Errorf("failed to marshal children list: %w", err)
						}

						if err := childrenBucket.Put(parentHash, childrenData); err != nil {
							return fmt.Errorf("failed to update children list: %w", err)
						}
					}
				}

			case "update_status":
				pathHash := []byte(HashPath(item.Path))

				// Get node data from nodes bucket
				nodesBucket := GetNodesBucket(tx, item.QueueType)
				if nodesBucket == nil {
					return fmt.Errorf("nodes bucket not found for %s", item.QueueType)
				}

				nodeData := nodesBucket.Get(pathHash)
				if nodeData == nil {
					return fmt.Errorf("node not found: %s", item.Path)
				}

				// Deserialize, update traversal status, and re-serialize
				ns, err := DeserializeNodeState(nodeData)
				if err != nil {
					return fmt.Errorf("failed to deserialize node state: %w", err)
				}
				ns.TraversalStatus = item.NewStatus

				updatedData, err := ns.Serialize()
				if err != nil {
					return fmt.Errorf("failed to serialize node state: %w", err)
				}

				if err := nodesBucket.Put(pathHash, updatedData); err != nil {
					return fmt.Errorf("failed to update node: %w", err)
				}

				// Update status bucket membership
				// Delete from old status bucket
				oldBucket := GetStatusBucket(tx, item.QueueType, item.Level, item.OldStatus)
				if oldBucket != nil {
					oldBucket.Delete(pathHash) // Ignore errors
				}

				// Add to new status bucket
				newBucket, err := GetOrCreateStatusBucket(tx, item.QueueType, item.Level, item.NewStatus)
				if err != nil {
					return fmt.Errorf("failed to get new status bucket: %w", err)
				}

				if err := newBucket.Put(pathHash, []byte{}); err != nil {
					return fmt.Errorf("failed to add to new status bucket: %w", err)
				}

			default:
				return fmt.Errorf("unknown write item type: %s", item.Type)
			}
		}
		return nil
	})
}

// WriteBuffer buffers WriteItem operations for batch processing.
// Similar to LogBuffer but for node state updates and inserts.
type WriteBuffer struct {
	db          *DB
	mu          sync.Mutex
	items       []WriteItem
	seen        map[string]struct{} // Deduplication tracking (temporary debug aid)
	batchSize   int
	flushTicker *time.Ticker
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// NewWriteBuffer creates a new write buffer that will flush every N items or every interval.
func NewWriteBuffer(db *DB, batchSize int, flushInterval time.Duration) *WriteBuffer {
	wb := &WriteBuffer{
		db:          db,
		items:       make([]WriteItem, 0, batchSize),
		seen:        make(map[string]struct{}),
		batchSize:   batchSize,
		flushTicker: time.NewTicker(flushInterval),
		stopChan:    make(chan struct{}),
	}

	wb.wg.Add(1)
	go wb.flushLoop()

	return wb
}

// itemKey generates a unique key for deduplication.
func itemKey(item WriteItem) string {
	switch item.Type {
	case "insert":
		return fmt.Sprintf("insert:%s:%d:%s:%s", item.QueueType, item.Level, item.Status, item.State.Path)
	case "update_status":
		return fmt.Sprintf("update:%s:%d:%s", item.QueueType, item.Level, item.Path)
	default:
		return fmt.Sprintf("unknown:%s", item.Type)
	}
}

// Add adds a write item to the buffer. If batch size is reached, it triggers a flush.
// Duplicates are detected and logged but skipped.
func (wb *WriteBuffer) Add(item WriteItem) {
	wb.mu.Lock()

	// Check for duplicates (temporary debug aid)
	key := itemKey(item)
	if _, exists := wb.seen[key]; exists {
		fmt.Printf("[WriteBuffer] DUPLICATE DETECTED: %s (type=%s, queue=%s, level=%d)\n",
			key, item.Type, item.QueueType, item.Level)
		wb.mu.Unlock()
		return // Skip duplicate
	}

	wb.seen[key] = struct{}{}
	wb.items = append(wb.items, item)
	shouldFlush := len(wb.items) >= wb.batchSize
	wb.mu.Unlock()

	if shouldFlush {
		_ = wb.Flush()
	}
}

// AddBatch adds multiple write items to the buffer.
// Duplicates are detected and logged but skipped.
func (wb *WriteBuffer) AddBatch(items []WriteItem) {
	if len(items) == 0 {
		return
	}

	wb.mu.Lock()
	addedCount := 0
	for _, item := range items {
		// Check for duplicates (temporary debug aid)
		key := itemKey(item)
		if _, exists := wb.seen[key]; exists {
			fmt.Printf("[WriteBuffer] DUPLICATE DETECTED: %s (type=%s, queue=%s, level=%d)\n",
				key, item.Type, item.QueueType, item.Level)
			continue // Skip duplicate
		}
		wb.seen[key] = struct{}{}
		wb.items = append(wb.items, item)
		addedCount++
	}

	if addedCount > 0 && addedCount < len(items) {
		fmt.Printf("[WriteBuffer] AddBatch: %d duplicates skipped out of %d items\n",
			len(items)-addedCount, len(items))
	}

	shouldFlush := len(wb.items) >= wb.batchSize
	wb.mu.Unlock()

	if shouldFlush {
		_ = wb.Flush()
	}
}

// Flush writes all buffered items to BoltDB.
// Updates are processed first, then inserts, in a single atomic transaction.
func (wb *WriteBuffer) Flush() error {
	wb.mu.Lock()
	if len(wb.items) == 0 {
		wb.mu.Unlock()
		return nil
	}

	// Take snapshot and clear buffer + seen map
	batch := wb.items
	wb.items = make([]WriteItem, 0, wb.batchSize)
	wb.seen = make(map[string]struct{}) // Clear deduplication tracking after flush
	wb.mu.Unlock()

	// Order matters: updates before inserts (parent status changes before children are added)
	var updates []WriteItem
	var inserts []WriteItem

	for _, item := range batch {
		switch item.Type {
		case "update_status":
			updates = append(updates, item)
		case "insert":
			inserts = append(inserts, item)
		default:
			fmt.Printf("[WriteBuffer] Unknown write item type: %s\n", item.Type)
		}
	}

	// Combine into a single atomic transaction: updates first, then inserts
	allItems := append(updates, inserts...)

	if len(allItems) > 0 {
		if err := WriteItems(wb.db, allItems); err != nil {
			fmt.Printf("[WriteBuffer] Failed to flush %d items (%d updates, %d inserts): %v\n",
				len(allItems), len(updates), len(inserts), err)
			return err
		}
	}

	return nil
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
func (wb *WriteBuffer) flushLoop() {
	defer wb.wg.Done()

	for {
		select {
		case <-wb.flushTicker.C:
			_ = wb.Flush()
		case <-wb.stopChan:
			wb.flushTicker.Stop()
			_ = wb.Flush() // Final flush before stopping
			return
		}
	}
}

// Stop gracefully stops the write buffer and flushes remaining entries.
func (wb *WriteBuffer) Stop() {
	close(wb.stopChan)
	wb.wg.Wait()
}
