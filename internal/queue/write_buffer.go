// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
)

// WriteBuffer batches database writes for queue tasks.
// It collects completed tasks and flushes them in batches to improve performance.
// Tasks are pre-organized by status during Add() to avoid O(n) scanning during flush.
type WriteBuffer struct {
	db              *db.DB
	tableName       string
	queueName       string
	mu              sync.Mutex
	successfulTasks []*TaskBase // Pre-organized successful tasks
	failedTasks     []*TaskBase // Pre-organized failed tasks
	batchSize       int
	flushTicker     *time.Ticker
	stopChan        chan struct{}
	wg              sync.WaitGroup
	flushing        int32 // Atomic flag: 1 if flush is in progress, 0 otherwise
}

// NewWriteBuffer creates a new write buffer that will flush every N tasks or every interval.
func NewWriteBuffer(database *db.DB, tableName string, queueName string, batchSize int, flushInterval time.Duration) *WriteBuffer {
	wb := &WriteBuffer{
		db:              database,
		tableName:       tableName,
		queueName:       queueName,
		successfulTasks: make([]*TaskBase, 0, batchSize),
		failedTasks:     make([]*TaskBase, 0, batchSize),
		batchSize:       batchSize,
		flushTicker:     time.NewTicker(flushInterval),
		stopChan:        make(chan struct{}),
	}

	wb.wg.Add(1)
	go wb.flushLoop()

	return wb
}

// Add adds a task to the buffer, pre-organized by status to avoid O(n) scanning during flush.
// If batch size is reached, it triggers a flush asynchronously.
func (wb *WriteBuffer) Add(task *TaskBase) {
	wb.mu.Lock()
	// Pre-organize by status during Add() - no scanning needed during flush
	switch task.Status {
	case "successful":
		wb.successfulTasks = append(wb.successfulTasks, task)
	case "failed":
		wb.failedTasks = append(wb.failedTasks, task)
	}
	totalTasks := len(wb.successfulTasks) + len(wb.failedTasks)
	shouldFlush := totalTasks >= wb.batchSize
	wb.mu.Unlock()

	if shouldFlush {
		// Trigger flush asynchronously to avoid blocking workers
		go wb.Flush()
	}
}

// Flush writes all buffered tasks to the database.
// Returns error if any DB operation fails.
// The flush pattern: lock, snapshot, clear, unlock (workers can continue), then work off snapshot.
// Uses atomic flag to prevent concurrent flushes - if already flushing, returns immediately.
func (wb *WriteBuffer) Flush() error {
	// Check if already flushing - if so, skip this flush
	if !atomic.CompareAndSwapInt32(&wb.flushing, 0, 1) {
		return nil // Already flushing, skip
	}
	defer atomic.StoreInt32(&wb.flushing, 0)

	wb.mu.Lock()
	if len(wb.successfulTasks) == 0 && len(wb.failedTasks) == 0 {
		wb.mu.Unlock()
		return nil
	}

	// Snapshot and drain buffers - workers can continue adding immediately after unlock
	successfulBatch := wb.successfulTasks
	failedBatch := wb.failedTasks
	wb.successfulTasks = make([]*TaskBase, 0, wb.batchSize)
	wb.failedTasks = make([]*TaskBase, 0, wb.batchSize)
	wb.mu.Unlock()

	// All DB operations happen outside the lock, working off the snapshots
	return wb.flushWriteBatch(successfulBatch, failedBatch)
}

// flushWriteBatch performs the actual DB operations for a batch of tasks.
// This is called after the snapshot/clear/unlock pattern to avoid blocking workers.
// Tasks are already pre-organized by status, so no scanning needed.
func (wb *WriteBuffer) flushWriteBatch(successfulTasks []*TaskBase, failedTasks []*TaskBase) error {
	// Collect parent IDs for status updates (no status grouping needed - already organized)
	var successfulIDs []string
	var failedIDs []string

	for _, task := range successfulTasks {
		if task.IsFolder() {
			successfulIDs = append(successfulIDs, task.Folder.Id)
		}
	}

	for _, task := range failedTasks {
		if task.IsFolder() {
			failedIDs = append(failedIDs, task.Folder.Id)
		}
	}

	// Batch update successful parent tasks
	if len(successfulIDs) > 0 {
		if err := wb.updateParentStatus(successfulIDs, "Successful"); err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to update successful parent status: %v", err), "write_buffer", wb.queueName, wb.queueName)
			}
			return fmt.Errorf("failed to update successful parent status: %w", err)
		}
	}

	// Batch update failed parent tasks
	if len(failedIDs) > 0 {
		if err := wb.updateParentStatus(failedIDs, "Failed"); err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to update failed parent status: %v", err), "write_buffer", wb.queueName, wb.queueName)
			}
			return fmt.Errorf("failed to update failed parent status: %w", err)
		}
	}

	// Collect all children from all tasks (combine both successful and failed)
	childrenRows := make([][]any, 0)
	for _, task := range successfulTasks {
		for _, child := range task.DiscoveredChildren {
			if child.IsFile {
				childrenRows = append(childrenRows, wb.getFileValues(child.File, child.Status))
			} else {
				childrenRows = append(childrenRows, wb.getFolderValues(child.Folder, child.Status))
			}
		}
	}
	for _, task := range failedTasks {
		for _, child := range task.DiscoveredChildren {
			if child.IsFile {
				childrenRows = append(childrenRows, wb.getFileValues(child.File, child.Status))
			} else {
				childrenRows = append(childrenRows, wb.getFolderValues(child.Folder, child.Status))
			}
		}
	}

	// Batch insert all children
	if len(childrenRows) > 0 {
		if err := wb.db.BulkWrite(wb.tableName, childrenRows); err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to insert children: %v", err), "write_buffer", wb.queueName, wb.queueName)
			}
			return fmt.Errorf("failed to insert children: %w", err)
		}
	}

	totalTasks := len(successfulTasks) + len(failedTasks)
	if logservice.LS != nil && totalTasks > 0 {
		_ = logservice.LS.Log("debug", fmt.Sprintf("Flushed %d tasks (%d successful, %d failed, %d children)", totalTasks, len(successfulTasks), len(failedTasks), len(childrenRows)), "write_buffer", wb.queueName, wb.queueName)
	}

	return nil
}

// updateParentStatus performs a batch update of parent task statuses.
func (wb *WriteBuffer) updateParentStatus(ids []string, status string) error {
	if len(ids) == 0 {
		return nil
	}

	// Build IN clause with placeholders for parameterized query
	placeholders := ""
	args := make([]any, 0, len(ids)+1)
	args = append(args, status)

	for i, id := range ids {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += "?"
		args = append(args, id)
	}

	query := fmt.Sprintf("UPDATE %s SET traversal_status = ? WHERE id IN (%s)", wb.tableName, placeholders)

	_, err := wb.db.Conn().Exec(query, args...)
	if err != nil {
		return fmt.Errorf("batch update failed: %w", err)
	}

	return nil
}

// getFolderValues returns the values slice for a folder insert.
func (wb *WriteBuffer) getFolderValues(folder fsservices.Folder, status string) []any {
	if wb.queueName == "src" {
		return []any{
			folder.Id,
			folder.ParentId,
			folder.DisplayName,
			folder.LocationPath,
			folder.ParentPath, // parent_path column
			fsservices.NodeTypeFolder,
			folder.DepthLevel,
			nil, // size (folders have no size)
			folder.LastUpdated,
			status,    // traversal_status
			"Pending", // copy_status
		}
	} else {
		return []any{
			folder.Id,
			folder.ParentId,
			folder.DisplayName,
			folder.LocationPath,
			folder.ParentPath, // parent_path column
			fsservices.NodeTypeFolder,
			folder.DepthLevel,
			nil, // size
			folder.LastUpdated,
			status, // traversal_status
		}
	}
}

// getFileValues returns the values slice for a file insert.
func (wb *WriteBuffer) getFileValues(file fsservices.File, status string) []any {
	if wb.queueName == "src" {
		// For src queue, files always get "Successful" traversal_status regardless of child status
		return []any{
			file.Id,
			file.ParentId,
			file.DisplayName,
			file.LocationPath,
			file.ParentPath, // parent_path column
			fsservices.NodeTypeFile,
			file.DepthLevel,
			file.Size,
			file.LastUpdated,
			"Successful", // traversal_status (files don't need traversal)
			"Pending",    // copy_status
		}
	} else {
		// For dst queue, use the status from comparison
		return []any{
			file.Id,
			file.ParentId,
			file.DisplayName,
			file.LocationPath,
			file.ParentPath, // parent_path column
			fsservices.NodeTypeFile,
			file.DepthLevel,
			file.Size,
			file.LastUpdated,
			status, // traversal_status (from comparison)
		}
	}
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
// Checks flushing flag and skips if flush is already in progress.
func (wb *WriteBuffer) flushLoop() {
	defer wb.wg.Done()

	for {
		select {
		case <-wb.flushTicker.C:
			// Only flush if not already flushing (atomic flag prevents concurrent flushes)
			wb.Flush()
		case <-wb.stopChan:
			wb.flushTicker.Stop()
			// Force final flush - wait for any in-progress flush to complete
			for atomic.LoadInt32(&wb.flushing) == 1 {
				time.Sleep(10 * time.Millisecond)
			}
			wb.Flush() // Final flush before stopping
			return
		}
	}
}

// Stop gracefully stops the write buffer and flushes remaining tasks.
func (wb *WriteBuffer) Stop() {
	close(wb.stopChan)
	wb.wg.Wait()
}
