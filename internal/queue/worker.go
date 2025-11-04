// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
)

// Worker represents a concurrent task executor.
// Each worker independently polls its queue for work, leases tasks,
// executes them, and reports results back to the queue and database.
type Worker interface {
	Run() // Main execution loop - polls queue and processes tasks
}

// ============================================================================
// TraversalWorker
// ============================================================================

// TraversalWorker executes traversal tasks by listing children and recording them to the database.
// Each worker runs independently in its own goroutine, continuously polling the queue for work.
type TraversalWorker struct {
	id          string
	queue       *Queue
	db          *db.DB
	fsAdapter   fsservices.FSAdapter
	tableName   string            // "src_nodes" or "dst_nodes"
	queueName   string            // "src" or "dst" for logging
	isDst       bool              // true if this is a destination worker (performs comparison)
	coordinator *QueueCoordinator // Shared coordinator for round synchronization
}

// NewTraversalWorker creates a worker that executes traversal tasks.
func NewTraversalWorker(
	id string,
	queue *Queue,
	database *db.DB,
	adapter fsservices.FSAdapter,
	tableName string,
	queueName string,
	coordinator *QueueCoordinator,
) *TraversalWorker {
	return &TraversalWorker{
		id:          id,
		queue:       queue,
		db:          database,
		fsAdapter:   adapter,
		tableName:   tableName,
		queueName:   queueName,
		isDst:       queueName == "dst",
		coordinator: coordinator,
	}
}

// Run is the main worker loop. It continuously polls the queue for tasks.
// When a task is found, it leases it, executes it, and reports the result.
// When no work is available or queue is paused, it briefly sleeps before polling again.
// When queue is exhausted, the worker exits.
func (w *TraversalWorker) Run() {
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Worker started", "worker", w.id, w.queueName)
	}

	for {
		// Check lifecycle state first
		if w.queue.IsPaused() {
			// Queue is paused, sleep and continue polling
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if queue is exhausted (traversal complete) - exit worker
		if w.queue.IsExhausted() {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info", "Worker exiting - queue exhausted", "worker", w.id, w.queueName)
			}
			return
		}

		// Try to lease a task from the queue
		task := w.queue.Lease()
		if task == nil {
			// No work available, sleep briefly before checking again
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Execute the task
		err := w.execute(task)
		if err != nil {
			// Task failed, let queue handle retry logic
			willRetry := w.queue.Fail(task)
			w.logError(task, err, willRetry)
		} else {
			// Task succeeded - write results to database immediately
			dbErr := w.writeResultsToDB(task)
			if dbErr != nil {
				// DB write failed, treat as task failure
				willRetry := w.queue.Fail(task)
				w.logError(task, dbErr, willRetry)
			} else {
				// DB write succeeded, mark task as complete in queue
				w.queue.Complete(task)
			}
		}
	}
}

// execute performs the actual traversal work.
// It populates task.DiscoveredChildren instead of writing directly to DB.
func (w *TraversalWorker) execute(task *TaskBase) error {
	// Only process folder tasks
	if !task.IsFolder() {
		return fmt.Errorf("traversal worker received non-folder task")
	}

	folder := task.Folder

	// List children using the filesystem adapter
	result, err := w.fsAdapter.ListChildren(folder.Id)
	if err != nil {
		return fmt.Errorf("failed to list children of %s: %w", folder.LocationPath, err)
	}

	// Check if this is a dst task with expected children (comparison mode)
	if w.isDst {
		return w.executeDstComparison(task, result)
	}

	// Source mode: all discovered children get "Pending" status
	task.DiscoveredChildren = make([]ChildResult, 0, len(result.Folders)+len(result.Files))

	for _, childFolder := range result.Folders {
		task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
			Folder: childFolder,
			Status: "Pending",
			IsFile: false,
		})
	}

	for _, childFile := range result.Files {
		task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
			File:   childFile,
			Status: "Successful", // Files are immediately successful (no traversal needed)
			IsFile: true,
		})
	}

	return nil
}

// executeDstComparison performs comparison between expected (src) and actual (dst) children.
// It populates task.DiscoveredChildren with comparison results.
func (w *TraversalWorker) executeDstComparison(task *TaskBase, actualResult fsservices.ListResult) error {
	// Extract expected children from task (populated by queue)
	expectedFolders := task.ExpectedFolders
	expectedFiles := task.ExpectedFiles

	task.DiscoveredChildren = make([]ChildResult, 0)

	// Build maps for quick lookup
	actualFolderMap := make(map[string]fsservices.Folder)
	for _, f := range actualResult.Folders {
		actualFolderMap[f.LocationPath] = f
	}

	actualFileMap := make(map[string]fsservices.File)
	for _, f := range actualResult.Files {
		actualFileMap[f.LocationPath] = f
	}

	expectedFolderMap := make(map[string]fsservices.Folder)
	for _, f := range expectedFolders {
		expectedFolderMap[f.LocationPath] = f
	}

	expectedFileMap := make(map[string]fsservices.File)
	for _, f := range expectedFiles {
		expectedFileMap[f.LocationPath] = f
	}

	// Compare folders
	for _, expectedFolder := range expectedFolders {
		if actualFolder, exists := actualFolderMap[expectedFolder.LocationPath]; exists {
			// Folder exists on both: mark as "Pending"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: actualFolder,
				Status: "Pending",
				IsFile: false,
			})
		} else {
			// Folder missing from dst: mark as "Missing"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: expectedFolder,
				Status: "Missing",
				IsFile: false,
			})
		}
	}

	// Check for extra folders on dst (not on src)
	for _, actualFolder := range actualResult.Folders {
		if _, exists := expectedFolderMap[actualFolder.LocationPath]; !exists {
			// Folder exists on dst but not src: mark as "NotOnSrc"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: actualFolder,
				Status: "NotOnSrc",
				IsFile: false,
			})
		}
	}

	// Compare files
	for _, expectedFile := range expectedFiles {
		if actualFile, exists := actualFileMap[expectedFile.LocationPath]; exists {
			// File exists on both: mark as "Successful" (files don't need traversal)
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   actualFile,
				Status: "Successful",
				IsFile: true,
			})
		} else {
			// File missing from dst: mark as "Missing"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   expectedFile,
				Status: "Missing",
				IsFile: true,
			})
		}
	}

	// Check for extra files on dst (not on src)
	for _, actualFile := range actualResult.Files {
		if _, exists := expectedFileMap[actualFile.LocationPath]; !exists {
			// File exists on dst but not src: mark as "NotOnSrc"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   actualFile,
				Status: "NotOnSrc",
				IsFile: true,
			})
		}
	}

	return nil
}

// writeResultsToDB performs the 2 database writes after task execution:
// 1. Updates parent folder traversal_status to "successful"
// 2. Batch inserts all discovered children
func (w *TraversalWorker) writeResultsToDB(task *TaskBase) error {
	folder := task.Folder

	// Write 1: Update parent folder status to "Successful"
	updateQuery := fmt.Sprintf("UPDATE %s SET traversal_status = 'Successful' WHERE id = ?", w.tableName)
	_, err := w.db.Conn().Exec(updateQuery, folder.Id)
	if err != nil {
		return fmt.Errorf("failed to update parent status: %w", err)
	}

	// Write 2: Batch insert all discovered children
	if len(task.DiscoveredChildren) > 0 {
		err = w.insertChildren(task)
		if err != nil {
			return fmt.Errorf("failed to insert children: %w", err)
		}
	}

	return nil
}

// insertChildren batch inserts all discovered children into the database in a single query.
func (w *TraversalWorker) insertChildren(task *TaskBase) error {
	if len(task.DiscoveredChildren) == 0 {
		return nil
	}

	rows := make([][]any, 0, len(task.DiscoveredChildren))
	for _, child := range task.DiscoveredChildren {
		if child.IsFile {
			rows = append(rows, w.getFileValues(child.File, child.Status))
		} else {
			rows = append(rows, w.getFolderValues(child.Folder, child.Status))
		}
	}

	if err := w.db.BulkWrite(w.tableName, rows); err != nil {
		return fmt.Errorf("failed to insert children: %w", err)
	}

	return nil
}

// getFolderValues returns the values slice for a folder insert.
func (w *TraversalWorker) getFolderValues(folder fsservices.Folder, status string) []any {
	if w.queueName == "src" {
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
func (w *TraversalWorker) getFileValues(file fsservices.File, status string) []any {
	if w.queueName == "src" {
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

// logError logs a failed task execution.
func (w *TraversalWorker) logError(task *TaskBase, err error, willRetry bool) {
	if logservice.LS == nil {
		return // Logger not initialized
	}
	path := task.LocationPath()
	retryMsg := "will retry"
	if !willRetry {
		retryMsg = "max retries exceeded"
	}

	_ = logservice.LS.Log(
		"error",
		fmt.Sprintf("Failed to traverse %s: %v (%s)", path, err, retryMsg),
		"worker",
		w.id,
		w.queueName,
	)
}
