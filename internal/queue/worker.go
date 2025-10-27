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
	id        int
	queue     *Queue
	db        *db.DB
	fsAdapter fsservices.FSAdapter
	tableName string // "src_nodes" or "dst_nodes"
	queueName string // "src" or "dst" for logging
	isDst     bool   // true if this is a destination worker (performs comparison)
}

// NewTraversalWorker creates a worker that executes traversal tasks.
func NewTraversalWorker(
	id int,
	queue *Queue,
	database *db.DB,
	adapter fsservices.FSAdapter,
	tableName string,
	queueName string,
) *TraversalWorker {
	return &TraversalWorker{
		id:        id,
		queue:     queue,
		db:        database,
		fsAdapter: adapter,
		tableName: tableName,
		queueName: queueName,
		isDst:     queueName == "dst",
	}
}

// Run is the main worker loop. It continuously polls the queue for tasks.
// When a task is found, it leases it, executes it, and reports the result.
// When no work is available or queue is paused, it briefly sleeps before polling again.
// When queue is exhausted, the worker exits.
func (w *TraversalWorker) Run() {
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug", "Worker starting run loop", "worker", fmt.Sprintf("%s-worker-%d", w.queueName, w.id))
	}

	for {
		// Check lifecycle state first
		if w.queue.IsPaused() {
			// Queue is paused, sleep and continue polling
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", "Queue is paused, sleeping", "worker", fmt.Sprintf("%s-worker-%d", w.queueName, w.id))
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if queue is exhausted (traversal complete) - exit worker
		if w.queue.IsExhausted() {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info", "Queue exhausted, worker exiting", "worker", fmt.Sprintf("%s-worker-%d", w.queueName, w.id))
			}
			return
		}

		// Try to lease a task from the queue
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", "Attempting to lease task", "worker", fmt.Sprintf("%s-worker-%d", w.queueName, w.id))
		}
		task := w.queue.Lease()
		if task == nil {
			// No work available, sleep briefly before checking again
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", "No task available, sleeping 50ms", "worker", fmt.Sprintf("%s-worker-%d", w.queueName, w.id))
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if logservice.LS != nil {
			_ = logservice.LS.Log("info", fmt.Sprintf("Leased task: %s", task.LocationPath()), "worker", fmt.Sprintf("%s-worker-%d", w.queueName, w.id))
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
				w.logSuccess(task)
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

	// Source mode: all discovered children get "pending" status
	task.DiscoveredChildren = make([]ChildResult, 0, len(result.Folders)+len(result.Files))

	for _, childFolder := range result.Folders {
		task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
			Folder: childFolder,
			Status: "pending",
			IsFile: false,
		})
	}

	for _, childFile := range result.Files {
		task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
			File:   childFile,
			Status: "successful", // Files are immediately successful (no traversal needed)
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
			// Folder exists on both: mark as "pending"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: actualFolder,
				Status: "pending",
				IsFile: false,
			})
		} else {
			// Folder missing from dst: mark as "missing"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: expectedFolder,
				Status: "missing",
				IsFile: false,
			})
		}
	}

	// Check for extra folders on dst (not on src)
	for _, actualFolder := range actualResult.Folders {
		if _, exists := expectedFolderMap[actualFolder.LocationPath]; !exists {
			// Folder exists on dst but not src: mark as "not_on_src"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: actualFolder,
				Status: "not_on_src",
				IsFile: false,
			})
		}
	}

	// Compare files
	for _, expectedFile := range expectedFiles {
		if actualFile, exists := actualFileMap[expectedFile.LocationPath]; exists {
			// File exists on both: mark as "successful" (files don't need traversal)
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   actualFile,
				Status: "successful",
				IsFile: true,
			})
		} else {
			// File missing from dst: mark as "missing"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   expectedFile,
				Status: "missing",
				IsFile: true,
			})
		}
	}

	// Check for extra files on dst (not on src)
	for _, actualFile := range actualResult.Files {
		if _, exists := expectedFileMap[actualFile.LocationPath]; !exists {
			// File exists on dst but not src: mark as "not_on_src"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   actualFile,
				Status: "not_on_src",
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

	// Write 1: Update parent folder status to "successful"
	updateQuery := fmt.Sprintf("UPDATE %s SET traversal_status = 'successful' WHERE id = ?", w.tableName)
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

	// Build batch insert query
	var values []any
	var placeholders string

	// Determine column count based on queue type
	colCount := 9 // dst has 9 columns
	if w.queueName == "src" {
		colCount = 10 // src has 10 columns (includes copy_status)
	}

	for i, child := range task.DiscoveredChildren {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += buildPlaceholderGroup(colCount)

		if child.IsFile {
			values = append(values, w.getFileValues(child.File, child.Status)...)
		} else {
			values = append(values, w.getFolderValues(child.Folder, child.Status)...)
		}
	}

	query := fmt.Sprintf("INSERT INTO %s VALUES %s", w.tableName, placeholders)
	_, err := w.db.Conn().Exec(query, values...)
	return err
}

// buildPlaceholderGroup creates "(?, ?, ?)" etc for SQL placeholders.
func buildPlaceholderGroup(n int) string {
	if n <= 0 {
		return "()"
	}
	s := "("
	for i := 0; i < n; i++ {
		if i > 0 {
			s += ", "
		}
		s += "?"
	}
	s += ")"
	return s
}

// getFolderValues returns the values slice for a folder insert.
func (w *TraversalWorker) getFolderValues(folder fsservices.Folder, status string) []any {
	if w.queueName == "src" {
		return []any{
			folder.Id,
			folder.ParentId,
			folder.DisplayName,
			folder.LocationPath,
			fsservices.NodeTypeFolder,
			folder.DepthLevel,
			nil, // size (folders have no size)
			folder.LastUpdated,
			status,    // traversal_status
			"pending", // copy_status
		}
	} else {
		return []any{
			folder.Id,
			folder.ParentId,
			folder.DisplayName,
			folder.LocationPath,
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
			fsservices.NodeTypeFile,
			file.DepthLevel,
			file.Size,
			file.LastUpdated,
			"successful", // traversal_status (files don't need traversal)
			"pending",    // copy_status
		}
	} else {
		return []any{
			file.Id,
			file.ParentId,
			file.DisplayName,
			file.LocationPath,
			fsservices.NodeTypeFile,
			file.DepthLevel,
			file.Size,
			file.LastUpdated,
			status, // traversal_status (from comparison)
		}
	}
}

// logSuccess logs a successful task execution.
func (w *TraversalWorker) logSuccess(task *TaskBase) {
	if logservice.LS == nil {
		return // Logger not initialized
	}
	path := task.LocationPath()
	_ = logservice.LS.Log(
		"debug",
		fmt.Sprintf("Successfully traversed %s", path),
		"worker",
		fmt.Sprintf("%s-worker-%d", w.queueName, w.id),
	)
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
		fmt.Sprintf("%s-worker-%d", w.queueName, w.id),
	)
}
