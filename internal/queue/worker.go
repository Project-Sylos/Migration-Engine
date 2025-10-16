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
// When no work is available, it briefly sleeps before polling again.
func (w *TraversalWorker) Run() {
	for {
		// Check if we should stop
		if w.queue.ShouldStop() {
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
			// Task succeeded, mark as complete
			w.queue.Complete(task)
			w.logSuccess(task)
		}

		// Immediately check for more work (no sleep)
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
