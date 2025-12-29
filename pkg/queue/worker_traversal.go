// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// TraversalWorker executes traversal tasks by listing children and recording them to BoltDB.
// Each worker runs independently in its own goroutine, continuously polling the queue for work.
type TraversalWorker struct {
	id          string
	queue       *Queue
	boltDB      *db.DB
	fsAdapter   types.FSAdapter
	queueName   string          // "src" or "dst" for logging
	isDst       bool            // true if this is a destination worker (performs comparison)
	shutdownCtx context.Context // Context for shutdown signaling (optional)
}

// NewTraversalWorker creates a worker that executes traversal tasks.
// shutdownCtx is optional - if provided, the worker will check for cancellation and exit on shutdown.
func NewTraversalWorker(
	id string,
	queue *Queue,
	boltInstance *db.DB,
	adapter types.FSAdapter,
	queueName string,
	shutdownCtx context.Context,
) *TraversalWorker {
	return &TraversalWorker{
		id:          id,
		queue:       queue,
		boltDB:      boltInstance,
		fsAdapter:   adapter,
		queueName:   queueName,
		isDst:       queueName == "dst",
		shutdownCtx: shutdownCtx,
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
		// Check for shutdown first (force exit)
		if w.shutdownCtx != nil {
			select {
			case <-w.shutdownCtx.Done():
				// Shutdown triggered - exit immediately
				if logservice.LS != nil {
					_ = logservice.LS.Log("info", "Worker exiting - shutdown requested", "worker", w.id, w.queueName)
				}
				return
			default:
				// Continue normal execution
			}
		}

		// Check lifecycle state
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

		// Execute the task (check for shutdown during execution if needed)
		err := w.execute(task)
		if err != nil {
			// Task failed, let queue handle retry logic
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug",
					fmt.Sprintf("Worker task execution failed: path=%s round=%d error=%v",
						task.LocationPath(), task.Round, err),
					"worker", w.id, w.queueName)
			}
			// Report failure - queue will handle retry logic
			w.queue.ReportTaskResult(task, TaskExecutionResultFailed)
			// Check if task was retried for logging
			nodeID := task.ID
			willRetry := w.queue.isInPendingSet(nodeID)
			w.logError(task, err, willRetry)
		} else {
			// Task succeeded
			w.queue.ReportTaskResult(task, TaskExecutionResultSuccessful)
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
	result, err := w.fsAdapter.ListChildren(folder.ServiceID)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to list children: path=%s folderId=%s error=%v",
					folder.LocationPath, folder.ServiceID, err),
				"worker", w.id, w.queueName)
		}
		return fmt.Errorf("failed to list children of %s: %w", folder.LocationPath, err)
	}

	// Wrap result in a pager so we can process children in fixed-size pages.
	// This mimics real cloud SDK pagination behavior and keeps per-page work bounded.
	const pageSize = 100
	pager := types.NewListPager(result, pageSize)

	// Check if this is a dst task with expected children (comparison mode)
	if w.isDst {
		// Aggregate all pages into a single ListResult for comparison.
		aggregated := types.ListResult{}
		for {
			page, ok := pager.Next()
			if !ok {
				break
			}
			aggregated.Folders = append(aggregated.Folders, page.Folders...)
			aggregated.Files = append(aggregated.Files, page.Files...)
		}
		return w.executeDstComparison(task, aggregated)
	}

	// Source mode: all discovered children get "Pending" status
	// DepthLevel is driven by BFS round: children of a task in round N live at depth N+1.
	task.DiscoveredChildren = make([]ChildResult, 0, len(result.Folders)+len(result.Files))

	for {
		page, ok := pager.Next()
		if !ok {
			break
		}

		for _, childFolder := range page.Folders {
			// Override adapter-provided depth with BFS depth based on current round.
			childFolder.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: childFolder,
				Status: db.StatusPending,
				IsFile: false,
			})
		}

		for _, childFile := range page.Files {
			// Override adapter-provided depth with BFS depth based on current round.
			childFile.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   childFile,
				Status: db.StatusSuccessful, // Files are immediately successful (no traversal needed)
				IsFile: true,
			})
		}
	}

	return nil
}

// executeDstComparison performs comparison between expected (src) and actual (dst) children.
// It populates task.DiscoveredChildren with comparison results.
// Matching is done by Type + Name, not LocationPath.
func (w *TraversalWorker) executeDstComparison(task *TaskBase, actualResult types.ListResult) error {
	// Extract expected children from task (populated by queue)
	expectedFolders := task.ExpectedFolders
	expectedFiles := task.ExpectedFiles
	srcIDMap := task.ExpectedSrcIDMap
	if srcIDMap == nil {
		srcIDMap = make(map[string]string)
	}

	task.DiscoveredChildren = make([]ChildResult, 0)

	// Build maps for quick lookup by Type+Name (matching key)
	actualFolderMap := make(map[string]types.Folder)
	for _, f := range actualResult.Folders {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := f.Type + ":" + f.DisplayName
		actualFolderMap[matchKey] = f
	}

	actualFileMap := make(map[string]types.File)
	for _, f := range actualResult.Files {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := f.Type + ":" + f.DisplayName
		actualFileMap[matchKey] = f
	}

	expectedFolderMap := make(map[string]types.Folder)
	for _, f := range expectedFolders {
		matchKey := f.Type + ":" + f.DisplayName
		expectedFolderMap[matchKey] = f
	}

	expectedFileMap := make(map[string]types.File)
	for _, f := range expectedFiles {
		matchKey := f.Type + ":" + f.DisplayName
		expectedFileMap[matchKey] = f
	}

	// Compare folders by Type + Name
	for _, expectedFolder := range expectedFolders {
		matchKey := expectedFolder.Type + ":" + expectedFolder.DisplayName
		if actualFolder, exists := actualFolderMap[matchKey]; exists {

			// Get SRC node ID from map
			srcID := srcIDMap[matchKey]

			// Folder exists on both: SRC copy status should be "successful"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder:        actualFolder,
				Status:        db.StatusPending,
				IsFile:        false,
				SrcID:         srcID,
				SrcCopyStatus: db.CopyStatusSuccessful, // Folder exists on both, no copy needed
			})
		}
	}

	// Check for extra folders on dst (not on src)
	for _, actualFolder := range actualResult.Folders {
		matchKey := actualFolder.Type + ":" + actualFolder.DisplayName
		if _, exists := expectedFolderMap[matchKey]; !exists {
			// Folder exists on dst but not src: mark as "NotOnSrc"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: actualFolder,
				Status: db.StatusNotOnSrc,
				IsFile: false,
				SrcID:  "", // No SRC node for items not on src
			})
		}
	}

	// Compare files by Type + Name
	for _, expectedFile := range expectedFiles {
		matchKey := expectedFile.Type + ":" + expectedFile.DisplayName
		if actualFile, exists := actualFileMap[matchKey]; exists {
			// Get SRC node ID from map
			srcID := srcIDMap[matchKey]

			// Compare timestamps to determine copy status for SRC node
			// If DST is newer: no copy needed (successful)
			// If SRC is newer or equal: copy needed (pending)
			srcCopyStatus := db.CopyStatusPending
			if compareTimestamps(expectedFile.LastUpdated, actualFile.LastUpdated) == "Successful" {
				// DST is newer, no copy needed
				srcCopyStatus = db.CopyStatusSuccessful
			}

			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:          actualFile,
				Status:        db.StatusSuccessful, // File exists on dst, no traversal needed
				IsFile:        true,
				SrcID:         srcID,
				SrcCopyStatus: srcCopyStatus,
			})
		}
	}

	// Check for extra files on dst (not on src)
	for _, actualFile := range actualResult.Files {
		matchKey := actualFile.Type + ":" + actualFile.DisplayName
		if _, exists := expectedFileMap[matchKey]; !exists {
			// File exists on dst but not src: mark as "NotOnSrc"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   actualFile,
				Status: "NotOnSrc",
				IsFile: true,
				SrcID:  "", // No SRC node for items not on src
			})
		}
	}

	return nil
}

// compareTimestamps compares src and dst timestamps and returns the appropriate status.
// Returns:
// - "Successful" if dst is newer (no copy needed)
// - "Pending" if src is newer (copy needed) or if timestamps are equal
func compareTimestamps(srcMTime, dstMTime string) string {
	// Parse timestamps (RFC3339 format)
	srcTime, err1 := time.Parse(time.RFC3339, srcMTime)
	dstTime, err2 := time.Parse(time.RFC3339, dstMTime)

	// If parsing fails, default to "Pending" (conservative - assume copy needed)
	if err1 != nil || err2 != nil {
		return "Pending"
	}

	// If dst is newer, no copy needed - mark as successful
	if dstTime.After(srcTime) {
		return "Successful"
	}

	// If src is newer or equal, copy is needed - mark as pending
	return "Pending"
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
