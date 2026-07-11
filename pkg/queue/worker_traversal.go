// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// TraversalWorker executes traversal tasks by listing children and recording them to DuckDB.
// Each worker runs independently in its own goroutine, continuously polling the queue for work.
type TraversalWorker struct {
	id          string
	queue       *Queue
	fsAdapter   types.FSAdapter
	queueName   string
	isDst       bool
	shutdownCtx context.Context
	workerCtx   context.Context
	idle        *atomic.Bool
	retire      *atomic.Bool
}

func NewTraversalWorker(
	id string,
	queue *Queue,
	adapter types.FSAdapter,
	queueName string,
	shutdownCtx context.Context,
	workerCtx context.Context,
	idle *atomic.Bool,
	retire *atomic.Bool,
) *TraversalWorker {
	if workerCtx == nil {
		workerCtx = shutdownCtx
	}
	return &TraversalWorker{
		id:          id,
		queue:       queue,
		fsAdapter:   adapter,
		queueName:   queueName,
		isDst:       queueName == "dst",
		shutdownCtx: shutdownCtx,
		workerCtx:   workerCtx,
		idle:        idle,
		retire:      retire,
	}
}

func (w *TraversalWorker) setBusy() {
	if w.idle != nil {
		w.idle.Store(false)
	}
}

func (w *TraversalWorker) setIdle() {
	if w.idle != nil {
		w.idle.Store(true)
	}
}

func (w *TraversalWorker) shouldRetire() bool {
	return w.retire != nil && w.retire.Load()
}

// Run is the main worker loop. It continuously polls the queue for tasks.
// When a task is found, it leases it, executes it, and reports the result.
// When no work is available or queue is paused, it briefly sleeps before polling again.
// When queue is exhausted, the worker exits.
func (w *TraversalWorker) Run() {
	if logservice.LS != nil {
		err := logservice.LS.Log("info", "Worker started", "worker", w.id, w.queueName)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	for {
		// Check for shutdown first (force exit)
		if w.shutdownCtx != nil {
			select {
			case <-w.shutdownCtx.Done():
				if logservice.LS != nil {
					err := logservice.LS.Log("info", "Worker exiting - shutdown requested", "worker", w.id, w.queueName)
					if err != nil {
						fmt.Println("error logging", err)
					}
				}
				return
			default:
			}
		}
		if w.workerCtx != nil {
			select {
			case <-w.workerCtx.Done():
				if logservice.LS != nil {
					err := logservice.LS.Log("info", "Worker exiting - scale down", "worker", w.id, w.queueName)
					if err != nil {
						fmt.Println("error logging", err)
					}
				}
				return
			default:
			}
		}

		if w.shouldRetire() {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info", "Worker exiting - scale down retire", "worker", w.id, w.queueName)
			}
			return
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
				err := logservice.LS.Log("info", "Worker exiting - queue exhausted", "worker", w.id, w.queueName)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			return
		}

		if wait := w.queue.RateLimitedWaitDuration(); wait > 0 {
			waitCtx := w.workerCtx
			if waitCtx == nil {
				waitCtx = w.shutdownCtx
			}
			if err := w.queue.WaitRateLimited(waitCtx, wait); err != nil {
				return
			}
			continue
		}

		// Try to lease a task from the queue
		task := w.queue.Lease()
		if task == nil {
			// No work available, sleep briefly before checking again
			time.Sleep(50 * time.Millisecond)
			continue
		}

		w.setBusy()
		// Execute the task (check for shutdown during execution if needed)
		err := w.execute(task)
		w.setIdle()
		if err != nil {
			task.LastError = err.Error()
			if IsThrottleError(err) {
				task.WorkerResult = "rate_limited"
				w.queue.ReportTaskResult(task, TaskExecutionResultRateLimited)
			} else {
				task.WorkerResult = "error"
				w.queue.ReportTaskResult(task, TaskExecutionResultFailed)
				willRetry := task.Attempts < w.queue.getMaxRetries()
				w.logError(task, err, willRetry)
			}
		} else {
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.WorkerResult = "success"
			w.queue.ReportTaskResult(task, TaskExecutionResultSuccessful)
		}
		if w.shouldRetire() {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info", "Worker exiting - scale down retire", "worker", w.id, w.queueName)
			}
			return
		}
	}
}

// execute performs the actual traversal work.
// It populates task.DiscoveredChildren instead of writing directly to DB.
func (w *TraversalWorker) execute(task *TaskBase) error {
	if !task.IsFolder() {
		return fmt.Errorf("traversal worker received non-folder task")
	}

	parent := w.shutdownCtx
	if parent == nil {
		parent = context.Background()
	}
	wd, ctx := NewProgressWatchdog(parent, traversalStallTimeout, w.queue.sealIOWaitActive)
	defer wd.Stop()

	w.queue.WaitInterOp(w.workerCtx)

	folder := task.Folder
	depth := folder.DepthLevel
	type listChildrenResult struct {
		result types.ListResult
		err    error
	}
	listDone := make(chan listChildrenResult, 1)
	go func(serviceID string, listDepth int, path string) {
		r, err := w.fsAdapter.ListChildren(ctx, serviceID, &listDepth, path)
		listDone <- listChildrenResult{result: r, err: err}
	}(folder.ServiceID, depth, folder.LocationPath)

	var (
		result types.ListResult
		err    error
	)
	select {
	case out := <-listDone:
		result = out.result
		err = out.err
		if err == nil {
			wd.Beat()
		}
	case <-ctx.Done():
		err = ctx.Err()
	case <-w.shutdownCtx.Done():
		err = fmt.Errorf("list children cancelled by shutdown")
	}
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to list children: path=%s folderId=%s error=%v",
					folder.LocationPath, folder.ServiceID, err),
				"worker", w.id, w.queueName)
		}
		return fmt.Errorf("failed to list children of %s: %w", folder.LocationPath, err)
	}

	w.queue.RecordListFill(len(result.Folders) + len(result.Files))

	const defaultPage = 100
	pageSize := w.queue.GetListPageSize()
	if pageSize <= 0 {
		pageSize = defaultPage
	}
	pager := types.NewListPager(result, pageSize)

	if w.isDst {
		aggregated := types.ListResult{}
		for {
			page, ok := pager.Next()
			if !ok {
				break
			}
			wd.Beat()
			aggregated.Folders = append(aggregated.Folders, page.Folders...)
			aggregated.Files = append(aggregated.Files, page.Files...)
		}
		return w.executeDstComparison(task, aggregated, wd)
	}

	task.DiscoveredChildren = make([]ChildResult, 0, len(result.Folders)+len(result.Files))

	for {
		page, ok := pager.Next()
		if !ok {
			break
		}
		wd.Beat()

		for _, childFolder := range page.Folders {
			childFolder.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				Folder: childFolder,
				Status: db.StatusPending,
				IsFile: false,
			})
		}

		for _, childFile := range page.Files {
			childFile.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   childFile,
				Status: db.StatusSuccessful,
				IsFile: true,
			})
		}
	}

	return nil
}

// executeDstComparison performs comparison between expected (src) and actual (dst) children.
// It populates task.DiscoveredChildren with comparison results.
// Matching uses Type + bare child name derived from LocationPath (adapters may put a full path in DisplayName).
func (w *TraversalWorker) executeDstComparison(task *TaskBase, actualResult types.ListResult, wd *ProgressWatchdog) error {
	wd.Beat()
	// Extract expected children from task (populated by queue)
	expectedFolders := task.ExpectedFolders
	expectedFiles := task.ExpectedFiles
	srcIDMap := task.ExpectedSrcIDMap
	if srcIDMap == nil {
		srcIDMap = make(map[string]string)
	}

	task.DiscoveredChildren = make([]ChildResult, 0)

	// Build maps for quick lookup by canonical Type:Name match key.
	actualFolderMap := make(map[string]types.Folder)
	for _, f := range actualResult.Folders {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := dstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		actualFolderMap[matchKey] = f
	}

	actualFileMap := make(map[string]types.File)
	for _, f := range actualResult.Files {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := dstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		actualFileMap[matchKey] = f
	}

	expectedFolderMap := make(map[string]types.Folder)
	for _, f := range expectedFolders {
		matchKey := dstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		expectedFolderMap[matchKey] = f
	}

	expectedFileMap := make(map[string]types.File)
	for _, f := range expectedFiles {
		matchKey := dstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		expectedFileMap[matchKey] = f
	}

	// Compare folders by canonical Type:Name key.
	for _, expectedFolder := range expectedFolders {
		matchKey := dstChildMatchKey(expectedFolder.Type, expectedFolder.DisplayName, expectedFolder.LocationPath)
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
		matchKey := dstChildMatchKey(actualFolder.Type, actualFolder.DisplayName, actualFolder.LocationPath)
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

	// Compare files by canonical Type:Name key.
	for _, expectedFile := range expectedFiles {
		matchKey := dstChildMatchKey(expectedFile.Type, expectedFile.DisplayName, expectedFile.LocationPath)
		if actualFile, exists := actualFileMap[matchKey]; exists {
			// Get SRC node ID from map
			srcID := srcIDMap[matchKey]

			// Compare timestamps to determine copy status for SRC node.
			// If DST is newer or equal: no copy needed (successful).
			// If SRC is newer: copy needed (pending).
			srcCopyStatus := db.CopyStatusPending
			switch compareTimestamps(expectedFile.LastUpdated, actualFile.LastUpdated) {
			case "Successful":
				srcCopyStatus = db.CopyStatusSuccessful
			case "Unparseable":
				// Provider mtimes missing or non-RFC3339: same-size match on both sides is treated as in sync.
				if expectedFile.Size > 0 && expectedFile.Size == actualFile.Size {
					srcCopyStatus = db.CopyStatusSuccessful
				}
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
		matchKey := dstChildMatchKey(actualFile.Type, actualFile.DisplayName, actualFile.LocationPath)
		if _, exists := expectedFileMap[matchKey]; !exists {
			// File exists on dst but not src: mark as "not_on_src"
			task.DiscoveredChildren = append(task.DiscoveredChildren, ChildResult{
				File:   actualFile,
				Status: db.StatusNotOnSrc,
				IsFile: true,
				SrcID:  "", // No SRC node for items not on src
			})
		}
	}

	return nil
}

// compareTimestamps compares src and dst timestamps for copy skip decisions.
func compareTimestamps(srcMTime, dstMTime string) string {
	srcTime, srcOK := parseItemMTime(srcMTime)
	dstTime, dstOK := parseItemMTime(dstMTime)
	if !srcOK || !dstOK {
		return "Unparseable"
	}
	if !dstTime.Before(srcTime) {
		return "Successful"
	}
	return "Pending"
}

func parseItemMTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// logError logs a failed task execution.
func (w *TraversalWorker) logError(task *TaskBase, paramErr error, willRetry bool) {
	if logservice.LS == nil {
		return // Logger not initialized
	}
	path := task.LocationPath()
	retryMsg := "will retry"
	if !willRetry {
		retryMsg = "max retries exceeded"
	}

	err := logservice.LS.Log(
		"error",
		fmt.Sprintf("Failed to traverse %s: %v (%s)", path, paramErr, retryMsg),
		"worker",
		w.id,
		w.queueName,
	)
	if err != nil {
		fmt.Println("error logging", err)
	}
}
