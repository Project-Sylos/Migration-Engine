// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"context"
	"errors"
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
	queue       *queue.Queue
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
	queue *queue.Queue,
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

func (w *TraversalWorker) shouldRetire() bool {
	return w.retire != nil && w.retire.Load()
}

// Run is the main worker loop. It continuously polls the queue for tasks.
// When a task is found, it leases it, executes it, and reports the result.
// When no work is available or queue is paused, it briefly sleeps before polling again.
// When queue is exhausted, the worker exits.
func (w *TraversalWorker) Run() {
	if logservice.LS != nil {
		err := logservice.LS.Log("info", "queue.Worker started", "worker", w.id, w.queueName)
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
					err := logservice.LS.Log("info", "queue.Worker exiting - shutdown requested", "worker", w.id, w.queueName)
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
					err := logservice.LS.Log("info", "queue.Worker exiting - scale down", "worker", w.id, w.queueName)
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
				_ = logservice.LS.Log("info", "queue.Worker exiting - scale down retire", "worker", w.id, w.queueName)
			}
			return
		}

		// Check lifecycle state
		if w.queue.IsPaused() {
			// queue.Queue is paused, sleep and continue polling
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if queue is exhausted (traversal complete) - exit worker
		if w.queue.IsExhausted() {
			if logservice.LS != nil {
				err := logservice.LS.Log("info", "queue.Worker exiting - queue exhausted", "worker", w.id, w.queueName)
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

		setWorkerBusy(w.idle)
		// Execute the task (check for shutdown during execution if needed)
		err := w.execute(task)
		setWorkerIdle(w.idle)
		if errors.Is(err, errTransferAbandoned) {
			// Scale-down / throttle abort already (or will be) requeued via ReleaseInFlightOnThrottle;
			// if still in-progress, yield without failure.
			if w.queue.HasInProgress(task.ID) {
				task.Locked = false
				w.queue.RemoveInProgress(task.ID)
				_ = w.queue.Add(task)
			}
			if w.shouldRetire() {
				return
			}
			continue
		}
		if err != nil {
			task.LastError = err.Error()
			if queue.IsThrottleError(err) {
				task.WorkerResult = "rate_limited"
				w.queue.ReportTaskResult(task, queue.TaskExecutionResultRateLimited)
			} else {
				task.WorkerResult = "error"
				w.queue.ReportTaskResult(task, queue.TaskExecutionResultFailed)
				willRetry := task.Attempts < w.queue.MaxRetries() && !queue.IsNonRetryableTraversalError(err.Error())
				w.logError(task, err, willRetry)
			}
		} else {
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.WorkerResult = "success"
			w.queue.ReportTaskResult(task, queue.TaskExecutionResultSuccessful)
		}
		if w.shouldRetire() {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info", "queue.Worker exiting - scale down retire", "worker", w.id, w.queueName)
			}
			return
		}
	}
}

// execute performs the actual traversal work.
// It populates task.DiscoveredChildren instead of writing directly to DB.
func (w *TraversalWorker) execute(task *queue.TaskBase) error {
	if w.queue.GetMode() == queue.QueueModeGPL {
		return w.executeGPL(task)
	}
	if !task.IsFolder() {
		return fmt.Errorf("traversal worker received non-folder task")
	}

	// Prefer workerCtx so scale-down / stop force-checkout aborts mid-ListChildren; fall back to shutdown.
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	// Suppress while seal flush or rate-limit wait; never fake heartbeats.
	// Hung ListChildren must cancel (15s) so the task can fail instead of wedging the round forever.
	wd, ctx := observe.NewProgressWatchdog(parent, traversalStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()

	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	w.queue.WaitInterOp(w.workerCtx)

	folder := task.Folder
	depth := folder.DepthLevel
	out, ok := awaitCancellable(ctx, func() fsOut[types.ListResult] {
		d := depth
		r, err := w.fsAdapter.ListChildren(ctx, folder.ServiceID, &d, folder.LocationPath)
		return fsOut[types.ListResult]{Val: r, Err: err}
	})
	if !ok {
		if parent.Err() != nil || (w.shutdownCtx != nil && w.shutdownCtx.Err() != nil) {
			return errTransferAbandoned
		}
		return fmt.Errorf("%w: list children of %s stalled after %s", errTraversalStalled, folder.LocationPath, traversalStallTimeout)
	}
	if out.Err != nil {
		err := out.Err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if parent.Err() != nil || (w.shutdownCtx != nil && w.shutdownCtx.Err() != nil) {
				return errTransferAbandoned
			}
			return fmt.Errorf("%w: list children of %s stalled after %s", errTraversalStalled, folder.LocationPath, traversalStallTimeout)
		}
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to list children: path=%s folderId=%s error=%v",
					folder.LocationPath, folder.ServiceID, err),
				"worker", w.id, w.queueName)
		}
		return fmt.Errorf("failed to list children of %s: %w", folder.LocationPath, err)
	}
	result := out.Val
	wd.Beat()
	if w.queue.HasWatchdog() {
		w.queue.BeatWatchdog()
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

	task.DiscoveredChildren = make([]queue.ChildResult, 0, len(result.Folders)+len(result.Files))

	for {
		page, ok := pager.Next()
		if !ok {
			break
		}
		wd.Beat()

		for _, childFolder := range page.Folders {
			childFolder.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				Folder: childFolder,
				Status: db.StatusPending,
				IsFile: false,
			})
		}

		for _, childFile := range page.Files {
			childFile.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
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
func (w *TraversalWorker) executeDstComparison(task *queue.TaskBase, actualResult types.ListResult, wd *observe.ProgressWatchdog) error {
	wd.Beat()
	// Extract expected children from task (populated by queue)
	expectedFolders := task.ExpectedFolders
	expectedFiles := task.ExpectedFiles
	srcIDMap := task.ExpectedSrcIDMap
	if srcIDMap == nil {
		srcIDMap = make(map[string]string)
	}

	task.DiscoveredChildren = make([]queue.ChildResult, 0)

	// Build maps for quick lookup by canonical Type:Name match key.
	actualFolderMap := make(map[string]types.Folder)
	for _, f := range actualResult.Folders {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		actualFolderMap[matchKey] = f
	}

	actualFileMap := make(map[string]types.File)
	for _, f := range actualResult.Files {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		actualFileMap[matchKey] = f
	}

	expectedFolderMap := make(map[string]types.Folder)
	for _, f := range expectedFolders {
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		expectedFolderMap[matchKey] = f
	}

	expectedFileMap := make(map[string]types.File)
	for _, f := range expectedFiles {
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		expectedFileMap[matchKey] = f
	}

	// Compare folders by canonical Type:Name key.
	for _, expectedFolder := range expectedFolders {
		matchKey := queue.DstChildMatchKey(expectedFolder.Type, expectedFolder.DisplayName, expectedFolder.LocationPath)
		if actualFolder, exists := actualFolderMap[matchKey]; exists {

			// Get SRC node ID from map
			srcID := srcIDMap[matchKey]

			// Folder exists on both: never a copy task for this migration.
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				Folder:        actualFolder,
				Status:        db.StatusPending,
				IsFile:        false,
				SrcID:         srcID,
				SrcCopyStatus: db.CopyStatusAlreadyExisted,
			})
		} 
	}

	// Check for extra folders on dst (not on src)
	for _, actualFolder := range actualResult.Folders {
		matchKey := queue.DstChildMatchKey(actualFolder.Type, actualFolder.DisplayName, actualFolder.LocationPath)
		if _, exists := expectedFolderMap[matchKey]; !exists {
			// Folder exists on dst but not src: mark as "NotOnSrc"
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				Folder: actualFolder,
				Status: db.StatusNotOnSrc,
				IsFile: false,
				SrcID:  "", // No SRC node for items not on src
			})
		}
	}

	// Compare files by canonical Type:Name key.
	for _, expectedFile := range expectedFiles {
		matchKey := queue.DstChildMatchKey(expectedFile.Type, expectedFile.DisplayName, expectedFile.LocationPath)
		if actualFile, exists := actualFileMap[matchKey]; exists {
			// Get SRC node ID from map
			srcID := srcIDMap[matchKey]

			// Compare timestamps to determine copy status for SRC node.
			// If DST is newer or equal: already on DST (already_existed).
			// If SRC is newer: copy needed (pending).
			srcCopyStatus := db.CopyStatusPending
			switch compareTimestamps(expectedFile.LastUpdated, actualFile.LastUpdated) {
			case "Successful":
				srcCopyStatus = db.CopyStatusAlreadyExisted
			case "Unparseable":
				// Provider mtimes missing or non-RFC3339: same-size match on both sides is treated as in sync.
				if expectedFile.Size > 0 && expectedFile.Size == actualFile.Size {
					srcCopyStatus = db.CopyStatusAlreadyExisted
				}
			}

			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
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
		matchKey := queue.DstChildMatchKey(actualFile.Type, actualFile.DisplayName, actualFile.LocationPath)
		if _, exists := expectedFileMap[matchKey]; !exists {
			// File exists on dst but not src: mark as "not_on_src"
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
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
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	// Google sometimes returns RFC3339 without zone; treat as UTC.
	if t, err := time.Parse("2006-01-02T15:04:05.999999999", s); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

// logError logs a failed task execution.
func (w *TraversalWorker) logError(task *queue.TaskBase, paramErr error, willRetry bool) {
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

// executeGPL revalidates path-scoped GPL rules for cascade after ancestor remaps.
func (w *TraversalWorker) executeGPL(task *queue.TaskBase) error {
	if w.isDst {
		// DST pending is acknowledgment only; path composition lives on SRC.
		return nil
	}
	if !gpl.PathChecksRequired(w.queue.ScalingSrcProvider(), w.queue.ScalingDstProvider(), w.queue.PathCheckProfile()) {
		return nil
	}
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := observe.NewProgressWatchdog(parent, gplStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()

	database := w.queue.Database()
	checkTarget := gpl.ResolvePathCheckTarget(w.queue.ScalingSrcProvider(), w.queue.ScalingDstProvider(), w.queue.PathCheckProfile())
	target := gpl.GPLTargetFromProvider(checkTarget)
	err := awaitErr(ctx, parent, gplStallTimeout, "gpl revalidate", task.LocationPath(), func() error {
		return gpl.ProcessGPLTaskSRC(database, target, task, w.queue.WindowsCompat())
	})
	if err == nil {
		wd.Beat()
	}
	return err
}
