// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const (
	// Default buffer size for streaming file transfers (64KB)
	defaultCopyBufferSize = 64 * 1024
)

// CopyWorker executes copy tasks by creating folders or streaming files from source to destination.
// Each worker runs independently in its own goroutine, continuously polling the queue for work.
type CopyWorker struct {
	id          string
	queue       *Queue
	srcAdapter  types.FSAdapter
	dstAdapter  types.FSAdapter
	queueName   string
	shutdownCtx context.Context
	workerCtx   context.Context
	idle        *atomic.Bool
	retire      *atomic.Bool
	copyBuffer  []byte
}

func NewCopyWorker(
	id string,
	queue *Queue,
	srcAdapter types.FSAdapter,
	dstAdapter types.FSAdapter,
	shutdownCtx context.Context,
	workerCtx context.Context,
	idle *atomic.Bool,
	retire *atomic.Bool,
) *CopyWorker {
	if workerCtx == nil {
		workerCtx = shutdownCtx
	}
	return &CopyWorker{
		id:          id,
		queue:       queue,
		srcAdapter:  srcAdapter,
		dstAdapter:  dstAdapter,
		queueName:   "copy",
		shutdownCtx: shutdownCtx,
		workerCtx:   workerCtx,
		idle:        idle,
		retire:      retire,
		copyBuffer:  make([]byte, defaultCopyBufferSize),
	}
}

func (w *CopyWorker) setBusy() {
	if w.idle != nil {
		w.idle.Store(false)
	}
}

func (w *CopyWorker) setIdle() {
	if w.idle != nil {
		w.idle.Store(true)
	}
}

func (w *CopyWorker) shouldRetire() bool {
	return w.retire != nil && w.retire.Load()
}

// Run is the main worker loop. It continuously polls the queue for tasks.
// When a task is found, it leases it, executes it, and reports the result.
// When no work is available or queue is paused, it briefly sleeps before polling again.
// When queue is exhausted, the worker exits.
func (w *CopyWorker) Run() {
	if logservice.LS != nil {
		err := logservice.LS.Log("info", "Copy worker started", "worker", w.id, w.queueName)
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
					err := logservice.LS.Log("info", "Copy worker exiting - shutdown requested", "worker", w.id, w.queueName)
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
					err := logservice.LS.Log("info", "Copy worker exiting - scale down", "worker", w.id, w.queueName)
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
				_ = logservice.LS.Log("info", "Copy worker exiting - scale down retire", "worker", w.id, w.queueName)
			}
			return
		}

		// Check lifecycle state
		if w.queue.IsPaused() {
			// Queue is paused, sleep and continue polling
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if queue is exhausted (copy complete) - exit worker
		if w.queue.IsExhausted() {
			if logservice.LS != nil {
				err := logservice.LS.Log("info", "Copy worker exiting - queue exhausted", "worker", w.id, w.queueName)
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
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.LastError = err.Error()
			if IsThrottleError(err) {
				task.WorkerResult = "rate_limited"
				w.queue.ReportTaskResult(task, TaskExecutionResultRateLimited)
			} else {
				task.WorkerResult = "error"
				if logservice.LS != nil {
					logMsg := fmt.Sprintf("Copy worker task execution failed: path=%s round=%d pass=%d error=%v",
						task.LocationPath(), task.Round, task.CopyPass, err)
					err := logservice.LS.Log("error",
						logMsg,
						"worker", w.id, w.queueName)
					if err != nil {
						fmt.Println("error logging", err)
					}
				}
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
				_ = logservice.LS.Log("info", "Copy worker exiting - scale down retire", "worker", w.id, w.queueName)
			}
			return
		}
	}
}

// execute performs the actual copy work.
// For folders: creates the folder on the destination.
// For files: streams the file from source to destination.
func (w *CopyWorker) execute(task *TaskBase) error {
	parent := w.shutdownCtx
	if parent == nil {
		parent = context.Background()
	}
	wd, ctx := NewProgressWatchdog(parent, copyStallTimeout, w.queue.sealIOWaitActive)
	defer wd.Stop()

	w.queue.WaitInterOp(w.workerCtx)

	// This whole block of code looks like an x-wing fighter from star wars lol...
	switch task.CopyPass {
	case 1:
		if !task.IsFolder() {
			return fmt.Errorf("copy worker received non-folder task in pass 1")
		}
		return w.createFolder(task, ctx, wd)
	case 2:
		if !task.IsFile() {
			return fmt.Errorf("copy worker received non-file task in pass 2")
		}
		return w.copyFile(task, ctx, wd)
	}

	return fmt.Errorf("invalid copy pass: %d", task.CopyPass)
}

// createFolder creates a folder on the destination filesystem.
func (w *CopyWorker) createFolder(task *TaskBase, ctx context.Context, wd *ProgressWatchdog) error {
	folder := task.Folder

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for %s", folder.LocationPath)
	}

	if w.queue.shouldApplyCopyDstResumeExistenceCheck() {
		done, err := w.applyResumeCopyDstFolderPrecheck(task, ctx, wd)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}

	folderName := filepath.Base(folder.LocationPath)
	if folderName == "" || folderName == "." {
		folderName = folder.DisplayName
	}

	wd.Beat()
	done := make(chan error, 1)
	go func() {
		created, err := w.dstAdapter.CreateFolder(ctx, dstParentServiceID, folderName)
		if err != nil {
			done <- fmt.Errorf("failed to create folder %s in parent %s: %w", folder.DisplayName, dstParentServiceID, err)
			return
		}
		task.Folder = created
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}
		wd.Beat()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("create folder cancelled by watchdog for %s: %w", folder.LocationPath, ctx.Err())
	}
}

// copyFile streams a file from source to destination.
// Uses a read/write loop with Beat() so the progress watchdog resets while data is flowing.
func (w *CopyWorker) copyFile(task *TaskBase, ctx context.Context, wd *ProgressWatchdog) error {
	file := task.File

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for file %s", file.LocationPath)
	}

	if w.queue.shouldApplyCopyDstResumeExistenceCheck() {
		skipCopy, err := w.applyResumeCopyDstFilePrecheck(task, ctx, wd)
		if err != nil {
			return err
		}
		if skipCopy {
			return nil
		}
	}

	srcReader, err := w.srcAdapter.OpenRead(ctx, file.ServiceID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("open source cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to open source file %s for reading: %w", file.LocationPath, err)
	}
	defer srcReader.Close()
	wd.Beat()

	fileName := filepath.Base(file.LocationPath)
	if fileName == "" || fileName == "." {
		fileName = file.DisplayName
	}
	srcLocationPath := file.LocationPath
	createMeta := map[string]string{"location_path": srcLocationPath}
	createdFile, err := w.dstAdapter.CreateFile(ctx, dstParentServiceID, fileName, file.Size, createMeta)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("create file cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to create destination file %s in parent %s: %w", fileName, dstParentServiceID, err)
	}
	// All of these beats are making me wanna jam. 
	wd.Beat()

	dstWriter, err := w.dstAdapter.OpenWrite(ctx, createdFile.ServiceID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("open write cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to open destination file %s for writing: %w", createdFile.ServiceID, err)
	}
	wd.Beat()

	var bytesTransferred int64
	buf := w.copyBuffer
	for {
		n, readErr := srcReader.Read(buf)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			_ = dstWriter.Close()
			if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
				return fmt.Errorf("copy cancelled by watchdog for %s: %w", file.LocationPath, readErr)
			}
			return fmt.Errorf("failed to copy file data for %s: %w", file.LocationPath, readErr)
		}
		if n > 0 {
			_, writeErr := dstWriter.Write(buf[:n])
			if writeErr != nil {
				_ = dstWriter.Close()
				if errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded) {
					return fmt.Errorf("copy cancelled by watchdog for %s: %w", file.LocationPath, writeErr)
				}
				return fmt.Errorf("failed to copy file data for %s: %w", file.LocationPath, writeErr)
			}
			bytesTransferred += int64(n)
			wd.Beat()
		}
		if readErr == io.EOF {
			break
		}
	}

	if err := dstWriter.Close(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("commit cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to commit upload for file %s: %w", file.DisplayName, err)
	}

	if committed, ok := dstWriter.(interface{ CommittedServiceID() string }); ok {
		if id := committed.CommittedServiceID(); id != "" {
			createdFile.ServiceID = id
		}
	}

	task.BytesTransferred = bytesTransferred
	createdFile.LocationPath = srcLocationPath
	task.File = createdFile
	return nil
}

// applyResumeCopyDstFolderPrecheck lists the dst parent and short-circuits if the folder already exists
// or errors on name/type clash. Used only when shouldApplyCopyDstResumeExistenceCheck() is true.
func (w *CopyWorker) applyResumeCopyDstFolderPrecheck(task *TaskBase, ctx context.Context, wd *ProgressWatchdog) (done bool, err error) {
	folder := task.Folder
	dstParentServiceID := task.DstParentID
	parentPath, parentDepth, err := copyTaskParentListArgs(task)
	if err != nil {
		return false, err
	}
	aggregated, err := w.listDstChildrenAggregated(dstParentServiceID, parentPath, parentDepth, ctx, wd)
	if err != nil {
		return false, fmt.Errorf("list destination children before folder create for %s: %w", folder.LocationPath, err)
	}
	folderMap, fileMap := copyTaskChildMaps(aggregated, task.Round)
	matchKey := folder.Type + ":" + folder.DisplayName
	if existing, ok := folderMap[matchKey]; ok {
		task.Folder = existing
		wd.Beat()
		return true, nil
	}
	if _, ok := fileMap[types.NodeTypeFile+":"+folder.DisplayName]; ok {
		return false, fmt.Errorf("destination has file %q but task expects folder at %s", folder.DisplayName, folder.LocationPath)
	}
	return false, nil
}

// applyResumeCopyDstFilePrecheck lists the dst parent and skips copy when the file exists and is up to date
// (same mtime rule as traversal dst comparison). Used only when shouldApplyCopyDstResumeExistenceCheck() is true.
func (w *CopyWorker) applyResumeCopyDstFilePrecheck(task *TaskBase, ctx context.Context, wd *ProgressWatchdog) (skipCopy bool, err error) {
	file := task.File
	dstParentServiceID := task.DstParentID
	parentPath, parentDepth, err := copyTaskParentListArgs(task)
	if err != nil {
		return false, err
	}
	aggregated, err := w.listDstChildrenAggregated(dstParentServiceID, parentPath, parentDepth, ctx, wd)
	if err != nil {
		return false, fmt.Errorf("list destination children before file copy for %s: %w", file.LocationPath, err)
	}
	folderMap, fileMap := copyTaskChildMaps(aggregated, task.Round)
	matchKey := file.Type + ":" + file.DisplayName
	if _, ok := folderMap[types.NodeTypeFolder+":"+file.DisplayName]; ok {
		return false, fmt.Errorf("destination has folder %q but task expects file at %s", file.DisplayName, file.LocationPath)
	}
	if existing, ok := fileMap[matchKey]; ok {
		if compareTimestamps(file.LastUpdated, existing.LastUpdated) == "Successful" {
			task.File = existing
			wd.Beat()
			return true, nil
		}
	}
	return false, nil
}

// copyTaskParentListArgs returns parent path and depth for ListChildren on the destination parent,
// matching traversal dst usage (normalized root-relative path; depth = parent level).
func copyTaskParentListArgs(task *TaskBase) (parentPath string, parentDepth int, err error) {
	loc := types.NormalizeLocationPath(task.LocationPath())
	if loc == "" || loc == "/" {
		return "", 0, fmt.Errorf("task missing or root LocationPath")
	}
	parentPath = types.NormalizeLocationPath(filepath.Dir(loc))
	parentDepth = task.Round - 1
	if parentDepth < 0 {
		parentDepth = 0
	}
	return parentPath, parentDepth, nil
}

// copyTaskChildMaps indexes listed dst children by Type+DisplayName (same as traversal dst comparison).
func copyTaskChildMaps(aggregated types.ListResult, childRound int) (map[string]types.Folder, map[string]types.File) {
	folderMap := make(map[string]types.Folder)
	for _, f := range aggregated.Folders {
		f.DepthLevel = childRound
		folderMap[f.Type+":"+f.DisplayName] = f
	}
	fileMap := make(map[string]types.File)
	for _, f := range aggregated.Files {
		f.DepthLevel = childRound
		fileMap[f.Type+":"+f.DisplayName] = f
	}
	return folderMap, fileMap
}

// listDstChildrenAggregated lists immediate children of the dst parent and merges pager pages (traversal pattern).
func (w *CopyWorker) listDstChildrenAggregated(dstParentID, parentPath string, parentDepth int, ctx context.Context, wd *ProgressWatchdog) (types.ListResult, error) {
	type listChildrenResult struct {
		result types.ListResult
		err    error
	}
	listDone := make(chan listChildrenResult, 1)
	depth := parentDepth
	go func() {
		r, err := w.dstAdapter.ListChildren(ctx, dstParentID, &depth, parentPath)
		listDone <- listChildrenResult{result: r, err: err}
	}()

	var shutdownCh <-chan struct{}
	if w.shutdownCtx != nil {
		shutdownCh = w.shutdownCtx.Done()
	}

	var out listChildrenResult
	select {
	case out = <-listDone:
		if out.err != nil {
			return types.ListResult{}, out.err
		}
		wd.Beat()
		w.queue.RecordListFill(len(out.result.Folders) + len(out.result.Files))
	case <-ctx.Done():
		return types.ListResult{}, fmt.Errorf("list destination children cancelled: %w", ctx.Err())
	case <-shutdownCh:
		return types.ListResult{}, fmt.Errorf("list destination children cancelled by shutdown")
	}

	const pageSize = 100
	pager := types.NewListPager(out.result, pageSize)
	var aggregated types.ListResult
	for {
		page, ok := pager.Next()
		if !ok {
			break
		}
		wd.Beat()
		aggregated.Folders = append(aggregated.Folders, page.Folders...)
		aggregated.Files = append(aggregated.Files, page.Files...)
	}
	return aggregated, nil
}

// logError logs a failed task execution.
func (w *CopyWorker) logError(task *TaskBase, paramErr error, willRetry bool) {
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
		fmt.Sprintf("Failed to copy %s: %v (%s)", path, paramErr, retryMsg),
		"worker",
		w.id,
		w.queueName,
	)
	if err != nil {
		fmt.Println("error logging", err)
	}
}
