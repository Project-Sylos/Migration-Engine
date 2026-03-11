// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
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
	srcAdapter  types.FSAdapter // Source adapter for reading files
	dstAdapter  types.FSAdapter // Destination adapter for writing files/folders
	queueName   string          // "copy" for logging
	shutdownCtx context.Context // Context for shutdown signaling (optional)
	copyBuffer  []byte          // Reusable buffer for streaming
}

// NewCopyWorker creates a worker that executes copy tasks.
// shutdownCtx is optional - if provided, the worker will check for cancellation and exit on shutdown.
func NewCopyWorker(
	id string,
	queue *Queue,
	srcAdapter types.FSAdapter,
	dstAdapter types.FSAdapter,
	shutdownCtx context.Context,
) *CopyWorker {
	return &CopyWorker{
		id:          id,
		queue:       queue,
		srcAdapter:  srcAdapter,
		dstAdapter:  dstAdapter,
		queueName:   "copy",
		shutdownCtx: shutdownCtx,
		copyBuffer:  make([]byte, defaultCopyBufferSize),
	}
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
				// Shutdown triggered - exit immediately
				if logservice.LS != nil {
					err := logservice.LS.Log("info", "Copy worker exiting - shutdown requested", "worker", w.id, w.queueName)
					if err != nil {
						fmt.Println("error logging", err)
					}
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
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.WorkerResult = "error"
			task.LastError = err.Error()
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
		} else {
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.WorkerResult = "success"
			w.queue.ReportTaskResult(task, TaskExecutionResultSuccessful)
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
	wd, ctx := NewProgressWatchdog(parent, copyStallTimeout)
	defer wd.Stop()

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
// CreateFolder does not take context; we run it in a goroutine and select on ctx.Done() for stall detection.
func (w *CopyWorker) createFolder(task *TaskBase, ctx context.Context, wd *ProgressWatchdog) error {
	folder := task.Folder

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for %s", folder.LocationPath)
	}

	folderName := filepath.Base(folder.LocationPath)
	if folderName == "" || folderName == "." {
		folderName = folder.DisplayName
	}

	wd.Beat()
	done := make(chan error, 1)
	go func() {
		created, err := w.dstAdapter.CreateFolder(dstParentServiceID, folderName)
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
	createdFile, err := w.dstAdapter.CreateFile(ctx, dstParentServiceID, fileName, file.Size, nil)
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

	task.BytesTransferred = bytesTransferred
	task.File = createdFile
	return nil
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
