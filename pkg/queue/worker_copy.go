// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
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
	boltDB      *db.DB
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
	boltInstance *db.DB,
	srcAdapter types.FSAdapter,
	dstAdapter types.FSAdapter,
	shutdownCtx context.Context,
) *CopyWorker {
	return &CopyWorker{
		id:          id,
		queue:       queue,
		boltDB:      boltInstance,
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
		_ = logservice.LS.Log("info", "Copy worker started", "worker", w.id, w.queueName)
	}

	for {
		// Check for shutdown first (force exit)
		if w.shutdownCtx != nil {
			select {
			case <-w.shutdownCtx.Done():
				// Shutdown triggered - exit immediately
				if logservice.LS != nil {
					_ = logservice.LS.Log("info", "Copy worker exiting - shutdown requested", "worker", w.id, w.queueName)
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
				_ = logservice.LS.Log("info", "Copy worker exiting - queue exhausted", "worker", w.id, w.queueName)
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
				_ = logservice.LS.Log("error",
					fmt.Sprintf("Copy worker task execution failed: path=%s round=%d pass=%d error=%v",
						task.LocationPath(), task.Round, task.CopyPass, err),
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

// execute performs the actual copy work.
// For folders: creates the folder on the destination.
// For files: streams the file from source to destination.
func (w *CopyWorker) execute(task *TaskBase) error {
	switch task.CopyPass {
	case 1:
		// Pass 1: Create folders
		if !task.IsFolder() {
			return fmt.Errorf("copy worker received non-folder task in pass 1")
		}
		return w.createFolder(task)
	case 2:
		// Pass 2: Copy files
		if !task.IsFile() {
			return fmt.Errorf("copy worker received non-file task in pass 2")
		}
		return w.copyFile(task)
	}

	return fmt.Errorf("invalid copy pass: %d", task.CopyPass)
}

// createFolder creates a folder on the destination filesystem.
func (w *CopyWorker) createFolder(task *TaskBase) error {
	folder := task.Folder

	// Processing folder task

	// Get destination parent folder ServiceID (should already be populated by queue)
	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for %s", folder.LocationPath)
	}

	// Create folder on destination using ServiceID
	// CreateFolder(parentIdentifier string, folderName string) (types.Folder, error)
	// For LocalFS: parentIdentifier is a path, for SpectraFS: parentIdentifier is a ServiceID
	createdFolder, err := w.dstAdapter.CreateFolder(dstParentServiceID, folder.DisplayName)
	if err != nil {
		return fmt.Errorf("failed to create folder %s in parent %s: %w", folder.DisplayName, dstParentServiceID, err)
	}

	// Store created folder info in task for queue to process on completion
	// The queue will create DST node entry and update join-lookup after task succeeds
	task.Folder = createdFolder

	return nil
}

// copyFile streams a file from source to destination.
func (w *CopyWorker) copyFile(task *TaskBase) error {
	file := task.File

	// Processing file task

	// Get destination parent folder ServiceID (should already be populated by queue)
	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for file %s", file.LocationPath)
	}

	// Get context for FS operations (use shutdownCtx if available, otherwise background)
	ctx := w.shutdownCtx
	if ctx == nil {
		ctx = context.Background()
	}

	// Step 1: Open source file for reading
	// OpenRead returns an io.ReadCloser for streaming reads
	srcReader, err := w.srcAdapter.OpenRead(ctx, file.ServiceID)
	if err != nil {
		return fmt.Errorf("failed to open source file %s for reading: %w", file.LocationPath, err)
	}
	defer srcReader.Close()

	// Step 2: Create destination file with metadata
	// CreateFile creates the file metadata and returns a types.File with ServiceID populated
	createdFile, err := w.dstAdapter.CreateFile(ctx, dstParentServiceID, file.DisplayName, file.Size, nil)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s in parent %s: %w", file.DisplayName, dstParentServiceID, err)
	}

	// Step 3: Open destination file for writing
	// OpenWrite returns an io.WriteCloser for streaming writes
	dstWriter, err := w.dstAdapter.OpenWrite(ctx, createdFile.ServiceID)
	if err != nil {
		return fmt.Errorf("failed to open destination file %s for writing: %w", createdFile.ServiceID, err)
	}

	// Step 4: Worker owns the copy loop - stream data directly from source to destination
	// io.CopyBuffer handles the streaming efficiently with our buffer
	bytesTransferred, err := io.CopyBuffer(dstWriter, srcReader, w.copyBuffer)
	if err != nil {
		// Close writer on copy error (may fail, but we already have the copy error)
		_ = dstWriter.Close()
		return fmt.Errorf("failed to copy file data for %s: %w", file.LocationPath, err)
	}

	// Step 5: Close writer to commit the upload
	// Close() finalizes the upload - if it fails, the upload failed
	// This is NOT deferred because we need to check the error to know if upload succeeded
	if err := dstWriter.Close(); err != nil {
		return fmt.Errorf("failed to commit upload for file %s: %w", file.DisplayName, err)
	}

	// Track bytes transferred
	task.BytesTransferred = bytesTransferred

	// Store created file info in task for queue to process on completion
	// The queue will create DST node entry and update join-lookup after task succeeds
	task.File = createdFile

	return nil
}

// logError logs a failed task execution.
func (w *CopyWorker) logError(task *TaskBase, err error, willRetry bool) {
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
		fmt.Sprintf("Failed to copy %s: %v (%s)", path, err, retryMsg),
		"worker",
		w.id,
		w.queueName,
	)
}
