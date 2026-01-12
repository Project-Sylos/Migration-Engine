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
			fmt.Printf("[Copy Worker] TASK FAILED: path=%s, error=%v\n", task.LocationPath(), err)
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug",
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

	// Creating folder on destination
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Creating folder: path=%s dstParentServiceID=%s name=%s", folder.LocationPath, dstParentServiceID, folder.DisplayName),
			"worker", w.id, w.queueName)
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

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Created folder: path=%s newID=%s", createdFolder.LocationPath, createdFolder.ServiceID),
			"worker", w.id, w.queueName)
	}

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

	// Copying file from source to destination
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Copying file: path=%s size=%d dstParentServiceID=%s", file.LocationPath, file.Size, dstParentServiceID),
			"worker", w.id, w.queueName)
	}

	// Step 3: Stream file from SRC to DST using chunked I/O
	// Download from SRC using SRC ServiceID, upload to DST using DST parent ServiceID
	// DownloadFile returns a ChunkedReader (implements io.ReadCloser) for efficient chunk-by-chunk reading
	// UploadFile accepts an io.Reader for streaming uploads
	// We use io.Pipe to connect the download and upload streams, allowing us to track bytes transferred

	// Open download stream from source (returns ChunkedReader)
	downloadStream, err := w.srcAdapter.DownloadFile(file.ServiceID)
	if err != nil {
		return fmt.Errorf("failed to open download stream for %s: %w", file.LocationPath, err)
	}
	defer downloadStream.Close()

	// Create a pipe to stream data from download to upload
	// This allows us to track bytes transferred while streaming chunk-by-chunk
	pr, pw := io.Pipe()

	// Track bytes transferred in a separate goroutine
	// io.CopyBuffer works with ChunkedReader since it implements io.ReadCloser
	// The ChunkedReader's Read() method will handle chunk boundaries internally
	var bytesTransferred int64
	var copyErr error
	done := make(chan struct{})

	go func() {
		defer close(done)
		// Copy from download stream (ChunkedReader) to pipe, tracking bytes
		// ChunkedReader implements io.ReadCloser, so io.CopyBuffer works seamlessly
		bytesTransferred, copyErr = io.CopyBuffer(pw, downloadStream, w.copyBuffer)
		pw.CloseWithError(copyErr)
	}()

	// Upload file to destination (streaming from pipe)
	// UploadFile will read from the pipe, which is being written to by the download goroutine
	// This provides true streaming: chunks flow from source → pipe → destination without buffering entire file
	uploadedFile, err := w.dstAdapter.UploadFile(dstParentServiceID, pr)
	if err != nil {
		// Close pipe to stop the download goroutine
		pr.Close()
		<-done // Wait for download goroutine to finish
		return fmt.Errorf("failed to upload file %s to parent %s: %w", file.DisplayName, dstParentServiceID, err)
	}

	// Wait for download goroutine to finish
	<-done
	if copyErr != nil && copyErr != io.EOF {
		return fmt.Errorf("failed to stream file %s: %w", file.LocationPath, copyErr)
	}

	// Track bytes transferred
	task.BytesTransferred = bytesTransferred

	// Store uploaded file info in task for queue to process on completion
	// The queue will create DST node entry and update join-lookup after task succeeds
	task.File = uploadedFile

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Copied file: path=%s bytes=%d newID=%s", uploadedFile.LocationPath, bytesTransferred, uploadedFile.ServiceID),
			"worker", w.id, w.queueName)
	}

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
