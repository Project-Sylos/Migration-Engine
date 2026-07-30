// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
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
	queue       *queue.Queue
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
	queue *queue.Queue,
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
			// queue.Queue is paused, sleep and continue polling
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
		if w.useFolderBatchLease() {
			w.runFolderBatchTurn()
			if w.shouldRetire() {
				if logservice.LS != nil {
					_ = logservice.LS.Log("info", "Copy worker exiting - scale down retire", "worker", w.id, w.queueName)
				}
				return
			}
			continue
		}
		if w.useFileBatchLease() {
			w.runFileBatchTurn()
			if w.shouldRetire() {
				if logservice.LS != nil {
					_ = logservice.LS.Log("info", "Copy worker exiting - scale down retire", "worker", w.id, w.queueName)
				}
				return
			}
			continue
		}

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
			// File path may already have checkpointed/removed; folder/list abandons still need yield.
			if w.queue.HasInProgress(task.ID) {
				task.Locked = false
				w.queue.RemoveInProgress(task.ID)
				if !w.queue.Spin.AbandonDBOnly.Load() {
					_ = w.queue.Add(task)
				}
			}
			if w.shouldRetire() {
				if logservice.LS != nil {
					_ = logservice.LS.Log("info", "Copy worker exiting - scale down retire after abandon", "worker", w.id, w.queueName)
				}
				return
			}
			continue
		}
		if err != nil {
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.LastError = err.Error()
			if queue.IsThrottleError(err) {
				task.WorkerResult = "rate_limited"
				w.queue.ReportTaskResult(task, queue.TaskExecutionResultRateLimited)
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
				w.queue.ReportTaskResult(task, queue.TaskExecutionResultFailed)
				willRetry := task.Attempts < w.queue.MaxRetries()
				w.logError(task, err, willRetry)
			}
		} else {
			// Mark worker result BEFORE calling ReportTaskResult (for stall diagnostics)
			task.WorkerResult = "success"
			w.queue.ReportTaskResult(task, queue.TaskExecutionResultSuccessful)
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
func (w *CopyWorker) execute(task *queue.TaskBase) error {
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	// Suppress only while seal flush blocks or an FS rate-limit window is open.
	// Idle hung transfers (no Write progress) must still cancel after copyStallTimeout.
	wd, ctx := observe.NewProgressWatchdog(parent, copyStallTimeout, progressStallSuppress(w.queue))
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
func (w *CopyWorker) createFolder(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog) error {
	folder := task.Folder

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for %s", folder.LocationPath)
	}

	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	if w.queue.ShouldApplyCopyDstResumeExistenceCheck() {
		skipCopy, err := w.applyResumeCopyDstFolderPrecheck(task, ctx, wd)
		if err != nil {
			return err
		}
		if skipCopy {
			return nil
		}
	}

	folderName := filepath.Base(folder.LocationPath)
	if folderName == "" || folderName == "." {
		folderName = folder.DisplayName
	}
	if task.ResolvedDstName != "" {
		folderName = db.NormalizeNodeBasename(task.ResolvedDstName)
	}

	wd.Beat()
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	beat := newProgressBeater(w.queue, wd)
	err := awaitErrBeating(ctx, parent, copyStallTimeout, beat, "create folder", folder.LocationPath, func() error {
		created, err := w.dstAdapter.CreateFolder(ctx, dstParentServiceID, folderName, copyTaskCreateMetadata(task))
		if err != nil {
			return fmt.Errorf("failed to create folder %s in parent %s: %w", folder.DisplayName, dstParentServiceID, err)
		}
		applyCopyDstFolderFromAdapter(task, created)
		return nil
	})
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			return errTransferAbandoned
		}
		return err
	}
	wd.Beat()
	return nil
}

// copyFile streams a file from source to destination.
// Uses a read/write loop with Beat() so the progress watchdog resets while data is flowing.
// Honors transfer checkpoints and FSTransferRestartPolicy for stop/scale-down resume.
func (w *CopyWorker) copyFile(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog) error {
	file := task.File

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for file %s", file.LocationPath)
	}

	skipCopy, updateTarget, err := w.applyCopyDstFilePrecheck(task, ctx, wd)
	if err != nil {
		return err
	}
	if skipCopy {
		return nil
	}

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	beat := newProgressBeater(w.queue, wd)
	resumeOffset, err := awaitResultBeating(ctx, parent, copyStallTimeout, beat, "prepare resume", file.LocationPath, func() (int64, error) {
		return queue.PrepareFileTransferResume(ctx, w.queue, w.dstAdapter, task)
	})
	if err != nil {
		return fmt.Errorf("prepare transfer resume for %s: %w", file.LocationPath, err)
	}
	w.queue.SetActiveLeaseSize(w.id, file.Size)
	defer w.queue.ClearActiveLeaseSize(w.id)

	srcReader, err := awaitResultBeating(ctx, parent, copyStallTimeout, beat, "open source", file.LocationPath, func() (io.ReadCloser, error) {
		return w.srcAdapter.OpenRead(ctx, file.ServiceID)
	})
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			mode := queue.TransferAbandonRequeue
			if w.queue.Spin.AbandonDBOnly.Load() {
				mode = queue.TransferAbandonDBOnly
			}
			_ = w.queue.AbandonTransferCheckpoint(ctx, task, resumeOffset, task.XferDstRef, mode)
			return errTransferAbandoned
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("open source cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to open source file %s for reading: %w", file.LocationPath, err)
	}
	defer srcReader.Close()
	wd.Beat()

	if resumeOffset > 0 {
		if err := awaitErrBeating(ctx, parent, copyStallTimeout, beat, "seek source", file.LocationPath, func() error {
			return queue.SeekReaderTo(srcReader, resumeOffset)
		}); err != nil {
			_ = w.queue.ClearTransferCheckpoint(ctx, task)
			return fmt.Errorf("seek source to checkpoint offset %d for %s: %w", resumeOffset, file.LocationPath, err)
		}
	}

	fileName := filepath.Base(file.LocationPath)
	if fileName == "" || fileName == "." {
		fileName = file.DisplayName
	}
	if task.ResolvedDstName != "" {
		fileName = db.NormalizeNodeBasename(task.ResolvedDstName)
	}
	var destFile types.File
	if updateTarget != nil {
		destFile = *updateTarget
	} else if resumeOffset > 0 && task.XferDstRef != "" {
		destFile = types.File{ServiceID: task.XferDstRef, DisplayName: fileName, Type: types.NodeTypeFile}
	} else {
		createMeta := copyTaskCreateMetadata(task)
		destFile, err = awaitResultBeating(ctx, parent, copyStallTimeout, beat, "create file", file.LocationPath, func() (types.File, error) {
			return w.dstAdapter.CreateFile(ctx, dstParentServiceID, fileName, file.Size, createMeta)
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("create file cancelled by watchdog for %s: %w", file.LocationPath, err)
			}
			return fmt.Errorf("failed to create destination file %s in parent %s: %w", fileName, dstParentServiceID, err)
		}
	}
	wd.Beat()

	var dstWriter io.WriteCloser
	dstWriter, err = awaitResultBeating(ctx, parent, copyStallTimeout, beat, "open write", file.LocationPath, func() (io.WriteCloser, error) {
		if resumeOffset > 0 {
			if rw, ok := types.OpenWriteFromOffsetFrom(w.dstAdapter); ok {
				return rw.OpenWriteFromOffset(ctx, destFile.ServiceID, resumeOffset)
			}
			if sw, ok := types.OpenWriteWithSizeFrom(w.dstAdapter); ok {
				return sw.OpenWriteWithSize(ctx, destFile.ServiceID, file.Size)
			}
			return w.dstAdapter.OpenWrite(ctx, destFile.ServiceID)
		}
		if sw, ok := types.OpenWriteWithSizeFrom(w.dstAdapter); ok {
			return sw.OpenWriteWithSize(ctx, destFile.ServiceID, file.Size)
		}
		return w.dstAdapter.OpenWrite(ctx, destFile.ServiceID)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("open write cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to open destination file %s for writing: %w", destFile.ServiceID, err)
	}
	wd.Beat()

	bytesTransferred := resumeOffset
	lastCheckpoint := resumeOffset
	const checkpointEvery = int64(16 << 20) // ~Dropbox chunk size
	buf := w.copyBuffer
	// Checkpoint only; leave in-progress removal to ReportTaskResult/failTask so attempts bump.
	persistCheckpoint := func() {
		if bytesTransferred > 0 {
			_ = w.queue.PersistTransferCheckpoint(context.WithoutCancel(ctx), task, bytesTransferred, destFile.ServiceID)
		}
	}
	dropWriter := func() {
		go func() { _ = dstWriter.Close() }()
		persistCheckpoint()
	}
	for {
		if w.shouldRetire() || w.queue.ForceCheckoutWorker(w.id) {
			go func() { _ = dstWriter.Close() }()
			mode := queue.TransferAbandonRequeue
			if w.queue.Spin.AbandonDBOnly.Load() {
				mode = queue.TransferAbandonDBOnly
			}
			_ = w.queue.AbandonTransferCheckpoint(ctx, task, bytesTransferred, destFile.ServiceID, mode)
			return errTransferAbandoned
		}
		// Read/Write often ignore cancel (e.g. Graph Write holds mu across a hung PUT).
		// Await async so ProgressWatchdog cancel frees the worker after copyStallTimeout.
		// Periodic Beats keep Dropbox/GDrive/Box/Graph/SFTP long ops from looking idle.
		type readOut struct {
			n   int
			err error
		}
		ro, ok := awaitCancellableBeating(ctx, beat, copyStallBeatInterval(copyStallTimeout), func() readOut {
			n, err := srcReader.Read(buf)
			return readOut{n: n, err: err}
		})
		if !ok {
			dropWriter()
			return stallOrAbandoned(ctx, parent, "copy read", file.LocationPath, copyStallTimeout)
		}
		n, readErr := ro.n, ro.err
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			dropWriter()
			if isForceCheckoutAbort(readErr, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
				return errTransferAbandoned
			}
			if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
				return fmt.Errorf("copy cancelled by watchdog for %s: %w", file.LocationPath, readErr)
			}
			return fmt.Errorf("failed to copy file data for %s: %w", file.LocationPath, readErr)
		}
		if n > 0 {
			writeErr, writeOk := awaitCancellableBeating(ctx, beat, copyStallBeatInterval(copyStallTimeout), func() error {
				_, err := dstWriter.Write(buf[:n])
				return err
			})
			if !writeOk {
				dropWriter()
				return stallOrAbandoned(ctx, parent, "copy write", file.LocationPath, copyStallTimeout)
			}
			if writeErr != nil {
				dropWriter()
				if isForceCheckoutAbort(writeErr, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
					return errTransferAbandoned
				}
				if errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded) {
					return fmt.Errorf("copy cancelled by watchdog for %s: %w", file.LocationPath, writeErr)
				}
				return fmt.Errorf("failed to copy file data for %s: %w", file.LocationPath, writeErr)
			}
			bytesTransferred += int64(n)
			w.queue.ReportTaskBytesTransferred(task, bytesTransferred)
			beat()
			if bytesTransferred-lastCheckpoint >= checkpointEvery {
				_ = w.queue.PersistTransferCheckpoint(ctx, task, bytesTransferred, destFile.ServiceID)
				lastCheckpoint = bytesTransferred
			}
		}
		if readErr == io.EOF {
			break
		}
	}

	// Close may wait on pipe upload finish (Dropbox/GDrive) or session commit (Box/Graph).
	closeErr, closeOk := awaitCancellableBeating(ctx, beat, copyStallBeatInterval(copyStallTimeout), func() error {
		return dstWriter.Close()
	})
	if !closeOk {
		persistCheckpoint() // Close already running in awaitCancellable goroutine
		return stallOrAbandoned(ctx, parent, "commit", file.LocationPath, copyStallTimeout)
	}
	if closeErr != nil {
		if errors.Is(closeErr, context.Canceled) || errors.Is(closeErr, context.DeadlineExceeded) {
			return fmt.Errorf("commit cancelled by watchdog for %s: %w", file.LocationPath, closeErr)
		}
		return fmt.Errorf("failed to commit upload for file %s: %w", file.DisplayName, closeErr)
	}

	if committed, ok := dstWriter.(interface{ CommittedServiceID() string }); ok {
		if id := committed.CommittedServiceID(); id != "" {
			destFile.ServiceID = id
		}
	}

	task.BytesTransferred = bytesTransferred
	w.queue.ReportTaskBytesTransferred(task, bytesTransferred)
	_ = w.queue.ClearTransferCheckpoint(ctx, task)
	applyCopyDstFileFromAdapter(task, destFile)
	return nil
}

// errTransferAbandoned is returned when a file copy cooperatively checkpoints and exits.
var errTransferAbandoned = errors.New("transfer abandoned for scale-down or stop")

// errTraversalStalled is returned when ListChildren makes no progress within traversalStallTimeout.
var errTraversalStalled = errors.New("traversal stalled")


// applyResumeCopyDstFolderPrecheck lists the dst parent and short-circuits if the folder already exists
// or errors on name/type clash. Used only when ShouldApplyCopyDstResumeExistenceCheck() is true.
func (w *CopyWorker) applyResumeCopyDstFolderPrecheck(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog) (done bool, err error) {
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
		applyCopyDstFolderFromAdapter(task, existing)
		wd.Beat()
		return true, nil
	}
	if _, ok := fileMap[types.NodeTypeFile+":"+folder.DisplayName]; ok {
		return false, fmt.Errorf("destination has file %q but task expects folder at %s", folder.DisplayName, folder.LocationPath)
	}
	return false, nil
}

// partitionFolderGroupForResumeExistence lists each unique destination parent once and completes
// folders that already exist, returning only tasks that still need CreateFolderBatch.
func (w *CopyWorker) partitionFolderGroupForResumeExistence(ctx context.Context, wd *observe.ProgressWatchdog, group []*queue.TaskBase) []*queue.TaskBase {
	type parentKey struct {
		id    string
		path  string
		depth int
	}
	byParent := make(map[parentKey][]*queue.TaskBase)
	for _, task := range group {
		if task == nil {
			continue
		}
		if task.CopyPass != 1 || !task.IsFolder() {
			reportTaskOutcome(w.queue, task, fmt.Errorf("folder batch leased non-folder or wrong pass task %s", task.LocationPath()))
			continue
		}
		if task.DstParentID == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing DstParentID for %s", task.LocationPath()))
			continue
		}
		parentPath, parentDepth, err := copyTaskParentListArgs(task)
		if err != nil {
			reportTaskOutcome(w.queue, task, err)
			continue
		}
		key := parentKey{id: task.DstParentID, path: parentPath, depth: parentDepth}
		byParent[key] = append(byParent[key], task)
	}

	needCreate := make([]*queue.TaskBase, 0, len(group))
	for key, tasks := range byParent {
		aggregated, err := w.listDstChildrenAggregated(key.id, key.path, key.depth, ctx, wd)
		if err != nil {
			for _, task := range tasks {
				reportTaskOutcome(w.queue, task, fmt.Errorf("list destination children before folder batch for %s: %w", task.LocationPath(), err))
			}
			continue
		}
		childRound := tasks[0].Round
		folderMap, fileMap := copyTaskChildMaps(aggregated, childRound)
		for _, task := range tasks {
			name := task.Folder.DisplayName
			if name == "" {
				name = filepath.Base(task.LocationPath())
			}
			matchKey := task.Folder.Type + ":" + name
			if existing, ok := folderMap[matchKey]; ok {
				applyCopyDstFolderFromAdapter(task, existing)
				wd.Beat()
				reportTaskOutcome(w.queue, task, nil)
				continue
			}
			if _, ok := fileMap[types.NodeTypeFile+":"+name]; ok {
				reportTaskOutcome(w.queue, task, fmt.Errorf("destination has file %q but task expects folder at %s", name, task.LocationPath()))
				continue
			}
			needCreate = append(needCreate, task)
		}
	}
	return needCreate
}

// partitionFileGroupForResumeExistence lists each unique destination parent once, completes
// files that are already up to date, and returns tasks that still need UploadFilesBatch
// (missing or src-newer; adapters overwrite on commit).
func (w *CopyWorker) partitionFileGroupForResumeExistence(ctx context.Context, wd *observe.ProgressWatchdog, group []*queue.TaskBase) []*queue.TaskBase {
	type parentKey struct {
		id    string
		path  string
		depth int
	}
	byParent := make(map[parentKey][]*queue.TaskBase)
	for _, task := range group {
		if task == nil {
			continue
		}
		if task.CopyPass != 2 || !task.IsFile() {
			reportTaskOutcome(w.queue, task, fmt.Errorf("file batch leased non-file or wrong pass task %s", task.LocationPath()))
			continue
		}
		if task.DstParentID == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing DstParentID for %s", task.LocationPath()))
			continue
		}
		parentPath, parentDepth, err := copyTaskParentListArgs(task)
		if err != nil {
			reportTaskOutcome(w.queue, task, err)
			continue
		}
		key := parentKey{id: task.DstParentID, path: parentPath, depth: parentDepth}
		byParent[key] = append(byParent[key], task)
	}

	needUpload := make([]*queue.TaskBase, 0, len(group))
	for key, tasks := range byParent {
		aggregated, err := w.listDstChildrenAggregated(key.id, key.path, key.depth, ctx, wd)
		if err != nil {
			for _, task := range tasks {
				reportTaskOutcome(w.queue, task, fmt.Errorf("list destination children before file batch for %s: %w", task.LocationPath(), err))
			}
			continue
		}
		childRound := tasks[0].Round
		folderMap, fileMap := copyTaskChildMaps(aggregated, childRound)
		for _, task := range tasks {
			name := task.File.DisplayName
			if name == "" {
				name = filepath.Base(task.LocationPath())
			}
			if _, ok := folderMap[types.NodeTypeFolder+":"+name]; ok {
				reportTaskOutcome(w.queue, task, fmt.Errorf("destination has folder %q but task expects file at %s", name, task.LocationPath()))
				continue
			}
			matchKey := task.File.Type + ":" + name
			if existing, ok := fileMap[matchKey]; ok {
				if compareTimestamps(task.File.LastUpdated, existing.LastUpdated) == "Successful" {
					applyCopyDstFileFromAdapter(task, existing)
					wd.Beat()
					reportTaskOutcome(w.queue, task, nil)
					continue
				}
				// Src newer or unparseable timestamps — keep for batch overwrite.
			}
			needUpload = append(needUpload, task)
		}
	}
	return needUpload
}

// applyCopyDstFilePrecheck lists the dst parent and either skips copy when the file is up to date,
// or returns the existing dst file to update in place when src is newer.
func (w *CopyWorker) applyCopyDstFilePrecheck(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog) (skipCopy bool, updateTarget *types.File, err error) {
	file := task.File
	dstParentServiceID := task.DstParentID
	parentPath, parentDepth, err := copyTaskParentListArgs(task)
	if err != nil {
		return false, nil, err
	}
	aggregated, err := w.listDstChildrenAggregated(dstParentServiceID, parentPath, parentDepth, ctx, wd)
	if err != nil {
		return false, nil, fmt.Errorf("list destination children before file copy for %s: %w", file.LocationPath, err)
	}
	folderMap, fileMap := copyTaskChildMaps(aggregated, task.Round)
	matchKey := file.Type + ":" + file.DisplayName
	if _, ok := folderMap[types.NodeTypeFolder+":"+file.DisplayName]; ok {
		return false, nil, fmt.Errorf("destination has folder %q but task expects file at %s", file.DisplayName, file.LocationPath)
	}
	if existing, ok := fileMap[matchKey]; ok {
		if compareTimestamps(file.LastUpdated, existing.LastUpdated) == "Successful" {
			applyCopyDstFileFromAdapter(task, existing)
			wd.Beat()
			return true, nil, nil
		}
		ex := existing
		return false, &ex, nil
	}
	return false, nil, nil
}

// copyTaskParentListArgs returns parent path and depth for ListChildren on the destination parent,
// matching traversal dst usage (normalized root-relative path; depth = parent level).
func copyTaskParentListArgs(task *queue.TaskBase) (parentPath string, parentDepth int, err error) {
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
func (w *CopyWorker) listDstChildrenAggregated(dstParentID, parentPath string, parentDepth int, ctx context.Context, wd *observe.ProgressWatchdog) (types.ListResult, error) {
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	depth := parentDepth
	result, err := awaitResultBeating(ctx, parent, copyStallTimeout, newProgressBeater(w.queue, wd), "list destination children", parentPath, func() (types.ListResult, error) {
		return w.dstAdapter.ListChildren(ctx, dstParentID, &depth, parentPath)
	})
	if err != nil {
		return types.ListResult{}, err
	}
	wd.Beat()
	w.queue.RecordListFill(len(result.Folders) + len(result.Files))

	const pageSize = 100
	pager := types.NewListPager(result, pageSize)
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
func (w *CopyWorker) logError(task *queue.TaskBase, paramErr error, willRetry bool) {
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
