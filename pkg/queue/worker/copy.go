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
	defer w.queue.NotifyWorkerExit(w.id)
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
		w.queue.BindLeaseOwner(task, w.id)

		setWorkerBusy(w.idle)
		// Execute the task (check for shutdown during execution if needed)
		err := w.execute(task)
		markWorkerIdle(w.queue, w.id, w.idle)
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
			markCopyAlreadyExists(task)
			return nil
		}
	}

	folderName := copyTaskCreateBasename(task)
	if folderName == "" {
		return fmt.Errorf("empty create basename for folder %s", folder.LocationPath)
	}

	wd.Beat()
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	err := awaitErrBeating(ctx, parent, copyStallTimeout, newQueueOnlyBeater(w.queue), "create folder", folder.LocationPath, func() error {
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
// Resume/attempt state is consulted before any DST existence precheck.
func (w *CopyWorker) copyFile(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog) error {
	file := task.File

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for file %s", file.LocationPath)
	}

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	beat := newProgressBeater(w.queue, wd)
	queueBeat := newQueueOnlyBeater(w.queue)
	plan, err := awaitResultBeating(ctx, parent, copyStallTimeout, queueBeat, "prepare resume", file.LocationPath, func() (queue.FileTransferPlan, error) {
		return queue.PrepareFileTransferResume(ctx, w.queue, w.dstAdapter, task)
	})
	if err != nil {
		return fmt.Errorf("prepare transfer resume for %s: %w", file.LocationPath, err)
	}

	var updateTarget *types.File
	// Skip existence precheck whenever this migration already touched the file
	// (resume, attempt marker, or delete-and-restart plan). Prepare may clear the
	// marker after delete; plan.Restart still must suppress already_exists.
	skipPrecheck := plan.ResumeOffset > 0 || plan.ResumeToken != "" || plan.Restart || plan.DstRef != "" || queue.HasCopyAttempt(task)
	if !skipPrecheck {
		skipCopy, target, preErr := w.applyCopyDstFilePrecheck(task, ctx, wd)
		if preErr != nil {
			return preErr
		}
		if skipCopy {
			markCopyAlreadyExists(task)
			return nil
		}
		updateTarget = target
	}

	resumeOffset := plan.ResumeOffset
	w.queue.SetActiveLeaseSize(w.id, file.Size)
	defer w.queue.ClearActiveLeaseSize(w.id)

	srcReader, err := awaitResultBeating(ctx, parent, copyStallTimeout, queueBeat, "open source", file.LocationPath, func() (io.ReadCloser, error) {
		// Stream lifetime uses parent (not ProgressWatchdog) so mid-transfer chunk
		// deadlines own hang protection; Open RPC is still bounded by this await.
		return w.srcAdapter.OpenRead(parent, file.ServiceID)
	})
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			mode := queue.TransferAbandonRequeue
			if w.queue.Spin.AbandonDBOnly.Load() {
				mode = queue.TransferAbandonDBOnly
			}
			_ = w.queue.AbandonTransferCheckpointToken(ctx, task, resumeOffset, task.XferDstRef, "", mode)
			return errTransferAbandoned
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("open source cancelled by watchdog for %s: %w", file.LocationPath, err)
		}
		return fmt.Errorf("failed to open source file %s for reading: %w", file.LocationPath, err)
	}
	defer srcReader.Close()
	wd.Beat()

	fileName := copyTaskCreateBasename(task)
	if fileName == "" {
		return fmt.Errorf("empty create basename for file %s", file.LocationPath)
	}
	var destFile types.File
	var dstWriter io.WriteCloser

	if plan.ResumeToken != "" {
		rw, ok := types.OpenWriteFromResumeTokenFrom(w.dstAdapter)
		if !ok {
			return fmt.Errorf("resume token present but destination does not support OpenWriteFromResumeToken for %s", file.LocationPath)
		}
		type resumeOut struct {
			w   io.WriteCloser
			off int64
		}
		ro, openErr := awaitResultBeating(ctx, parent, copyStallTimeout, queueBeat, "open write resume", file.LocationPath, func() (resumeOut, error) {
			wc, off, e := rw.OpenWriteFromResumeToken(parent, plan.ResumeToken, resumeOffset, file.Size)
			return resumeOut{wc, off}, e
		})
		if openErr != nil {
			return fmt.Errorf("failed to resume destination write for %s: %w", file.LocationPath, openErr)
		}
		dstWriter = ro.w
		resumeOffset = ro.off
		destFile = types.File{ServiceID: task.XferDstRef, DisplayName: fileName, Type: types.NodeTypeFile}
		if destFile.ServiceID == "" {
			destFile.ServiceID = plan.DstRef
		}
	} else {
		if updateTarget != nil {
			destFile = *updateTarget
		} else if task.XferDstRef != "" {
			destFile = types.File{ServiceID: task.XferDstRef, DisplayName: fileName, Type: types.NodeTypeFile}
		} else if plan.DstRef != "" {
			destFile = types.File{ServiceID: plan.DstRef, DisplayName: fileName, Type: types.NodeTypeFile}
		} else {
			createMeta := copyTaskCreateMetadata(task)
			destFile, err = awaitResultBeating(ctx, parent, copyStallTimeout, queueBeat, "create file", file.LocationPath, func() (types.File, error) {
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

		dstWriter, err = awaitResultBeating(ctx, parent, copyStallTimeout, queueBeat, "open write", file.LocationPath, func() (io.WriteCloser, error) {
			if resumeOffset > 0 {
				if rw, ok := types.OpenWriteFromOffsetFrom(w.dstAdapter); ok {
					return rw.OpenWriteFromOffset(parent, destFile.ServiceID, resumeOffset)
				}
			}
			if sw, ok := types.OpenWriteWithSizeFrom(w.dstAdapter); ok {
				return sw.OpenWriteWithSize(parent, destFile.ServiceID, file.Size)
			}
			return w.dstAdapter.OpenWrite(parent, destFile.ServiceID)
		})
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("open write cancelled by watchdog for %s: %w", file.LocationPath, err)
			}
			return fmt.Errorf("failed to open destination file %s for writing: %w", destFile.ServiceID, err)
		}
	}
	wd.Beat()

	// Attempt marker at OpenWrite (offset may still be 0).
	_ = w.queue.PersistTransferCheckpointToken(context.WithoutCancel(ctx), task, resumeOffset, destFile.ServiceID, types.WriterResumeToken(dstWriter))

	if resumeOffset > 0 {
		if err := awaitErrBeating(ctx, parent, copyStallTimeout, queueBeat, "seek source", file.LocationPath, func() error {
			return queue.SeekReaderTo(srcReader, resumeOffset)
		}); err != nil {
			_ = w.queue.ClearResumeState(ctx, task)
			return fmt.Errorf("seek source to checkpoint offset %d for %s: %w", resumeOffset, file.LocationPath, err)
		}
	}

	bytesTransferred := resumeOffset
	lastCheckpoint := resumeOffset
	const checkpointEvery = int64(16 << 20) // ~Dropbox chunk size
	buf := w.copyBuffer
	persistCheckpoint := func() {
		token := types.WriterResumeToken(dstWriter)
		_ = w.queue.PersistTransferCheckpointToken(context.WithoutCancel(ctx), task, bytesTransferred, destFile.ServiceID, token)
	}
	dropWriter := func() {
		go func() { _ = types.SuspendWrite(dstWriter) }()
		persistCheckpoint()
	}
	for {
		if w.shouldRetire() || w.queue.ForceCheckoutWorker(w.id) {
			go func() { _ = types.SuspendWrite(dstWriter) }()
			mode := queue.TransferAbandonRequeue
			if w.queue.Spin.AbandonDBOnly.Load() {
				mode = queue.TransferAbandonDBOnly
			}
			token := types.WriterResumeToken(dstWriter)
			_ = w.queue.AbandonTransferCheckpointToken(ctx, task, bytesTransferred, destFile.ServiceID, token, mode)
			return errTransferAbandoned
		}
		type readOut struct {
			n   int
			err error
		}
		readCtx, readCancel := context.WithTimeout(parent, copyIOReadTimeout)
		ro, ok := awaitCancellableBeating(readCtx, queueBeat, copyStallBeatInterval(copyIOReadTimeout), func() readOut {
			n, err := srcReader.Read(buf)
			return readOut{n: n, err: err}
		})
		readCancel()
		if !ok {
			dropWriter()
			return stallOrAbandoned(readCtx, parent, "copy read", file.LocationPath, copyIOReadTimeout)
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
			writeCtx, writeCancel := context.WithTimeout(parent, copyIOWriteTimeout)
			writeErr, writeOk := awaitCancellableBeating(writeCtx, queueBeat, copyStallBeatInterval(copyIOWriteTimeout), func() error {
				_, err := dstWriter.Write(buf[:n])
				return err
			})
			writeCancel()
			if !writeOk {
				dropWriter()
				return stallOrAbandoned(writeCtx, parent, "copy write", file.LocationPath, copyIOWriteTimeout)
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
				persistCheckpoint()
				lastCheckpoint = bytesTransferred
			}
		}
		if readErr == io.EOF {
			break
		}
	}

	closeWd, closeCtx := observe.NewProgressWatchdog(parent, copyCommitStallTimeout, progressStallSuppress(w.queue))
	defer closeWd.Stop()
	closeErr, closeOk := awaitCancellableBeating(closeCtx, queueBeat, copyStallBeatInterval(copyCommitStallTimeout), func() error {
		return dstWriter.Close()
	})
	if !closeOk {
		persistCheckpoint()
		return stallOrAbandoned(closeCtx, parent, "commit", file.LocationPath, copyCommitStallTimeout)
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
			name := copyTaskCreateBasename(task)
			if name == "" {
				reportTaskOutcome(w.queue, task, fmt.Errorf("empty match basename for folder %s", task.LocationPath()))
				continue
			}
			matchKey := task.Folder.Type + ":" + name
			if existing, ok := folderMap[matchKey]; ok {
				applyCopyDstFolderFromAdapter(task, existing)
				markCopyAlreadyExists(task)
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
			name := copyTaskCreateBasename(task)
			if name == "" {
				reportTaskOutcome(w.queue, task, fmt.Errorf("empty match basename for file %s", task.LocationPath()))
				continue
			}
			if _, ok := folderMap[types.NodeTypeFolder+":"+name]; ok {
				reportTaskOutcome(w.queue, task, fmt.Errorf("destination has folder %q but task expects file at %s", name, task.LocationPath()))
				continue
			}
			matchKey := task.File.Type + ":" + name
			if existing, ok := fileMap[matchKey]; ok {
				_ = w.queue.LoadTransferCheckpointOntoTask(ctx, task)
				if queue.HasCopyAttempt(task) {
					needUpload = append(needUpload, task)
					continue
				}
				if copyFileMatchesDestination(task.File, existing) {
					applyCopyDstFileFromAdapter(task, existing)
					markCopyAlreadyExists(task)
					wd.Beat()
					reportTaskOutcome(w.queue, task, nil)
					continue
				}
				// Src newer, size mismatch, or unparseable timestamps — keep for batch overwrite.
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
		if copyFileMatchesDestination(file, existing) {
			applyCopyDstFileFromAdapter(task, existing)
			wd.Beat()
			return true, nil, nil
		}
		ex := existing
		return false, &ex, nil
	}
	return false, nil, nil
}

func copyFileMatchesDestination(src, dst types.File) bool {
	return src.Size == dst.Size &&
		compareTimestamps(src.LastUpdated, dst.LastUpdated) == "Successful"
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
	result, err := awaitResultBeating(ctx, parent, copyStallTimeout, newQueueOnlyBeater(w.queue), "list destination children", parentPath, func() (types.ListResult, error) {
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
