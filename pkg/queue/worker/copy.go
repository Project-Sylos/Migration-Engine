// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const (
	// Default buffer size for streaming file transfers (64KB)
	defaultCopyBufferSize = 64 * 1024
)

// CopyWorker executes copy tasks by creating folders or streaming files from source to destination.
// Each worker runs independently in its own goroutine, continuously polling the queue for work.
type CopyWorker struct {
	workerBase
	srcAdapter types.FSAdapter
	dstAdapter types.FSAdapter
	queueName  string
	copyBuffer []byte
}

func NewCopyWorker(
	id string,
	q *queue.Queue,
	srcAdapter types.FSAdapter,
	dstAdapter types.FSAdapter,
	shutdownCtx context.Context,
	workerCtx context.Context,
	idle *atomic.Bool,
	retire *atomic.Bool,
) *CopyWorker {
	w := &CopyWorker{
		workerBase: newWorkerBase(id, q, shutdownCtx, workerCtx, idle, retire),
		srcAdapter: srcAdapter,
		dstAdapter: dstAdapter,
		queueName:  "copy",
		copyBuffer: make([]byte, defaultCopyBufferSize),
	}
	w.loop = loopSpec{
		label:     "Copy worker",
		queueName: "copy",
		execute:   w.execute,
		batchTurn: w.batchTurn,
		logFailure: func(task *queue.TaskBase, err error) {
			w.logWorker(w.queueName, "error", fmt.Sprintf("Copy worker task execution failed: path=%s round=%d pass=%d error=%v",
				task.DisplayPath, task.Round, task.CopyPass, err))
			w.logError(task, err, task.Attempts < w.queue.MaxRetries())
		},
	}
	return w
}

// execute performs the actual copy work.
// For folders: creates the folder on the destination.
// For files: streams the file from source to destination.
func (w *CopyWorker) execute(task *queue.TaskBase) (err error) {
	base := fsOpContext(w.workerCtx, w.shutdownCtx)
	parent, leaseCancel := context.WithCancel(base)
	w.queue.BindLeaseCancel(task.ID, leaseCancel)
	defer w.queue.ClearLeaseCancel(task.ID)
	defer func() {
		// Hard user-cancel and stall ForceAbort both set UserCancelHard; scrub DST partial
		// so the next lease takes the scratch path in PrepareFileTransferResume.
		if task.IsFile() && task.UserCancelHard {
			w.scrubHardCancelledPartial(task)
		}
	}()
	if task.UserCancelRequested {
		leaseCancel()
		return errUserCancelled
	}
	// FS-wait Open/Close arms the stall clock; rate-limit pauses it. Byte Beats reset
	// the budget during an open wait. onStall force-fails if the syscall ignores cancel.
	copyTimeout := copyStallBudget(w.queue)
	wd, ctx := observe.NewProgressWatchdog(parent, copyTimeout, progressStallSuppress(w.queue),
		stallOnFail(w.queue, task.ID, copyTimeout, "copy"))
	defer wd.Stop()

	w.queue.WaitInterOp(w.workerCtx)
	if task.UserCancelRequested {
		return errUserCancelled
	}

	// This whole block of code looks like an x-wing fighter from star wars lol...
	switch task.CopyPass {
	case 1:
		if !task.IsFolder() {
			return fmt.Errorf("copy worker received non-folder task in pass 1")
		}
		return w.createFolder(task, ctx, wd, parent)
	case 2:
		if !task.IsFile() {
			return fmt.Errorf("copy worker received non-file task in pass 2")
		}
		return w.copyFile(task, ctx, wd, parent)
	}

	return fmt.Errorf("invalid copy pass: %d", task.CopyPass)
}

// createFolder creates a folder on the destination filesystem.
func (w *CopyWorker) createFolder(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog, parent context.Context) error {
	folder := task.Folder

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for %s", task.DisplayPath)
	}

	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	needsOverwrite := task.DstPeerCopyStatus == db.CopyStatusNeedsOverwrite
	if !needsOverwrite && w.queue.ShouldApplyCopyDstResumeExistenceCheck() && !opsdb.IsForceReplaceMark(task.ExclusionSource) {
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
		return fmt.Errorf("empty create basename for folder %s", task.DisplayPath)
	}

	wd.Beat()
	if task.UserCancelRequested {
		return errUserCancelled
	}
	if needsOverwrite {
		if err := w.deleteMappedDstPeer(task, ctx, parent, wd); err != nil {
			return err
		}
	}
	err := awaitErrBeating(w.queue, ctx, parent, copyStallBudget(w.queue), newQueueOnlyBeater(w.queue), "create folder", task.DisplayPath, wd, func() error {
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
		if task.UserCancelRequested {
			return errUserCancelled
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
func (w *CopyWorker) copyFile(task *queue.TaskBase, ctx context.Context, wd *observe.ProgressWatchdog, parent context.Context) error {
	file := task.File

	dstParentServiceID := task.DstParentID
	if dstParentServiceID == "" {
		return fmt.Errorf("task missing DstParentID (ServiceID) for file %s", task.DisplayPath)
	}

	if task.UserCancelRequested {
		return errUserCancelled
	}
	beat := newProgressBeater(w.queue, wd)
	queueBeat := newQueueOnlyBeater(w.queue)
	plan, err := awaitResultBeating(w.queue, ctx, parent, copyStallBudget(w.queue), queueBeat, "prepare resume", task.DisplayPath, wd, func() (queue.FileTransferPlan, error) {
		return queue.PrepareFileTransferResume(ctx, w.queue, w.dstAdapter, task)
	})
	if err != nil {
		if mapped := mapCopyCancelErr(task, err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)); mapped != nil {
			return mapped
		}
		return fmt.Errorf("prepare transfer resume for %s: %w", task.DisplayPath, err)
	}

	var updateTarget *types.File
	needsOverwrite := task.DstPeerCopyStatus == db.CopyStatusNeedsOverwrite
	// Skip existence precheck whenever this migration already touched the file
	// (resume, attempt marker, or delete-and-restart plan). Prepare may clear the
	// marker after delete; plan.Restart still must suppress already_exists.
	// needs_overwrite always delete-then-create (never in-place updateTarget).
	skipPrecheck := needsOverwrite || plan.ResumeOffset > 0 || plan.ResumeToken != "" || plan.Restart || plan.DstRef != "" || queue.HasCopyAttempt(task) || opsdb.IsForceReplaceMark(task.ExclusionSource)
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

	if logservice.LS != nil {
		_ = logservice.LS.LogStructured("trace",
			fmt.Sprintf("copy start: path=%s size=%d", task.DisplayPath, file.Size),
			"worker", w.id,
			logservice.LogFields{Action: "copy", NodeID: task.ID, Queue: "copy"})
	}
	srcReader, err := awaitResultBeating(w.queue, ctx, parent, copyStallBudget(w.queue), queueBeat, "open source", task.DisplayPath, wd, func() (io.ReadCloser, error) {
		// Stream lifetime uses parent (not ProgressWatchdog) so mid-transfer chunk
		// deadlines own hang protection; Open RPC is still bounded by this await.
		return w.srcAdapter.OpenRead(parent, file.ServiceID)
	})
	if err != nil {
		if mapped := mapCopyCancelErr(task, err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)); mapped != nil {
			if errors.Is(mapped, errTransferAbandoned) {
				mode := queue.TransferAbandonRequeue
				if w.queue.Spin.AbandonDBOnly.Load() {
					mode = queue.TransferAbandonDBOnly
				}
				_ = w.queue.AbandonTransferCheckpointToken(ctx, task, resumeOffset, task.XferDstRef, "", mode)
			}
			return mapped
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("open source cancelled by watchdog for %s: %w", task.DisplayPath, err)
		}
		return fmt.Errorf("failed to open source file %s for reading: %w", task.DisplayPath, err)
	}
	defer srcReader.Close()
	wd.Beat()

	fileName := copyTaskCreateBasename(task)
	if fileName == "" {
		return fmt.Errorf("empty create basename for file %s", task.DisplayPath)
	}
	var destFile types.File
	var dstWriter io.WriteCloser

	if plan.ResumeToken != "" {
		rw, ok := types.OpenWriteFromResumeTokenFrom(w.dstAdapter)
		if !ok {
			return fmt.Errorf("resume token present but destination does not support OpenWriteFromResumeToken for %s", task.DisplayPath)
		}
		type resumeOut struct {
			w   io.WriteCloser
			off int64
		}
		ro, openErr := awaitResultBeating(w.queue, ctx, parent, copyStallBudget(w.queue), queueBeat, "open write resume", task.DisplayPath, wd, func() (resumeOut, error) {
			wc, off, e := rw.OpenWriteFromResumeToken(parent, plan.ResumeToken, resumeOffset, file.Size)
			return resumeOut{wc, off}, e
		})
		if openErr != nil {
			return fmt.Errorf("failed to resume destination write for %s: %w", task.DisplayPath, openErr)
		}
		dstWriter = ro.w
		resumeOffset = ro.off
		destFile = types.File{ServiceID: task.XferDstRef, DisplayName: fileName, Type: types.NodeTypeFile}
		if destFile.ServiceID == "" {
			destFile.ServiceID = plan.DstRef
		}
	} else {
		if needsOverwrite {
			if delErr := w.deleteMappedDstPeer(task, ctx, parent, wd); delErr != nil {
				return delErr
			}
			updateTarget = nil
		}
		if updateTarget != nil {
			destFile = *updateTarget
		} else if task.XferDstRef != "" {
			destFile = types.File{ServiceID: task.XferDstRef, DisplayName: fileName, Type: types.NodeTypeFile}
		} else if plan.DstRef != "" {
			destFile = types.File{ServiceID: plan.DstRef, DisplayName: fileName, Type: types.NodeTypeFile}
		} else {
			createMeta := copyTaskCreateMetadata(task)
			destFile, err = awaitResultBeating(w.queue, ctx, parent, copyStallBudget(w.queue), queueBeat, "create file", task.DisplayPath, wd, func() (types.File, error) {
				return w.dstAdapter.CreateFile(ctx, dstParentServiceID, fileName, file.Size, createMeta)
			})
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("create file cancelled by watchdog for %s: %w", task.DisplayPath, err)
				}
				return fmt.Errorf("failed to create destination file %s in parent %s: %w", fileName, dstParentServiceID, err)
			}
		}
		wd.Beat()

		dstWriter, err = awaitResultBeating(w.queue, ctx, parent, copyStallBudget(w.queue), queueBeat, "open write", task.DisplayPath, wd, func() (io.WriteCloser, error) {
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
				return fmt.Errorf("open write cancelled by watchdog for %s: %w", task.DisplayPath, err)
			}
			return fmt.Errorf("failed to open destination file %s for writing: %w", task.DisplayPath, err)
		}
	}
	wd.Beat()

	// Attempt marker at OpenWrite (offset may still be 0).
	_ = w.queue.PersistTransferCheckpointToken(context.WithoutCancel(ctx), task, resumeOffset, destFile.ServiceID, types.WriterResumeToken(dstWriter))

	if resumeOffset > 0 {
		if err := awaitErrBeating(w.queue, ctx, parent, copyStallBudget(w.queue), queueBeat, "seek source", task.DisplayPath, wd, func() error {
			return queue.SeekReaderTo(srcReader, resumeOffset)
		}); err != nil {
			_ = w.queue.ClearResumeState(ctx, task)
			return fmt.Errorf("seek source to checkpoint offset %d for %s: %w", resumeOffset, task.DisplayPath, err)
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
		if task.UserCancelRequested {
			dropWriter()
			return errUserCancelled
		}
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
		ro, ok := awaitCancellableBeating(w.queue, readCtx, wd, queueBeat, copyStallBeatInterval(copyIOReadTimeout), func() readOut {
			n, err := srcReader.Read(buf)
			return readOut{n: n, err: err}
		})
		readCancel()
		if !ok {
			dropWriter()
			if task.UserCancelRequested {
				return errUserCancelled
			}
			return stallOrAbandoned(readCtx, parent, "copy read", task.DisplayPath, copyIOReadTimeout)
		}
		n, readErr := ro.n, ro.err
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			dropWriter()
			if mapped := mapCopyCancelErr(task, readErr, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)); mapped != nil {
				return mapped
			}
			if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
				return fmt.Errorf("copy cancelled by watchdog for %s: %w", task.DisplayPath, readErr)
			}
			return fmt.Errorf("failed to copy file data for %s: %w", task.DisplayPath, readErr)
		}
		if n > 0 {
			writeCtx, writeCancel := context.WithTimeout(parent, copyIOWriteTimeout)
			writeErr, writeOk := awaitCancellableBeating(w.queue, writeCtx, wd, queueBeat, copyStallBeatInterval(copyIOWriteTimeout), func() error {
				_, err := dstWriter.Write(buf[:n])
				return err
			})
			writeCancel()
			if !writeOk {
				dropWriter()
				if task.UserCancelRequested {
					return errUserCancelled
				}
				return stallOrAbandoned(writeCtx, parent, "copy write", task.DisplayPath, copyIOWriteTimeout)
			}
			if writeErr != nil {
				dropWriter()
				if mapped := mapCopyCancelErr(task, writeErr, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)); mapped != nil {
					return mapped
				}
				if errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded) {
					return fmt.Errorf("copy cancelled by watchdog for %s: %w", task.DisplayPath, writeErr)
				}
				return fmt.Errorf("failed to copy file data for %s: %w", task.DisplayPath, writeErr)
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

	closeWd, closeCtx := observe.NewProgressWatchdog(parent, copyCommitStallTimeout, progressStallSuppress(w.queue),
		stallOnFail(w.queue, task.ID, copyCommitStallTimeout, "commit"))
	defer closeWd.Stop()
	closeErr, closeOk := awaitCancellableBeating(w.queue, closeCtx, closeWd, queueBeat, copyStallBeatInterval(copyCommitStallTimeout), func() error {
		return dstWriter.Close()
	})
	if !closeOk {
		persistCheckpoint()
		return stallOrAbandoned(closeCtx, parent, "commit", task.DisplayPath, copyCommitStallTimeout)
	}
	if closeErr != nil {
		if mapped := mapCopyCancelErr(task, closeErr, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)); mapped != nil {
			return mapped
		}
		if errors.Is(closeErr, context.Canceled) || errors.Is(closeErr, context.DeadlineExceeded) {
			return fmt.Errorf("commit cancelled by watchdog for %s: %w", task.DisplayPath, closeErr)
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

// errUserCancelled is returned when Progress Monitor soft-cancels a live copy-file lease.
var errUserCancelled = errors.New("cancelled by user")

// scrubHardCancelledPartial deletes the partial destination object after a hard
// cancel (user grace expiry or FS stall ForceAbort) so the next attempt takes the
// scratch path in PrepareFileTransferResume.
func (w *CopyWorker) scrubHardCancelledPartial(task *queue.TaskBase) {
	if w == nil || w.dstAdapter == nil || task == nil {
		return
	}
	ref := task.XferDstRef
	if ref == "" {
		ref = task.DstPeerServiceID
	}
	if ref == "" {
		return
	}
	_ = w.dstAdapter.DeleteNode(context.Background(), ref, types.NodeTypeFile)
}

func mapCopyCancelErr(task *queue.TaskBase, err error, retire, forceCheckout bool) error {
	if task != nil && task.UserCancelRequested {
		return errUserCancelled
	}
	if isForceCheckoutAbort(err, retire, forceCheckout) {
		return errTransferAbandoned
	}
	return nil
}

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
		return false, fmt.Errorf("list destination children before folder create for %s: %w", task.DisplayPath, err)
	}
	folderMap, fileMap := copyTaskChildMaps(aggregated, task.Round)
	matchKey := folder.Type + ":" + folder.DisplayName
	if existing, ok := folderMap[matchKey]; ok {
		applyCopyDstFolderFromAdapter(task, existing)
		wd.Beat()
		return true, nil
	}
	if _, ok := fileMap[types.NodeTypeFile+":"+folder.DisplayName]; ok {
		return false, fmt.Errorf("destination has file %q but task expects folder at %s", folder.DisplayName, task.DisplayPath)
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
			reportTaskOutcome(w.queue, task, fmt.Errorf("folder batch leased non-folder or wrong pass task %s", task.DisplayPath))
			continue
		}
		if task.DstParentID == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing DstParentID for %s", task.DisplayPath))
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
				reportTaskOutcome(w.queue, task, fmt.Errorf("list destination children before folder batch for %s: %w", task.DisplayPath, err))
			}
			continue
		}
		childRound := tasks[0].Round
		folderMap, fileMap := copyTaskChildMaps(aggregated, childRound)
		for _, task := range tasks {
			name := copyTaskCreateBasename(task)
			if name == "" {
				reportTaskOutcome(w.queue, task, fmt.Errorf("empty match basename for folder %s", task.DisplayPath))
				continue
			}
			matchKey := task.Folder.Type + ":" + name
			if task.DstPeerCopyStatus == db.CopyStatusNeedsOverwrite {
				needCreate = append(needCreate, task)
				continue
			}
			if existing, ok := folderMap[matchKey]; ok {
				applyCopyDstFolderFromAdapter(task, existing)
				// Force-replace on folders: adopt existing dest (no re-create) and
				// seal as successful so the force mark clears; descendants carry the work.
				if !opsdb.IsForceReplaceMark(task.ExclusionSource) {
					markCopyAlreadyExists(task)
				}
				wd.Beat()
				reportTaskOutcome(w.queue, task, nil)
				continue
			}
			if _, ok := fileMap[types.NodeTypeFile+":"+name]; ok {
				reportTaskOutcome(w.queue, task, fmt.Errorf("destination has file %q but task expects folder at %s", name, task.DisplayPath))
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
			reportTaskOutcome(w.queue, task, fmt.Errorf("file batch leased non-file or wrong pass task %s", task.DisplayPath))
			continue
		}
		if task.DstParentID == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing DstParentID for %s", task.DisplayPath))
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
				reportTaskOutcome(w.queue, task, fmt.Errorf("list destination children before file batch for %s: %w", task.DisplayPath, err))
			}
			continue
		}
		childRound := tasks[0].Round
		folderMap, fileMap := copyTaskChildMaps(aggregated, childRound)
		for _, task := range tasks {
			name := copyTaskCreateBasename(task)
			if name == "" {
				reportTaskOutcome(w.queue, task, fmt.Errorf("empty match basename for file %s", task.DisplayPath))
				continue
			}
			if _, ok := folderMap[types.NodeTypeFolder+":"+name]; ok {
				reportTaskOutcome(w.queue, task, fmt.Errorf("destination has folder %q but task expects file at %s", name, task.DisplayPath))
				continue
			}
			matchKey := task.File.Type + ":" + name
			if task.DstPeerCopyStatus == db.CopyStatusNeedsOverwrite {
				needUpload = append(needUpload, task)
				continue
			}
			if existing, ok := fileMap[matchKey]; ok {
				_ = w.queue.LoadTransferCheckpointOntoTask(ctx, task)
				if queue.HasCopyAttempt(task) || opsdb.IsForceReplaceMark(task.ExclusionSource) {
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

// deleteMappedDstPeer removes the paired DST object before create when copy status is needs_overwrite.
func (w *CopyWorker) deleteMappedDstPeer(task *queue.TaskBase, ctx, parent context.Context, wd *observe.ProgressWatchdog) error {
	serviceID := task.DstPeerServiceID
	if serviceID == "" {
		return fmt.Errorf("needs_overwrite peer missing DstPeerServiceID for %s", task.DisplayPath)
	}
	nodeType := types.NodeTypeFile
	if task.IsFolder() {
		nodeType = types.NodeTypeFolder
	}
	err := awaitErrBeating(w.queue, ctx, parent, copyStallBudget(w.queue), newQueueOnlyBeater(w.queue), "delete dst peer", task.DisplayPath, wd, func() error {
		return w.dstAdapter.DeleteNode(ctx, serviceID, nodeType)
	})
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			return errTransferAbandoned
		}
		return fmt.Errorf("delete destination peer before overwrite for %s: %w", task.DisplayPath, err)
	}
	wd.Beat()
	return nil
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
		return false, nil, fmt.Errorf("list destination children before file copy for %s: %w", task.DisplayPath, err)
	}
	folderMap, fileMap := copyTaskChildMaps(aggregated, task.Round)
	matchKey := file.Type + ":" + file.DisplayName
	if _, ok := folderMap[types.NodeTypeFolder+":"+file.DisplayName]; ok {
		return false, nil, fmt.Errorf("destination has folder %q but task expects file at %s", file.DisplayName, task.DisplayPath)
	}
	if existing, ok := fileMap[matchKey]; ok {
		if w.queue.ShouldCopyResumeRecoveryHashVerify() {
			equal, _, _, herr := hashVerifyPair(
				ctx, w.srcAdapter, w.dstAdapter, file, existing, task.DisplayPath, task.ID, wd, w.queue,
			)
			if herr == nil && equal {
				applyCopyDstFileFromAdapter(task, existing)
				wd.Beat()
				return true, nil, nil
			}
			// Hash mismatch or verify failure: redo the copy (in-place update target).
			ex := existing
			return false, &ex, nil
		}
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
	// Soft-skip only for newest_mtime keep semantics; decide/source_wins enroll
	// needs_overwrite / force-replace so this path is not used for those policies.
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
	result, err := awaitResultBeating(w.queue, ctx, parent, copyStallBudget(w.queue), newQueueOnlyBeater(w.queue), "list destination children", parentPath, wd, func() (types.ListResult, error) {
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
	path := task.DisplayPath
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
