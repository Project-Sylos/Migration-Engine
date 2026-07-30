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
	"path"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const folderBatchStallTimeout = 15 * time.Minute

// File upload batching (FSUploadFilesBatch) collapses upload_session/finish into one finish_batch
// per LeaseGroup; bytes still move via per-file append_v2. It does not reduce transferred bytes.

func reportTaskOutcome(q *queue.Queue, task *queue.TaskBase, err error) {
	if err != nil {
		task.LastError = err.Error()
		if queue.IsThrottleError(err) {
			task.WorkerResult = "rate_limited"
			q.ReportTaskResult(task, queue.TaskExecutionResultRateLimited)
			return
		}
		task.WorkerResult = "error"
		q.ReportTaskResult(task, queue.TaskExecutionResultFailed)
		return
	}
	task.WorkerResult = "success"
	q.ReportTaskResult(task, queue.TaskExecutionResultSuccessful)
}

// abandonLeasedGroup yields in-flight batch tasks without failure accounting (scale-down / stop).
func abandonLeasedGroup(q *queue.Queue, tasks []*queue.TaskBase) {
	if q == nil {
		return
	}
	dbOnly := q.Spin.AbandonDBOnly.Load()
	for _, task := range tasks {
		if task == nil || !q.HasInProgress(task.ID) {
			continue
		}
		task.Locked = false
		q.RemoveInProgress(task.ID)
		if dbOnly {
			continue
		}
		_ = q.Add(task)
	}
}

func isForceCheckoutAbort(err error, retire bool, forceCheckout bool) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Non-cancel errors while retiring / force-checking out: still yield without failure.
	return retire || forceCheckout
}

func isDstPathConflictError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "conflict") || strings.Contains(s, "already_exists") || strings.Contains(s, "already exists")
}

// fsOpContext prefers workerCtx so scale-down / stop force-checkout can abort mid-RPC.
func fsOpContext(workerCtx, shutdownCtx context.Context) context.Context {
	if workerCtx != nil {
		return workerCtx
	}
	if shutdownCtx != nil {
		return shutdownCtx
	}
	return context.Background()
}

func reportGroupRateLimited(q *queue.Queue, group []*queue.TaskBase, err error) {
	msg := "rate limited"
	if err != nil {
		msg = err.Error()
	}
	for _, task := range group {
		if task == nil {
			continue
		}
		task.LastError = msg
		task.WorkerResult = "rate_limited"
		q.ReportTaskResult(task, queue.TaskExecutionResultRateLimited)
	}
}

type progressBeater interface {
	Beat()
}

func beatWhile(ctx context.Context, wd progressBeater, interval time.Duration) (stop func()) {
	if wd == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				wd.Beat()
			}
		}
	}()
	return func() { close(done) }
}

func beatBatchWatchdogs(ctx context.Context, q *queue.Queue, wd *observe.ProgressWatchdog) (stop func()) {
	// Keep both watchdogs alive for Dropbox UploadFilesBatch finish (and similar) where
	// transferProgressReader may go quiet after bodies are consumed but before commit returns.
	var stopProgress, stopQueue func()
	if wd != nil {
		stopProgress = beatWhile(ctx, progressWatchdogBeater{wd}, 5*time.Second)
	}
	if q != nil && q.HasWatchdog() {
		stopQueue = beatWhile(ctx, queueWatchdogBeater{q}, 5*time.Second)
	}
	return func() {
		if stopProgress != nil {
			stopProgress()
		}
		if stopQueue != nil {
			stopQueue()
		}
	}
}

type progressWatchdogBeater struct{ wd *observe.ProgressWatchdog }

func (b progressWatchdogBeater) Beat() {
	if b.wd != nil {
		b.wd.Beat()
	}
}

type queueWatchdogBeater struct{ q *queue.Queue }

func (b queueWatchdogBeater) Beat() { b.q.BeatWatchdog() }

func adapterBatchMax[T any](adapter types.FSAdapter, from func(any) (T, bool), maxN func(T) int, defaultMax int) (T, int, bool) {
	b, ok := from(adapter)
	if !ok {
		var zero T
		return zero, 1, false
	}
	n := maxN(b)
	if n <= 0 {
		n = defaultMax
	}
	return b, n, true
}

func copyBatchLeaseEnabled(pass, requiredPass int, batchOK bool) bool {
	return pass == requiredPass && batchOK
}

func (w *CopyWorker) useFolderBatchLease() bool {
	_, _, ok := adapterBatchMax(w.dstAdapter, types.CreateFolderBatchFrom, func(b types.FSCreateFolderBatch) int {
		return b.CreateFolderBatchMax()
	}, types.DefaultCreateFolderBatchMax)
	return copyBatchLeaseEnabled(w.queue.GetCopyPass(), 1, ok)
}

func (w *CopyWorker) runFolderBatchTurn() {
	batch, maxN, ok := adapterBatchMax(w.dstAdapter, types.CreateFolderBatchFrom, func(b types.FSCreateFolderBatch) int {
		return b.CreateFolderBatchMax()
	}, types.DefaultCreateFolderBatchMax)
	if !ok {
		return
	}
	w.queue.EnsureLeaseBatchSizeAtLeast(types.DefaultCreateFolderBatchPullSize)

	group := w.queue.LeaseGroupBudget(queue.LeaseBudgetOpts{MaxCount: maxN})
	if len(group) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}

	setWorkerBusy(w.idle)
	defer setWorkerIdle(w.idle)
	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := observe.NewProgressWatchdog(parent, folderBatchStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()
	stopBeat := beatBatchWatchdogs(ctx, w.queue, wd)
	defer stopBeat()

	w.queue.WaitInterOp(w.workerCtx)

	if w.queue.ShouldApplyCopyDstResumeExistenceCheck() {
		group = w.partitionFolderGroupForResumeExistence(ctx, wd, group)
		if len(group) == 0 {
			return
		}
	}

	items := make([]types.CreateFolderBatchItem, 0, len(group))
	aligned := make([]*queue.TaskBase, 0, len(group))
	for _, task := range group {
		if task.CopyPass != 1 || !task.IsFolder() {
			reportTaskOutcome(w.queue, task, fmt.Errorf("folder batch leased non-folder or wrong pass task %s", task.LocationPath()))
			continue
		}
		dstParent := task.DstParentID
		if dstParent == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing DstParentID for %s", task.LocationPath()))
			continue
		}
		name := task.Folder.DisplayName
		if name == "" {
			name = path.Base(task.LocationPath())
		}
		items = append(items, types.CreateFolderBatchItem{
			ParentID: dstParent,
			Name:     name,
			Metadata: copyTaskCreateMetadata(task),
		})
		aligned = append(aligned, task)
	}
	if len(items) == 0 {
		return
	}

	results, err := awaitResult(ctx, parent, folderBatchStallTimeout, "create folder batch", "", func() ([]types.CreateFolderBatchEntryResult, error) {
		return batch.CreateFolderBatch(ctx, items)
	})
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		if queue.IsThrottleError(err) {
			reportGroupRateLimited(w.queue, aligned, err)
			return
		}
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}
	wd.Beat()
	if len(results) != len(aligned) {
		err := fmt.Errorf("create folder batch returned %d results for %d tasks", len(results), len(aligned))
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}

	for i, task := range aligned {
		res := results[i]
		if res.Err != nil {
			// Race: folder appeared between list filter and batch create; adopt existing.
			if w.queue.ShouldApplyCopyDstResumeExistenceCheck() || isDstPathConflictError(res.Err) {
				done, preErr := w.applyResumeCopyDstFolderPrecheck(task, ctx, wd)
				if preErr == nil && done {
					reportTaskOutcome(w.queue, task, nil)
					continue
				}
			}
			reportTaskOutcome(w.queue, task, res.Err)
			continue
		}
		applyCopyDstFolderFromAdapter(task, res.Folder)
		reportTaskOutcome(w.queue, task, nil)
	}
}

func (w *CopyWorker) useFileBatchLease() bool {
	_, _, ok := adapterBatchMax(w.dstAdapter, types.UploadFilesBatchFrom, func(b types.FSUploadFilesBatch) int {
		return b.UploadFilesBatchMax()
	}, types.DefaultUploadFilesBatchMax)
	return copyBatchLeaseEnabled(w.queue.GetCopyPass(), 2, ok)
}

const fileBatchStallTimeout = 30 * time.Minute

func (w *CopyWorker) runFileBatchTurn() {
	batch, maxN, ok := adapterBatchMax(w.dstAdapter, types.UploadFilesBatchFrom, func(b types.FSUploadFilesBatch) int {
		return b.UploadFilesBatchMax()
	}, types.DefaultUploadFilesBatchMax)
	if !ok {
		return
	}
	w.queue.EnsureLeaseBatchSizeAtLeast(types.DefaultUploadFilesBatchPullSize)

	group := w.queue.LeaseGroupBudget(queue.LeaseBudgetOpts{MaxCount: maxN, UseByteBudget: true})
	if len(group) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}

	setWorkerBusy(w.idle)
	defer setWorkerIdle(w.idle)
	var leaseBytes int64
	for _, task := range group {
		if task != nil && task.IsFile() {
			leaseBytes += task.File.Size
		}
	}
	w.queue.SetActiveLeaseSize(w.id, leaseBytes)
	defer w.queue.ClearActiveLeaseSize(w.id)

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := observe.NewProgressWatchdog(parent, fileBatchStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()
	stopBeat := beatBatchWatchdogs(ctx, w.queue, wd)
	defer stopBeat()

	w.queue.WaitInterOp(w.workerCtx)

	if w.queue.ShouldApplyCopyDstResumeExistenceCheck() {
		group = w.partitionFileGroupForResumeExistence(ctx, wd, group)
		if len(group) == 0 {
			return
		}
		// Recompute lease bytes after skipping already-present files.
		leaseBytes = 0
		for _, task := range group {
			if task != nil && task.IsFile() {
				leaseBytes += task.File.Size
			}
		}
		w.queue.SetActiveLeaseSize(w.id, leaseBytes)
	}

	items := make([]types.UploadFilesBatchItem, 0, len(group))
	aligned := make([]*queue.TaskBase, 0, len(group))
	opened := make([]io.ReadCloser, 0, len(group))
	defer func() {
		// Close any readers not handed to UploadFilesBatch (e.g. early return after open).
		for _, rc := range opened {
			if rc != nil {
				_ = rc.Close()
			}
		}
	}()

	for _, task := range group {
		if task.CopyPass != 2 || !task.IsFile() {
			reportTaskOutcome(w.queue, task, fmt.Errorf("file batch leased non-file or wrong pass task %s", task.LocationPath()))
			continue
		}
		dstParent := task.DstParentID
		if dstParent == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing DstParentID for %s", task.LocationPath()))
			continue
		}
		srcID := task.File.ServiceID
		if srcID == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("task missing source ServiceID for %s", task.LocationPath()))
			continue
		}
		resumeOffset, err := awaitResult(ctx, parent, fileBatchStallTimeout, "prepare resume", task.LocationPath(), func() (int64, error) {
			return queue.PrepareFileTransferResume(ctx, w.queue, w.dstAdapter, task)
		})
		if err != nil {
			reportTaskOutcome(w.queue, task, fmt.Errorf("prepare transfer resume for %s: %w", task.LocationPath(), err))
			continue
		}
		name := task.File.DisplayName
		if name == "" {
			name = path.Base(task.LocationPath())
		}
		rc, err := awaitResult(ctx, parent, fileBatchStallTimeout, "open source", task.LocationPath(), func() (io.ReadCloser, error) {
			return w.srcAdapter.OpenRead(ctx, srcID)
		})
		if err != nil {
			if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
				abandonLeasedGroup(w.queue, group)
				return
			}
			reportTaskOutcome(w.queue, task, fmt.Errorf("open source for batch upload %s: %w", task.LocationPath(), err))
			continue
		}
		if resumeOffset > 0 {
			if seekErr := awaitErr(ctx, parent, fileBatchStallTimeout, "seek source", task.LocationPath(), func() error {
				return queue.SeekReaderTo(rc, resumeOffset)
			}); seekErr != nil {
				_ = rc.Close()
				_ = w.queue.ClearTransferCheckpoint(ctx, task)
				reportTaskOutcome(w.queue, task, fmt.Errorf("seek source for batch resume %s: %w", task.LocationPath(), seekErr))
				continue
			}
		}
		opened = append(opened, rc)
		body := newTransferProgressReader(w.queue, wd, task, rc, resumeOffset)
		items = append(items, types.UploadFilesBatchItem{
			ParentID: dstParent,
			Name:     name,
			Body:     body,
			Metadata: copyTaskCreateMetadata(task),
		})
		aligned = append(aligned, task)
		// Ownership transferred to batch; clear so defer does not double-close.
		opened[len(opened)-1] = nil
	}
	if len(items) == 0 {
		return
	}

	results, err := awaitResult(ctx, parent, fileBatchStallTimeout, "upload files batch", "", func() ([]types.UploadFilesBatchEntryResult, error) {
		return batch.UploadFilesBatch(ctx, items)
	})
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		if queue.IsThrottleError(err) {
			reportGroupRateLimited(w.queue, aligned, err)
			return
		}
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}
	wd.Beat()
	if len(results) != len(aligned) {
		err := fmt.Errorf("upload files batch returned %d results for %d tasks", len(results), len(aligned))
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}

	for i, task := range aligned {
		res := results[i]
		if res.Err != nil {
			reportTaskOutcome(w.queue, task, res.Err)
			continue
		}
		if task.BytesTransferred < task.File.Size {
			w.queue.ReportTaskBytesTransferred(task, task.File.Size)
		}
		applyCopyDstFileFromAdapter(task, res.File)
		reportTaskOutcome(w.queue, task, nil)
	}
}

// transferProgressReader counts bytes read during batch upload and reports them onto the leased task
// so the observer can show live bytes while UploadFilesBatch is in flight.
type transferProgressReader struct {
	q      *queue.Queue
	wd     *observe.ProgressWatchdog
	task   *queue.TaskBase
	inner  io.ReadCloser
	offset int64
}

func newTransferProgressReader(q *queue.Queue, wd *observe.ProgressWatchdog, task *queue.TaskBase, inner io.ReadCloser, startOffset int64) *transferProgressReader {
	if startOffset < 0 {
		startOffset = 0
	}
	r := &transferProgressReader{q: q, wd: wd, task: task, inner: inner, offset: startOffset}
	if startOffset > 0 && q != nil {
		q.ReportTaskBytesTransferred(task, startOffset)
	}
	return r
}

func (r *transferProgressReader) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		r.offset += int64(n)
		if r.q != nil {
			r.q.ReportTaskBytesTransferred(r.task, r.offset)
			if r.q.HasWatchdog() {
				r.q.BeatWatchdog()
			}
		}
		if r.wd != nil {
			r.wd.Beat()
		}
	}
	return n, err
}

func (r *transferProgressReader) Close() error {
	if r.inner == nil {
		return nil
	}
	return r.inner.Close()
}

func (w *DeleteWorker) useDeleteBatchLease() bool {
	_, _, ok := adapterBatchMax(w.srcAdapter, types.DeleteBatchFrom, func(b types.FSDeleteBatch) int {
		return b.DeleteBatchMax()
	}, types.DefaultDeleteBatchMax)
	return ok
}

func (w *DeleteWorker) runDeleteBatchTurn() {
	batch, maxN, ok := adapterBatchMax(w.srcAdapter, types.DeleteBatchFrom, func(b types.FSDeleteBatch) int {
		return b.DeleteBatchMax()
	}, types.DefaultDeleteBatchMax)
	if !ok {
		return
	}
	w.queue.EnsureLeaseBatchSizeAtLeast(types.DefaultDeleteBatchPullSize)

	group := w.queue.LeaseGroupBudget(queue.LeaseBudgetOpts{MaxCount: maxN})
	if len(group) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}

	setWorkerBusy(w.idle)
	defer setWorkerIdle(w.idle)
	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := observe.NewProgressWatchdog(parent, deleteStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()
	// Queue watchdog only: fake beats quiet STALL DETECTED during a slow batch.
	// Do not fake ProgressWatchdog beats; hung DeleteBatch must cancel after deleteStallTimeout.
	if w.queue.HasWatchdog() {
		stopQ := beatWhile(ctx, queueWatchdogBeater{w.queue}, 5*time.Second)
		defer stopQ()
	}

	items := make([]types.DeleteBatchItem, 0, len(group))
	aligned := make([]*queue.TaskBase, 0, len(group))
	for _, task := range group {
		serviceID := task.Identifier()
		if serviceID == "" {
			reportTaskOutcome(w.queue, task, fmt.Errorf("empty service id for delete task %s", task.LocationPath()))
			continue
		}
		nodeType := types.NodeTypeFile
		if task.IsFolder() {
			nodeType = types.NodeTypeFolder
		}
		items = append(items, types.DeleteBatchItem{NodeID: serviceID, NodeType: nodeType})
		aligned = append(aligned, task)
	}
	if len(items) == 0 {
		return
	}

	out, ok := awaitCancellable(ctx, func() fsOut[[]types.DeleteBatchEntryResult] {
		r, e := batch.DeleteBatch(ctx, items)
		return fsOut[[]types.DeleteBatchEntryResult]{Val: r, Err: e}
	})
	if !ok {
		if parent.Err() != nil || (w.shutdownCtx != nil && w.shutdownCtx.Err() != nil) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		stallErr := fmt.Errorf("delete batch stalled after %s: %w", deleteStallTimeout, ctx.Err())
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, stallErr)
		}
		return
	}
	results, err := out.Val, out.Err
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		if queue.IsThrottleError(err) {
			reportGroupRateLimited(w.queue, aligned, err)
			return
		}
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}
	wd.Beat()
	if len(results) != len(aligned) {
		err := fmt.Errorf("delete batch returned %d results for %d tasks", len(results), len(aligned))
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}

	for i, task := range aligned {
		reportTaskOutcome(w.queue, task, results[i].Err)
	}
}
