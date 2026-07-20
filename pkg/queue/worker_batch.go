// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
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

// EnsureLeaseBatchSizeAtLeast raises the DuckDB→pendingBuff pull size when below n
// (used when the active FS adapter supports batch mutations).
func (q *Queue) EnsureLeaseBatchSizeAtLeast(n int) {
	if q == nil || n <= 0 {
		return
	}
	if q.EffectiveLeaseBatchSize() >= n {
		return
	}
	if n > maxLeaseBatchSize {
		n = maxLeaseBatchSize
	}
	q.SetLeaseBatchSize(n)
}

func reportTaskOutcome(q *Queue, task *TaskBase, err error) {
	if err != nil {
		task.LastError = err.Error()
		if IsThrottleError(err) {
			task.WorkerResult = "rate_limited"
			q.ReportTaskResult(task, TaskExecutionResultRateLimited)
			return
		}
		task.WorkerResult = "error"
		q.ReportTaskResult(task, TaskExecutionResultFailed)
		return
	}
	task.WorkerResult = "success"
	q.ReportTaskResult(task, TaskExecutionResultSuccessful)
}

// abandonLeasedGroup yields in-flight batch tasks without failure accounting (scale-down / stop).
func abandonLeasedGroup(q *Queue, tasks []*TaskBase) {
	if q == nil {
		return
	}
	dbOnly := q.abandonModeForStop()
	for _, task := range tasks {
		if task == nil || !q.hasInProgress(task.ID) {
			continue
		}
		task.Locked = false
		q.removeInProgress(task.ID)
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

func reportGroupRateLimited(q *Queue, group []*TaskBase, err error) {
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
		q.ReportTaskResult(task, TaskExecutionResultRateLimited)
	}
}

func beatWatchdogWhile(ctx context.Context, wd *ProgressWatchdog, interval time.Duration) (stop func()) {
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

func beatQueueWatchdogWhile(ctx context.Context, wd *QueueWatchdog, interval time.Duration) (stop func()) {
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

func (w *CopyWorker) folderBatchMax() (types.FSCreateFolderBatch, int, bool) {
	b, ok := types.CreateFolderBatchFrom(w.dstAdapter)
	if !ok {
		return nil, 1, false
	}
	maxN := b.CreateFolderBatchMax()
	if maxN <= 0 {
		maxN = types.DefaultCreateFolderBatchMax
	}
	return b, maxN, true
}

func (w *CopyWorker) useFolderBatchLease() bool {
	if w.queue.GetCopyPass() != 1 {
		return false
	}
	// Resume still uses batch create: already-present folders are filtered via per-parent
	// ListChildren (see partitionFolderGroupForResumeExistence), not single CreateFolder.
	_, _, ok := w.folderBatchMax()
	return ok
}

func (w *CopyWorker) runFolderBatchTurn() {
	batch, maxN, ok := w.folderBatchMax()
	if !ok {
		return
	}
	w.queue.EnsureLeaseBatchSizeAtLeast(types.DefaultCreateFolderBatchPullSize)

	group := w.queue.LeaseGroupBudget(LeaseBudgetOpts{MaxCount: maxN})
	if len(group) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}

	w.setBusy()
	defer w.setIdle()
	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := NewProgressWatchdog(parent, folderBatchStallTimeout, w.queue.sealIOWaitActive)
	defer wd.Stop()
	stopBeat := beatWatchdogWhile(ctx, wd, 10*time.Second)
	defer stopBeat()
	// Tasks stay in-progress until the whole batch returns; keep QueueWatchdog from false-stalling.
	if w.queue.watchdog != nil {
		stopQ := beatQueueWatchdogWhile(ctx, w.queue.watchdog, 5*time.Second)
		defer stopQ()
	}

	w.queue.WaitInterOp(w.workerCtx)

	if w.queue.shouldApplyCopyDstResumeExistenceCheck() {
		group = w.partitionFolderGroupForResumeExistence(ctx, wd, group)
		if len(group) == 0 {
			return
		}
	}

	items := make([]types.CreateFolderBatchItem, 0, len(group))
	aligned := make([]*TaskBase, 0, len(group))
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

	results, err := batch.CreateFolderBatch(ctx, items)
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.forceCheckoutWorker(w.id)) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		if IsThrottleError(err) {
			reportGroupRateLimited(w.queue, aligned, err)
			return
		}
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}
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
			// Race: folder appeared between list filter and batch create — adopt existing.
			if w.queue.shouldApplyCopyDstResumeExistenceCheck() || isDstPathConflictError(res.Err) {
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

func (w *CopyWorker) fileBatchMax() (types.FSUploadFilesBatch, int, bool) {
	b, ok := types.UploadFilesBatchFrom(w.dstAdapter)
	if !ok {
		return nil, 1, false
	}
	maxN := b.UploadFilesBatchMax()
	if maxN <= 0 {
		maxN = types.DefaultUploadFilesBatchMax
	}
	return b, maxN, true
}

func (w *CopyWorker) useFileBatchLease() bool {
	if w.queue.GetCopyPass() != 2 {
		return false
	}
	// Resume still uses batch upload: already-up-to-date files are filtered via per-parent
	// ListChildren (see partitionFileGroupForResumeExistence), not single-file copy.
	_, _, ok := w.fileBatchMax()
	return ok
}

const fileBatchStallTimeout = 30 * time.Minute

func (w *CopyWorker) runFileBatchTurn() {
	batch, maxN, ok := w.fileBatchMax()
	if !ok {
		return
	}
	w.queue.EnsureLeaseBatchSizeAtLeast(types.DefaultUploadFilesBatchPullSize)

	group := w.queue.LeaseGroupBudget(LeaseBudgetOpts{MaxCount: maxN, UseByteBudget: true})
	if len(group) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}

	w.setBusy()
	defer w.setIdle()
	var leaseBytes int64
	for _, task := range group {
		if task != nil && task.IsFile() {
			leaseBytes += task.File.Size
		}
	}
	w.queue.SetActiveLeaseSize(w.id, leaseBytes)
	defer w.queue.ClearActiveLeaseSize(w.id)

	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := NewProgressWatchdog(parent, fileBatchStallTimeout, w.queue.sealIOWaitActive)
	defer wd.Stop()
	stopBeat := beatWatchdogWhile(ctx, wd, 10*time.Second)
	defer stopBeat()
	// Tasks stay in-progress until the whole batch returns; keep QueueWatchdog from false-stalling.
	if w.queue.watchdog != nil {
		stopQ := beatQueueWatchdogWhile(ctx, w.queue.watchdog, 5*time.Second)
		defer stopQ()
	}

	w.queue.WaitInterOp(w.workerCtx)

	if w.queue.shouldApplyCopyDstResumeExistenceCheck() {
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
	aligned := make([]*TaskBase, 0, len(group))
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
		resumeOffset, err := prepareFileTransferResume(ctx, w.queue, w.dstAdapter, task)
		if err != nil {
			reportTaskOutcome(w.queue, task, fmt.Errorf("prepare transfer resume for %s: %w", task.LocationPath(), err))
			continue
		}
		name := task.File.DisplayName
		if name == "" {
			name = path.Base(task.LocationPath())
		}
		rc, err := w.srcAdapter.OpenRead(ctx, srcID)
		if err != nil {
			if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.forceCheckoutWorker(w.id)) {
				abandonLeasedGroup(w.queue, group)
				return
			}
			reportTaskOutcome(w.queue, task, fmt.Errorf("open source for batch upload %s: %w", task.LocationPath(), err))
			continue
		}
		if resumeOffset > 0 {
			if seekErr := seekReaderTo(rc, resumeOffset); seekErr != nil {
				_ = rc.Close()
				_ = w.queue.ClearTransferCheckpoint(ctx, task)
				reportTaskOutcome(w.queue, task, fmt.Errorf("seek source for batch resume %s: %w", task.LocationPath(), seekErr))
				continue
			}
		}
		opened = append(opened, rc)
		body := newTransferProgressReader(w.queue, task, rc, resumeOffset)
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

	results, err := batch.UploadFilesBatch(ctx, items)
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.forceCheckoutWorker(w.id)) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		if IsThrottleError(err) {
			reportGroupRateLimited(w.queue, aligned, err)
			return
		}
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}
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
	q      *Queue
	task   *TaskBase
	inner  io.ReadCloser
	offset int64
}

func newTransferProgressReader(q *Queue, task *TaskBase, inner io.ReadCloser, startOffset int64) *transferProgressReader {
	if startOffset < 0 {
		startOffset = 0
	}
	r := &transferProgressReader{q: q, task: task, inner: inner, offset: startOffset}
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

func (w *DeleteWorker) deleteBatchMax() (types.FSDeleteBatch, int, bool) {
	b, ok := types.DeleteBatchFrom(w.srcAdapter)
	if !ok {
		return nil, 1, false
	}
	maxN := b.DeleteBatchMax()
	if maxN <= 0 {
		maxN = types.DefaultDeleteBatchMax
	}
	return b, maxN, true
}

func (w *DeleteWorker) useDeleteBatchLease() bool {
	_, _, ok := w.deleteBatchMax()
	return ok
}

func (w *DeleteWorker) runDeleteBatchTurn() {
	batch, maxN, ok := w.deleteBatchMax()
	if !ok {
		return
	}
	w.queue.EnsureLeaseBatchSizeAtLeast(types.DefaultDeleteBatchPullSize)

	group := w.queue.LeaseGroupBudget(LeaseBudgetOpts{MaxCount: maxN})
	if len(group) == 0 {
		time.Sleep(50 * time.Millisecond)
		return
	}

	w.setBusy()
	defer w.setIdle()
	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	ctx := fsOpContext(w.workerCtx, w.shutdownCtx)
	// Tasks stay in-progress until the whole batch returns; keep QueueWatchdog from false-stalling.
	if w.queue.watchdog != nil {
		stopQ := beatQueueWatchdogWhile(ctx, w.queue.watchdog, 5*time.Second)
		defer stopQ()
	}

	items := make([]types.DeleteBatchItem, 0, len(group))
	aligned := make([]*TaskBase, 0, len(group))
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

	results, err := batch.DeleteBatch(ctx, items)
	if err != nil {
		if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.forceCheckoutWorker(w.id)) {
			abandonLeasedGroup(w.queue, aligned)
			return
		}
		if IsThrottleError(err) {
			reportGroupRateLimited(w.queue, aligned, err)
			return
		}
		for _, task := range aligned {
			reportTaskOutcome(w.queue, task, err)
		}
		return
	}
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
