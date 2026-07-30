// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// Scaling mode strings for operation-based autoscaler profile resolution.
const (
	ScalingModeTraversal   = "traversal"
	ScalingModeRetry       = "retry"
	ScalingModeCopy        = "copy"
	ScalingModeCopyRetry   = "copy-retry"
	ScalingModeDelete      = "delete"
	ScalingModeDeleteRetry = "delete-retry"
)

// ScalingContext describes queue state used to resolve operation-based profiles.
type ScalingContext struct {
	QueueName   string
	Mode        string
	CopyPass    int
	SrcProvider string
	DstProvider string
	SrcGroupID  string
	DstGroupID  string
}

func (q *Queue) applyQueueLocked(fn func()) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	fn()
}

type managedWorker struct {
	cancel context.CancelFunc
	idle   atomic.Bool
	retire atomic.Bool
}

// workerPool holds dynamic worker lifecycle state.
type workerPool struct {
	mu              sync.Mutex
	handles         []*managedWorker
	nextID          int
	traversalAdapter types.FSAdapter
	copySrcAdapter   types.FSAdapter
	copyDstAdapter   types.FSAdapter
	deleteSrcAdapter types.FSAdapter
	isCopy            bool
	isDelete          bool
	listPageSize      atomic.Int64
	interOpDelay      atomic.Int64 // nanoseconds; autoscaler pacing fallback at worker floor
	rateLimitSources  []RateLimitTelemetry
}

func (q *Queue) initWorkerPool(traversalAdapter types.FSAdapter) {
	q.pool.mu.Lock()
	q.pool.traversalAdapter = traversalAdapter
	q.pool.isCopy = false
	q.pool.isDelete = false
	if q.pool.listPageSize.Load() == 0 {
		q.pool.listPageSize.Store(100)
	}
	q.pool.mu.Unlock()
	q.syncAdapterWorkerHint()
}

func (q *Queue) initDeleteWorkerPool(src types.FSAdapter) {
	q.pool.mu.Lock()
	q.pool.deleteSrcAdapter = src
	q.pool.isCopy = false
	q.pool.isDelete = true
	if q.pool.listPageSize.Load() == 0 {
		q.pool.listPageSize.Store(100)
	}
	q.pool.mu.Unlock()
}

func (q *Queue) initCopyWorkerPool(src, dst types.FSAdapter) {
	q.pool.mu.Lock()
	q.pool.copySrcAdapter = src
	q.pool.copyDstAdapter = dst
	q.pool.isCopy = true
	q.pool.isDelete = false
	q.pool.mu.Unlock()
	q.syncCopyAdapterWorkerHints()
}

func (q *Queue) syncAdapterWorkerHint() {
	if q == nil {
		return
	}
	q.pool.mu.Lock()
	n := len(q.pool.handles)
	adapter := q.pool.traversalAdapter
	isCopy := q.pool.isCopy
	q.pool.mu.Unlock()
	if isCopy {
		return
	}
	if h, ok := adapter.(types.FSConcurrencyHint); ok {
		h.SetActiveWorkers(n)
	}
}

func (q *Queue) syncCopyAdapterWorkerHints() {
	if q == nil {
		return
	}
	q.pool.mu.Lock()
	n := len(q.pool.handles)
	src := q.pool.copySrcAdapter
	dst := q.pool.copyDstAdapter
	q.pool.mu.Unlock()
	if h, ok := src.(types.FSConcurrencyHint); ok {
		h.SetActiveWorkers(n)
	}
	if h, ok := dst.(types.FSConcurrencyHint); ok {
		h.SetActiveWorkers(n)
	}
}

// GetInterOpDelay returns the autoscaler pacing delay before FS operations.
func (q *Queue) GetInterOpDelay() time.Duration {
	if q == nil {
		return 0
	}
	ns := q.pool.interOpDelay.Load()
	if ns <= 0 {
		return 0
	}
	return time.Duration(ns)
}

// SetInterOpDelay sets pacing delay before FS operations (0 disables).
func (q *Queue) SetInterOpDelay(d time.Duration) {
	if q == nil {
		return
	}
	if d <= 0 {
		q.pool.interOpDelay.Store(0)
		return
	}
	q.pool.interOpDelay.Store(int64(d))
}

// WaitInterOp sleeps for the configured inter-op delay, respecting ctx cancellation.
func (q *Queue) WaitInterOp(ctx context.Context) {
	if q == nil {
		return
	}
	d := q.GetInterOpDelay()
	if d <= 0 {
		return
	}
	if ctx == nil {
		time.Sleep(d)
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// SetTargetWorkerCount scales worker goroutines up or down safely.
// While provisional freeze is active (spin-down grace), this is a no-op so AIMD
// classifiers keep accumulating but pool size does not change.
func (q *Queue) SetTargetWorkerCount(target int) error {
	if target < 1 {
		return fmt.Errorf("worker count must be at least 1")
	}
	if q.Spin.Freeze.Load() {
		return nil
	}
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	cur := len(q.pool.handles)
	if target == cur {
		return nil
	}
	shutdownCtx := q.ShutdownCtx()
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	if target > cur {
		for i := cur; i < target; i++ {
			workerCtx, cancel := context.WithCancel(shutdownCtx)
			h := &managedWorker{cancel: cancel}
			h.idle.Store(true)
			q.pool.handles = append(q.pool.handles, h)
			id := q.pool.nextID
			q.pool.nextID++
			if q.pool.isCopy {
				if workerHooks.NewCopyWorker == nil {
					return fmt.Errorf("copy worker factory not registered: import codeberg.org/Sylos/Migration-Engine/pkg/queue/worker")
				}
				w := workerHooks.NewCopyWorker(fmt.Sprintf("%s-worker-%d", q.name, id), q, q.pool.copySrcAdapter, q.pool.copyDstAdapter, shutdownCtx, workerCtx, &h.idle, &h.retire)
				q.workers = append(q.workers, w)
				go w.Run()
			} else if q.pool.isDelete {
				if workerHooks.NewDeleteWorker == nil {
					return fmt.Errorf("delete worker factory not registered: import codeberg.org/Sylos/Migration-Engine/pkg/queue/worker")
				}
				w := workerHooks.NewDeleteWorker(fmt.Sprintf("%s-worker-%d", q.name, id), q, q.pool.deleteSrcAdapter, shutdownCtx, workerCtx, &h.idle, &h.retire)
				q.workers = append(q.workers, w)
				go w.Run()
			} else {
				if workerHooks.NewTraversalWorker == nil {
					return fmt.Errorf("traversal worker factory not registered: import codeberg.org/Sylos/Migration-Engine/pkg/queue/worker")
				}
				w := workerHooks.NewTraversalWorker(fmt.Sprintf("%s-worker-%d", q.name, id), q, q.pool.traversalAdapter, q.name, shutdownCtx, workerCtx, &h.idle, &h.retire)
				q.workers = append(q.workers, w)
				go w.Run()
			}
		}
		q.applyWorkerHintLocked()
		return nil
	}
	// Scale down: cancel retiring workers immediately (including busy) so mid-flight
	// ListChildren/transfers abort; mark retire for cooperative exit. FS_THROTTLE also
	// calls ReleaseInFlightOnThrottle to requeue abandoned leases.
	busyRetiring := false
	for i := cur - 1; i >= target; i-- {
		h := q.pool.handles[i]
		if !h.idle.Load() {
			busyRetiring = true
		}
		h.retire.Store(true)
		h.cancel()
	}
	q.pool.handles = q.pool.handles[:target]
	q.workers = q.workers[:target]
	q.applyWorkerHintLocked()
	if busyRetiring {
		// Freeze further actuation for grace; after grace, smallest file workers force-checkout.
		q.EnterProvisionalFreeze(DefaultSpinDownGrace)
	}
	return nil
}

// applyWorkerHintLocked updates FS adapter concurrency hints. Caller must hold q.pool.mu.
func (q *Queue) applyWorkerHintLocked() {
	n := len(q.pool.handles)
	if q.pool.isCopy {
		if h, ok := q.pool.copySrcAdapter.(types.FSConcurrencyHint); ok {
			h.SetActiveWorkers(n)
		}
		if h, ok := q.pool.copyDstAdapter.(types.FSConcurrencyHint); ok {
			h.SetActiveWorkers(n)
		}
		return
	}
	if q.pool.isDelete {
		if h, ok := q.pool.deleteSrcAdapter.(types.FSConcurrencyHint); ok {
			h.SetActiveWorkers(n)
		}
		return
	}
	if h, ok := q.pool.traversalAdapter.(types.FSConcurrencyHint); ok {
		h.SetActiveWorkers(n)
	}
}

// SetLeaseBatchSize updates lease batch size (does not shrink pending buffer cap).
func (q *Queue) SetLeaseBatchSize(n int) {
	if n <= 0 {
		return
	}
	q.mu.Lock()
	q.leaseBatchSize = n
	if cap(q.pendingBuff) < n {
		newBuf := make([]*TaskBase, len(q.pendingBuff), n)
		copy(newBuf, q.pendingBuff)
		q.pendingBuff = newBuf
	}
	q.mu.Unlock()
}

// SetRefillBatchSize updates traversal DB refill batch size.
func (q *Queue) SetRefillBatchSize(n int) {
	if n <= 0 {
		return
	}
	q.mu.Lock()
	q.refillBatchSize = n
	q.mu.Unlock()
}

// GetListPageSize returns the list page size for traversal workers.
func (q *Queue) GetListPageSize() int {
	n := int(q.pool.listPageSize.Load())
	if n <= 0 {
		return 100
	}
	return n
}

// SetListPageSize sets list page size for traversal workers.
func (q *Queue) SetListPageSize(n int) {
	if n <= 0 {
		return
	}
	q.pool.listPageSize.Store(int64(n))
}

// PullLowWM returns 25% of effective lease batch (pull refill watermark).
func (q *Queue) PullLowWM() int {
	lease := q.EffectiveLeaseBatchSize()
	if lease <= 0 {
		return 0
	}
	return lease / 4
}

// AvgExecutionTime returns the rolling average task execution time.
func (q *Queue) AvgExecutionTime() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgExecutionTime
}

func (q *Queue) ConfigureScalingContext(srcProvider, dstProvider, srcGroupID, dstGroupID, pathCheckProfile string, windowsCompat bool) {
	q.applyQueueLocked(func() {
		q.scalingSrcProvider = srcProvider
		q.scalingDstProvider = dstProvider
		q.scalingSrcGroupID = srcGroupID
		q.scalingDstGroupID = dstGroupID
		q.pathCheckProfile = pathCheckProfile
		q.windowsCompat = windowsCompat
	})
}

// ScalingContext returns the current scaling context for autoscaler profile resolution.
func (q *Queue) ScalingContext() ScalingContext {
	if q == nil {
		return ScalingContext{}
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	mode := string(q.mode)
	if mode == "" {
		mode = ScalingModeTraversal
	}
	return ScalingContext{
		QueueName:   q.name,
		Mode:        mode,
		CopyPass:    q.passNumber,
		SrcProvider: q.scalingSrcProvider,
		DstProvider: q.scalingDstProvider,
		SrcGroupID:  q.scalingSrcGroupID,
		DstGroupID:  q.scalingDstGroupID,
	}
}

func (q *Queue) ScalingAdapter(srcSide bool) types.FSAdapter {
	if q == nil {
		return nil
	}
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	if q.pool.isCopy {
		if srcSide {
			return q.pool.copySrcAdapter
		}
		return q.pool.copyDstAdapter
	}
	if srcSide && q.name == "src" {
		return q.pool.traversalAdapter
	}
	if !srcSide && q.name == "dst" {
		return q.pool.traversalAdapter
	}
	return nil
}
