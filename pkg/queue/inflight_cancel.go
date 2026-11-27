// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// User-cancel grace before hard-cancelling the lease context.
// Grace does not pause for rate limit or seal I/O.
const (
	userCancelGraceCopy      = 30 * time.Second
	userCancelGraceTraversal = 5 * time.Second
)

// userCancelGraceForMode returns the soft-cancel window before hard-cancel.
// Overridable in tests via userCancelGraceOverride.
var userCancelGraceOverride time.Duration

func userCancelGraceForMode(mode QueueMode) time.Duration {
	if userCancelGraceOverride > 0 {
		return userCancelGraceOverride
	}
	switch mode {
	case QueueModeCopy, QueueModeCopyRetry:
		return userCancelGraceCopy
	default:
		return userCancelGraceTraversal
	}
}

// BindLeaseCancel stores a lease-scoped cancel on the in-flight entry.
// If the lease was already hard-cancelled (grace expired before bind), cancel immediately.
func (q *Queue) BindLeaseCancel(nodeID string, cancel context.CancelFunc) {
	if q == nil || nodeID == "" || cancel == nil {
		return
	}
	q.mu.Lock()
	f, ok := q.inProgress[nodeID]
	if !ok {
		q.mu.Unlock()
		return
	}
	f.cancel = cancel
	hard := f.task != nil && f.task.UserCancelHard
	q.inProgress[nodeID] = f
	q.mu.Unlock()
	if hard {
		cancel()
	}
}

// ClearLeaseCancel clears the lease-scoped cancel without invoking it.
func (q *Queue) ClearLeaseCancel(nodeID string) {
	if q == nil || nodeID == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	f, ok := q.inProgress[nodeID]
	if !ok {
		return
	}
	f.cancel = nil
	q.inProgress[nodeID] = f
}

// RequestUserCancel soft-cancels a live lease on this queue (copy, traversal, or delete).
// Sets UserCancelRequested immediately so the worker can exit cooperatively, then after
// a mode-specific grace hard-cancels the lease context and force-fails the task from the
// control plane (same Working-on clear as migration hard-stop abandoning the lease).
func (q *Queue) RequestUserCancel(nodeID string) bool {
	if q == nil || nodeID == "" {
		return false
	}

	q.mu.Lock()
	f, ok := q.inProgress[nodeID]
	if !ok || f.task == nil {
		q.mu.Unlock()
		return false
	}
	already := f.task.UserCancelRequested
	f.task.UserCancelRequested = true
	if f.task.LastError == "" {
		f.task.LastError = "cancelled by user"
	}
	q.inProgress[nodeID] = f
	mode := q.mode
	q.mu.Unlock()

	if already {
		return true
	}

	grace := userCancelGraceForMode(mode)
	go q.hardCancelAfterGrace(nodeID, grace)
	return true
}

func (q *Queue) hardCancelAfterGrace(nodeID string, grace time.Duration) {
	timer := time.NewTimer(grace)
	defer timer.Stop()
	<-timer.C

	q.mu.Lock()
	f, ok := q.inProgress[nodeID]
	if !ok || f.task == nil || !f.task.UserCancelRequested {
		q.mu.Unlock()
		return
	}
	f.task.UserCancelHard = true
	q.inProgress[nodeID] = f
	q.mu.Unlock()

	q.ForceAbortInFlightLease(nodeID, "cancelled by user")
}

// hardDropLease clears Working-on immediately (same as hard stop), then seals the
// failure and replaces the worker off the caller goroutine.
func (q *Queue) hardDropLease(task *TaskBase, lastError string) {
	if q == nil || task == nil || task.ID == "" || !q.HasInProgress(task.ID) {
		return
	}
	task.UserCancelHard = true
	if lastError != "" {
		task.LastError = lastError
	} else if task.LastError == "" {
		task.LastError = "force aborted"
	}
	task.Attempts = q.MaxRetries()

	q.mu.Lock()
	f, ok := q.inProgress[task.ID]
	var cancel context.CancelFunc
	owner := ""
	if ok {
		cancel = f.cancel
		owner = f.owner
		if f.task != nil {
			f.task.UserCancelHard = true
			f.task.LastError = task.LastError
			f.task.Attempts = task.Attempts
		}
		q.inProgress[task.ID] = f
	}
	q.mu.Unlock()

	// Leave the filesystem call immediately. The failure is still published into
	// the seal buffer (overflow while backpressure is on); that publish does not
	// run on this goroutine, so a flush cannot hold the cancel.
	if cancel != nil {
		cancel()
	}
	go q.sealHardDrop(task)

	q.RememberSettledOutcome(task, false)
	q.IncrementRoundStatsCompleted(task.Round)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(task.Round, false)
	q.RemoveInProgress(task.ID)

	if owner != "" {
		go q.forceCheckoutAndReplaceWorker(owner)
	}
}

func (q *Queue) sealHardDrop(task *TaskBase) {
	database := q.Database()
	if database == nil || task == nil || task.ID == "" {
		return
	}
	queueType := GetQueueType(q.Name())
	nodeType := db.NodeTypeFolder
	if task.IsFile() {
		nodeType = db.NodeTypeFile
	}
	ev := db.StatusEvent{
		ID:        task.ID,
		EventTime: time.Now().UnixNano(),
		Depth:     task.Round,
		NodeType:  nodeType,
	}
	switch q.GetMode() {
	case QueueModeCopy, QueueModeCopyRetry:
		ev.CopyStatus = db.CopyStatusFailed
		ev.PrevCopyStatus = task.CopyStatus
	case QueueModeDelete, QueueModeDeleteRetry:
		ev.DeleteStatus = db.DeleteStatusFailed
		ev.PrevDeleteStatus = task.DeleteStatus
	default:
		ev.TraversalStatus = db.StatusFailed
		prev := task.SrcTraversalStatus
		if prev == "" {
			prev = db.StatusPending
		}
		ev.PrevTraversalStatus = prev
	}
	if task.LastError != "" {
		database.AppendTaskError(queueType, string(q.GetMode()), task.ID, task.LastError, task.Attempts, task.DisplayPath)
	}
	database.AppendStatusEvent(queueType, ev, false)
}

// ForceAbortInFlightLease drops the lease immediately, then cancels and replaces the worker.
func (q *Queue) ForceAbortInFlightLease(nodeID, lastError string) {
	if q == nil || nodeID == "" {
		return
	}
	q.mu.RLock()
	f, ok := q.inProgress[nodeID]
	q.mu.RUnlock()
	if !ok || f.task == nil {
		return
	}
	if lastError == "" {
		lastError = "cancelled by user"
	}
	q.hardDropLease(f.task, lastError)
}

// forceCheckoutAndReplaceWorker marks owner for force-checkout, cancels its workerCtx,
// removes the handle, and starts a replacement worker (hard-stop cancels without replace
// because the run is ending).
func (q *Queue) forceCheckoutAndReplaceWorker(workerID string) {
	if q == nil || workerID == "" {
		return
	}
	q.Spin.forceCheckout.Store(workerID, struct{}{})

	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()

	idx := -1
	var old *managedWorker
	for i, h := range q.pool.handles {
		if h != nil && h.id == workerID {
			idx = i
			old = h
			break
		}
	}
	if old == nil {
		return
	}
	old.retire.Store(true)
	if old.cancel != nil {
		old.cancel()
	}

	shutdownCtx := q.shutdownCtx
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(shutdownCtx)
	id := q.pool.nextID
	q.pool.nextID++
	newID := fmt.Sprintf("%s-worker-%d", q.name, id)
	h := &managedWorker{id: newID, cancel: cancel}
	h.idle.Store(true)

	var w Worker
	switch q.pool.kind {
	case PoolCopy:
		if workerHooks.NewCopyWorker == nil {
			cancel()
			return
		}
		w = workerHooks.NewCopyWorker(newID, q, q.pool.copySrcAdapter, q.pool.copyDstAdapter, shutdownCtx, workerCtx, &h.idle, &h.retire)
	case PoolDelete:
		if workerHooks.NewDeleteWorker == nil {
			cancel()
			return
		}
		w = workerHooks.NewDeleteWorker(newID, q, q.pool.deleteSrcAdapter, shutdownCtx, workerCtx, &h.idle, &h.retire)
	default:
		if workerHooks.NewTraversalWorker == nil {
			cancel()
			return
		}
		w = workerHooks.NewTraversalWorker(newID, q, q.pool.traversalAdapter, q.name, shutdownCtx, workerCtx, &h.idle, &h.retire)
	}

	q.pool.handles[idx] = h
	if idx < len(q.workers) {
		q.workers[idx] = w
	} else {
		q.workers = append(q.workers, w)
	}
	go w.Run()
	q.applyWorkerHintLocked()
}

// UserCancelRequested reports whether the leased task asked for user cancel.
func (q *Queue) UserCancelRequested(nodeID string) bool {
	if q == nil || nodeID == "" {
		return false
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	f, ok := q.inProgress[nodeID]
	return ok && f.task != nil && f.task.UserCancelRequested
}
