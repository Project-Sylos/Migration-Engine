// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// DefaultSpinDownGrace is the static v1 grace window before force-checkout
// of busy FS work (adaptive/p95 grace is a documented follow-up).
const DefaultSpinDownGrace = 15 * time.Second

// deferredRetiree is a busy worker removed from the active pool but not yet cancelled.
// It has retire set so it will not lease new work; if it finishes within the grace window
// it exits cleanly. On timeout it is force-checked out.
type deferredRetiree struct {
	handle *managedWorker
}

// Queue grace / provisional freeze state (embedded on Queue as Spin).
type SpinDownState struct {
	Freeze          atomic.Bool // provisional freeze: SetTargetWorkerCount no-ops
	AbandonDBOnly   atomic.Bool // stop/soft-suspend: checkpoint without requeue
	graceTimer      *time.Timer
	graceMu         sync.Mutex
	forceCheckout   sync.Map // worker id string -> struct{}
	activeLeaseSize sync.Map // worker id string -> int64 (file bytes; 0 for non-file FS work)
	// deferredRetire holds busy workers selected for scale-down that are finishing
	// their current task (or will be force-checked out when grace expires).
	deferredRetire map[string]*deferredRetiree
}

// EnterProvisionalFreeze gates scale actuation for d (default 15s). AIMD counters keep accumulating.
// After grace, force-checkouts deferred busy retirees (smallest lease first), then lifts the freeze.
// When abandonDBOnly is set (stop/soft-suspend), also cancels busy workers' contexts so blocking
// RPCs (folder batch, list, delete) abort.
func (q *Queue) EnterProvisionalFreeze(d time.Duration) {
	if d <= 0 {
		d = DefaultSpinDownGrace
	}
	q.Spin.Freeze.Store(true)
	q.Spin.graceMu.Lock()
	if q.Spin.graceTimer != nil {
		q.Spin.graceTimer.Stop()
	}
	q.Spin.graceTimer = time.AfterFunc(d, func() {
		q.forceCheckoutDeferredRetirees()
		if q.Spin.AbandonDBOnly.Load() {
			q.CancelBusyWorkerContexts()
		}
		q.Spin.Freeze.Store(false)
	})
	q.Spin.graceMu.Unlock()
}

// EnterStopAbandonWindow marks abandon-as-DB-only and enters the same provisional freeze.
func (q *Queue) EnterStopAbandonWindow(d time.Duration) {
	q.Spin.AbandonDBOnly.Store(true)
	q.EnterProvisionalFreeze(d)
}

// SetActiveLeaseSize records the worker's current FS lease for force-checkout tracking.
// size is file bytes when known; use 0 for non-file work (folder create, list, delete, batches).
func (q *Queue) SetActiveLeaseSize(workerID string, size int64) {
	if workerID == "" {
		return
	}
	if size < 0 {
		size = 0
	}
	q.Spin.activeLeaseSize.Store(workerID, size)
}

// ClearActiveLeaseSize clears lease size tracking when a worker finishes a turn.
// If the worker is a deferred scale-down retiree that has become idle, this is a no-op
// for retirement claiming (NotifyWorkerExit / TryClaimIdleRetirement handle that).
func (q *Queue) ClearActiveLeaseSize(workerID string) {
	q.Spin.activeLeaseSize.Delete(workerID)
	q.Spin.forceCheckout.Delete(workerID)
}

// TryClaimIdleRetirement is called when a worker becomes idle after finishing a task.
// If that worker was selected for deferred scale-down, cancel its context so it exits
// without leasing new work (retire is already set).
func (q *Queue) TryClaimIdleRetirement(workerID string) {
	if q == nil || workerID == "" {
		return
	}
	q.Spin.graceMu.Lock()
	defer q.Spin.graceMu.Unlock()
	if q.Spin.deferredRetire == nil {
		return
	}
	dr, ok := q.Spin.deferredRetire[workerID]
	if !ok || dr == nil || dr.handle == nil {
		return
	}
	delete(q.Spin.deferredRetire, workerID)
	if dr.handle.cancel != nil {
		dr.handle.cancel()
	}
	if len(q.Spin.deferredRetire) == 0 {
		q.stopGraceTimerLocked()
		q.Spin.Freeze.Store(false)
	}
}

// NotifyWorkerExit clears deferred-retire bookkeeping when a worker goroutine returns.
func (q *Queue) NotifyWorkerExit(workerID string) {
	if q == nil || workerID == "" {
		return
	}
	q.Spin.graceMu.Lock()
	defer q.Spin.graceMu.Unlock()
	if q.Spin.deferredRetire == nil {
		return
	}
	delete(q.Spin.deferredRetire, workerID)
	if len(q.Spin.deferredRetire) == 0 {
		q.stopGraceTimerLocked()
		if !q.Spin.AbandonDBOnly.Load() {
			q.Spin.Freeze.Store(false)
		}
	}
}

func (q *Queue) stopGraceTimerLocked() {
	if q.Spin.graceTimer != nil {
		q.Spin.graceTimer.Stop()
		q.Spin.graceTimer = nil
	}
}

// ForceCheckoutWorker reports whether this worker id should abort mid-flight (grace force-checkout).
func (q *Queue) ForceCheckoutWorker(workerID string) bool {
	if workerID == "" {
		return false
	}
	_, ok := q.Spin.forceCheckout.Load(workerID)
	return ok
}

// trackDeferredRetiree records a busy worker selected for scale-down (retire already set).
// Caller must not hold pool.mu while calling EnterProvisionalFreeze afterward.
func (q *Queue) trackDeferredRetiree(id string, h *managedWorker) {
	if id == "" || h == nil {
		return
	}
	q.Spin.graceMu.Lock()
	defer q.Spin.graceMu.Unlock()
	if q.Spin.deferredRetire == nil {
		q.Spin.deferredRetire = make(map[string]*deferredRetiree)
	}
	q.Spin.deferredRetire[id] = &deferredRetiree{handle: h}
}

// forceCheckoutDeferredRetirees marks deferred busy retirees for cooperative abandon
// (smallest active lease first) and cancels their contexts.
func (q *Queue) forceCheckoutDeferredRetirees() {
	type item struct {
		id   string
		size int64
		h    *managedWorker
	}
	q.Spin.graceMu.Lock()
	deferred := q.Spin.deferredRetire
	q.Spin.deferredRetire = nil
	q.Spin.graceMu.Unlock()
	if len(deferred) == 0 {
		return
	}
	items := make([]item, 0, len(deferred))
	for id, dr := range deferred {
		if dr == nil || dr.handle == nil {
			continue
		}
		size := int64(0)
		if v, ok := q.Spin.activeLeaseSize.Load(id); ok {
			size, _ = v.(int64)
		}
		items = append(items, item{id: id, size: size, h: dr.handle})
	}
	for i := 1; i < len(items); i++ {
		j := i
		for j > 0 && items[j].size < items[j-1].size {
			items[j], items[j-1] = items[j-1], items[j]
			j--
		}
	}
	for _, it := range items {
		q.Spin.forceCheckout.Store(it.id, struct{}{})
		it.h.retire.Store(true)
		if it.h.cancel != nil {
			it.h.cancel()
		}
	}
}

// forceCheckoutBusyWorkers marks every tracked busy FS lease for cooperative abandon.
// Used by stop/soft-suspend paths that must reclaim all in-flight work.
func (q *Queue) forceCheckoutBusyWorkers() {
	type item struct {
		id   string
		size int64
	}
	var items []item
	q.Spin.activeLeaseSize.Range(func(k, v any) bool {
		id, _ := k.(string)
		size, _ := v.(int64)
		if id != "" {
			items = append(items, item{id: id, size: size})
		}
		return true
	})
	if len(items) == 0 {
		return
	}
	for i := 1; i < len(items); i++ {
		j := i
		for j > 0 && items[j].size < items[j-1].size {
			items[j], items[j-1] = items[j-1], items[j]
			j--
		}
	}
	for _, it := range items {
		q.Spin.forceCheckout.Store(it.id, struct{}{})
	}
}

// CancelBusyWorkerContexts cancels workerCtx for every non-idle pool handle so mid-flight
// FS RPCs that prefer workerCtx (batches, list, delete) abort. Idempotent per handle.
func (q *Queue) CancelBusyWorkerContexts() {
	if q == nil {
		return
	}
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	for _, h := range q.pool.handles {
		if h == nil || h.cancel == nil {
			continue
		}
		if h.idle.Load() {
			continue
		}
		h.cancel()
	}
	q.Spin.graceMu.Lock()
	for _, dr := range q.Spin.deferredRetire {
		if dr != nil && dr.handle != nil && dr.handle.cancel != nil && !dr.handle.idle.Load() {
			dr.handle.cancel()
		}
	}
	q.Spin.graceMu.Unlock()
}

// RequestForceCheckoutAllWorkersForStop marks every active FS lease for cooperative abandon
// and cancels busy worker contexts (used after stop/soft-suspend grace when drain is incomplete).
func (q *Queue) RequestForceCheckoutAllWorkersForStop() {
	q.forceCheckoutBusyWorkers()
	q.CancelBusyWorkerContexts()
}
