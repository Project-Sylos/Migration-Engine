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

// Queue grace / provisional freeze state (embedded conceptually via fields on Queue).
type spinDownState struct {
	freeze          atomic.Bool // provisional freeze: SetTargetWorkerCount no-ops
	abandonDBOnly   atomic.Bool // stop/soft-suspend: checkpoint without requeue
	graceTimer      *time.Timer
	graceMu         sync.Mutex
	forceCheckout   sync.Map // worker id string -> struct{}
	activeLeaseSize sync.Map // worker id string -> int64 (file bytes; 0 for non-file FS work)
}

// PoolSizeFrozen reports whether SetTargetWorkerCount is gated (provisional freeze).
func (q *Queue) PoolSizeFrozen() bool {
	return q.spin.freeze.Load()
}

// EnterProvisionalFreeze gates scale actuation for d (default 15s). AIMD counters keep accumulating.
// After grace, force-checkouts all tracked busy FS leases (files smallest-first), then lifts the freeze.
// When abandonDBOnly is set (stop/soft-suspend), also cancels busy workers' contexts so blocking
// RPCs (folder batch, list, delete) abort; scale-down relies on retire cancel instead.
func (q *Queue) EnterProvisionalFreeze(d time.Duration) {
	if d <= 0 {
		d = DefaultSpinDownGrace
	}
	q.spin.freeze.Store(true)
	q.spin.graceMu.Lock()
	if q.spin.graceTimer != nil {
		q.spin.graceTimer.Stop()
	}
	q.spin.graceTimer = time.AfterFunc(d, func() {
		q.forceCheckoutBusyWorkers()
		if q.spin.abandonDBOnly.Load() {
			q.CancelBusyWorkerContexts()
		}
		q.spin.freeze.Store(false)
	})
	q.spin.graceMu.Unlock()
}

// EnterStopAbandonWindow marks abandon-as-DB-only and enters the same provisional freeze.
func (q *Queue) EnterStopAbandonWindow(d time.Duration) {
	q.spin.abandonDBOnly.Store(true)
	q.EnterProvisionalFreeze(d)
}

// ClearStopAbandonWindow clears DB-only abandon mode after drain completes.
func (q *Queue) ClearStopAbandonWindow() {
	q.spin.abandonDBOnly.Store(false)
}

func (q *Queue) abandonModeForStop() bool {
	return q.spin.abandonDBOnly.Load()
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
	q.spin.activeLeaseSize.Store(workerID, size)
}

// ClearActiveLeaseSize clears lease size tracking when a worker finishes a turn.
func (q *Queue) ClearActiveLeaseSize(workerID string) {
	q.spin.activeLeaseSize.Delete(workerID)
	q.spin.forceCheckout.Delete(workerID)
}

func (q *Queue) forceCheckoutWorker(workerID string) bool {
	if workerID == "" {
		return false
	}
	_, ok := q.spin.forceCheckout.Load(workerID)
	return ok
}

// forceCheckoutBusyWorkers marks every tracked busy FS lease for cooperative abandon.
// Ordering is ascending by size so small file transfers yield first; size 0 (non-file) leads.
func (q *Queue) forceCheckoutBusyWorkers() {
	type item struct {
		id   string
		size int64
	}
	var items []item
	q.spin.activeLeaseSize.Range(func(k, v any) bool {
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
	// Sort ascending by size (simple insertion; N is worker count).
	for i := 1; i < len(items); i++ {
		j := i
		for j > 0 && items[j].size < items[j-1].size {
			items[j], items[j-1] = items[j-1], items[j]
			j--
		}
	}
	for _, it := range items {
		q.spin.forceCheckout.Store(it.id, struct{}{})
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
}

// RequestForceCheckoutAllWorkersForStop marks every active FS lease for cooperative abandon
// and cancels busy worker contexts (used after stop/soft-suspend grace when drain is incomplete).
func (q *Queue) RequestForceCheckoutAllWorkersForStop() {
	q.forceCheckoutBusyWorkers()
	q.CancelBusyWorkerContexts()
}

// RequestForceCheckoutAllFileWorkersForStop is retained for callers; it force-checkouts all FS ops.
func (q *Queue) RequestForceCheckoutAllFileWorkersForStop() {
	q.RequestForceCheckoutAllWorkersForStop()
}

// RequestForceCheckoutForTest marks a worker for cooperative abandon (unit tests).
func (q *Queue) RequestForceCheckoutForTest(workerID string) {
	q.spin.forceCheckout.Store(workerID, struct{}{})
}
