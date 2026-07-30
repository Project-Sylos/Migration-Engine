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

// Queue grace / provisional freeze state (embedded on Queue as Spin).
type SpinDownState struct {
	Freeze          atomic.Bool // provisional freeze: SetTargetWorkerCount no-ops
	AbandonDBOnly   atomic.Bool // stop/soft-suspend: checkpoint without requeue
	graceTimer      *time.Timer
	graceMu         sync.Mutex
	forceCheckout   sync.Map // worker id string -> struct{}
	activeLeaseSize sync.Map // worker id string -> int64 (file bytes; 0 for non-file FS work)
}

// EnterProvisionalFreeze gates scale actuation for d (default 15s). AIMD counters keep accumulating.
// After grace, force-checkouts all tracked busy FS leases (files smallest-first), then lifts the freeze.
// When abandonDBOnly is set (stop/soft-suspend), also cancels busy workers' contexts so blocking
// RPCs (folder batch, list, delete) abort; scale-down relies on retire cancel instead.
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
		q.forceCheckoutBusyWorkers()
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
func (q *Queue) ClearActiveLeaseSize(workerID string) {
	q.Spin.activeLeaseSize.Delete(workerID)
	q.Spin.forceCheckout.Delete(workerID)
}

// ForceCheckoutWorker reports whether this worker id should abort mid-flight (grace force-checkout).
func (q *Queue) ForceCheckoutWorker(workerID string) bool {
	if workerID == "" {
		return false
	}
	_, ok := q.Spin.forceCheckout.Load(workerID)
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
	// Sort ascending by size (simple insertion; N is worker count).
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
}

// RequestForceCheckoutAllWorkersForStop marks every active FS lease for cooperative abandon
// and cancels busy worker contexts (used after stop/soft-suspend grace when drain is incomplete).
func (q *Queue) RequestForceCheckoutAllWorkersForStop() {
	q.forceCheckoutBusyWorkers()
	q.CancelBusyWorkerContexts()
}
