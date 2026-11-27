// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"context"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

const (
	defaultQueueStallTimeout   = 30 * time.Second
	queueWatchdogCheckInterval = 5 * time.Second
)

// ProgressWatchdog cancels a task when an FS wait stays open longer than timeout of
// active (non-suppressed) time. Open starts the clock; Close stops it. Beat resets the
// budget during an open wait (byte progress). Rate-limit suppress pauses the budget
// without resetting it: 10s FS wait + 60s rate-limit leaves ~50s of a 60s clock.
// Time outside Open/Close (CPU, seal publish, worker bookkeeping) does not count.
type ProgressWatchdog struct {
	ctx           context.Context
	cancel        context.CancelFunc
	timeout       time.Duration
	stallSuppress func() bool // when non-nil and true, active elapsed does not advance
	onStall       func()      // optional control-plane abort (force-fail lease / replace worker)
	armed         atomic.Bool // true only between Open and Close
	activeNs      atomic.Int64 // accumulated non-suppressed ns since last Beat while armed
	lastTickNs    atomic.Int64 // UnixNano of last monitor sample
}

// NewProgressWatchdog creates a watchdog and a child context. The stall clock runs only
// while Open has been called and Close has not (FS await bracket). When no Beat() occurs
// within timeout of active FS time, the context is cancelled.
// If stallSuppress is non-nil and returns true, that wall interval is excluded from the budget.
// If onStall is non-nil, it runs after the context cancel (e.g. control-plane force-fail).
func NewProgressWatchdog(parent context.Context, timeout time.Duration, stallSuppress func() bool, onStall func()) (*ProgressWatchdog, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	wd := &ProgressWatchdog{
		ctx:           ctx,
		cancel:        cancel,
		timeout:       timeout,
		stallSuppress: stallSuppress,
		onStall:       onStall,
	}
	wd.lastTickNs.Store(time.Now().UnixNano())
	go wd.monitor()
	return wd, ctx
}

// Open arms the FS-wait stall clock and resets the active budget. Call at the start of
// an adapter await; Close when the await returns (do not wait for a hung syscall).
func (w *ProgressWatchdog) Open() {
	if w == nil {
		return
	}
	now := time.Now().UnixNano()
	w.activeNs.Store(0)
	w.lastTickNs.Store(now)
	w.armed.Store(true)
}

// Close disarms the FS-wait stall clock. Safe after Stop or when never Opened.
func (w *ProgressWatchdog) Close() {
	if w == nil {
		return
	}
	w.armed.Store(false)
	w.activeNs.Store(0)
	w.lastTickNs.Store(time.Now().UnixNano())
}

// Beat records progress during an open FS wait and resets the active-time stall budget.
func (w *ProgressWatchdog) Beat() {
	if w == nil {
		return
	}
	now := time.Now().UnixNano()
	w.activeNs.Store(0)
	w.lastTickNs.Store(now)
}

// Stop cancels the context and stops the monitor goroutine.
func (w *ProgressWatchdog) Stop() {
	w.armed.Store(false)
	w.cancel()
}

func (w *ProgressWatchdog) monitor() {
	// Sub-second ticks so short timeouts (tests) and remaining budgets stay accurate.
	tick := time.Second
	if w.timeout > 0 && w.timeout < 2*time.Second {
		tick = 20 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			prev := w.lastTickNs.Swap(now)
			if prev <= 0 || !w.armed.Load() {
				continue
			}
			delta := now - prev
			if delta < 0 {
				continue
			}
			if w.timeout <= 0 {
				continue // 0 = unlimited: Open/Close still bracket BusyFS, never force-fail
			}
			if w.stallSuppress == nil || !w.stallSuppress() {
				active := w.activeNs.Add(delta)
				if time.Duration(active) > w.timeout {
					w.armed.Store(false)
					w.cancel()
					if w.onStall != nil {
						w.onStall()
					}
					return
				}
			}
		}
	}
}

// QueueWatchdog monitors queue progress and dumps state when stalled.
// A user-facing stall is detected when no tasks complete for the configured
// timeout while tasks remain in-progress or pending. Idle queues that have
// drained the current round (partial pull, empty buffers) are not flagged.
// Active FS rate-limit windows and completion growth reset the timer (not stalls).
type QueueWatchdog struct {
	queue         *queue.Queue
	stallTimeout  time.Duration
	lastProgress  atomic.Int64 // UnixNano of last progress
	lastCompleted atomic.Int64 // last observed round Completed count
	possibleStall atomic.Bool
	stopCh        chan struct{}
	stopped       atomic.Bool
}

// NewQueueWatchdog creates a watchdog for the given queue.
func NewQueueWatchdog(q *queue.Queue, stallTimeout time.Duration) *QueueWatchdog {
	if stallTimeout <= 0 {
		stallTimeout = defaultQueueStallTimeout
	}
	wd := &QueueWatchdog{
		queue:        q,
		stallTimeout: stallTimeout,
		stopCh:       make(chan struct{}),
	}
	wd.lastProgress.Store(time.Now().UnixNano())
	return wd
}

// Beat records progress (call when a task completes).
func (wd *QueueWatchdog) Beat() {
	wd.lastProgress.Store(time.Now().UnixNano())
	wd.possibleStall.Store(false)
}

// PossibleStall reports whether the watchdog recently detected a stall.
func (wd *QueueWatchdog) PossibleStall() bool {
	if wd == nil {
		return false
	}
	return wd.possibleStall.Load()
}

// Start begins monitoring in a background goroutine.
func (wd *QueueWatchdog) Start() {
	go wd.monitor()
}

// Stop stops the watchdog monitoring.
func (wd *QueueWatchdog) Stop() {
	if wd.stopped.CompareAndSwap(false, true) {
		close(wd.stopCh)
	}
}

func (wd *QueueWatchdog) monitor() {
	ticker := time.NewTicker(queueWatchdogCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wd.stopCh:
			return
		case <-ticker.C:
			wd.checkForStall()
		}
	}
}

func (wd *QueueWatchdog) checkForStall() {
	if wd.queue.State() == queue.QueueStatePaused {
		wd.lastProgress.Store(time.Now().UnixNano())
		return
	}
	if wd.queue.SealIOWaitActive() {
		wd.lastProgress.Store(time.Now().UnixNano())
		return
	}
	// Rate-limit windows are expected idle time, not a deadlock.
	if wd.queue.IsRateLimitActive() {
		wd.lastProgress.Store(time.Now().UnixNano())
		wd.possibleStall.Store(false)
		return
	}
	// Shared pull-ticket wait or in-flight DB pull: not a completion stall.
	if wd.queue.IsWaitingOnDB() || wd.queue.IsPulling() {
		wd.lastProgress.Store(time.Now().UnixNano())
		wd.possibleStall.Store(false)
		return
	}

	// Progress = task completions (ReportTaskResult Beat) or explicit Beats during rate-limit waits.
	// Also treat round Completed growth as progress so mid-flight FS work that hasn't reported
	// yet doesn't false-positive if other workers are completing.
	round := wd.queue.GetRound()
	var completed int64
	if stats := wd.queue.GetRoundStats(round); stats != nil {
		completed = int64(stats.Completed)
	}
	prev := wd.lastCompleted.Load()
	if completed > prev {
		wd.lastCompleted.Store(completed)
		wd.lastProgress.Store(time.Now().UnixNano())
		wd.possibleStall.Store(false)
		return
	}

	lastBeat := time.Unix(0, wd.lastProgress.Load())
	elapsed := time.Since(lastBeat)

	if elapsed < wd.stallTimeout {
		return // Not stalled yet
	}

	// Check if queue has work that should be progressing
	inProgress := wd.queue.InProgressCount()
	pending := wd.queue.GetPendingCount()

	if inProgress == 0 && pending == 0 {
		st := wd.queue.State()
		if st == queue.QueueStateRunning && elapsed >= wd.stallTimeout {
			if wd.queue.ConfirmRoundAdvanceGate(round) {
				// Round keyspace exhausted; coordinator advances round or checks completion.
				wd.lastProgress.Store(time.Now().UnixNano())
				return
			}
			// Diagnostic only: idle without terminal pull may mean coordinator is stuck.
			wd.queue.DumpCompletionStall(elapsed)
			wd.lastProgress.Store(time.Now().UnixNano())
		}
		return
	}

	// Queue appears stalled - dump state
	wd.queue.DumpStallState(elapsed, inProgress, pending)
	wd.possibleStall.Store(true)

	// Reset timer to avoid spamming (will dump again in another stallTimeout if still stuck)
	wd.lastProgress.Store(time.Now().UnixNano())
}
