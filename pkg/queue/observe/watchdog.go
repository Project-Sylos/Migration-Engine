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
	defaultQueueStallTimeout = 30 * time.Second
	queueWatchdogCheckInterval = 5 * time.Second
)

// ProgressWatchdog detects stalled tasks by requiring progress (Beat) within a timeout.
// If no progress occurs, it cancels the context so adapter operations abort.
type ProgressWatchdog struct {
	ctx            context.Context
	cancel         context.CancelFunc
	timeout        time.Duration
	stallSuppress  func() bool // when non-nil and true, stall timer is frozen (e.g. seal flush / backpressure)
	lastBeat       atomic.Int64 // UnixNano (time.Now().UnixNano())
}

// NewProgressWatchdog creates a watchdog and a child context. When no Beat() occurs
// within timeout, the context is cancelled. Call Stop() when the task ends to release resources.
// If stallSuppress is non-nil and returns true, the elapsed stall window does not advance (workers blocked on seal I/O are not adapter stalls).
func NewProgressWatchdog(parent context.Context, timeout time.Duration, stallSuppress func() bool) (*ProgressWatchdog, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	wd := &ProgressWatchdog{
		ctx:           ctx,
		cancel:        cancel,
		timeout:       timeout,
		stallSuppress: stallSuppress,
	}
	wd.lastBeat.Store(time.Now().UnixNano())
	go wd.monitor()
	return wd, ctx
}

// Beat records progress. Call whenever meaningful work completes.
func (w *ProgressWatchdog) Beat() {
	w.lastBeat.Store(time.Now().UnixNano())
}

// Stop cancels the context and stops the monitor goroutine.
func (w *ProgressWatchdog) Stop() {
	w.cancel()
}

func (w *ProgressWatchdog) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if w.stallSuppress != nil && w.stallSuppress() {
				w.lastBeat.Store(time.Now().UnixNano())
				continue
			}
			lastNano := w.lastBeat.Load()
			last := time.Unix(lastNano/1e9, lastNano%1e9)
			if time.Since(last) > w.timeout {
				w.cancel()
				return
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
	// Rate-limit windows are expected idle time — not a deadlock.
	if wd.queue.IsRateLimitActive() {
		wd.lastProgress.Store(time.Now().UnixNano())
		wd.possibleStall.Store(false)
		return
	}
	// Shared pull-ticket wait or in-flight DuckDB pull: not a completion stall.
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
