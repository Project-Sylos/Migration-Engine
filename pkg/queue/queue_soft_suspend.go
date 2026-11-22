// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"time"
)

// StopWatchdog stops stall monitoring (e.g. during intentional soft suspend). Safe if nil or already stopped.
func (q *Queue) StopWatchdog() {
	q.mu.Lock()
	wd := q.watchdog
	q.mu.Unlock()
	if wd != nil {
		wd.Stop()
	}
}

// ClearPendingBufferForSuspend drops non-leased pending tasks; in-progress tasks are unchanged.
func (q *Queue) ClearPendingBufferForSuspend() {
	q.mu.Lock()
	q.releasePendingBufferWeightLocked()
	capacity := pendingBuffCapFromLeaseSizing(q.leaseBatchSize)
	q.pendingBuff = make([]*TaskBase, 0, capacity)
	q.mu.Unlock()
}

// BeginMemoryShed pauses the queue so pulls/leases stop. Returns the prior state
// for FinishMemoryShed. In-flight leased work is left alone until drained.
func (q *Queue) BeginMemoryShed() (resumeState QueueState) {
	resumeState = q.State()
	q.SetState(QueueStatePaused)
	q.StopWatchdog()
	return resumeState
}

// FinishMemoryShed restores queue state after a memory shed drain+flush.
// Waiting is preserved when that was the prior state (DST coordinator gate).
func (q *Queue) FinishMemoryShed(resumeState QueueState) {
	switch resumeState {
	case QueueStatePaused, QueueStateCompleted, QueueStateStopped:
		q.SetState(resumeState)
	case QueueStateWaiting:
		q.SetState(QueueStateWaiting)
	default:
		q.SetState(QueueStateRunning)
	}
}

// WaitInProgressZeroFunc blocks until InProgressCount is zero or ctx is canceled.
// onTick is optional and receives the current in-progress count each poll.
func (q *Queue) WaitInProgressZeroFunc(ctx context.Context, every time.Duration, onTick func(inProgress int)) error {
	if every <= 0 {
		every = 50 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		n := q.InProgressCount()
		if n == 0 {
			return nil
		}
		if onTick != nil {
			onTick(n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
