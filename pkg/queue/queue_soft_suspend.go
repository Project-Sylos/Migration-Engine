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
	capacity := pendingBuffCapFromLeaseSizing(q.leaseBatchSize)
	q.pendingBuff = make([]*TaskBase, 0, capacity)
	q.mu.Unlock()
}

// WaitInProgressZero blocks until InProgressCount is zero or ctx is canceled.
func (q *Queue) WaitInProgressZero(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		every = 50 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if q.InProgressCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
