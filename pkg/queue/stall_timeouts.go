// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "time"

// Default FS ProgressWatchdog budgets when no override is set on the queue.
const (
	DefaultTraversalStallTimeout = 60 * time.Second
	DefaultDeleteStallTimeout    = 60 * time.Second
	DefaultCopyStallTimeout      = 60 * time.Second
)

// FSStallTimeouts are per-kind ProgressWatchdog budgets for FS awaits.
// A zero duration means stall abort is disabled for that kind (wait forever).
type FSStallTimeouts struct {
	Traversal time.Duration
	Delete    time.Duration
	Copy      time.Duration
}

// SetFSStallTimeouts stores per-kind FS stall budgets for workers on this queue.
// Pass zero for a kind to disable ProgressWatchdog abort for that kind.
func (q *Queue) SetFSStallTimeouts(t FSStallTimeouts) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.fsStallTimeouts = t
	q.fsStallTimeoutsSet = true
}

// TraversalStallTimeout returns the ListChildren / GPL FS-wait stall budget.
func (q *Queue) TraversalStallTimeout() time.Duration {
	if q != nil && q.fsStallTimeoutsSet {
		return q.fsStallTimeouts.Traversal
	}
	return DefaultTraversalStallTimeout
}

// DeleteStallTimeout returns the DeleteNode / DeleteBatch FS-wait stall budget.
func (q *Queue) DeleteStallTimeout() time.Duration {
	if q != nil && q.fsStallTimeoutsSet {
		return q.fsStallTimeouts.Delete
	}
	return DefaultDeleteStallTimeout
}

// CopyStallTimeout returns the copy Open/Create/chunk FS-wait stall budget
// (not commit Close, which keeps its own longer constant).
func (q *Queue) CopyStallTimeout() time.Duration {
	if q != nil && q.fsStallTimeoutsSet {
		return q.fsStallTimeouts.Copy
	}
	return DefaultCopyStallTimeout
}

// StallTimeoutFromSeconds maps prefs seconds to a duration.
// sec < 0 uses defaultSec; sec == 0 means unlimited (duration 0); sec > 0 is that many seconds.
func StallTimeoutFromSeconds(sec, defaultSec int) time.Duration {
	if sec < 0 {
		if defaultSec < 0 {
			defaultSec = 60
		}
		return time.Duration(defaultSec) * time.Second
	}
	if sec == 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}
