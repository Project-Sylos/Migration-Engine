// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"time"
)

// PullStatus classifies the outcome of a single pull attempt.
type PullStatus int

const (
	// PullOK — DB was queried and results were committed to RoundInfo for Round.
	PullOK PullStatus = iota
	// PullSkipped — no DB query (contention, gate, buffer watermark, etc.).
	PullSkipped
	// PullStaleRound — DB was queried but the queue round changed before results were committed.
	PullStaleRound
	// PullAborted — queue state prevents pulling (paused, completed, nil DB).
	PullAborted
)

// PullResult is returned by pull operations. Only PullOK increments RoundInfo.PullCount.
type PullResult struct {
	Round     int
	Yield     int
	Partial   bool
	QueriedDB bool
	Status    PullStatus
}

func (r PullResult) OK() bool { return r.Status == PullOK }

const (
	pullRetryMaxAttempts = 8
	pullRetryMaxWall     = 100 * time.Millisecond
)

// PullTasksOnce dispatches a single pull attempt for the queue mode.
func (q *Queue) PullTasksOnce(force bool) PullResult {
	switch q.GetMode() {
	case QueueModeRetry:
		return q.PullTasks(ModeRetry, force)
	case QueueModeGPL:
		return q.PullTasks(ModeGPL, force)
	case QueueModeCopy, QueueModeCopyRetry:
		return q.PullTasks(ModeCopy, force)
	case QueueModeDelete, QueueModeDeleteRetry:
		return q.PullTasks(ModeDelete, force)
	default:
		return q.PullTasks(ModeTraversal, force)
	}
}

// shouldDeferForcePull returns true when a force pull would only contend with an in-flight worker pull
// or when the buffer still has tasks and the keyspace is not exhausted (workers will refill).
func (q *Queue) shouldDeferForcePull() bool {
	if q.IsPulling() {
		return true
	}
	if q.GetPendingCount() > 0 && !q.GetLastPullWasPartial() {
		return true
	}
	return false
}

// PullWithRetryIfNeeded runs PullWithRetry unless a worker pull or active refill makes force pull pointless.
func (q *Queue) PullWithRetryIfNeeded(force bool) PullResult {
	if force && q.shouldDeferForcePull() {
		return PullResult{Round: q.GetRound(), Status: PullSkipped}
	}
	return q.PullWithRetry(force)
}

// PullWithRetry retries skipped pulls with exponential backoff. Stale-round results retry immediately.
func (q *Queue) PullWithRetry(force bool) PullResult {
	start := time.Now()
	backoff := time.Millisecond
	attempts := 0
	var last PullResult
	for {
		last = q.PullTasksOnce(force)
		if last.OK() {
			return last
		}
		if last.Status == PullAborted {
			return last
		}
		if last.Status == PullStaleRound {
			attempts = 0
			backoff = time.Millisecond
			continue
		}
		attempts++
		if attempts >= pullRetryMaxAttempts || time.Since(start) >= pullRetryMaxWall {
			return last
		}
		time.Sleep(backoff)
		if backoff < 50*time.Millisecond {
			backoff *= 2
		}
	}
}

// RoundHasCountedPull reports whether RoundInfo has at least one DB-committed pull for the round.
func (q *Queue) RoundHasCountedPull(round int) bool {
	info := q.RoundInfoReadOnly(round)
	return info != nil && info.PullCount > 0
}

// confirmRoundAdvanceGate returns true when in-memory state allows advancing from currentRound.
func (q *Queue) ConfirmRoundAdvanceGate(currentRound int) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.round != currentRound {
		return false
	}
	if len(q.pendingBuff) > 0 || len(q.inProgress) > 0 {
		return false
	}
	if q.pulling {
		return false
	}
	if !q.lastPullWasPartial {
		return false
	}
	info := q.roundInfoMap[currentRound]
	return info != nil && info.PullCount > 0
}
