// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
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
	pullRetryLogInterval = 5 * time.Second
)

// pullTasksOnce dispatches a single pull attempt for the queue mode.
func (q *Queue) pullTasksOnce(force bool) PullResult {
	switch q.GetMode() {
	case QueueModeRetry:
		return q.PullRetryTasks(force)
	case QueueModeCopy, QueueModeCopyRetry:
		return q.PullCopyTasks(force)
	default:
		return q.PullTraversalTasks(force)
	}
}

// shouldDeferForcePull returns true when a force pull would only contend with an in-flight worker pull
// or when the buffer still has tasks and the keyspace is not exhausted (workers will refill).
func (q *Queue) shouldDeferForcePull() bool {
	if q.getPulling() {
		return true
	}
	if q.GetPendingCount() > 0 && !q.GetLastPullWasPartial() {
		return true
	}
	return false
}

// pullWithRetryIfNeeded runs pullWithRetry unless a worker pull or active refill makes force pull pointless.
func (q *Queue) pullWithRetryIfNeeded(force bool) PullResult {
	if force && q.shouldDeferForcePull() {
		return PullResult{Round: q.GetRound(), Status: PullSkipped}
	}
	return q.pullWithRetry(force)
}

// pullWithRetry retries skipped pulls with exponential backoff. Stale-round results retry immediately.
func (q *Queue) pullWithRetry(force bool) PullResult {
	start := time.Now()
	backoff := time.Millisecond
	attempts := 0
	var last PullResult
	for {
		last = q.pullTasksOnce(force)
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
			if last.Status == PullSkipped {
				q.logPullRetryExhausted(attempts)
			}
			return last
		}
		time.Sleep(backoff)
		if backoff < 50*time.Millisecond {
			backoff *= 2
		}
	}
}

func (q *Queue) logPullRetryExhausted(attempts int) {
	round := q.GetRound()
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	if q.pullRetryWarnRound == round && now.Sub(q.pullRetryWarnAt) < pullRetryLogInterval {
		return
	}
	q.pullRetryWarnRound = round
	q.pullRetryWarnAt = now
	fmt.Printf("[pull-retry] queue=%s round=%d exhausted after %d attempts (contention; will retry on next tick)\n",
		q.name, round, attempts)
}

// roundHasCountedPull reports whether RoundInfo has at least one DB-committed pull for the round.
func (q *Queue) roundHasCountedPull(round int) bool {
	info := q.getRoundInfoReadOnly(round)
	return info != nil && info.PullCount > 0
}

// confirmRoundAdvanceGate returns true when in-memory state allows advancing from currentRound.
func (q *Queue) confirmRoundAdvanceGate(currentRound int) bool {
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

// tryCommitRoundAdvance flushes, re-verifies the round gate under lock, and advances to the next round.
func (q *Queue) tryCommitRoundAdvance(currentRound int) bool {
	if !q.confirmRoundAdvanceGate(currentRound) {
		if q.GetPendingCount() == 0 && q.InProgressCount() == 0 && !q.GetLastPullWasPartial() {
			q.pullWithRetryIfNeeded(true)
		}
		if !q.confirmRoundAdvanceGate(currentRound) {
			return false
		}
	}

	database := q.getDatabase()
	mode := q.GetMode()
	if database != nil {
		var err error
		for attempt := 0; attempt < flushRetryAttempts; attempt++ {
			if attempt > 0 {
				time.Sleep(flushRetryBackoff)
			}
			err = database.FlushAppenderBuffer()
			if err == nil {
				break
			}
		}
		if err != nil {
			return false
		}
	}

	if !q.confirmRoundAdvanceGate(currentRound) {
		return false
	}

	q.resetThisQueueKeysetCursor()
	state := q.State()
	if state == QueueStateWaiting {
		q.SetState(QueueStateRunning)
	}

	if mode == QueueModeCopy || mode == QueueModeCopyRetry {
		q.AdvanceCopyRound()
		return true
	}
	q.AdvanceTraversalRound()
	return true
}
