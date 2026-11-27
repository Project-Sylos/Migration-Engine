// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// PassPhase describes a two-pass (folders, then files) forward-BFS phase. Copy and delete
// both track their pass in the queue's copy pass accessors.
type PassPhase struct {
	CompleteMsg  string
	ExhaustedFmt string
	AdvancedFmt  string
}

var (
	CopyPass = PassPhase{
		CompleteMsg:  "Copy phase complete - both passes finished",
		ExhaustedFmt: "Pass %d exhausted rounds (past maxKnownDepth %d), checking for completion",
		AdvancedFmt:  "Advanced to round %d (pass %d: %s)",
	}
	DeletePass = PassPhase{
		CompleteMsg:  "Delete phase complete - both passes finished",
		ExhaustedFmt: "Pass %d exhausted depths (past maxKnownDepth %d), checking for completion",
		AdvancedFmt:  "Advanced delete to depth %d (pass %d: %s)",
	}
)

func logQueue(q *queue.Queue, level, msg string) {
	if logservice.LS == nil {
		return
	}
	if err := logservice.LS.Log(level, msg, "queue", q.Name(), q.Name()); err != nil {
		fmt.Println("error logging", err)
	}
}

// CheckPassCompletion checks if the phase should switch passes or complete.
// Only called when we're past maxKnownDepth: the pass has exhausted itself round-by-round.
// Trust the per-round logic; no re-checking of pending/inProgress/wasFirstPull.
func CheckPassCompletion(q *queue.Queue, currentRound int, p PassPhase) bool {
	maxKnownDepth := q.GetMaxKnownDepth()

	// Only consider pass switch/phase complete when we've exhausted all rounds for this pass
	if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
		return false
	}

	// if any in progress or pending in the current queue
	// just to be safe.
	if q.InProgressCount() > 0 || q.GetPendingCount() > 0 {
		return false
	}

	// Run() polls CheckFinalCompletion every tick, including before the first pull.
	// Do not switch passes / complete until this round has actually pulled (same gate as
	// round completion). Otherwise pass 1 is skipped when maxKnownDepth == start round.
	info := q.RoundInfoReadOnly(currentRound)
	if info == nil || info.PullCount == 0 {
		return false
	}
	if !q.GetLastPullWasPartial() {
		return false
	}

	if q.GetCopyPass() == 1 {
		// Pass 1 (folders) done - switch to pass 2 (files); find starting round
		q.SetCopyPass(2)
		q.ResetRoundStatsCompleted()
		q.SetRound(1)
		q.SetExpectedFromStatsBucket(1)
		q.SetLastPullWasPartial(false)
		return false
	}

	// Pass 2 (files) complete
	return q.MarkComplete("%s", p.CompleteMsg)
}

// AdvancePassRound handles round advancement for a two-pass phase.
// Round completion is determined by lastPullWasPartial (memory/keyset only); we never query the DB for in-round advancement.
// When called, the current round has just completed - we always advance to currentRound+1.
func AdvancePassRound(q *queue.Queue, p PassPhase) {
	currentRound := q.GetRound()
	pass := q.GetCopyPass()
	maxKnownDepth := q.GetMaxKnownDepth()

	// We just completed currentRound (lastPullWasPartial, pending empty, inProgress empty).
	// Advance sequentially - no DB queries for "does this round have pending?" (DB is stale during round).
	newRound := currentRound + 1

	if maxKnownDepth >= 0 && newRound > maxKnownDepth {
		// Past maxKnownDepth - check for pass switch or phase complete.
		// DB can be used here: we're at phase boundary, flush has run, deciding phase-level completion.
		logQueue(q, "info", fmt.Sprintf(p.ExhaustedFmt, pass, maxKnownDepth))
		if q.CheckCompletion(currentRound, queue.CompletionCheckOptions{CheckFinalCompletion: true}) {
			return
		}
		q.PullWithRetryIfNeeded(true)
		return
	}

	q.SetRound(newRound)
	q.SetExpectedFromStatsBucket(newRound)
	q.SetLastPullWasPartial(false)

	passName := "folders"
	if pass == 2 {
		passName = "files"
	}
	logQueue(q, "info", fmt.Sprintf(p.AdvancedFmt, newRound, pass, passName))

	// Pull tasks for the new round
	q.PullWithRetryIfNeeded(true)
}

// pullGate selects how a forward-BFS or cascade pull decides to skip or abort.
// runningOnly skips unless the queue is running (GPL); otherwise only a paused queue aborts.
// skipRoundZero skips round 0 (copy and delete start at round 1).
type pullGate struct {
	runningOnly   bool
	skipRoundZero bool
}

// pullSession is an acquired pull: the pull lock and the shared DB pull ticket are held
// until end.
type pullSession struct {
	q        *queue.Queue
	Database *db.DB
	Round    int
}

// end releases the DB pull ticket and the pull lock.
func (ps pullSession) end() {
	ps.q.ReleaseDBPull()
	ps.q.SetPulling(false)
}

// beginPull owns the shared pull preamble: pull lock, completed/paused/low-watermark gates,
// round-0 skip, and the shared DB pull ticket. When ok is false the returned PullResult is the
// early exit and nothing is held.
func beginPull(q *queue.Queue, force bool, gate pullGate) (ps pullSession, early queue.PullResult, ok bool) {
	database := q.Database()
	if database == nil {
		return ps, queue.PullResult{Status: queue.PullAborted}, false
	}
	if !q.TryBeginPulling() {
		return ps, queue.PullResult{Round: q.GetRound(), Status: queue.PullSkipped}, false
	}
	if q.State() == queue.QueueStateCompleted {
		q.SetPulling(false)
		return ps, queue.PullResult{Status: queue.PullAborted}, false
	}

	snapshot := q.StateSnapshot()
	switch {
	case snapshot.State == queue.QueueStatePaused && (!gate.runningOnly || force):
		early = queue.PullResult{Round: snapshot.Round, Status: queue.PullAborted}
	case !force && (snapshot.PendingCount > snapshot.PullLowWM || gate.runningOnly && snapshot.State != queue.QueueStateRunning):
		early = queue.PullResult{Round: snapshot.Round, Status: queue.PullSkipped}
	case gate.skipRoundZero && snapshot.Round == 0:
		early = queue.PullResult{Round: 0, Status: queue.PullSkipped}
	default:
		q.BeginDBPull()
		return pullSession{q: q, Database: database, Round: snapshot.Round}, early, true
	}
	q.SetPulling(false)
	return ps, early, false
}

// keysetPage is one consumed keyset page: the matched rows to enqueue and whether the
// keyspace is exhausted.
type keysetPage struct {
	Matched []db.FetchResult
	Partial bool
}

// pullKeysetPage fetches batchSize+1 rows (the extra row only detects exhaustion), advances the
// queue's keyset cursor past the consumed rows, and records partial. When ok is false the
// returned PullResult is the early exit for the caller.
func pullKeysetPage(q *queue.Queue, round int, fetchName string, fetch func(limit int) ([]db.FetchResult, error)) (page keysetPage, early queue.PullResult, ok bool) {
	batchSize := q.EffectiveLeaseBatchSize()
	pullStart := time.Now()
	results, err := fetch(batchSize + 1)
	q.RecordDBPull(len(results), time.Since(pullStart))
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("%s failed: %v", fetchName, err), "queue", q.Name(), q.Name())
		}
		return page, queue.PullResult{Round: round, Status: queue.PullSkipped}, false
	}
	if q.GetRound() != round {
		return page, queue.PullResult{Round: round, QueriedDB: true, Status: queue.PullStaleRound}, false
	}
	// Process at most batchSize items this pull; the extra (+1) is only for exhaustion detection
	processLimit := min(batchSize, len(results))
	for _, r := range results[:processLimit] {
		if r.State == nil {
			continue
		}
		page.Matched = append(page.Matched, r)
	}
	// Cursor = last item we consumed.
	if len(results) > 0 {
		q.SetKeysetCursor(results[max(0, processLimit-1)].Key)
	}
	page.Partial = len(results) <= batchSize
	q.SetLastPullWasPartial(page.Partial)
	return page, queue.PullResult{}, true
}

// finishPull records a completed pull and returns its OK result.
func finishPull(q *queue.Queue, round, yield int, partial bool) queue.PullResult {
	q.SetLastPullWasPartial(partial)
	q.RecordPull(round, yield, partial)
	q.SetFirstPullForRound(false)
	return queue.PullResult{Round: round, Yield: yield, Partial: partial, QueriedDB: true, Status: queue.PullOK}
}

// beginTerminalSuccess records execution time and success bookkeeping for a completed task.
// It returns nil (after dropping the lease) when the queue has no database.
func beginTerminalSuccess(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) *db.DB {
	q.RecordExecutionTime(executionDelta)
	q.RememberSettledOutcome(task, true)
	database := q.Database()
	if database == nil {
		q.ReleaseTaskInFlightWeight(task)
		q.RemoveInProgress(task.ID)
		return nil
	}
	task.Locked = false
	task.Status = "successful"
	q.IncrementRoundStatsCompleted(task.Round)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(task.Round, true)
	return database
}

// requeueForRetry unlocks the task, drops its lease, and re-enqueues it for another
// attempt. It reports whether the queue accepted it.
func requeueForRetry(q *queue.Queue, task *queue.TaskBase) bool {
	task.Locked = false
	q.RemoveInProgress(task.ID)
	return q.Add(task)
}

// beginTerminalFailure records permanent-failure bookkeeping once retries are exhausted.
// It returns nil (after dropping the lease) when the queue has no database.
func beginTerminalFailure(q *queue.Queue, task *queue.TaskBase) *db.DB {
	q.RememberSettledOutcome(task, false)
	task.Locked = false
	task.Status = "failed"
	q.IncrementRoundStatsCompleted(task.Round)
	q.IncrementRoundStatsFailed(task.Round)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(task.Round, false)
	database := q.Database()
	if database == nil {
		q.RemoveInProgress(task.ID)
		return nil
	}
	// Drop the lease before seal I/O. A stuck flush must not keep the row in Working-on.
	q.RemoveInProgress(task.ID)
	return database
}

// appendFailureEvent records the task error and appends the SRC failure status event
// carrying the failure log. kind is "copy" or "delete".
func appendFailureEvent(q *queue.Queue, task *queue.TaskBase, database *db.DB, kind string, ev db.StatusEvent) bool {
	if task.LastError != "" {
		database.AppendTaskError("SRC", kind, task.ID, task.LastError, task.Attempts, task.DisplayPath)
	}
	failurelog.AttachTaskFailureLog(&ev, kind, q.Name(), task.ID, task.DisplayPath, task.Attempts, task.LastError)
	return database.AppendStatusEvent("SRC", ev, false)
}

// recordTerminal reports a terminal outcome unless the status event was rejected, in which
// case it only drops the lease. It reports whether progress was recorded.
func recordTerminal(q *queue.Queue, task *queue.TaskBase, accepted bool, outcome queue.TerminalProgressOutcome, bytes int64) bool {
	if !accepted {
		q.RemoveInProgress(task.ID)
		return false
	}
	q.RecordTerminalProgress(outcome, task.ID, task.IsFolder(), task.IsFile(), bytes)
	return true
}
