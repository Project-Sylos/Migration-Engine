// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// ============================================================================
// Thread-safe getter/setter methods for queue state
// All locking is centralized here - logic functions should use these methods
// ============================================================================

// Getters (read-only, use RLock)

// State returns the current queue lifecycle state.
func (q *Queue) State() QueueState {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state
}

// GetRound returns the current BFS round.
func (q *Queue) GetRound() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.round
}

// GetMode returns the current queue mode.
func (q *Queue) GetMode() QueueMode {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.mode
}

func (q *Queue) getPulling() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.pulling
}

func (q *Queue) getMaxKnownDepth() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.maxKnownDepth
}

// GetCopyPass returns the current copy pass.
func (q *Queue) GetCopyPass() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.copyPass
}

// incrementTasksCompletedTotal increments the queue's completed-task counter.
// Call once per task when it is marked successful or failed (past retries).
func (q *Queue) incrementTasksCompletedTotal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasksCompletedTotal++
}

// SetCopyPass sets the current copy pass.
func (q *Queue) SetCopyPass(pass int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.copyPass = pass
}

// SetWorkers sets the workers associated with this queue.
func (q *Queue) SetWorkers(workers []Worker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers = workers
}

func (q *Queue) getCoordinator() *QueueCoordinator {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.coordinator
}

func (q *Queue) getDatabase() *db.DB {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.database
}

func (q *Queue) getShutdownCtx() context.Context {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shutdownCtx
}

// InProgressCount returns the number of tasks currently being executed.
func (q *Queue) InProgressCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inProgress)
}

// GetPendingCount returns the number of tasks in the pending buffer.
func (q *Queue) GetPendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pendingBuff)
}

func (q *Queue) GetLastPullWasPartial() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastPullWasPartial
}

// GetWorkerCount returns the number of workers registered with this queue.
func (q *Queue) GetWorkerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.workers)
}

func (q *Queue) setFirstPullForRound(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.firstPullForRound = value
}

func (q *Queue) getMaxRetries() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.maxRetries
}

// getPullLowWM returns the low watermark for pulling more work: 25% of lease batch size, minimum 1.
func (q *Queue) getPullLowWM() int {
	bs := effectiveLeaseBatchSize()
	wm := bs / 4
	if wm < 1 {
		wm = 1
	}
	return wm
}

// Keyset cursors are strictly round-scoped per queue. Each queue (src, dst, copy) has its own cursor and runtime;
// we only reset the cursor for the queue that advanced or changed mode. Resets: round advance (setRound), mode switch (setMode), and after that queue's seal.

func (q *Queue) getSrcKeysetCursor() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.srcKeysetCursor
}

func (q *Queue) setSrcKeysetCursor(cursor string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.srcKeysetCursor = cursor
}

func (q *Queue) getDstKeysetCursor() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.dstKeysetCursor
}

func (q *Queue) setDstKeysetCursor(cursor string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dstKeysetCursor = cursor
}

func (q *Queue) getCopyKeysetCursor() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.copyKeysetCursor
}

func (q *Queue) setCopyKeysetCursor(cursor string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.copyKeysetCursor = cursor
}

// resetThisQueueKeysetCursor clears only this queue's keyset cursor. Call when this queue's round advances (setRound), mode changes (setMode), or immediately after this queue's seal. Other queues' cursors are untouched—each queue has its own batching and runtime.
func (q *Queue) resetThisQueueKeysetCursor() {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch q.name {
	case "src":
		q.srcKeysetCursor = ""
	case "dst":
		q.dstKeysetCursor = ""
	case "copy":
		q.copyKeysetCursor = ""
	}
}

func (q *Queue) GetFilesDiscoveredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.filesDiscoveredTotal
}

func (q *Queue) GetFoldersDiscoveredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.foldersDiscoveredTotal
}

// GetTotalDiscovered returns the total number of items discovered (files + folders).
func (q *Queue) GetTotalDiscovered() int64 {
	return q.GetFilesDiscoveredTotal() + q.GetFoldersDiscoveredTotal()
}

// GetBytesTransferredTotal returns the total bytes transferred during copy phase.
func (q *Queue) GetBytesTransferredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.bytesTransferredTotal
}

// GetFoldersCreatedTotal returns the total folders created during copy phase.
func (q *Queue) GetFoldersCreatedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.foldersCreatedTotal
}

// GetFilesCreatedTotal returns the total files created during copy phase.
func (q *Queue) GetFilesCreatedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.filesCreatedTotal
}

// GetTotalFailed returns the total number of failed tasks across all rounds.
func (q *Queue) GetTotalFailed() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	total := 0
	for _, roundInfo := range q.roundInfoMap {
		if roundInfo != nil {
			total += roundInfo.TasksFailed
		}
	}
	return total
}

// GetRoundStats returns the statistics for a specific round. Returns nil if the round has no stats yet.
func (q *Queue) GetRoundStats(round int) *RoundStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.roundStats[round]
}

func (q *Queue) getStatsChan() chan QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.statsChan
}

func (q *Queue) getStatsTick() *time.Ticker {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.statsTick
}

// GetAverageExecutionTime returns the current average task execution time.
func (q *Queue) GetAverageExecutionTime() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgExecutionTime
}

// GetExecutionTimeDeltas returns a copy of the execution time deltas buffer.
func (q *Queue) GetExecutionTimeDeltas() []time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]time.Duration, len(q.executionTimeDeltas))
	copy(result, q.executionTimeDeltas)
	return result
}

func (q *Queue) getLastAvgTime() time.Time {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastAvgTime
}

func (q *Queue) getAvgInterval() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgInterval
}

// Setters (write, use Lock)

// SetState sets the queue lifecycle state.
func (q *Queue) SetState(state QueueState) {
	q.mu.Lock()
	q.state = state
	watchdog := q.watchdog
	q.mu.Unlock()

	// Stop watchdog when queue completes or stops
	if (state == QueueStateCompleted || state == QueueStateStopped) && watchdog != nil {
		watchdog.Stop()
	}
}

// SetRound sets the queue's current round. Used for resume operations.
func (q *Queue) SetRound(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.round = round
	q.firstPullForRound = true // next pull for this round is the first (used for "first pull with 0 items → complete")
	switch q.name {
	case "src":
		q.srcKeysetCursor = ""
	case "dst":
		q.dstKeysetCursor = ""
	case "copy":
		q.copyKeysetCursor = ""
	}
}

// SetMode sets the queue mode (traversal, retry, or copy).
func (q *Queue) SetMode(mode QueueMode) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mode = mode
	q.firstPullForRound = true // first pull in new mode counts as first for completion logic
	switch q.name {
	case "src":
		q.srcKeysetCursor = ""
	case "dst":
		q.dstKeysetCursor = ""
	case "copy":
		q.copyKeysetCursor = ""
	}
}

func (q *Queue) setPulling(pulling bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = pulling
}

// SetMaxKnownDepth sets the maximum depth for traversal/copy. Set to -1 to auto-detect.
func (q *Queue) SetMaxKnownDepth(depth int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxKnownDepth = depth
}

// getRoundInfo returns the RoundInfo for the specified round, creating it if it doesn't exist.
func (q *Queue) getRoundInfo(round int) *RoundInfo {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.roundInfoMap[round] == nil {
		q.roundInfoMap[round] = &RoundInfo{
			Round:     round,
			StartTime: time.Now(),
		}
	}
	return q.roundInfoMap[round]
}

// getRoundInfoReadOnly returns a read-only copy of RoundInfo for the specified round.
// Returns nil if the round doesn't exist yet.
func (q *Queue) getRoundInfoReadOnly(round int) *RoundInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.roundInfoMap[round] == nil {
		return nil
	}

	// Return a copy to prevent external mutation
	info := *q.roundInfoMap[round]
	return &info
}

// recordPull records a pull operation for the current round.
func (q *Queue) recordPull(round int, itemsYielded int, wasPartial bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.roundInfoMap[round] == nil {
		q.roundInfoMap[round] = &RoundInfo{
			Round:     round,
			StartTime: time.Now(),
		}
	}

	info := q.roundInfoMap[round]
	info.PullCount++
	info.ItemsYielded += itemsYielded
	info.LastPullTime = time.Now()
	info.LastPartialPull = wasPartial

	// Calculate avg tasks/sec if we have a start time
	if !info.StartTime.IsZero() {
		elapsed := time.Since(info.StartTime).Seconds()
		if elapsed > 0 {
			info.AvgTasksPerSec = float64(info.TasksCompleted) / elapsed
		}
	}
}

// recordTaskCompletion records a completed task for the current round.
func (q *Queue) recordTaskCompletion(round int, success bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.roundInfoMap[round] == nil {
		q.roundInfoMap[round] = &RoundInfo{
			Round:     round,
			StartTime: time.Now(),
		}
	}

	info := q.roundInfoMap[round]
	if success {
		info.TasksCompleted++
	} else {
		info.TasksFailed++
	}

	// Update avg tasks/sec
	if !info.StartTime.IsZero() {
		elapsed := time.Since(info.StartTime).Seconds()
		if elapsed > 0 {
			info.AvgTasksPerSec = float64(info.TasksCompleted) / elapsed
		}
	}
}

// SetTraversalCacheLoaded sets whether we have completed the first pull for the current round.
// Until true, CheckTraversalCompletion returns false so the queue does not complete before the first pull.
func (q *Queue) SetTraversalCacheLoaded(loaded bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.traversalCacheLoaded = loaded
}

func (q *Queue) getTraversalCacheLoaded() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.traversalCacheLoaded
}

func (q *Queue) setLastPullWasPartial(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastPullWasPartial = value
}

// SetShutdownContext sets the shutdown context for the queue.
func (q *Queue) SetShutdownContext(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdownCtx = ctx
}

func (q *Queue) setDatabase(database *db.DB) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.database = database
}

func (q *Queue) setStatsChan(ch chan QueueStats) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.statsChan = ch
}

func (q *Queue) setStatsTick(tick *time.Ticker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.statsTick = tick
}

func (q *Queue) setAvgExecutionTime(duration time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.avgExecutionTime = duration
}

func (q *Queue) setLastAvgTime(t time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastAvgTime = t
}

func (q *Queue) addInProgress(nodeID string, task *TaskBase) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.inProgress[nodeID] = task
}

func (q *Queue) removeInProgress(nodeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inProgress, nodeID)
}

func (q *Queue) incrementRoundStatsCompleted(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Completed++
}

// resetRoundStatsCompleted zeros Completed for all rounds. Call when switching copy pass
// so pass 2 stats (files) don't include pass 1 completions (folders).
func (q *Queue) resetRoundStatsCompleted() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, stats := range q.roundStats {
		if stats != nil {
			stats.Completed = 0
		}
	}
}

func (q *Queue) incrementRoundStatsFailed(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Failed++
}

// getOrCreateRoundStatsUnlocked is a helper used internally by other locked methods
func (q *Queue) getOrCreateRoundStatsUnlocked(round int) *RoundStats {
	if q.roundStats[round] == nil {
		q.roundStats[round] = &RoundStats{}
	}
	return q.roundStats[round]
}

// setExpectedFromStatsBucket sets roundStats[round].Expected from the live DB count (or stats for init/resume).
// For traversal/retry: always compute from live pending count when advancing, to avoid stale stats from prior runs.
// For copy: compute from live. For init (EnsureRoundExpectedFromStats): try stats first for resume, else compute.
func (q *Queue) setExpectedFromStatsBucket(round int) {
	database := q.getDatabase()
	if database == nil {
		return
	}
	queueType := getQueueType(q.name)
	mode := q.GetMode()
	var expected int64
	var err error

	// For traversal/retry, always compute from live count when advancing (no stale stats from prior runs).
	// EnsureRoundExpectedFromStats (init/resume) still goes through this; we compute for traversal there too.
	switch mode {
	case QueueModeTraversal:
		if round == 0 {
			expected = 1
		} else {
			expected, err = database.GetPendingTraversalCountAtDepthFromLive(queueType, round)
			if err != nil {
				fmt.Println("error getting pending traversal count at depth from live", err)
				return
			}
		}
	case QueueModeRetry:
		expected, err = database.GetPendingTraversalCountAtDepthFromLive(queueType, round)
		if err != nil {
			fmt.Println("error getting pending traversal count at depth from live", err)
			return
		}
	case QueueModeCopy:
		copyPass := q.GetCopyPass()
		nodeType := db.NodeTypeFolder
		if copyPass == 2 {
			nodeType = db.NodeTypeFile
		}
		expected, err = database.GetCopyCountAtDepth(round, nodeType, db.CopyStatusPending, false)
		if err != nil {
			fmt.Println("error getting copy count at depth", err)
			return
		}
	case QueueModeCopyRetry:
		copyPass := q.GetCopyPass()
		nodeType := db.NodeTypeFolder
		if copyPass == 2 {
			nodeType = db.NodeTypeFile
		}
		expected, err = database.GetCopyCountAtDepth(round, nodeType, db.CopyStatusFailed, false)
		if err != nil {
			fmt.Println("error getting copy count at depth", err)
			return
		}
	default:
		return
	}
	// Persist to stats for resume
	if expected > 0 {
		_ = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
			return s.WithTx(func(w *db.Writer) error {
				return w.SetStatsCountForDepth(queueType, round, db.StatsKeyExpected, expected)
			})
		})
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Expected = int(expected)
}

func (q *Queue) appendExecutionTimeDelta(delta time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Add to buffer (max 100 items)
	if len(q.executionTimeDeltas) >= 100 {
		// Remove oldest item (FIFO)
		q.executionTimeDeltas = q.executionTimeDeltas[1:]
	}
	q.executionTimeDeltas = append(q.executionTimeDeltas, delta)
}

func (q *Queue) clearExecutionTimeDeltas() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.executionTimeDeltas = q.executionTimeDeltas[:0]
}

// Batch operations

// getStateSnapshot returns a snapshot of queue state for use in logic functions
type QueueStateSnapshot struct {
	State              QueueState
	Round              int
	PendingCount       int
	InProgressCount    int
	Pulling            bool
	LastPullWasPartial bool
	FirstPullForRound  bool
	PullLowWM          int
	Database           *db.DB
	Mode               QueueMode
}

func (q *Queue) getStateSnapshot() QueueStateSnapshot {
	q.mu.RLock()
	defer q.mu.RUnlock()
	wm := effectiveLeaseBatchSize() / 4
	if wm < 1 {
		wm = 1
	}
	return QueueStateSnapshot{
		State:              q.state,
		Round:              q.round,
		PendingCount:       len(q.pendingBuff),
		InProgressCount:    len(q.inProgress),
		Pulling:            q.pulling,
		LastPullWasPartial: q.lastPullWasPartial,
		FirstPullForRound:  q.firstPullForRound,
		PullLowWM:          wm,
		Database:           q.database,
		Mode:               q.mode,
	}
}

func (q *Queue) recordDequeueSkip(reason string, currentRound int) {
	q.mu.Lock()
	switch reason {
	case "old_round":
		q.dequeueSkipOldRound++
	case "empty_id":
		q.dequeueSkipEmptyID++
	case "already_in_progress":
		q.dequeueSkipInProgress++
	}
	shouldLog := false
	queueRound := q.round
	state := q.state
	workers := len(q.workers)
	pendingBuf := len(q.pendingBuff)
	inProgress := len(q.inProgress)
	oldRoundSkips := q.dequeueSkipOldRound
	emptyIDSkips := q.dequeueSkipEmptyID
	inProgressSkips := q.dequeueSkipInProgress
	if q.name == "dst" && q.coordinator != nil && q.coordinator.IsCompleted("src") {
		now := time.Now()
		if q.dequeueDebugLastLogAt.IsZero() || now.Sub(q.dequeueDebugLastLogAt) >= 2*time.Second {
			q.dequeueDebugLastLogAt = now
			shouldLog = true
			q.dequeueSkipOldRound = 0
			q.dequeueSkipEmptyID = 0
			q.dequeueSkipInProgress = 0
		}
	}
	q.mu.Unlock()
	if shouldLog {
		fmt.Printf("[dequeue-skip] queue=%s currentRound=%d queueRound=%d state=%s workers=%d pendingBuf=%d inProgress=%d oldRound=%d emptyID=%d alreadyInProgress=%d\n",
			q.name, currentRound, queueRound, state, workers, pendingBuf, inProgress, oldRoundSkips, emptyIDSkips, inProgressSkips)
	}
}

// Add enqueues a task into the pending buffer. Returns false if task is nil, has empty ID, or is already in progress.
func (q *Queue) Add(task *TaskBase) bool {
	if task == nil {
		return false
	}
	nodeID := task.ID
	if nodeID == "" {
		// With deterministic IDs, task.ID should always be pre-computed
		// This is a programming error if we reach here
		if logservice.LS != nil {
			err := logservice.LS.Log("error",
				"enqueuePending called with empty task.ID - this indicates a bug in ID generation",
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return false
	}

	// Atomically check and add in a single lock
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.inProgress[nodeID]; exists {
		return false
	}

	q.pendingBuff = append(q.pendingBuff, task)
	return true
}

// dequeuePending atomically dequeues a task from the pending buffer
func (q *Queue) dequeuePending() *TaskBase {
	for {
		q.mu.Lock()
		if len(q.pendingBuff) == 0 {
			q.mu.Unlock()
			return nil
		}
		task := q.pendingBuff[0]
		q.pendingBuff = q.pendingBuff[1:]
		nodeID := task.ID
		if nodeID == "" {
			// With deterministic IDs, task.ID should always be pre-computed
			// Skip this task and log an error
			if logservice.LS != nil {
				err := logservice.LS.Log("error",
					"dequeuePending found task with empty ID - this indicates a bug in ID generation",
					"queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			q.mu.Unlock()
			q.recordDequeueSkip("empty_id", 0)
			continue
		}

		// Check if task is for current or future round
		currentRound := q.round
		// Check if already in progress
		_, inProgress := q.inProgress[nodeID]
		q.mu.Unlock()

		if task.Round < currentRound {
			q.recordDequeueSkip("old_round", currentRound)
			continue // Skip old round tasks
		}

		if inProgress {
			q.recordDequeueSkip("already_in_progress", currentRound)
			continue
		}

		return task
	}
}
