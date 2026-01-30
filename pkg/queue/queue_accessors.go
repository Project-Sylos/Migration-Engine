// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// ============================================================================
// Thread-safe getter/setter methods for queue state
// All locking is centralized here - logic functions should use these methods
// ============================================================================

// Getters (read-only, use RLock)

func (q *Queue) getState() QueueState {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state
}

func (q *Queue) getRound() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.round
}

func (q *Queue) getMode() QueueMode {
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

func (q *Queue) getCopyPass() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.copyPass
}

// getTasksCompletedTotal returns the number of tasks completed (success or final failure) for this queue.
// Used by the output buffer to push the current value into the stats bucket on each flush.
func (q *Queue) getTasksCompletedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.tasksCompletedTotal
}

// incrementTasksCompletedTotal increments the queue's completed-task counter.
// Call once per task when it is marked successful or failed (past retries).
func (q *Queue) incrementTasksCompletedTotal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasksCompletedTotal++
}

// GetCopyPass returns the current copy pass (public getter).
func (q *Queue) GetCopyPass() int {
	return q.getCopyPass()
}

// SetCopyPass sets the current copy pass (public setter).
func (q *Queue) SetCopyPass(pass int) {
	q.setCopyPass(pass)
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

func (q *Queue) getBoltDB() *db.DB {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.boltDB
}

func (q *Queue) getOutputBuffer() *db.OutputBuffer {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.outputBuffer
}

func (q *Queue) getShutdownCtx() context.Context {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shutdownCtx
}

func (q *Queue) getInProgressCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inProgress)
}

func (q *Queue) getPendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pendingBuff)
}

func (q *Queue) getLastPullWasPartial() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastPullWasPartial
}

func (q *Queue) getMaxRetries() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.maxRetries
}

func (q *Queue) getPullLowWM() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.pullLowWM
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

func (q *Queue) isLeased(nodeID string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, exists := q.leasedKeys[nodeID]
	return exists
}

func (q *Queue) isInPendingSet(nodeID string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, exists := q.pendingSet[nodeID]
	return exists
}

func (q *Queue) getRoundStats(round int) *RoundStats {
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

func (q *Queue) getAvgExecutionTime() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgExecutionTime
}

func (q *Queue) getExecutionTimeDeltas() []time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	// Return a copy to avoid race conditions
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

func (q *Queue) setState(state QueueState) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.state = state
}

func (q *Queue) setRound(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.round = round
}

func (q *Queue) setMode(mode QueueMode) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mode = mode
}

func (q *Queue) setPulling(pulling bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = pulling
}

func (q *Queue) setMaxKnownDepth(depth int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxKnownDepth = depth
}

func (q *Queue) setCopyPass(pass int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.copyPass = pass
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

// Convenience getters for current round
func (q *Queue) getCurrentRoundPullCount() int {
	currentRound := q.getRound()
	info := q.getRoundInfoReadOnly(currentRound)
	if info == nil {
		return 0
	}
	return info.PullCount
}

func (q *Queue) getCurrentRoundItemsYielded() int {
	currentRound := q.getRound()
	info := q.getRoundInfoReadOnly(currentRound)
	if info == nil {
		return 0
	}
	return info.ItemsYielded
}

func (q *Queue) setLastPullWasPartial(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastPullWasPartial = value
}

func (q *Queue) setShutdownCtx(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdownCtx = ctx
}

func (q *Queue) setBoltDB(boltDB *db.DB) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.boltDB = boltDB
}

func (q *Queue) setOutputBuffer(outputBuffer *db.OutputBuffer) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.outputBuffer = outputBuffer
	if outputBuffer != nil {
		outputBuffer.SetOnCompletedCountGetter(func() (string, int64) {
			return getQueueType(q.name), q.getTasksCompletedTotal()
		})
	}
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

func (q *Queue) addLeasedKey(nodeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.leasedKeys[nodeID] = struct{}{}
}

func (q *Queue) removeLeasedKey(nodeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.leasedKeys, nodeID)
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

// setExpectedFromStatsBucket sets roundStats[round].Expected from the stats bucket (O(1) lookup).
// Round 0 for traversal/retry is always 1; otherwise uses pending count from BoltDB.
// Used at the start of each round so Expected reflects actual DB state and survives restarts.
func (q *Queue) setExpectedFromStatsBucket(round int) {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}
	mode := q.getMode()
	var expected int
	switch mode {
	case QueueModeTraversal:
		if round == 0 {
			expected = 1
		} else {
			count, err := boltDB.CountStatusBucket(getQueueType(q.name), round, db.StatusPending)
			if err == nil {
				expected = count
			}
		}
	case QueueModeRetry:
		count, err := boltDB.CountStatusBucket(getQueueType(q.name), round, db.StatusPending)
		if err == nil {
			expected = count
		}
	case QueueModeCopy:
		copyPass := q.getCopyPass()
		nodeType := db.NodeTypeFolder
		if copyPass == 2 {
			nodeType = db.NodeTypeFile
		}
		count, err := boltDB.CountCopyStatusBucket(round, nodeType, db.CopyStatusPending)
		if err == nil {
			expected = count
		}
	default:
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Expected = expected
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
	PullLowWM          int
	BoltDB             *db.DB
	OutputBuffer       *db.OutputBuffer
	Mode               QueueMode
}

func (q *Queue) getStateSnapshot() QueueStateSnapshot {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return QueueStateSnapshot{
		State:              q.state,
		Round:              q.round,
		PendingCount:       len(q.pendingBuff),
		InProgressCount:    len(q.inProgress),
		Pulling:            q.pulling,
		LastPullWasPartial: q.lastPullWasPartial,
		PullLowWM:          q.pullLowWM,
		BoltDB:             q.boltDB,
		OutputBuffer:       q.outputBuffer,
		Mode:               q.mode,
	}
}

// enqueuePending atomically enqueues a task to the pending buffer
func (q *Queue) enqueuePending(task *TaskBase) bool {
	if task == nil {
		return false
	}
	nodeID := task.ID
	if nodeID == "" {
		// With deterministic IDs, task.ID should always be pre-computed
		// This is a programming error if we reach here
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				"enqueuePending called with empty task.ID - this indicates a bug in ID generation",
				"queue", q.name, q.name)
		}
		return false
	}

	// Atomically check and add in a single lock
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if already in progress or pending
	if _, exists := q.inProgress[nodeID]; exists {
		return false
	}
	if _, exists := q.pendingSet[nodeID]; exists {
		return false
	}

	// Add to pending buffer and set
	q.pendingBuff = append(q.pendingBuff, task)
	q.pendingSet[nodeID] = struct{}{}
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
				_ = logservice.LS.Log("error",
					"dequeuePending found task with empty ID - this indicates a bug in ID generation",
					"queue", q.name, q.name)
			}
			q.mu.Unlock()
			continue
		}

		// Remove from pending set
		delete(q.pendingSet, nodeID)

		// Check if task is for current or future round
		currentRound := q.round
		// Check if already in progress
		_, inProgress := q.inProgress[nodeID]
		q.mu.Unlock()

		if task.Round < currentRound {
			continue // Skip old round tasks
		}

		if inProgress {
			continue
		}

		return task
	}
}
