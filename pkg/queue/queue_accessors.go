// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
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

func (q *Queue) getShouldCheckRoundComplete() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shouldCheckRoundComplete
}

func (q *Queue) getShouldCheckTraversalComplete() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shouldCheckTraversalComplete
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

func (q *Queue) setFirstPullForRound(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.firstPullForRound = value
}

func (q *Queue) setLastPullWasPartial(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastPullWasPartial = value
}

func (q *Queue) setShouldCheckRoundComplete(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shouldCheckRoundComplete = value
}

func (q *Queue) setShouldCheckTraversalComplete(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shouldCheckTraversalComplete = value
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

func (q *Queue) incrementRoundStatsExpected(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Expected++
}

// getOrCreateRoundStatsUnlocked is a helper used internally by other locked methods
func (q *Queue) getOrCreateRoundStatsUnlocked(round int) *RoundStats {
	if q.roundStats[round] == nil {
		q.roundStats[round] = &RoundStats{}
	}
	return q.roundStats[round]
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
	FirstPullForRound  bool
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
		FirstPullForRound:  q.firstPullForRound,
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
		// Generate ULID if not present (for newly created tasks)
		nodeID = db.GenerateNodeID()
		task.ID = nodeID
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

// getPendingCountAndEnqueue returns the pending count before enqueueing, then enqueues
// Returns (count before, count after, was added)
func (q *Queue) getPendingCountAndEnqueue(task *TaskBase) (int, int, bool) {
	beforeCount := q.getPendingCount()
	wasAdded := q.enqueuePending(task)
	afterCount := q.getPendingCount()
	return beforeCount, afterCount, wasAdded
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
			// Generate ULID if not present
			nodeID = db.GenerateNodeID()
			task.ID = nodeID
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
