// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
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

func (q *Queue) setMaxKnownDepth(depth int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxKnownDepth = depth
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

func (q *Queue) isLeased(nodeID string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, exists := q.leasedKeys[nodeID]
	return exists
}

// Setters (write, use Lock)

func (q *Queue) setState(state QueueState) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.state = state
}

func (q *Queue) setPulling(pulling bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = pulling
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

func (q *Queue) addLeasedKey(nodeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.leasedKeys[nodeID] = struct{}{}
}

// Batch operations (require lock, but provide convenience methods)

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
	}
}

// Wrapper methods for operations that need to modify internal state

// enqueuePending atomically enqueues a task to the pending buffer
func (q *Queue) enqueuePending(task *TaskBase) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	beforeCount := len(q.pendingBuff)
	q.enqueuePendingLocked(task)
	return len(q.pendingBuff) > beforeCount
}

// getPendingCountAndEnqueue returns the pending count before enqueueing, then enqueues
// Returns (count before, count after, was added)
func (q *Queue) getPendingCountAndEnqueue(task *TaskBase) (int, int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	beforeCount := len(q.pendingBuff)
	q.enqueuePendingLocked(task)
	afterCount := len(q.pendingBuff)
	return beforeCount, afterCount, afterCount > beforeCount
}
