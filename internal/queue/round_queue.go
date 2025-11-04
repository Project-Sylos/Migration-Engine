// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync"
)

// RoundQueue manages two sub-queues for a specific round: pending and successful tasks.
type RoundQueue struct {
	mu         sync.RWMutex
	pending    []*TaskBase
	successful []*TaskBase
}

// NewRoundQueue creates a new RoundQueue instance.
func NewRoundQueue() *RoundQueue {
	return &RoundQueue{
		pending:    make([]*TaskBase, 0),
		successful: make([]*TaskBase, 0),
	}
}

// AddPending adds a task to the pending queue.
func (rq *RoundQueue) AddPending(task *TaskBase) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.pending = append(rq.pending, task)
}

// AddSuccessful adds a task to the successful queue.
func (rq *RoundQueue) AddSuccessful(task *TaskBase) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.successful = append(rq.successful, task)
}

// PopPending removes and returns the first pending task, or nil if empty.
func (rq *RoundQueue) PopPending() *TaskBase {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if len(rq.pending) == 0 {
		return nil
	}
	task := rq.pending[0]
	rq.pending = rq.pending[1:]
	return task
}

// GetSuccessfulByPath returns the first successful task matching the given path, or nil if not found.
// Does not remove the task from the queue.
func (rq *RoundQueue) GetSuccessfulByPath(path string) *TaskBase {
	rq.mu.RLock()
	defer rq.mu.RUnlock()
	for _, task := range rq.successful {
		if task.LocationPath() == path {
			return task
		}
	}
	return nil
}

// PopSuccessfulByPath removes and returns the first successful task matching the given path, or nil if not found.
func (rq *RoundQueue) PopSuccessfulByPath(path string) *TaskBase {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	for i, task := range rq.successful {
		if task.LocationPath() == path {
			// Remove task from slice
			rq.successful = append(rq.successful[:i], rq.successful[i+1:]...)
			return task
		}
	}
	return nil
}

// PendingCount returns the number of pending tasks.
func (rq *RoundQueue) PendingCount() int {
	rq.mu.RLock()
	defer rq.mu.RUnlock()
	return len(rq.pending)
}

// SuccessfulCount returns the number of successful tasks.
func (rq *RoundQueue) SuccessfulCount() int {
	rq.mu.RLock()
	defer rq.mu.RUnlock()
	return len(rq.successful)
}

// AllPending returns a copy of all pending tasks.
func (rq *RoundQueue) AllPending() []*TaskBase {
	rq.mu.RLock()
	defer rq.mu.RUnlock()
	result := make([]*TaskBase, len(rq.pending))
	copy(result, rq.pending)
	return result
}

// IsEmpty returns true if both pending and successful queues are empty.
func (rq *RoundQueue) IsEmpty() bool {
	rq.mu.RLock()
	defer rq.mu.RUnlock()
	return len(rq.pending) == 0 && len(rq.successful) == 0
}
