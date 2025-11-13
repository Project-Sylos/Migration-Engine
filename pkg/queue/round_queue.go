// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

// RoundQueue manages two sub-queues for a specific round: pending and successful tasks.
// Uses both slices (for ordered access) and maps (for O(1) lookups by path).
// All methods must be called while holding Queue.mu.
type RoundQueue struct {
	pending       []*TaskBase
	pendingMap    map[string]*TaskBase // path -> task for O(1) lookup
	successful    []*TaskBase
	successfulMap map[string]*TaskBase // path -> task for O(1) lookup
}

// NewRoundQueue creates a new RoundQueue instance.
func NewRoundQueue() *RoundQueue {
	return &RoundQueue{
		pending:       make([]*TaskBase, 0),
		pendingMap:    make(map[string]*TaskBase),
		successful:    make([]*TaskBase, 0),
		successfulMap: make(map[string]*TaskBase),
	}
}

// AddPending adds a task to the pending queue and map.
// Returns true if the task was added, false if a task with the same path already exists.
// Must be called with Queue.mu held.
func (rq *RoundQueue) AddPending(task *TaskBase) bool {
	path := task.LocationPath()

	// Check if a task with this path already exists in pending or successful
	if _, exists := rq.pendingMap[path]; exists {
		return false // Already in pending
	}
	if _, exists := rq.successfulMap[path]; exists {
		return false // Already completed
	}

	rq.pending = append(rq.pending, task)
	rq.pendingMap[path] = task
	return true
}

// AddSuccessful adds a task to the successful queue and map.
// Returns true if the task was added, false if a task with the same path already exists.
// Must be called with Queue.mu held.
func (rq *RoundQueue) AddSuccessful(task *TaskBase) bool {
	path := task.LocationPath()

	// Check if a task with this path already exists in successful
	if _, exists := rq.successfulMap[path]; exists {
		return false // Already completed
	}

	rq.successful = append(rq.successful, task)
	rq.successfulMap[path] = task
	return true
}

// PopPending removes and returns the first pending task, or nil if empty.
// Also removes the task from the pending map.
// Must be called with Queue.mu held.
func (rq *RoundQueue) PopPending() *TaskBase {
	if len(rq.pending) == 0 {
		return nil
	}
	task := rq.pending[0]
	rq.pending = rq.pending[1:]
	path := task.LocationPath()
	delete(rq.pendingMap, path)
	return task
}

// GetSuccessfulByPath returns the successful task matching the given path, or nil if not found.
// Does not remove the task from the queue. O(1) lookup using map.
// Must be called with Queue.mu held (or RLock).
func (rq *RoundQueue) GetSuccessfulByPath(path string) *TaskBase {
	return rq.successfulMap[path]
}

// PopSuccessfulByPath removes and returns the successful task matching the given path, or nil if not found.
// O(1) lookup using map, then removes from both map and slice.
// Must be called with Queue.mu held.
func (rq *RoundQueue) PopSuccessfulByPath(path string) *TaskBase {
	task, exists := rq.successfulMap[path]
	if !exists {
		return nil
	}
	// Remove from map
	delete(rq.successfulMap, path)
	// Remove from slice
	for i, t := range rq.successful {
		if t.LocationPath() == path {
			rq.successful = append(rq.successful[:i], rq.successful[i+1:]...)
			break
		}
	}
	return task
}

// PendingCount returns the number of pending tasks.
// Must be called with Queue.mu held (or RLock).
func (rq *RoundQueue) PendingCount() int {
	return len(rq.pending)
}

// SuccessfulCount returns the number of successful tasks.
// Must be called with Queue.mu held (or RLock).
func (rq *RoundQueue) SuccessfulCount() int {
	return len(rq.successful)
}

// AllPending returns a copy of all pending tasks.
// Must be called with Queue.mu held (or RLock).
func (rq *RoundQueue) AllPending() []*TaskBase {
	result := make([]*TaskBase, len(rq.pending))
	copy(result, rq.pending)
	return result
}

// IsEmpty returns true if both pending and successful queues are empty.
// Must be called with Queue.mu held (or RLock).
func (rq *RoundQueue) IsEmpty() bool {
	return len(rq.pending) == 0 && len(rq.successful) == 0
}
