// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
)

// QueueState represents the lifecycle state of a queue.
type QueueState string

const (
	QueueStateRunning   QueueState = "running"   // Queue is active and processing
	QueueStatePaused    QueueState = "paused"    // Queue is paused
	QueueStateStopped   QueueState = "stopped"   // Queue is stopped
	QueueStateCompleted QueueState = "completed" // Traversal complete (max depth reached)
)

// Queue maintains round-based task queues for BFS traversal coordination.
// It handles task leasing, retry logic, and cross-queue task propagation.
type Queue struct {
	name        string                     // Queue name ("src" or "dst")
	mu          sync.RWMutex               // Protects all internal state
	state       QueueState                 // Lifecycle state (running/paused/stopped/completed)
	inProgress  map[string]*TaskBase       // Tasks currently being executed (keyed by identifier)
	maxRetries  int                        // Maximum retry attempts per task
	round       int                        // Current BFS round/depth level
	workers     []Worker                   // Workers associated with this queue (for reference only)
	srcCtx      *fsservices.ServiceContext // For dst queue: needed to query src nodes
	srcQueue    *Queue                     // For dst queue: reference to src queue for round coordination
	coordinator *QueueCoordinator          // Shared coordinator for round synchronization
	roundQueues map[int]*RoundQueue        // Round-indexed queues (pending/successful per round)
	// Round-based statistics for completion detection
	roundStats map[int]*RoundStats // Per-round statistics (key: round number, value: stats for that round)
}

// NewQueue creates a new Queue instance.
func NewQueue(name string, maxRetries int, workerCount int, coordinator *QueueCoordinator) *Queue {
	return &Queue{
		name:        name,
		state:       QueueStateRunning,
		inProgress:  make(map[string]*TaskBase),
		maxRetries:  maxRetries,
		round:       0,
		workers:     make([]Worker, 0, workerCount),
		roundStats:  make(map[int]*RoundStats),
		coordinator: coordinator,
		roundQueues: make(map[int]*RoundQueue),
	}
}

// Initialize sets up the queue with database, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// For dst queues, srcContext and srcQueue are required for round coordination.
func (q *Queue) Initialize(database *db.DB, tableName string, adapter fsservices.FSAdapter, srcContext *fsservices.ServiceContext, srcQueue *Queue) {
	q.mu.Lock()
	q.srcCtx = srcContext
	q.srcQueue = srcQueue
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.Unlock()

	// Create and start workers - they manage themselves
	for i := 0; i < workerCount; i++ {
		worker := NewTraversalWorker(
			fmt.Sprintf("%s-worker-%d", q.name, i),
			q,
			database,
			adapter,
			tableName,
			q.name,
			q.coordinator,
		)
		q.AddWorker(worker)
		go worker.Run()
	}

	// Queues are initialized - tasks will be seeded externally or propagated through Complete()
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("%s queue initialized", strings.ToUpper(q.name)), "queue", q.name, q.name)
	}
}

// Name returns the queue's name.
func (q *Queue) Name() string {
	return q.name
}

// Round returns the current BFS round.
func (q *Queue) Round() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.round
}

// IsExhausted returns true if the queue has finished all traversal.
func (q *Queue) IsExhausted() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state == QueueStateCompleted
}

// State returns the current queue lifecycle state.
func (q *Queue) State() QueueState {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state
}

// IsPaused returns true if the queue is paused.
func (q *Queue) IsPaused() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state == QueueStatePaused
}

// IsPulling returns false (task pulling removed, using RoundQueue system instead).
func (q *Queue) IsPulling() bool {
	return false
}

// Pause pauses the queue (workers will not lease new tasks).
func (q *Queue) Pause() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.state = QueueStatePaused
}

// Resume resumes the queue after a pause.
func (q *Queue) Resume() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.state = QueueStateRunning
}

// Add enqueues a new task to the current round's pending queue.
func (q *Queue) Add(task *TaskBase) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Get or create round queue for task's round
	rq := q.getOrCreateRoundQueue(task.Round)
	if !rq.AddPending(task) {
		return false // Task with this path already exists
	}

	// Track stats
	roundStats := q.getOrCreateRoundStats(task.Round)
	roundStats.Queued++

	return true
}

// ============================================================================
// Locked attribute accessors - these methods handle locking internally
// ============================================================================

// getRound returns the current round. Thread-safe.
func (q *Queue) getRound() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.round
}

// setState sets the state. Thread-safe.
func (q *Queue) setState(state QueueState) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.state = state
}

// getInProgressCount returns the number of in-progress tasks. Thread-safe.
func (q *Queue) getInProgressCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inProgress)
}

// getRoundQueue returns the RoundQueue for the specified round, or nil if not found. Thread-safe.
func (q *Queue) getRoundQueue(round int) *RoundQueue {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.roundQueues[round]
}

// getOrCreateRoundQueue returns the RoundQueue for the specified round, creating it if needed.
// Must be called with q.mu.Lock() held (use within locked sections).
func (q *Queue) getOrCreateRoundQueue(round int) *RoundQueue {
	if q.roundQueues[round] == nil {
		q.roundQueues[round] = NewRoundQueue()
	}
	return q.roundQueues[round]
}

// deleteRoundQueue deletes the RoundQueue for the specified round.
// Must be called with q.mu.Lock() held (use within locked sections).
func (q *Queue) deleteRoundQueue(round int) {
	delete(q.roundQueues, round)
}

// getOrCreateRoundStats returns the RoundStats for the current round, creating it if it doesn't exist.
// Must be called with q.mu.Lock() held (use within locked sections).
func (q *Queue) getOrCreateRoundStats(round int) *RoundStats {
	if q.roundStats[round] == nil {
		q.roundStats[round] = &RoundStats{}
	}
	return q.roundStats[round]
}

// AddBatch enqueues multiple tasks atomically to their respective round queues.
func (q *Queue) AddBatch(tasks []*TaskBase) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	added := 0
	for _, task := range tasks {
		// Get or create round queue for task's round
		rq := q.getOrCreateRoundQueue(task.Round)
		if rq.AddPending(task) {
			added++
			// Track stats per round
			roundStats := q.getOrCreateRoundStats(task.Round)
			roundStats.Queued++
		}
	}
	return added
}

// Lease attempts to lease a task for execution atomically.
// Returns nil if no tasks are available, queue is paused, or completed.
// The task is atomically removed from current round's pending queue and moved to in-progress.
func (q *Queue) Lease() *TaskBase {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Don't lease if paused or completed
	if q.state == QueueStatePaused || q.state == QueueStateCompleted {
		return nil
	}

	// Get current round's queue
	rq := q.getOrCreateRoundQueue(q.round)

	// Pop a pending task
	task := rq.PopPending()
	if task == nil {
		return nil
	}

	// Mark as locked and move to in-progress
	task.Locked = true
	id := task.Identifier()
	q.inProgress[id] = task

	return task
}

// GetSuccessfulTask returns a successful task from the specified round matching the path, without removing it.
// This is used by DST queues to get expected children from SRC queues.
// Note: We don't remove the task because multiple DST tasks may need to reference the same SRC task
// when they complete and need to create next-round tasks for their children.
func (q *Queue) GetSuccessfulTask(round int, path string) *TaskBase {
	rq := q.getRoundQueue(round)
	if rq == nil {
		return nil
	}
	return rq.GetSuccessfulByPath(path)
}

// checkRoundComplete checks if the specified round is complete (no pending tasks and no in-progress tasks).
// Returns true if the round should be marked complete. Thread-safe.
func (q *Queue) checkRoundComplete(currentRound int) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// Check if queue is still running
	if q.state != QueueStateRunning {
		return false
	}

	// Verify we're still on the same round
	if q.round != currentRound {
		return false // Round already advanced
	}

	// Get round queue
	currentRoundQueue := q.roundQueues[currentRound]
	if currentRoundQueue == nil {
		return false // Round queue was deleted
	}

	// Check if round is complete: no pending tasks and no in-progress tasks
	pendingCount := currentRoundQueue.PendingCount()
	inProgressCount := len(q.inProgress)

	return pendingCount == 0 && inProgressCount == 0
}

// Complete marks a task as successfully completed and propagates children to the next round.
func (q *Queue) Complete(task *TaskBase) {
	currentRound := task.Round

	q.mu.Lock()
	id := task.Identifier()
	delete(q.inProgress, id)

	task.Locked = false
	task.Status = "successful"

	// Get or create round queue (must hold lock)
	currentRoundQueue := q.getOrCreateRoundQueue(currentRound)

	// Move task to successful queue - if already successful, skip processing
	wasAdded := currentRoundQueue.AddSuccessful(task)
	if !wasAdded {
		// Task already completed, just remove from in-progress and return
		q.mu.Unlock()
		return
	}

	// Increment completed count for current round
	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++

	q.mu.Unlock()

	// Handle task propagation based on queue type (may take time, don't hold lock)
	nextRound := currentRound + 1
	switch q.name {
	case "src":
		// SRC: Extract folder children and create tasks for next round
		q.handleSrcComplete(task, nextRound)
	case "dst":
		// DST: For each child, get corresponding SRC task and create DST tasks for next round
		q.handleDstComplete(task, nextRound)
	}

	// Check if round is complete and should advance
	if q.checkRoundComplete(currentRound) {
		q.markRoundComplete()
		return
	}
}

// handleSrcComplete creates tasks for discovered folder children and adds them to the next round's pending queue.
func (q *Queue) handleSrcComplete(task *TaskBase, nextRound int) {
	// Extract only folder children (files don't need traversal)
	var childFolders []fsservices.Folder
	for _, child := range task.DiscoveredChildren {
		if !child.IsFile && child.Status == "Pending" {
			childFolders = append(childFolders, child.Folder)
		}
	}

	numChildren := len(childFolders)

	if numChildren == 0 {
		return // No folders to traverse
	} else {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Found %d children from path %s", numChildren, task.LocationPath()), "queue", q.name, q.name)
		}
	}

	// Create tasks for next round
	var newTasks []*TaskBase
	for _, folder := range childFolders {
		newTasks = append(newTasks, &TaskBase{
			Type:   TaskTypeSrcTraversal,
			Folder: folder,
			Round:  nextRound,
		})
	}

	// Add to next round's pending queue
	q.mu.Lock()
	nextRoundQueue := q.getOrCreateRoundQueue(nextRound)
	actuallyAdded := 0
	for _, newTask := range newTasks {
		if nextRoundQueue.AddPending(newTask) {
			actuallyAdded++
		}
	}
	roundStats := q.getOrCreateRoundStats(nextRound)
	roundStats.Queued += actuallyAdded
	q.mu.Unlock()
}

// handleDstComplete creates DST tasks for next round by matching with SRC successful tasks.
// This follows the same logic as seeding: for each discovered child folder, we look up the
// corresponding SRC task for that child's path at the same round to get expected children.
func (q *Queue) handleDstComplete(task *TaskBase, nextRound int) {
	if q.srcQueue == nil {
		return // No src queue to match against
	}

	// Extract folder children from discovered children
	// Only include "Pending" folders - "Missing" and "NotOnSrc" are diagnostic only and should not be traversed
	var childFolders []fsservices.Folder
	for _, child := range task.DiscoveredChildren {
		if !child.IsFile && child.Status == "Pending" {
			childFolders = append(childFolders, child.Folder)
		}
	}

	numChildren := len(childFolders)

	if numChildren == 0 {
		return // No folders to traverse
	} else {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Found %d children from path %s", numChildren, task.LocationPath()), "queue", q.name, q.name)
		}
	}

	// For each child folder, look up its corresponding SRC task at the next round
	// When DST task at round N completes, it creates tasks for round N+1
	// The SRC tasks for those children were completed in round N+1 (because SRC is one round ahead)
	var newTasks []*TaskBase

	for _, childFolder := range childFolders {
		// Get the SRC task for this child folder at the next round
		// This SRC task's discovered children become expected children for the child DST task
		childPath := childFolder.LocationPath
		srcTask := q.srcQueue.GetSuccessfulTask(nextRound, childPath)

		// Extract expected children from SRC task's discovered children (if found)
		var expectedFolders []fsservices.Folder
		var expectedFiles []fsservices.File
		if srcTask != nil {
			for _, srcChild := range srcTask.DiscoveredChildren {
				if srcChild.IsFile {
					expectedFiles = append(expectedFiles, srcChild.File)
				} else {
					expectedFolders = append(expectedFolders, srcChild.Folder)
				}
			}
		}

		// Create DST task for this child with expected children from its corresponding SRC task
		newTasks = append(newTasks, &TaskBase{
			Type:            TaskTypeDstTraversal,
			Folder:          childFolder,
			ExpectedFolders: expectedFolders,
			ExpectedFiles:   expectedFiles,
			Round:           nextRound,
		})
	}

	// Add to next round's pending queue
	q.mu.Lock()
	nextRoundQueue := q.getOrCreateRoundQueue(nextRound)
	actuallyAdded := 0
	for _, newTask := range newTasks {
		if nextRoundQueue.AddPending(newTask) {
			actuallyAdded++
		}
	}
	roundStats := q.getOrCreateRoundStats(nextRound)
	roundStats.Queued += actuallyAdded
	q.mu.Unlock()
}

// Fail handles a failed task. If retry limit is not exceeded, re-queues the task.
// Otherwise, marks it as failed and drops it from memory (DB write already happened).
// Also checks for round completion when all tasks are exhausted (succeeded or failed).
func (q *Queue) Fail(task *TaskBase) bool {
	currentRound := task.Round

	q.mu.Lock()
	id := task.Identifier()
	task.Attempts++

	// Remove from in-progress
	delete(q.inProgress, id)

	// Get round queue (must hold lock)
	currentRoundQueue := q.getOrCreateRoundQueue(currentRound)

	// Check if we should retry
	if task.Attempts < q.maxRetries {
		// Re-queue for retry in the same round
		task.Locked = false
		currentRoundQueue.AddPending(task)
		q.mu.Unlock()
		return true // Will retry
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to list children from path %s", task.LocationPath()), "queue", q.name, q.name)
	}

	// Max retries exceeded - task is already written to DB with failed status
	// Just drop it from memory (no need to track it further)
	task.Locked = false
	task.Status = "failed"

	// Increment completed count for current round (failed tasks still count as completed work)
	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++

	// Check if round is complete and should advance
	if q.checkRoundComplete(currentRound) {
		q.mu.Unlock()
		q.markRoundComplete()
		return false
	}

	q.mu.Unlock()
	return false // Will not retry
}

// PendingCount returns the number of tasks waiting to be leased in the current round.
func (q *Queue) PendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	rq := q.roundQueues[q.round]
	if rq == nil {
		return 0
	}
	return rq.PendingCount()
}

// InProgressCount returns the number of tasks currently being executed.
func (q *Queue) InProgressCount() int {
	return q.getInProgressCount()
}

// TotalTracked returns the total number of tasks across all rounds (pending + in-progress).
func (q *Queue) TotalTracked() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.totalTrackedLocked()
}

// IsEmpty returns whether the queue has no pending or in-progress tasks in the current round.
func (q *Queue) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	rq := q.roundQueues[q.round]
	return (rq == nil || rq.PendingCount() == 0) && len(q.inProgress) == 0
}

// Clear removes all tasks from all round queues and resets in-progress tracking.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.roundQueues = make(map[int]*RoundQueue)
	q.inProgress = make(map[string]*TaskBase)
}

// RoundStats tracks statistics for a specific round.
type RoundStats struct {
	Queued    int // Tasks queued in this round (from AddBatch)
	Completed int // Tasks completed in this round (successful + failed)
}

// Stats returns current queue statistics.
type QueueStats struct {
	Name         string
	Round        int
	Pending      int
	InProgress   int
	TotalTracked int
	Workers      int
}

// Stats returns a snapshot of the queue's current state.
func (q *Queue) Stats() QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	rq := q.roundQueues[q.round]
	pending := 0
	if rq != nil {
		pending = rq.PendingCount()
	}

	return QueueStats{
		Name:         q.name,
		Round:        q.round,
		Pending:      pending,
		InProgress:   len(q.inProgress),
		TotalTracked: q.totalTrackedLocked(),
		Workers:      len(q.workers),
	}
}

// totalTrackedLocked returns total tracked tasks. Must be called with lock held.
func (q *Queue) totalTrackedLocked() int {
	total := len(q.inProgress)
	for _, rq := range q.roundQueues {
		total += rq.PendingCount()
	}
	return total
}

// AddWorker registers a worker with this queue for reference.
// Workers manage their own lifecycle - this is just for tracking/debugging.
func (q *Queue) AddWorker(worker Worker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers = append(q.workers, worker)
}

// WorkerCount returns the number of workers associated with this queue.
func (q *Queue) WorkerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.workers)
}

// markRoundComplete marks the current round as complete and automatically advances to next round.
func (q *Queue) markRoundComplete() {
	currentRound := q.getRound()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Round %d complete", currentRound), "queue", q.name, q.name)
	}

	// Auto-advance to next round
	q.advanceToNextRound()
}

// advanceToNextRound advances the queue to the next round and cleans up old round queues.
// Uses coordinator to ensure bounded concurrency between src and dst queues.
func (q *Queue) advanceToNextRound() {
	// Check coordinator conditions before advancing
	if q.coordinator != nil {
		switch q.name {
		case "src":
			// Src waits if it's too far ahead of dst, or if dst has completed early
			if !q.coordinator.CanSrcAdvance() {
				// Check if dst completed - if so, mark src as completed (migration failed)
				if q.coordinator.IsDstCompleted() {
					q.setState(QueueStateCompleted)
					q.coordinator.UpdateSrcCompleted()
					if logservice.LS != nil {
						_ = logservice.LS.Log("info",
							"SRC queue stopping - DST completed early (migration will fail)",
							"queue",
							q.name,
							q.name)
					}
					return
				}
				// Otherwise wait for dst to catch up
				for !q.coordinator.CanSrcAdvance() {
					time.Sleep(50 * time.Millisecond)
				}
			}
		case "dst":
			// Dst waits until src is at least 3 rounds ahead (via CanDstAdvance check)
			// This ensures src has completed the round we need to query before dst advances
			for !q.coordinator.CanDstAdvance() {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	q.mu.Lock()
	completedRound := q.round
	q.round++
	newRound := q.round
	// Initialize stats for the new round (if not already exists)
	q.getOrCreateRoundStats(newRound)

	// Cleanup: When DST advances, clear previous round queues for both DST and SRC
	// Note: We don't delete SRC round queues here because DST workers might still be
	// accessing them via GetSuccessfulTask(). Instead, we only delete DST's own completed round.
	// SRC round queues will be cleaned up later when safe (after DST round advances further).
	if q.name == "dst" {
		// Clean up DST's completed round
		q.deleteRoundQueue(completedRound)

		// Don't delete SRC round queues here - workers may still be accessing them
		// SRC queues will be cleaned up naturally as rounds progress
	}

	// Check if the new round has no tasks (max depth reached)
	newRoundQueue := q.roundQueues[newRound]
	hasTasks := false
	if newRoundQueue != nil {
		hasTasks = newRoundQueue.PendingCount() > 0
	}

	if q.state == QueueStateRunning && !hasTasks && len(q.inProgress) == 0 {
		// No tasks in new round means we've reached max depth - traversal complete
		q.state = QueueStateCompleted
		switch q.name {
		case "dst":
			if q.coordinator != nil {
				q.coordinator.UpdateDstCompleted()
			}
		case "src":
			if q.coordinator != nil {
				q.coordinator.UpdateSrcCompleted()
			}
		}
		q.mu.Unlock()
		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				fmt.Sprintf("Advanced to round %d with no tasks - traversal complete", newRound),
				"queue",
				q.name,
				q.name)
		}
		return
	}

	q.mu.Unlock()

	// Update coordinator state
	if q.coordinator != nil {
		switch q.name {
		case "src":
			q.coordinator.UpdateSrcRound(newRound)
		case "dst":
			q.coordinator.UpdateDstRound(newRound)
		}
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.name, q.name)
	}
}
