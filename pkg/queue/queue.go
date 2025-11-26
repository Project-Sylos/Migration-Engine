// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// QueueState represents the lifecycle state of a queue.
type QueueState string

const (
	QueueStateRunning   QueueState = "running"   // Queue is active and processing
	QueueStatePaused    QueueState = "paused"    // Queue is paused
	QueueStateStopped   QueueState = "stopped"   // Queue is stopped
	QueueStateCompleted QueueState = "completed" // Traversal complete (max depth reached)
)

const (
	defaultLeaseBatchSize    = 1000
	defaultLeaseLowWatermark = 250
)

// Queue maintains round-based task queues for BFS traversal coordination.
// It handles task leasing, retry logic, and cross-queue task propagation.
// All operational state lives in BadgerDB, flushed via per-queue buffers.
type Queue struct {
	name               string               // Queue name ("src" or "dst")
	mu                 sync.RWMutex         // Protects all internal state
	state              QueueState           // Lifecycle state (running/paused/stopped/completed)
	inProgress         map[string]*TaskBase // Tasks currently being executed (keyed by path)
	pendingBuff        []*TaskBase          // Local task buffer fetched from Badger
	pendingSet         map[string]struct{}  // Fast lookup for pending buffer dedupe
	pulling            bool                 // Indicates a pull operation is active
	pullLowWM          int                  // Low watermark threshold for pulling more work
	lastPullWasPartial bool                 // True if last pull returned < batch size (signals round end)

	maxRetries  int               // Maximum retry attempts per task
	round       int               // Current BFS round/depth level
	workers     []Worker          // Workers associated with this queue (for reference only)
	badgerDB    *db.DB            // BadgerDB for operational queue storage
	srcQueue    *Queue            // For dst queue: reference to src queue for BadgerDB lookups
	coordinator *QueueCoordinator // Shared coordinator for round synchronization
	// Round-based statistics for completion detection
	roundStats  map[int]*RoundStats // Per-round statistics (key: round number, value: stats for that round)
	shutdownCtx context.Context     // Context for shutdown signaling (optional)
}

// NewQueue creates a new Queue instance.
func NewQueue(name string, maxRetries int, workerCount int, coordinator *QueueCoordinator) *Queue {
	return &Queue{
		name:        name,
		state:       QueueStateRunning,
		inProgress:  make(map[string]*TaskBase),
		pendingBuff: make([]*TaskBase, 0, defaultLeaseBatchSize),
		pendingSet:  make(map[string]struct{}),
		pullLowWM:   defaultLeaseLowWatermark,
		maxRetries:  maxRetries,
		round:       0,
		workers:     make([]Worker, 0, workerCount),
		roundStats:  make(map[int]*RoundStats),
		coordinator: coordinator,
	}
}

// InitializeWithContext sets up the queue with BadgerDB, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// For dst queues, srcQueue should be provided for BadgerDB expected children lookups.
// shutdownCtx is optional - if provided, workers will check for cancellation and exit on shutdown.
func (q *Queue) InitializeWithContext(badgerInstance *db.DB, adapter fsservices.FSAdapter, srcQueue *Queue, shutdownCtx context.Context) {
	q.mu.Lock()
	q.badgerDB = badgerInstance
	q.srcQueue = srcQueue
	q.shutdownCtx = shutdownCtx
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.Unlock()

	// Create and start workers - they manage themselves
	for i := 0; i < workerCount; i++ {
		worker := NewTraversalWorker(
			fmt.Sprintf("%s-worker-%d", q.name, i),
			q,
			badgerInstance,
			adapter,
			q.name,
			q.coordinator,
			shutdownCtx,
		)
		q.AddWorker(worker)
		go worker.Run()
	}

	// Queues are initialized - tasks will be seeded externally or propagated through Complete()
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("%s queue initialized", strings.ToUpper(q.name)), "queue", q.name, q.name)
	}
}

// Initialize is a convenience wrapper that calls InitializeWithContext with nil shutdown context.
func (q *Queue) Initialize(badgerInstance *db.DB, adapter fsservices.FSAdapter, srcQueue *Queue) {
	q.InitializeWithContext(badgerInstance, adapter, srcQueue, nil)
}

// SetShutdownContext sets the shutdown context for the queue.
// This can be called before or after Initialize, and will affect all workers.
func (q *Queue) SetShutdownContext(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdownCtx = ctx
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

// IsPulling returns false (task pulling replaced by Badger-backed leasing).
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

// WaitForCoordinatorGate blocks until the coordinator allows this queue to proceed.
// Used during startup/resume to mirror the same gating logic applied when advancing rounds.
func (q *Queue) WaitForCoordinatorGate(reason string) {
	if q.coordinator == nil {
		return
	}

	var canProceed func() bool
	switch q.name {
	case "src":
		canProceed = q.coordinator.CanSrcAdvance
	case "dst":
		canProceed = q.coordinator.CanDstAdvance
	default:
		return
	}

	if canProceed() {
		return
	}

	if reason == "" {
		reason = "coordination gate"
	}

	for !canProceed() {
		if q.shutdownCtx != nil {
			select {
			case <-q.shutdownCtx.Done():
				if logservice.LS != nil {
					_ = logservice.LS.Log(
						"debug",
						fmt.Sprintf("%s queue aborting %s due to shutdown", strings.ToUpper(q.name), reason),
						"queue",
						q.name,
						q.name,
					)
				}
				return
			default:
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"debug",
			fmt.Sprintf("%s queue coordinator gate released (%s)", strings.ToUpper(q.name), reason),
			"queue",
			q.name,
			q.name,
		)
	}
}

// Add enqueues a task into the in-memory buffer only (no Badger writes).
func (q *Queue) Add(task *TaskBase) bool {
	if task == nil {
		return false
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueuePendingLocked(task)
	return true
}

// SetRound sets the queue's current round. Used for resume operations.
func (q *Queue) SetRound(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.round = round
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

// getInProgressCount returns the number of in-progress tasks. Thread-safe.
func (q *Queue) getInProgressCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inProgress)
}

// pruneRoundsBefore removes stats for rounds less than the specified round.
// BadgerDB handles its own pruning, so we only need to clean up in-memory stats.
func (q *Queue) pruneRoundsBefore(pruneRound int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	prunedStats := 0

	// Prune round stats
	for round := range q.roundStats {
		if round < pruneRound {
			delete(q.roundStats, round)
			prunedStats++
		}
	}

	if prunedStats > 0 && logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Pruned %d round stats (rounds < %d)", prunedStats, pruneRound),
			"queue",
			q.name,
			q.name)
	}
}

// getOrCreateRoundStats returns the RoundStats for the current round, creating it if it doesn't exist.
// Must be called with q.mu.Lock() held (use within locked sections).
func (q *Queue) getOrCreateRoundStats(round int) *RoundStats {
	if q.roundStats[round] == nil {
		q.roundStats[round] = &RoundStats{}
	}
	return q.roundStats[round]
}

// Lease attempts to lease a task for execution atomically.
// Returns nil if no tasks are available, queue is paused, or completed.
func (q *Queue) Lease() *TaskBase {
	q.pullTasksIfNeeded(false)

	for attempt := 0; attempt < 2; attempt++ {
		q.mu.Lock()
		if q.state == QueueStatePaused || q.state == QueueStateCompleted || q.badgerDB == nil {
			q.mu.Unlock()
			return nil
		}

		task := q.dequeuePendingLocked()
		if task != nil {
			path := task.LocationPath()
			task.Locked = true
			q.inProgress[path] = task
			q.mu.Unlock()
			return task
		}
		q.mu.Unlock()

		// Check if we need to pull more tasks (only if buffer is low and last pull wasn't partial)
		q.pullTasksIfNeeded(false)

	}

	return nil
}

// checkRoundComplete checks if the specified round is complete (no pending tasks and no in-progress tasks).
// Returns true if the round should be marked complete. Thread-safe.
func (q *Queue) checkRoundComplete(currentRound int) bool {
	q.mu.RLock()
	// Check if queue is still running
	if q.state != QueueStateRunning {
		q.mu.RUnlock()
		return false
	}

	// Verify we're still on the same round
	if q.round != currentRound {
		q.mu.RUnlock()
		return false // Round already advanced
	}

	// Check in-progress and pending buffer first
	inProgressCount := len(q.inProgress)
	pendingBuffCount := len(q.pendingBuff)
	lastPullWasPartial := q.lastPullWasPartial
	q.mu.RUnlock()

	// If we have tasks in progress or in buffer, round isn't complete
	if inProgressCount > 0 || pendingBuffCount > 0 {
		return false
	}

	// If last pull was NOT partial (full batch), there might be more tasks in Badger
	if !lastPullWasPartial {
		return false
	}

	// Last pull was partial AND buffer is empty AND no tasks in progress
	// This means we've exhausted this round - trigger advancement
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Round %d complete: lastPullPartial=%v pendingBuff=%d inProgress=%d",
				currentRound, lastPullWasPartial, pendingBuffCount, inProgressCount),
			"queue", q.name, q.name)
	}

	return true
}

// Complete marks a task as successfully completed and propagates children to the next round.
// Moves task from processing to visited state in BadgerDB.
func (q *Queue) Complete(task *TaskBase) {
	currentRound := task.Round

	if q.badgerDB == nil {
		return
	}

	// Convert task to NodeState for BadgerDB
	state := taskToNodeState(task)
	if state == nil {
		return
	}

	path := state.Path

	q.mu.Lock()
	// Remove from in-progress (keyed by path)
	delete(q.inProgress, path)
	task.Locked = false
	task.Status = "successful"

	// Update from pending to successful in BadgerDB
	queueType := getQueueType(q.name)
	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++

	q.mu.Unlock()

	copyStatus := db.CopyStatusPending
	if !state.CopyNeeded {
		copyStatus = db.CopyStatusSuccessful
	}
	if q.name == "dst" {
		copyStatus = ""
	}

	q.enqueueStatusUpdate(queueType, currentRound, db.TraversalStatusPending, db.TraversalStatusSuccessful, copyStatus, path)

	// Buffer discovered children/file entries so they're visible for future rounds
	q.bufferDiscoveredChildren(task)

	// Handle task propagation based on queue type (may take time, don't hold lock)
	nextRound := currentRound + 1
	switch q.name {
	case "src":
		// SRC: Extract folder children and create tasks for next round
		q.handleSrcComplete(task)
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

// handleSrcComplete logs folder children discovery.
// Note: Children are already written to Badger by bufferDiscoveredChildren(), so no additional writes needed here.
func (q *Queue) handleSrcComplete(task *TaskBase) {
	// Extract only folder children (files don't need traversal)
	var childFolders []fsservices.Folder
	for _, child := range task.DiscoveredChildren {
		if !child.IsFile && child.Status == "Pending" {
			childFolders = append(childFolders, child.Folder)
		}
	}

	numChildren := len(childFolders)

	if numChildren > 0 && logservice.LS != nil {
		_ = logservice.LS.Log("debug", fmt.Sprintf("Found %d children from path %s", numChildren, task.LocationPath()), "queue", q.name, q.name)
	}

	// Children already written to Badger by bufferDiscoveredChildren() - no additional writes needed
	// They will be pulled from Badger when advancing to nextRound
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
			// Ensure folder depth reflects BFS depth (child of round N is depth N+1)
			f := child.Folder
			f.DepthLevel = nextRound
			childFolders = append(childFolders, f)
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

	var newTasks []*TaskBase
	for _, childFolder := range childFolders {
		childPath := childFolder.LocationPath
		expectedFolders, expectedFiles, err := LoadExpectedChildren(q.badgerDB, childPath)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to load expected children for %s: %v", childPath, err), "queue", q.name, q.name)
			}
			continue
		}

		newTask := &TaskBase{
			Type:            TaskTypeDstTraversal,
			Folder:          childFolder,
			ExpectedFolders: expectedFolders,
			ExpectedFiles:   expectedFiles,
			Round:           nextRound,
		}
		newTasks = append(newTasks, newTask)
	}

	if len(newTasks) == 0 {
		return
	}

	for _, newTask := range newTasks {
		q.bufferTaskInsert(newTask)
	}

	// Increment expected count for DST folder children
	q.mu.Lock()
	roundStats := q.getOrCreateRoundStats(nextRound)
	roundStats.Expected += len(newTasks)
	q.mu.Unlock()
}

func (q *Queue) bufferDiscoveredChildren(task *TaskBase) {
	if q.badgerDB == nil || len(task.DiscoveredChildren) == 0 {
		return
	}

	parentPath := fsservices.NormalizeLocationPath(task.LocationPath())
	nextRound := task.Round + 1
	indexPrefix := getChildrenIndexPrefix(q.name)
	queueType := getQueueType(q.name)

	for _, child := range task.DiscoveredChildren {
		if child.Status == "Missing" {
			continue
		}

		// For DST queues, skip folder children here - they will be written as tasks
		// with ExpectedFolders/ExpectedFiles by handleDstComplete()
		if queueType == "dst" && !child.IsFile {
			continue
		}

		state := childResultToNodeState(child, parentPath, nextRound)
		if state == nil {
			continue
		}

		traversalStatus := db.TraversalStatusPending
		switch child.Status {
		case "Successful":
			traversalStatus = db.TraversalStatusSuccessful
		case "NotOnSrc":
			traversalStatus = db.TraversalStatusNotOnSrc
		}

		copyStatus := db.CopyStatusPending
		if queueType == "dst" {
			copyStatus = ""
		}

		q.enqueueNodeInsert(queueType, nextRound, traversalStatus, copyStatus, state, indexPrefix)

		// Increment expected count for folder children (only folders need traversal)
		if !child.IsFile {
			q.mu.Lock()
			roundStats := q.getOrCreateRoundStats(nextRound)
			roundStats.Expected++
			q.mu.Unlock()
		}
	}
}

func childResultToNodeState(child ChildResult, parentPath string, depth int) *db.NodeState {
	if child.IsFile {
		file := child.File
		return &db.NodeState{
			ID:         file.Id,
			ParentID:   file.ParentId,
			ParentPath: parentPath,
			Name:       file.DisplayName,
			Path:       fsservices.NormalizeLocationPath(file.LocationPath),
			Type:       fsservices.NodeTypeFile,
			Size:       file.Size,
			MTime:      file.LastUpdated,
			Depth:      depth,
			CopyNeeded: false,
			Status:     child.Status,
		}
	}

	folder := child.Folder
	return &db.NodeState{
		ID:         folder.Id,
		ParentID:   folder.ParentId,
		ParentPath: parentPath,
		Name:       folder.DisplayName,
		Path:       fsservices.NormalizeLocationPath(folder.LocationPath),
		Type:       fsservices.NodeTypeFolder,
		Size:       0,
		MTime:      folder.LastUpdated,
		Depth:      depth,
		CopyNeeded: false,
		Status:     child.Status,
	}
}

func (q *Queue) bufferTaskInsert(task *TaskBase) {
	if q.badgerDB == nil || task == nil {
		return
	}

	queueType := getQueueType(q.name)
	copyStatus := db.CopyStatusPending
	if queueType == "dst" {
		copyStatus = ""
	}

	state := taskToNodeState(task)
	if state == nil {
		return
	}

	indexPrefix := getChildrenIndexPrefix(q.name)
	q.enqueueNodeInsert(queueType, task.Round, db.TraversalStatusPending, copyStatus, state, indexPrefix)
}

func (q *Queue) enqueueNodeInsert(queueType string, level int, traversalStatus, copyStatus string, state *db.NodeState, indexPrefix string) {
	if q.badgerDB == nil || state == nil {
		return
	}

	// Direct write to BadgerDB (no buffering)
	key := db.KeyNode(queueType, level, traversalStatus, copyStatus, state.Path)
	if err := db.InsertNodeWithIndex(q.badgerDB, key, state, indexPrefix); err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to insert node to BadgerDB: %v", err),
				"queue", q.name, q.name)
		}
	}
}

// PullTasks forces a pull from Badger to refill the in-memory buffer.
func (q *Queue) PullTasks(force bool) {
	q.pullTasks(force)
}

func (q *Queue) pullTasksIfNeeded(force bool) {
	if q.badgerDB == nil {
		return
	}

	q.mu.RLock()
	// Don't pull if queue is paused
	if q.state == QueueStatePaused {
		q.mu.RUnlock()
		return
	}
	q.mu.RUnlock()

	if force {
		q.pullTasks(true)
		return
	}

	q.mu.RLock()
	// Only pull if: queue is running, buffer is low, not already pulling, and last pull wasn't partial
	// If last pull was partial, we've exhausted this round - don't pull again until round advances
	lastPullWasPartial := q.lastPullWasPartial
	needPull := q.state == QueueStateRunning && len(q.pendingBuff) <= q.pullLowWM && !q.pulling && !lastPullWasPartial
	q.mu.RUnlock()
	if needPull {
		q.pullTasks(false)
	}
}

func (q *Queue) pullTasks(force bool) {
	if q.badgerDB == nil {
		return
	}

	queueType := getQueueType(q.name)
	taskType := TaskTypeSrcTraversal
	if q.name == "dst" {
		taskType = TaskTypeDstTraversal
	}

	q.mu.Lock()
	if !force {
		if q.state != QueueStateRunning || len(q.pendingBuff) > q.pullLowWM {
			q.mu.Unlock()
			return
		}
	}
	if q.pulling {
		q.mu.Unlock()
		return
	}
	q.pulling = true
	currentRound := q.round
	q.mu.Unlock()

	prefix := db.PrefixForStatus(queueType, currentRound, db.TraversalStatusPending)
	batch, err := db.BatchFetchByPrefix(q.badgerDB, prefix, defaultLeaseBatchSize)

	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = false

	if err != nil || len(batch) == 0 {
		// if logservice.LS != nil {
		// 	_ = logservice.LS.Log("trace",
		// 		fmt.Sprintf("PullTasks: fetched=%d err=%v round=%d", len(batch), err, currentRound),
		// 		"queue", q.name, q.name)
		// }
		return
	}

	added := 0
	for _, state := range batch {
		task := nodeStateToTask(state, taskType)
		beforeCount := len(q.pendingBuff)
		q.enqueuePendingLocked(task)
		if len(q.pendingBuff) > beforeCount {
			added++
		}
	}

	// Track if this was a partial pull (< batch size) - signals round is ending
	q.lastPullWasPartial = len(batch) < defaultLeaseBatchSize

	// if logservice.LS != nil {
	// 	_ = logservice.LS.Log("debug",
	// 		fmt.Sprintf("PullTasks: fetched=%d added=%d round=%d pendingBuff=%d inProgress=%d lastPullPartial=%v",
	// 			len(batch), added, currentRound, len(q.pendingBuff), len(q.inProgress), q.lastPullWasPartial),
	// 		"queue", q.name, q.name)
	// }
}

func (q *Queue) enqueuePendingLocked(task *TaskBase) {
	if task == nil {
		return
	}
	path := task.LocationPath()
	if path == "" {
		return
	}
	if _, exists := q.inProgress[path]; exists {
		return
	}
	if _, exists := q.pendingSet[path]; exists {
		return
	}
	q.pendingBuff = append(q.pendingBuff, task)
	q.pendingSet[path] = struct{}{}
}

func (q *Queue) dequeuePendingLocked() *TaskBase {
	for len(q.pendingBuff) > 0 {
		task := q.pendingBuff[0]
		q.pendingBuff = q.pendingBuff[1:]
		if task == nil {
			continue
		}
		path := task.LocationPath()
		delete(q.pendingSet, path)

		if task.Round < q.round {
			continue
		}
		if _, exists := q.inProgress[path]; exists {
			continue
		}
		return task
	}
	return nil
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

	// Check if we should retry
	if task.Attempts < q.maxRetries {
		task.Locked = false
		q.enqueuePendingLocked(task)
		q.mu.Unlock()
		q.pullTasksIfNeeded(false)
		return true // Will retry
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to list children from path %s", task.LocationPath()), "queue", q.name, q.name)
	}

	task.Locked = false
	task.Status = "failed"

	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++
	q.mu.Unlock()

	// Update traversal status to failed so leasing stops retrying this node.
	queueType := getQueueType(q.name)
	copyStatus := db.CopyStatusPending
	if queueType == "dst" {
		copyStatus = ""
	}

	state := taskToNodeState(task)
	if state != nil {
		q.enqueueStatusUpdate(queueType, currentRound, db.TraversalStatusPending, db.TraversalStatusFailed, copyStatus, state.Path)
	}

	if q.checkRoundComplete(currentRound) {
		q.markRoundComplete()
	}

	return false // Will not retry
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

// Clear removes all tasks from BadgerDB and resets in-progress tracking.
// Note: This is a destructive operation - use with caution.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Clear in-progress tracking
	q.inProgress = make(map[string]*TaskBase)
	q.pendingBuff = make([]*TaskBase, 0, defaultLeaseBatchSize)
	q.pendingSet = make(map[string]struct{})
	q.pulling = false

	// BadgerDB clearing would require deleting all prefixes - typically not needed
	// as BadgerDB is ephemeral and cleaned up after ETL #2
}

// RoundStats tracks statistics for a specific round.
type RoundStats struct {
	Expected  int // Expected tasks for this round (folder children inserted)
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
// Uses only in-memory counters - no BadgerDB queries.
func (q *Queue) Stats() QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return QueueStats{
		Name:         q.name,
		Round:        q.round,
		Pending:      len(q.pendingBuff),
		InProgress:   len(q.inProgress),
		TotalTracked: q.totalTrackedLocked(),
		Workers:      len(q.workers),
	}
}

// RoundStats returns the statistics for a specific round.
// Returns nil if the round has no stats yet.
func (q *Queue) RoundStats(round int) *RoundStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.roundStats[round]
}

// IncrementExpected increments the expected task count for a specific round.
// This is used to initialize Expected count (e.g., for root tasks).
func (q *Queue) IncrementExpected(round int, count int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	roundStats := q.getOrCreateRoundStats(round)
	roundStats.Expected += count
}

// totalTrackedLocked returns total active tasks (pending + in-progress) using in-memory counters. Must be called with lock held.
func (q *Queue) totalTrackedLocked() int {
	// Total = pending in buffer + in progress (active tasks only, not completed)
	return len(q.pendingBuff) + len(q.inProgress)
}

// AddWorker registers a worker with this queue for reference.
// Workers manage their own lifecycle - this is just for tracking/debugging.
func (q *Queue) AddWorker(worker Worker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers = append(q.workers, worker)
}

// Close stops the queue (no buffers to clean up anymore).
func (q *Queue) Close() {
	// No-op: Direct BadgerDB writes don't need cleanup
}

func (q *Queue) enqueueStatusUpdate(queueType string, round int, oldStatus, newStatus, copyStatus, path string) {
	if q.badgerDB == nil || path == "" {
		return
	}

	// Direct write to BadgerDB (no buffering)
	_, err := db.UpdateNodeStatus(q.badgerDB, queueType, round, oldStatus, newStatus, copyStatus, path)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to update node status in BadgerDB: %v", err),
				"queue", q.name, q.name)
		}
	}
}

func (q *Queue) enqueueCopyUpdate(queueType string, round int, traversalStatus, oldCopyStatus, newCopyStatus, path string) {
	if q.badgerDB == nil || path == "" {
		return
	}

	// Direct write to BadgerDB (no buffering)
	_, err := db.UpdateNodeCopyStatus(q.badgerDB, queueType, round, traversalStatus, oldCopyStatus, newCopyStatus, path)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to update copy status in BadgerDB: %v", err),
				"queue", q.name, q.name)
		}
	}
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
			// Src waits if it's too far ahead of dst (but continues if dst has completed)
			if !q.coordinator.CanSrcAdvance() {
				// Wait for dst to catch up
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
	q.round++
	newRound := q.round
	// Reset lastPullWasPartial since we're advancing to a new round
	q.lastPullWasPartial = false
	// Initialize stats for the new round (if not already exists)
	q.getOrCreateRoundStats(newRound)

	// Cleanup: When DST advances, prune stats for old rounds
	// BadgerDB data persists and will be cleaned up during ETL phases
	if q.name == "dst" {
		// Prune SRC round stats that are no longer needed
		// DST on round N queries SRC round N+1 when completing tasks
		// DST on round N+1 will query SRC round N+2
		// So we can safely prune SRC stats for rounds < N-1 (keeping a buffer of 2 rounds for safety)
		if q.srcQueue != nil {
			pruneRound := newRound - 2 // Prune rounds < newRound - 2
			if pruneRound > 0 {
				q.srcQueue.pruneRoundsBefore(pruneRound)
			}
		}
	}
	q.mu.Unlock()

	// Check if the new round has no tasks (max depth reached)
	hasTasks := false
	if q.badgerDB != nil {
		queueType := getQueueType(q.name)
		pref := db.PrefixForStatus(queueType, newRound, db.TraversalStatusPending)
		count, err := q.badgerDB.CountByPrefix(pref)
		if err == nil && count > 0 {
			hasTasks = true
		}
	}

	q.mu.Lock()
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

	// Cleanup: When SRC advances and DST is done, prune old SRC rounds
	// (Must be done after releasing lock to avoid deadlock)
	// Only prune if SRC is still running (not completed) and DST is completed
	if q.name == "src" && q.state == QueueStateRunning {
		// If DST is completed (but SRC is still running), we no longer need to keep old rounds for DST to query
		// Prune all rounds up to (but not including) the current round
		// Important: Only prune when DST is done, NOT when SRC is done
		if q.coordinator != nil && q.coordinator.IsDstCompleted() && !q.coordinator.IsSrcCompleted() {
			pruneRound := newRound // Prune rounds < newRound (keep only current round)
			if pruneRound > 0 {
				q.pruneRoundsBefore(pruneRound)
			}
		}
	}

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
