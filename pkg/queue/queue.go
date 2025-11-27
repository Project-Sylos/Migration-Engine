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
// All operational state lives in BoltDB, flushed via per-queue buffers.
type Queue struct {
	name               string               // Queue name ("src" or "dst")
	mu                 sync.RWMutex         // Protects all internal state
	state              QueueState           // Lifecycle state (running/paused/stopped/completed)
	inProgress         map[string]*TaskBase // Tasks currently being executed (keyed by path)
	pendingBuff        []*TaskBase          // Local task buffer fetched from BoltDB
	pendingSet         map[string]struct{}  // Fast lookup for pending buffer dedupe
	leasedKeys         map[string]struct{}  // Path hashes already pulled/leased - prevents duplicate pulls from stale views
	pulling            bool                 // Indicates a pull operation is active
	pullLowWM          int                  // Low watermark threshold for pulling more work
	lastPullWasPartial bool                 // True if last pull returned < batch size (signals round end)

	maxRetries  int               // Maximum retry attempts per task
	round       int               // Current BFS round/depth level
	workers     []Worker          // Workers associated with this queue (for reference only)
	boltDB      *db.DB            // BoltDB for operational queue storage
	writeBuffer *db.WriteBuffer   // Buffer for batching database writes
	srcQueue    *Queue            // For dst queue: reference to src queue for BoltDB lookups
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
		leasedKeys:  make(map[string]struct{}),
		pullLowWM:   defaultLeaseLowWatermark,
		maxRetries:  maxRetries,
		round:       0,
		workers:     make([]Worker, 0, workerCount),
		roundStats:  make(map[int]*RoundStats),
		coordinator: coordinator,
	}
}

// InitializeWithContext sets up the queue with BoltDB, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// For dst queues, srcQueue should be provided for BoltDB expected children lookups.
// shutdownCtx is optional - if provided, workers will check for cancellation and exit on shutdown.
func (q *Queue) InitializeWithContext(boltInstance *db.DB, adapter fsservices.FSAdapter, srcQueue *Queue, shutdownCtx context.Context) {
	q.mu.Lock()
	q.boltDB = boltInstance
	// Initialize write buffer with reasonable defaults: 1000 items or 3 seconds
	q.writeBuffer = db.NewWriteBuffer(boltInstance, 1000, 3*time.Second)
	q.srcQueue = srcQueue
	q.shutdownCtx = shutdownCtx
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.Unlock()

	// Create and start workers - they manage themselves
	for i := 0; i < workerCount; i++ {
		worker := NewTraversalWorker(
			fmt.Sprintf("%s-worker-%d", q.name, i),
			q,
			boltInstance,
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
func (q *Queue) Initialize(boltInstance *db.DB, adapter fsservices.FSAdapter, srcQueue *Queue) {
	q.InitializeWithContext(boltInstance, adapter, srcQueue, nil)
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

// Add enqueues a task into the in-memory buffer only (no database writes).
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
		if q.state == QueueStatePaused || q.state == QueueStateCompleted || q.boltDB == nil {
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

	// If last pull was NOT partial (full batch), there might be more tasks in BoltDB
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
// Complete marks a task as successfully completed.
// It removes the task from the queue, bulk inserts discovered children into BoltDB,
// updates the parent node status to "successful traversal", increments the completed count,
// and checks if the round is complete.
func (q *Queue) Complete(task *TaskBase) {
	currentRound := task.Round

	if q.boltDB == nil {
		return
	}

	// Convert task to NodeState for BoltDB
	state := taskToNodeState(task)
	if state == nil {
		return
	}

	path := state.Path
	queueType := getQueueType(q.name)
	nextRound := currentRound + 1

	q.mu.Lock()
	// Remove from in-progress (keyed by path)
	delete(q.inProgress, path)

	// Remove the path hash from leased set
	pathHash := db.HashPath(path)
	delete(q.leasedKeys, pathHash)

	task.Locked = false
	task.Status = "successful"

	// Increment completed count (even if failed, this is a "processed" counter)
	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++
	q.mu.Unlock()

	// Prepare bulk write items
	var writeItems []db.WriteItem

	// 1. Update parent node status to "successful traversal"
	writeItems = append(writeItems, db.WriteItem{
		Type:      "update_status",
		QueueType: queueType,
		Level:     currentRound,
		OldStatus: db.StatusPending,
		NewStatus: db.StatusSuccessful,
		Path:      path,
	})

	// 2. Bulk insert discovered children
	parentPath := fsservices.NormalizeLocationPath(task.LocationPath())

	for _, child := range task.DiscoveredChildren {

		// For DST queues, skip folder children here - they'll be handled separately
		if queueType == "dst" && !child.IsFile {
			continue
		}

		childState := childResultToNodeState(child, parentPath, nextRound)
		if childState == nil {
			continue
		}

		// Determine traversal status based on child status
		// For SRC: folders are "Pending" (need traversal), files are "Successful" (no traversal needed)
		// For DST: status comes from comparison ("Pending", "Successful", "NotOnSrc", "Missing")
		status := db.StatusPending
		switch child.Status {
		case "Successful":
			status = db.StatusSuccessful
		case "NotOnSrc":
			status = db.StatusNotOnSrc
		}

		// Populate traversal status in the NodeState metadata
		childState.TraversalStatus = status

		writeItems = append(writeItems, db.WriteItem{
			Type:      "insert",
			QueueType: queueType,
			Level:     nextRound,
			Status:    status,
			State:     childState,
		})

		// Increment expected count for folder children (only folders need traversal)
		if !child.IsFile {
			q.mu.Lock()
			roundStats := q.getOrCreateRoundStats(nextRound)
			roundStats.Expected++
			q.mu.Unlock()
		}
	}

	// 3. Handle DST queue special case: create tasks with ExpectedFolders/ExpectedFiles
	if q.name == "dst" && q.srcQueue != nil {
		var childFolders []fsservices.Folder
		for _, child := range task.DiscoveredChildren {
			if !child.IsFile && child.Status == "Pending" {
				f := child.Folder
				f.DepthLevel = nextRound
				childFolders = append(childFolders, f)
			}
		}

		for _, childFolder := range childFolders {
			childPath := childFolder.LocationPath
			expectedFolders, expectedFiles, err := LoadExpectedChildren(q.boltDB, childPath, nextRound)
			if err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to load expected children for %s at level %d: %v", childPath, nextRound, err), "queue", q.name, q.name)
				}
				continue
			}

			// Create task state for DST child
			taskState := taskToNodeState(&TaskBase{
				Type:            TaskTypeDstTraversal,
				Folder:          childFolder,
				ExpectedFolders: expectedFolders,
				ExpectedFiles:   expectedFiles,
				Round:           nextRound,
			})
			if taskState != nil {
				// Populate traversal status in the NodeState metadata
				taskState.TraversalStatus = db.StatusPending

				writeItems = append(writeItems, db.WriteItem{
					Type:      "insert",
					QueueType: queueType,
					Level:     nextRound,
					Status:    db.StatusPending,
					State:     taskState,
				})

				q.mu.Lock()
				roundStats := q.getOrCreateRoundStats(nextRound)
				roundStats.Expected++
				q.mu.Unlock()
			}
		}
	}

	// Add all writes to the buffer (will auto-flush if threshold reached)
	if len(writeItems) > 0 && q.writeBuffer != nil {
		q.writeBuffer.AddBatch(writeItems)
	}

	// Check if round is complete and should advance
	if q.checkRoundComplete(currentRound) {
		q.markRoundComplete()
		return
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

// PullTasks forces a pull from BoltDB to refill the in-memory buffer.
func (q *Queue) PullTasks(force bool) {
	q.pullTasks(force)
}

func (q *Queue) pullTasksIfNeeded(force bool) {
	if q.boltDB == nil {
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
	if q.boltDB == nil {
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
	writeBuffer := q.writeBuffer
	q.mu.Unlock()

	// Force flush any pending writes before pulling to ensure we see all updates
	if writeBuffer != nil {
		if err := writeBuffer.Flush(); err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to flush write buffer before pull: %v", err), "queue", q.name, q.name)
			}
		}
	}

	batch, err := db.BatchFetchWithKeys(q.boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)

	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = false

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch batch from BoltDB: %v", err), "queue", q.name, q.name)
		}
		q.pulling = false
		return
	}

	added := 0
	for _, item := range batch {
		// Skip path hashes we've already leased (prevents duplicate pulls from stale views)
		if _, exists := q.leasedKeys[item.Key]; exists {
			continue
		}

		// Mark this path hash as leased
		q.leasedKeys[item.Key] = struct{}{}

		task := nodeStateToTask(item.State, taskType)

		// For DST tasks, populate ExpectedFolders/ExpectedFiles from SRC using the children index
		if q.name == "dst" && task.IsFolder() {
			expectedFolders, expectedFiles, err := LoadExpectedChildren(q.boltDB, task.Folder.LocationPath, currentRound)
			if err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to load expected children for %s at level %d: %v", task.Folder.LocationPath, currentRound, err), "queue", q.name, q.name)
				}
				// Continue anyway - empty expected children will mark everything as NotOnSrc
			} else {
				task.ExpectedFolders = expectedFolders
				task.ExpectedFiles = expectedFiles
			}
		}

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
	path := task.LocationPath() // Use path (not ID) to match Lease() and Complete()
	task.Attempts++

	// Remove from in-progress
	delete(q.inProgress, path)

	// Check if we should retry
	if task.Attempts < q.maxRetries {
		task.Locked = false
		q.enqueuePendingLocked(task) // Re-adds to tracked automatically
		q.mu.Unlock()
		q.pullTasksIfNeeded(false)
		return true // Will retry
	}

	// Max retries reached - task is truly done
	// Remove the path hash from leased set
	pathHash := db.HashPath(path)
	delete(q.leasedKeys, pathHash)

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to list children from path %s", task.LocationPath()), "queue", q.name, q.name)
	}

	task.Locked = false
	task.Status = "failed"

	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++
	q.mu.Unlock()

	// Update traversal status to failed so leasing stops retrying this node.
	state := taskToNodeState(task)
	if state != nil && q.writeBuffer != nil {
		queueType := getQueueType(q.name)
		writeItem := db.WriteItem{
			Type:      "update_status",
			QueueType: queueType,
			Level:     currentRound,
			OldStatus: db.StatusPending,
			NewStatus: db.StatusFailed,
			Path:      state.Path,
		}
		q.writeBuffer.Add(writeItem)
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

// Clear removes all tasks from BoltDB and resets in-progress tracking.
// Note: This is a destructive operation - use with caution.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Clear in-progress tracking
	q.inProgress = make(map[string]*TaskBase)
	q.pendingBuff = make([]*TaskBase, 0, defaultLeaseBatchSize)
	q.pendingSet = make(map[string]struct{})
	q.pulling = false

	// BoltDB clearing would require deleting all buckets - typically not needed
	// as BoltDB is ephemeral and cleaned up after ETL #2
}

// Shutdown gracefully shuts down the queue, stopping the write buffer and flushing pending writes.
func (q *Queue) Shutdown() {
	q.mu.Lock()
	wb := q.writeBuffer
	q.mu.Unlock()

	if wb != nil {
		wb.Stop()
	}
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
// Uses only in-memory counters - no database queries.
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
	// Stop write buffer and flush pending writes
	q.Shutdown()
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
	writeBuffer := q.writeBuffer
	q.mu.Unlock()

	// This ensures all child inserts from previous round are visible in DB
	if writeBuffer != nil {
		if err := writeBuffer.Flush(); err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to flush buffer after round advance: %v", err), "queue", q.name, q.name)
			}
		}
	}

	// Check if the new round has no tasks (max depth reached)
	hasTasks := false
	if q.boltDB != nil {
		queueType := getQueueType(q.name)
		count, err := q.boltDB.CountByPrefix(queueType, newRound, db.StatusPending)
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

	// Pull tasks for the new round (will flush buffer again and load tasks into memory)
	q.pullTasksIfNeeded(true)
}
