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
	QueueStateWaiting   QueueState = "waiting"   // Queue is waiting for coordinator to allow advancement (DST only)
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
	state              QueueState           // Lifecycle state (running/paused/stopped/completed/waiting)
	inProgress         map[string]*TaskBase // Tasks currently being executed (keyed by path)
	pendingBuff        []*TaskBase          // Local task buffer fetched from BoltDB
	pendingSet         map[string]struct{}  // Fast lookup for pending buffer dedupe
	leasedKeys         map[string]struct{}  // Path hashes already pulled/leased - prevents duplicate pulls from stale views
	pulling            bool                 // Indicates a pull operation is active
	pullLowWM          int                  // Low watermark threshold for pulling more work
	lastPullWasPartial bool                 // True if last pull returned < batch size (signals round end)
	firstPullForRound  bool                 // True if we haven't done the first pull for the current round yet (used for DST gate check)
	maxRetries         int                  // Maximum retry attempts per task
	round              int                  // Current BFS round/depth level
	workers            []Worker             // Workers associated with this queue (for reference only)
	boltDB             *db.DB               // BoltDB for operational queue storage
	srcQueue           *Queue               // For dst queue: reference to src queue for BoltDB lookups
	coordinator        *QueueCoordinator    // Coordinator for round advancement gates (DST only)
	// Round-based statistics for completion detection
	roundStats  map[int]*RoundStats // Per-round statistics (key: round number, value: stats for that round)
	shutdownCtx context.Context     // Context for shutdown signaling (optional)
	// Stats publishing for UDP logging
	statsChan chan QueueStats // Channel for publishing stats (optional, set via SetStatsChannel)
	statsTick *time.Ticker    // Ticker for periodic stats publishing (optional)
}

// NewQueue creates a new Queue instance.
func NewQueue(name string, maxRetries int, workerCount int, coordinator *QueueCoordinator) *Queue {
	return &Queue{
		name:              name,
		state:             QueueStateRunning,
		inProgress:        make(map[string]*TaskBase),
		pendingBuff:       make([]*TaskBase, 0, defaultLeaseBatchSize),
		pendingSet:        make(map[string]struct{}),
		leasedKeys:        make(map[string]struct{}),
		pullLowWM:         defaultLeaseLowWatermark,
		maxRetries:        maxRetries,
		round:             0,
		workers:           make([]Worker, 0, workerCount),
		roundStats:        make(map[int]*RoundStats),
		coordinator:       coordinator,
		firstPullForRound: true, // Start with true so first pull checks gate
	}
}

// InitializeWithContext sets up the queue with BoltDB, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// For dst queues, srcQueue should be provided for BoltDB expected children lookups.
// shutdownCtx is optional - if provided, workers will check for cancellation and exit on shutdown.
func (q *Queue) InitializeWithContext(boltInstance *db.DB, adapter fsservices.FSAdapter, srcQueue *Queue, shutdownCtx context.Context) {
	q.mu.Lock()
	q.boltDB = boltInstance
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
			shutdownCtx,
		)
		q.AddWorker(worker)
		go worker.Run()
	}

	// Coordinator handles round advancement gates for DST
	// SRC just updates coordinator when it advances

	// Start the queue's Run() method to coordinate pulling tasks and advancing rounds
	go q.Run()

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

// IsExhausted returns true if the queue has finished all traversal or has been stopped.
func (q *Queue) IsExhausted() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state == QueueStateCompleted || q.state == QueueStateStopped
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
	// Check if queue is completed before attempting to pull tasks
	q.mu.RLock()
	isCompleted := q.state == QueueStateCompleted
	q.mu.RUnlock()

	if isCompleted {
		return nil
	}

	q.pullTasksIfNeeded(false)

	for attempt := 0; attempt < 2; attempt++ {
		q.mu.Lock()
		// Block if paused, completed, or no DB (waiting doesn't block - rounds continue once started)
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
// This function uses the database as the authoritative source of truth.
// It will flush buffered writes before checking to ensure all data is persisted.
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

	// Quick check: if we have tasks in progress or in buffer, round isn't complete
	inProgressCount := len(q.inProgress)
	pendingBuffCount := len(q.pendingBuff)
	lastPullWasPartial := q.lastPullWasPartial
	boltDB := q.boltDB
	q.mu.RUnlock()

	// Fast path: If we have tasks in progress or in buffer, round isn't complete
	if inProgressCount > 0 || pendingBuffCount > 0 {
		return false
	}

	// If last pull was NOT partial (full batch), there might be more tasks in BoltDB
	// We still need to check the DB to be sure, but this is a hint we're not done
	if !lastPullWasPartial {
		return false
	}

	// At this point, in-memory state suggests completion, but we need to verify with the DB
	// No buffer to flush - all writes are direct/synchronous now

	// Now query the database as the authoritative source
	if boltDB == nil {
		return false
	}

	queueType := getQueueType(q.name)

	// Check if there are any pending items in the current round's status bucket
	// The DB is the source of truth - if there are pending items here, we're not done
	pendingCount, err := boltDB.CountStatusBucket(queueType, currentRound, db.StatusPending)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to count pending items in DB during round completion check: %v", err), "queue", q.name, q.name)
		}
		// If we can't query, assume not complete to be safe
		return false
	}

	// Round is complete only if DB confirms no pending items
	if pendingCount > 0 {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Round %d completion check: DB shows %d pending items (in-memory was empty)", currentRound, pendingCount), "queue", q.name, q.name)
		}
		return false
	}

	// DB confirms no pending items - round is truly complete
	return true
}

// checkAndAdvanceRoundIfComplete checks if the current round is complete after a task finishes.
// This should be called after removing a task from inProgress to catch completion immediately.
// This is a lightweight check that only triggers the full check if conditions look right.
func (q *Queue) checkAndAdvanceRoundIfComplete() {
	q.mu.RLock()
	currentRound := q.round
	state := q.state

	// Only check if queue is in a state where it can complete
	if state != QueueStateRunning && state != QueueStateWaiting {
		q.mu.RUnlock()
		return
	}

	// Quick check: if we still have in-progress or pending tasks, we're not done
	inProgressCount := len(q.inProgress)
	pendingBuffCount := len(q.pendingBuff)
	q.mu.RUnlock()

	// Only proceed with full check if we have no in-progress or pending tasks
	if inProgressCount > 0 || pendingBuffCount > 0 {
		return // Still have work to do, no need to check
	}

	// Use the existing checkRoundComplete which does the full check (including DB query and lastPullWasPartial)
	if q.checkRoundComplete(currentRound) {
		q.markRoundComplete()
	}
}

// Complete marks a task as successfully completed and propagates children to the next round.
// Complete marks a task as successfully completed.
// It removes the task from the queue, writes discovered children directly to BoltDB,
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
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Complete() called with task that couldn't be converted to NodeState: %v", task),
				"queue", q.name, q.name)
		}
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

	// Note: We'll check for completion again at the end after all writes are done
	// This early check helps catch completion as soon as possible

	// Prepare child nodes for insertion
	parentPath := fsservices.NormalizeLocationPath(task.LocationPath())
	var childNodesToInsert []db.InsertOperation

	// Log folder discovery with details
	if logservice.LS != nil {
		totalChildren := len(task.DiscoveredChildren)
		if totalChildren > 0 {
			foldersCount := 0
			filesCount := 0
			for _, child := range task.DiscoveredChildren {
				if child.IsFile {
					filesCount++
				} else {
					foldersCount++
				}
			}
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Discovered %d total children (folders: %d, files: %d) from path %s",
					totalChildren, foldersCount, filesCount, parentPath),
				"queue", q.name, q.name)
		}
	}

	// 2. Collect discovered children for insertion
	for _, child := range task.DiscoveredChildren {
		// For DST queues, skip folder children here - they'll be handled separately
		if queueType == "DST" && !child.IsFile {
			continue
		}

		childState := childResultToNodeState(child, parentPath, nextRound)
		if childState == nil {
			continue
		}

		// Determine traversal status based on child status
		// For SRC: folders are "Pending" (need traversal), files are "Successful" (no traversal needed)
		// For DST: folders get status from comparison, files are ALWAYS "Successful" (no traversal)
		//          Note: DST file "Pending" status means COPY needed, not traversal needed
		var status string

		if child.IsFile {
			// Files (both SRC and DST) NEVER need traversal - always mark as successful
			// For DST files, "Pending" from comparison means copy needed, not traversal
			status = db.StatusSuccessful
		} else {
			// For folders, determine traversal status from comparison/discovery
			status = db.StatusPending // Default for folders needing traversal
			switch child.Status {
			case "Successful":
				status = db.StatusSuccessful
			case "NotOnSrc":
				status = db.StatusNotOnSrc
			}
		}

		// Populate traversal status in the NodeState metadata
		childState.TraversalStatus = status

		childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
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
		totalFolders := 0
		pendingFolders := 0
		for _, child := range task.DiscoveredChildren {
			if !child.IsFile {
				totalFolders++
				if child.Status == "Pending" {
					pendingFolders++
					f := child.Folder
					f.DepthLevel = nextRound
					childFolders = append(childFolders, f)
				} else {
					if logservice.LS != nil {
						_ = logservice.LS.Log("debug",
							fmt.Sprintf("DST: Skipping folder %s at round %d - status is %s (not Pending)", child.Folder.LocationPath, nextRound, child.Status),
							"queue", q.name, q.name)
					}
				}
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

				childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
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

	// Write directly to database in a single transaction
	if q.boltDB != nil {
		// Update parent status and insert children in separate transactions (atomicity at transaction level)
		// First update parent status
		_, err := db.UpdateNodeStatus(q.boltDB, queueType, currentRound, db.StatusPending, db.StatusSuccessful, path)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to update parent node status: %v", err), "queue", q.name, q.name)
			}
		}

		// Then insert children
		if len(childNodesToInsert) > 0 {
			if err := db.BatchInsertNodes(q.boltDB, childNodesToInsert); err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("error", fmt.Sprintf("Failed to insert children: %v", err), "queue", q.name, q.name)
				}
			}
		}
	}

	// Check if round is complete after removing from inProgress and queuing writes
	// This catches completion immediately after the last task finishes
	q.checkAndAdvanceRoundIfComplete()
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
	// Don't pull if queue is paused or completed (waiting doesn't block pulls - rounds continue once started)
	if q.state == QueueStatePaused || q.state == QueueStateCompleted {
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

	// Don't pull if queue is completed (prevents deadlock on coordinator gate)
	q.mu.RLock()
	isCompleted := q.state == QueueStateCompleted
	q.mu.RUnlock()
	if isCompleted {
		return
	}

	queueType := getQueueType(q.name)
	taskType := TaskTypeSrcTraversal
	if q.name == "dst" {
		taskType = TaskTypeDstTraversal
	}

	q.mu.Lock()
	queueState := q.state
	pendingCount := len(q.pendingBuff)
	isPulling := q.pulling

	if !force {
		// Only pull if queue is running (not paused or completed)
		if queueState != QueueStateRunning || pendingCount > q.pullLowWM {
			q.mu.Unlock()
			return
		}
	} else {
		// Even when forcing, don't pull if paused
		if queueState == QueueStatePaused {
			q.mu.Unlock()
			return
		}
	}
	if isPulling {
		q.mu.Unlock()
		return
	}

	// Check coordinator gate ONLY on first pull of the round (DST only)
	// This prevents pulling tasks for a round that hasn't been approved yet
	if q.name == "dst" && q.coordinator != nil && q.firstPullForRound {
		currentRound := q.round
		canStartRound := q.coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - don't pull tasks
			q.mu.Unlock()
			return
		}
		// Gate passed - mark that we've done the first pull for this round
		q.firstPullForRound = false
		if logservice.LS != nil {
			q.coordinator.mu.RLock()
			srcRound := q.coordinator.srcRound
			q.coordinator.mu.RUnlock()
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("DST can pull first tasks for round %d (srcRound=%d)", currentRound, srcRound),
				"queue", q.name, q.name)
		}
	}

	q.pulling = true
	currentRound := q.round
	q.mu.Unlock()

	batch, err := db.BatchFetchWithKeys(q.boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)

	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = false

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch batch from BoltDB: %v", err), "queue", q.name, q.name)
		}
		// On error, don't change lastPullWasPartial - keep existing state
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
	// Empty batch (no more tasks) also counts as partial pull
	// For round 0 with only root task(s), this will be true (e.g., 1 < 1000)
	q.lastPullWasPartial = len(batch) < defaultLeaseBatchSize

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
	path := task.LocationPath() // Use path (not ID) to match Lease() and Complete()

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Failing task: path=%s round=%d type=%s currentAttempts=%d maxRetries=%d",
				path, currentRound, task.Type, task.Attempts, q.maxRetries),
			"queue", q.name, q.name)
	}

	q.mu.Lock()
	task.Attempts++

	// Remove from in-progress
	delete(q.inProgress, path)

	// Check if we should retry
	if task.Attempts < q.maxRetries {
		task.Locked = false
		q.enqueuePendingLocked(task) // Re-adds to tracked automatically
		q.mu.Unlock()
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Retrying task: path=%s round=%d attempt=%d/%d",
					path, currentRound, task.Attempts, q.maxRetries),
				"queue", q.name, q.name)
		}
		q.pullTasksIfNeeded(false)
		return true // Will retry
	}

	// Max retries reached - task is truly done
	// Remove the path hash from leased set
	pathHash := db.HashPath(path)
	delete(q.leasedKeys, pathHash)

	if logservice.LS != nil {
		_ = logservice.LS.Log("error",
			fmt.Sprintf("Failed to traverse folder %s after %d attempts (max retries exceeded) round=%d",
				path, task.Attempts, currentRound),
			"queue", q.name, q.name)
	}

	task.Locked = false
	task.Status = "failed"

	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++

	// Check if we should look for completion after removing this task
	q.mu.Unlock()

	// Update traversal status to failed so leasing stops retrying this node.
	state := taskToNodeState(task)
	if state != nil && q.boltDB != nil {
		queueType := getQueueType(q.name)
		_, err := db.UpdateNodeStatus(q.boltDB, queueType, currentRound, db.StatusPending, db.StatusFailed, state.Path)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("Failed to update node status to failed: %v", err), "queue", q.name, q.name)
			}
		}
	}

	// Check if round is complete after removing from inProgress and updating status
	// This catches completion immediately after the last task finishes
	q.checkAndAdvanceRoundIfComplete()

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

// Shutdown gracefully shuts down the queue.
// No buffer to stop - all writes are direct/synchronous now.
// SetStatsChannel sets the channel for publishing queue statistics for UDP logging.
// The queue will periodically publish stats to this channel.
func (q *Queue) SetStatsChannel(ch chan QueueStats) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Stop existing ticker if any
	if q.statsTick != nil {
		q.statsTick.Stop()
	}

	q.statsChan = ch
	if ch != nil {
		// Start publishing loop
		q.statsTick = time.NewTicker(1 * time.Second)
		go q.publishStatsLoop()
	} else {
		q.statsTick = nil
	}
}

// publishStatsLoop periodically publishes queue statistics to the stats channel.
// Exits when the queue is completed/stopped or the channel/ticker is cleared.
func (q *Queue) publishStatsLoop() {
	if q.statsChan == nil || q.statsTick == nil {
		return
	}

	for range q.statsTick.C {
		q.mu.RLock()
		state := q.state
		// Exit if queue is completed or stopped
		if state == QueueStateCompleted || state == QueueStateStopped {
			q.mu.RUnlock()
			return
		}
		stats := QueueStats{
			Name:         q.name,
			Round:        q.round,
			Pending:      len(q.pendingBuff),
			InProgress:   len(q.inProgress),
			TotalTracked: len(q.pendingBuff) + len(q.inProgress),
			Workers:      len(q.workers),
		}
		chanRef := q.statsChan
		tickRef := q.statsTick
		q.mu.RUnlock()

		// Check if channel/ticker were cleared
		if chanRef == nil || tickRef == nil {
			return
		}

		// Non-blocking send (don't block if receiver isn't ready)
		select {
		case chanRef <- stats:
		default:
		}
	}
}

// Shutdown stops the stats publishing loop and cleans up resources.
func (q *Queue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.statsTick != nil {
		q.statsTick.Stop()
		q.statsTick = nil
	}
	q.statsChan = nil
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

// Close stops the queue and cleans up resources.
// Stops stats publishing loop and ensures clean shutdown.
// If the queue is already completed, this is a no-op (queues clean themselves up on completion).
func (q *Queue) Close() {
	fmt.Printf("[DEBUG] Queue.Close() called for %s queue\n", q.name)

	// Quick check - if already completed, nothing to do (queues clean themselves up)
	q.mu.RLock()
	state := q.state
	hasStatsTick := q.statsTick != nil
	q.mu.RUnlock()

	if state == QueueStateCompleted {
		fmt.Printf("[DEBUG] Queue.Close() skipping - %s queue already completed (workers and Run() already exited)\n", q.name)
		// Still need to clean up stats ticker if it exists
		if hasStatsTick {
			// Try to stop it, but don't block if we can't get the lock
			done := make(chan struct{}, 1)
			go func() {
				q.mu.Lock()
				if q.statsTick != nil {
					q.statsTick.Stop()
					q.statsTick = nil
				}
				q.statsChan = nil
				q.mu.Unlock()
				done <- struct{}{}
			}()
			select {
			case <-done:
				fmt.Printf("[DEBUG] Queue.Close() cleaned up stats ticker for completed %s queue\n", q.name)
			case <-time.After(500 * time.Millisecond):
				fmt.Printf("[DEBUG] Queue.Close() timeout cleaning stats ticker for %s queue - continuing anyway\n", q.name)
			}
		}
		return
	}

	// Queue not completed - acquire lock normally (should be quick since workers/Run() should have exited)
	fmt.Printf("[DEBUG] Queue.Close() acquiring lock for %s queue (state=%s)\n", q.name, state)
	q.mu.Lock()
	fmt.Printf("[DEBUG] Queue.Close() acquired lock for %s queue\n", q.name)
	if q.statsTick != nil {
		q.statsTick.Stop()
		q.statsTick = nil
	}
	q.statsChan = nil
	if q.state != QueueStateCompleted && q.state != QueueStateStopped {
		q.state = QueueStateStopped
	}
	q.mu.Unlock()
	fmt.Printf("[DEBUG] Queue.Close() completed for %s queue\n", q.name)
}

// WorkerCount returns the number of workers associated with this queue.
func (q *Queue) WorkerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.workers)
}

// Run is the main queue coordination loop. It has an outer loop for rounds and an inner loop
// for each round. The outer loop checks coordinator gates before starting each round (DST only).
// The inner loop processes tasks until the round is complete.
func (q *Queue) Run() {
	// OUTER LOOP: Iterate through rounds
	for {
		// Check for shutdown
		if q.shutdownCtx != nil {
			select {
			case <-q.shutdownCtx.Done():
				return
			default:
			}
		}

		q.mu.RLock()
		state := q.state
		boltDB := q.boltDB
		q.mu.RUnlock()

		if boltDB == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// If queue is completed or stopped, exit
		if state == QueueStateCompleted || state == QueueStateStopped {
			if logservice.LS != nil {
				currentRound := q.getRound()
				_ = logservice.LS.Log("info",
					fmt.Sprintf("Run() exiting - queue %s (round %d)", state, currentRound),
					"queue", q.name, q.name)
			}
			return
		}

		// Get current round
		currentRound := q.getRound()

		// GATE CHECK: Can we start processing this round? (DST only - SRC has no gates)
		// This check happens IMMEDIATELY at the start of each iteration, before any task processing.
		if q.name == "dst" && q.coordinator != nil {
			canStartRound := q.coordinator.CanDstStartRound(currentRound)

			if !canStartRound {
				// Red light - cannot start this round yet, wait and poll
				q.mu.Lock()
				if q.state != QueueStateWaiting {
					q.state = QueueStateWaiting
					if logservice.LS != nil {
						q.coordinator.mu.RLock()
						srcRound := q.coordinator.srcRound
						srcDone := q.coordinator.srcDone
						q.coordinator.mu.RUnlock()
						if srcDone {
							_ = logservice.LS.Log("debug",
								fmt.Sprintf("DST waiting to start round %d (SRC completed, should proceed)", currentRound),
								"queue", q.name, q.name)
						} else {
							_ = logservice.LS.Log("debug",
								fmt.Sprintf("DST waiting to start round %d (srcRound=%d, need src>=%d)",
									currentRound, srcRound, currentRound+2),
								"queue", q.name, q.name)
						}
					}
				}
				q.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				continue // Loop back and check again
			}

			// Green light - can start this round, ensure state is running
			q.mu.Lock()
			if q.state == QueueStateWaiting {
				q.state = QueueStateRunning
				if logservice.LS != nil {
					q.coordinator.mu.RLock()
					srcRound := q.coordinator.srcRound
					q.coordinator.mu.RUnlock()
					_ = logservice.LS.Log("debug",
						fmt.Sprintf("DST can start round %d (srcRound=%d)", currentRound, srcRound),
						"queue", q.name, q.name)
				}
			}
			q.mu.Unlock()
		}

		// INNER LOOP: Process tasks for current round until round is complete
		for {
			// Check for shutdown
			if q.shutdownCtx != nil {
				select {
				case <-q.shutdownCtx.Done():
					return
				default:
				}
			}

			q.mu.RLock()
			innerState := q.state
			q.mu.RUnlock()

			// If paused, block here (pause blocks everything)
			if innerState == QueueStatePaused {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// If stopped or completed, exit inner loop
			if innerState == QueueStateStopped || innerState == QueueStateCompleted {
				return
			}

			// Pull tasks for current round
			q.pullTasks(true)

			// Check if round is complete
			if q.checkRoundComplete(currentRound) {
				// Round complete - exit inner loop, advance round in outer loop
				q.markRoundComplete()
				break
			}

			// Sleep briefly before checking again
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// markRoundComplete marks the current round as complete and automatically advances to next round.
func (q *Queue) markRoundComplete() {
	// Auto-advance to next round
	q.advanceToNextRound()
}

// advanceToNextRound advances the queue to the next round and cleans up old round queues.
// Note: Round advancement is now free - gating only happens when STARTING a round (checked in Run()).
func (q *Queue) advanceToNextRound() {
	// No gating here - rounds advance freely
	// Gating only happens when STARTING a round (checked in Run() outer loop)

	q.mu.Lock()
	// Ensure state is running if it was waiting
	if q.state == QueueStateWaiting {
		q.state = QueueStateRunning
	}
	q.round++
	newRound := q.round
	// Reset lastPullWasPartial since we're advancing to a new round
	q.lastPullWasPartial = false
	// Reset firstPullForRound so next pull will check coordinator gate (for DST)
	q.firstPullForRound = true
	// Initialize stats for the new round (if not already exists)
	q.getOrCreateRoundStats(newRound)
	q.mu.Unlock()

	// Update coordinator when rounds advance
	if q.coordinator != nil {
		switch q.name {
		case "src":
			q.coordinator.UpdateSrcRound(newRound)
		case "dst":
			q.coordinator.UpdateDstRound(newRound)
		}
	}

	// No buffer to flush - all writes are direct/synchronous now

	// Check if the new round has no tasks (max depth reached)
	// NOTE: This check happens immediately after advancing, but children from the previous round
	// should have already been inserted synchronously in Complete() before this was called.
	// However, we need to be careful - if no children were inserted (e.g., all were "Successful"
	// or "NotOnSrc" for DST), then this round will legitimately have no tasks.
	hasTasks := false
	if q.boltDB != nil {
		queueType := getQueueType(q.name)
		count, err := q.boltDB.CountByPrefix(queueType, newRound, db.StatusPending)
		if err == nil && count > 0 {
			hasTasks = true
		} else {
			// Debug: Log when we find no tasks in a new round
			q.mu.RLock()
			expectedCount := 0
			if stats := q.roundStats[newRound]; stats != nil {
				expectedCount = stats.Expected
			}
			q.mu.RUnlock()
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug",
					fmt.Sprintf("Round %d: No pending tasks found after advancing (count=%d, err=%v). Expected count from stats: %d",
						newRound, count, err, expectedCount),
					"queue", q.name, q.name)
			}
		}
	}

	q.mu.Lock()
	inProgressCount := len(q.inProgress)
	currentState := q.state
	q.mu.Unlock()

	// Allow completion if state is Running or Waiting (but not Paused or Stopped)
	// Waiting state can occur when DST completes all work but is waiting to start next round
	canComplete := (currentState == QueueStateRunning || currentState == QueueStateWaiting) && !hasTasks && inProgressCount == 0

	if canComplete {
		// Potential completion detected - give any in-flight Complete() calls a moment to finish
		// Since writes are now synchronous, we just need a brief pause for in-flight operations
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Round %d: No tasks detected, doing final re-check", newRound),
				"queue", q.name, q.name)
		}

		// Give any in-flight Complete() calls a moment to finish their synchronous writes
		time.Sleep(100 * time.Millisecond)

		// Re-check task count after final flush
		queueType := getQueueType(q.name)
		finalCount, err := q.boltDB.CountByPrefix(queueType, newRound, db.StatusPending)

		// Debug logging
		if logservice.LS != nil {
			q.mu.RLock()
			state := q.state
			q.mu.RUnlock()
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Completion check after flush for round %d: finalCount=%d state=%s", newRound, finalCount, state),
				"queue", q.name, q.name)
		}

		if err == nil && finalCount > 0 {
			// Tasks appeared after flush - continue processing
			// Check queue state before pulling to avoid deadlock if state changed
			q.mu.RLock()
			state := q.state
			q.mu.RUnlock()
			if state == QueueStateRunning || state == QueueStateWaiting {
				// Allow pulling even in Waiting state - tasks need to be processed
				q.pullTasks(true) // Pull the newly discovered tasks
			}
			return
		}

		// Still no tasks after final flush - mark as completed
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("No tasks found after flush for round %d - marking queue as completed", newRound),
				"queue", q.name, q.name)
		}

		q.mu.Lock()
		q.state = QueueStateCompleted
		// Get round stats while we have the lock
		roundStats := q.roundStats[newRound]
		q.mu.Unlock()

		// Stats publishing removed - queries are done directly from run.go

		// Coordinator updates and logging don't need the queue lock - do them after releasing it
		// This prevents blocking Close() calls
		switch q.name {
		case "dst":
			// Mark DST as completed in coordinator
			if q.coordinator != nil {
				q.coordinator.MarkDstCompleted()
			}
			// log round stats
			if roundStats != nil {
				_ = logservice.LS.Log("info",
					fmt.Sprintf("Round %d stats: Expected=%d Completed=%d", newRound, roundStats.Expected, roundStats.Completed),
					"queue", q.name, q.name)
			}
		case "src":
			// Mark SRC as completed - coordinator will allow DST to proceed
			if q.coordinator != nil {
				q.coordinator.MarkSrcCompleted()
				if logservice.LS != nil {
					_ = logservice.LS.Log("info",
						"SRC traversal complete - DST can now proceed",
						"queue", q.name, q.name)
				}
			}

			// log round stats
			if roundStats != nil {
				_ = logservice.LS.Log("info",
					fmt.Sprintf("Round %d stats: Expected=%d Completed=%d", newRound, roundStats.Expected, roundStats.Completed),
					"queue", q.name, q.name)
			}
		}
		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				fmt.Sprintf("Advanced to round %d with no tasks - traversal complete", newRound),
				"queue",
				q.name,
				q.name)
		}
		return
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.name, q.name)
	}

	// Pull tasks for the new round (will flush buffer again and load tasks into memory)
	q.pullTasksIfNeeded(true)
}
