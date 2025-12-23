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
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// RoundInfo tracks statistics and metadata for a specific BFS round.
type RoundInfo struct {
	Round           int       // Round number
	PullCount       int       // Number of pull operations performed
	ItemsYielded    int       // Total items pulled across all operations
	ExpectedCount   int       // Expected items from DB (if known)
	TasksCompleted  int       // Successfully completed tasks
	TasksFailed     int       // Failed tasks
	StartTime       time.Time // When this round started
	LastPullTime    time.Time // Timestamp of last pull operation
	AvgTasksPerSec  float64   // Rolling average tasks/sec
	LastPartialPull bool      // Whether the last pull was partial (< batch size)
}

// Worker represents a concurrent task executor.
// Each worker independently polls its queue for work, leases tasks,
// executes them, and reports results back to the queue and database.
type Worker interface {
	Run() // Main execution loop - polls queue and processes tasks
}

// QueueState represents the lifecycle state of a queue.
type QueueState string

const (
	QueueStateRunning   QueueState = "running"   // Queue is active and processing
	QueueStatePaused    QueueState = "paused"    // Queue is paused
	QueueStateStopped   QueueState = "stopped"   // Queue is stopped
	QueueStateWaiting   QueueState = "waiting"   // Queue is waiting for coordinator to allow advancement (DST only)
	QueueStateCompleted QueueState = "completed" // Traversal complete (max depth reached)
)

// QueueMode represents the operation mode of a queue.
type QueueMode string

const (
	QueueModeTraversal QueueMode = "traversal" // Normal BFS traversal
	QueueModeRetry     QueueMode = "retry"     // Retry failed tasks sweep
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
	mode               QueueMode            // Operation mode (traversal/exclusion/retry)
	mu                 sync.RWMutex         // Protects all internal state
	state              QueueState           // Lifecycle state (running/paused/stopped/completed/waiting)
	inProgress         map[string]*TaskBase // Tasks currently being executed (keyed by ULID)
	pendingBuff        []*TaskBase          // Local task buffer fetched from BoltDB
	pendingSet         map[string]struct{}  // Fast lookup for pending buffer dedupe (keyed by ULID)
	leasedKeys         map[string]struct{}  // ULIDs already pulled/leased - prevents duplicate pulls from stale views
	pulling            bool                 // Indicates a pull operation is active
	pullLowWM          int                  // Low watermark threshold for pulling more work
	lastPullWasPartial bool                 // True if last pull returned fewer tasks than requested (partial batch)
	maxRetries         int                  // Maximum retry attempts per task
	round              int                  // Current BFS round/depth level
	roundInfoMap       map[int]*RoundInfo   // Per-round statistics and metadata (key: round number)
	workers            []Worker             // Workers associated with this queue (for reference only)
	boltDB             *db.DB               // BoltDB for operational queue storage
	coordinator        *QueueCoordinator    // Coordinator for round advancement gates (DST only)
	outputBuffer       *db.OutputBuffer     // Buffer for batched write operations
	// Round-based statistics for completion detection
	roundStats  map[int]*RoundStats // Per-round statistics (key: round number, value: stats for that round)
	shutdownCtx context.Context     // Context for shutdown signaling (optional)
	// Stats publishing for UDP logging
	statsChan chan QueueStats // Channel for publishing stats (optional, set via SetStatsChannel)
	statsTick *time.Ticker    // Ticker for periodic stats publishing (optional)
	// Task execution time tracking
	executionTimeDeltas []time.Duration // Buffer of task execution times (lease to complete/fail)
	avgExecutionTime    time.Duration   // Average execution time (calculated periodically)
	lastAvgTime         time.Time       // Last time average was calculated
	avgInterval         time.Duration   // Interval for calculating averages
	// Retry sweep specific fields
	maxKnownDepth int // Maximum known depth from previous traversal (for retry sweep)
}

// NewQueue creates a new Queue instance.
func NewQueue(name string, maxRetries int, workerCount int, coordinator *QueueCoordinator) *Queue {
	return &Queue{
		name:                name,
		mode:                QueueModeTraversal, // Default to traversal mode
		state:               QueueStateRunning,
		inProgress:          make(map[string]*TaskBase),
		pendingBuff:         make([]*TaskBase, 0, defaultLeaseBatchSize),
		pendingSet:          make(map[string]struct{}),
		leasedKeys:          make(map[string]struct{}),
		pullLowWM:           defaultLeaseLowWatermark,
		maxRetries:          maxRetries,
		round:               0,
		workers:             make([]Worker, 0, workerCount),
		roundStats:          make(map[int]*RoundStats),
		roundInfoMap:        make(map[int]*RoundInfo), // Initialize round info map
		coordinator:         coordinator,
		executionTimeDeltas: make([]time.Duration, 0, 100), // Buffer capacity 100
		avgInterval:         5 * time.Second,               // Calculate average every 5 seconds
		lastAvgTime:         time.Now(),
		maxKnownDepth:       -1, // -1 means not set yet
	}
}

// SetMode sets the queue operation mode.
func (q *Queue) SetMode(mode QueueMode) {
	q.setMode(mode)
}

// SetMaxKnownDepth sets the maximum known depth for retry sweeps.
// Only applicable when mode is QueueModeRetry.
// Set to -1 to auto-detect from database, or a non-negative value to use a specific depth.
func (q *Queue) SetMaxKnownDepth(depth int) {
	q.setMaxKnownDepth(depth)
}

// GetMode returns the current queue mode (implements worker.QueueAccessor).
func (q *Queue) GetMode() string {
	return string(q.getMode())
}

// GetBoltDB returns the BoltDB instance (implements worker.QueueAccessor).
func (q *Queue) GetBoltDB() interface{} {
	return q.getBoltDB()
}

// GetOutputBuffer returns the output buffer (implements worker.QueueAccessor).
func (q *Queue) GetOutputBuffer() interface{} {
	return q.getOutputBuffer()
}

// GetPendingSet returns a copy of the pending set (implements worker.QueueAccessor).
// This is thread-safe but returns a snapshot.
func (q *Queue) GetPendingSet() map[string]struct{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	// Return a copy to avoid race conditions
	result := make(map[string]struct{})
	for k, v := range q.pendingSet {
		result[k] = v
	}
	return result
}

// InitializeWithContext sets up the queue with BoltDB, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// shutdownCtx is optional - if provided, workers will check for cancellation and exit on shutdown.
func (q *Queue) InitializeWithContext(boltInstance *db.DB, adapter types.FSAdapter, shutdownCtx context.Context) {
	// Set BoltDB and shutdownCtx using setters
	q.setBoltDB(boltInstance)
	q.setShutdownCtx(shutdownCtx)

	// Get worker count from capacity (workers haven't been added yet, so length is 0)
	q.mu.RLock()
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.RUnlock()

	// Initialize output buffer for batched writes
	// Default: 1000 operations or 1 second interval
	outputBuffer := db.NewOutputBuffer(boltInstance, 10000, 3*time.Second)
	q.setOutputBuffer(outputBuffer)

	// Create and start workers - they manage themselves
	for i := 0; i < workerCount; i++ {
		var w Worker
		// Traversal or retry mode use traversal workers
		w = NewTraversalWorker(
			fmt.Sprintf("%s-worker-%d", q.name, i),
			q,
			boltInstance,
			adapter,
			q.name,
			shutdownCtx,
		)
		q.AddWorker(w)
		go w.Run()
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
func (q *Queue) Initialize(boltInstance *db.DB, adapter types.FSAdapter) {
	q.InitializeWithContext(boltInstance, adapter, nil)
}

// SetShutdownContext sets the shutdown context for the queue.
// This can be called before or after Initialize, and will affect all workers.
func (q *Queue) SetShutdownContext(ctx context.Context) {
	q.setShutdownCtx(ctx)
}

// Name returns the queue's name.
func (q *Queue) Name() string {
	return q.name
}

// Round returns the current BFS round.
func (q *Queue) Round() int {
	return q.getRound()
}

// IsExhausted returns true if the queue has finished all traversal or has been stopped.
func (q *Queue) IsExhausted() bool {
	state := q.getState()
	return state == QueueStateCompleted || state == QueueStateStopped
}

// State returns the current queue lifecycle state.
func (q *Queue) State() QueueState {
	return q.getState()
}

// IsPaused returns true if the queue is paused.
func (q *Queue) IsPaused() bool {
	return q.getState() == QueueStatePaused
}

// Pause pauses the queue (workers will not lease new tasks).
func (q *Queue) Pause() {
	q.setState(QueueStatePaused)
	outputBuffer := q.getOutputBuffer()
	// Pause buffer (which will force-flush before pausing)
	if outputBuffer != nil {
		outputBuffer.Pause()
	}
}

// Resume resumes the queue after a pause.
func (q *Queue) Resume() {
	q.setState(QueueStateRunning)
	outputBuffer := q.getOutputBuffer()
	// Resume buffer
	if outputBuffer != nil {
		outputBuffer.Resume()
	}
}

// Add enqueues a task into the in-memory buffer only (no database writes).
func (q *Queue) Add(task *TaskBase) bool {
	return q.enqueuePending(task)
}

// SetRound sets the queue's current round. Used for resume operations.
func (q *Queue) SetRound(round int) {
	q.setRound(round)
}

// Lease attempts to lease a task for execution atomically.
// Returns nil if no tasks are available, queue is paused, or completed.
func (q *Queue) Lease() *TaskBase {
	// Check if queue is completed before attempting to pull tasks
	if q.getState() == QueueStateCompleted {
		return nil
	}

	q.PullTasksIfNeeded(false)

	for attempt := 0; attempt < 2; attempt++ {
		// Block if paused, completed, or no DB (waiting doesn't block - rounds continue once started)
		state := q.getState()
		boltDB := q.getBoltDB()
		if state == QueueStatePaused || state == QueueStateCompleted || boltDB == nil {
			return nil
		}

		task := q.dequeuePending()
		if task != nil {
			nodeID := task.ID
			if nodeID == "" {
				// Task doesn't have ULID - this shouldn't happen for tasks pulled from DB
				// Generate one for safety (shouldn't happen in normal flow)
				nodeID = db.GenerateNodeID()
				task.ID = nodeID
			}
			task.Locked = true
			task.LeaseTime = time.Now() // Record lease time for execution tracking
			q.addInProgress(nodeID, task)
			return task
		}

		// Check if we need to pull more tasks (only if buffer is low and last pull wasn't partial)
		q.PullTasksIfNeeded(false)
	}

	return nil
}

// CompletionCheckOptions configures what actions to take during completion checks.
type CompletionCheckOptions struct {
	CheckRoundComplete     bool // Check if current round is complete
	CheckFinalCompletion   bool // Check if traversal is complete (first pull with 0 items)
	AdvanceRoundIfComplete bool // Advance to next round if current round is complete
	WasFirstPull           bool // Whether this is the first pull of the round (passed from caller)
	FlushBuffer            bool // Flush buffer before checking DB
}

// checkCompletion performs completion checks based on the provided options.
// Returns true if queue was marked as completed, false otherwise.
func (q *Queue) checkCompletion(currentRound int, opts CompletionCheckOptions) bool {
	// Flush buffer first (if requested) to ensure all writes are persisted before checking
	outputBuffer := q.getOutputBuffer()
	if opts.FlushBuffer && outputBuffer != nil {
		outputBuffer.Flush()
	}

	// For traversal/sweep completion check (handles all modes)
	// Called when first pull returns 0 entries - decides if we're completely done
	if opts.CheckFinalCompletion {
		// Skip check if queue is in waiting state (DST gating)
		queueState := q.getState()
		if queueState == QueueStateWaiting {
			return false
		}

		// Check state and conditions after flush
		inProgressCount := q.getInProgressCount()
		pendingBuffCount := q.getPendingCount()
		wasFirstPull := opts.WasFirstPull
		boltDB := q.getBoltDB()

		// If we have tasks in progress or in buffer, we're definitely not done
		if inProgressCount > 0 || pendingBuffCount > 0 {
			return false
		}

		// Only check completion on first pull (prevents premature completion mid-round)

		if boltDB == nil {
			return false
		}

		queueType := getQueueType(q.name)
		mode := q.getMode()

		// Mode-specific completion conditions
		switch mode {
		case QueueModeTraversal:
			// Traversal: first pull with 0 entries → complete (exhausted all depths)
			// Check if any pending tasks exist at current round
			hasPending, err := boltDB.HasStatusBucketItems(queueType, currentRound, db.StatusPending)
			if err != nil {
				return false
			}

			// only end if we don't have any pending items and thi was our first pull of the round.
			if !hasPending {
				if !wasFirstPull {
					return false
				}
				return q.markComplete("No pending tasks found for round %d - traversal complete (first pull)", currentRound)
			}


		case QueueModeRetry:
			// Retry: use traversal rules past maxKnownDepth (exhausted all possible depths beyond known tree)
			// Below maxKnownDepth, keep scanning all known levels
			maxKnownDepth := q.getMaxKnownDepth()
			if maxKnownDepth >= 0 && currentRound > maxKnownDepth {
				// Past maxKnownDepth - apply traversal completion rules
				hasPending, err1 := boltDB.HasStatusBucketItems(queueType, currentRound, db.StatusPending)
				if err1 == nil && !hasPending && wasFirstPull {
					return q.markComplete("Retry sweep complete - past maxKnownDepth (%d), no pending/failed tasks at round %d", maxKnownDepth, currentRound)
				}
			}
			// At or below maxKnownDepth, or still have tasks - rounds will advance naturally
		}

		return false
	}

	// For round completion check
	if opts.CheckRoundComplete {
		// Check state first
		if q.getState() != QueueStateRunning {
			return false
		}

		// Check conditions after flush
		inProgressCount := q.getInProgressCount()
		pendingBuffCount := q.getPendingCount()
		lastPullWasPartial := q.getLastPullWasPartial()

		// Round is complete if: no in-progress, no pending, and last pull was partial
		if inProgressCount > 0 || pendingBuffCount > 0 || !lastPullWasPartial {
			return false
		}

		// Round is complete - advance if requested
		if opts.AdvanceRoundIfComplete {
			q.advanceToNextRound()
		}

		return true
	}

	return false
}

// markComplete marks the queue as completed and notifies the coordinator.
// Returns true if successfully marked complete.
func (q *Queue) markComplete(format string, args ...interface{}) bool {
	state := q.getState()
	if state != QueueStateRunning && state != QueueStateWaiting {
		return false
	}

	// Calculate total statistics across all rounds
	q.mu.RLock()
	totalTasksProcessed := 0
	totalChildrenDiscovered := 0
	for _, roundStats := range q.roundStats {
		if roundStats != nil {
			totalTasksProcessed += roundStats.Completed
			totalChildrenDiscovered += roundStats.Expected
		}
	}
	q.mu.RUnlock()

	if logservice.LS != nil {
		message := fmt.Sprintf(format, args...)
		_ = logservice.LS.Log("info", message, "queue", q.name, q.name)
		_ = logservice.LS.Log("info",
			fmt.Sprintf("Queue %s completion stats: %d tasks processed, %d children discovered",
				q.name, totalTasksProcessed, totalChildrenDiscovered),
			"queue", q.name, q.name)
	}

	// Also print to stdout for test visibility
	fmt.Printf("\n[%s Queue Complete] Tasks Processed: %d | Children Discovered: %d\n",
		q.name, totalTasksProcessed, totalChildrenDiscovered)

	q.setState(QueueStateCompleted)

	// Notify coordinator
	coordinator := q.getCoordinator()
	queueType := getQueueType(q.name)
	if coordinator != nil {
		switch queueType {
		case "SRC":
			coordinator.MarkSrcCompleted()
		case "DST":
			coordinator.MarkDstCompleted()
		}
	}
	return true
}

// TaskExecutionResult represents the result of a task execution.
type TaskExecutionResult string

const (
	TaskExecutionResultSuccessful TaskExecutionResult = "successful"
	TaskExecutionResultFailed     TaskExecutionResult = "failed"
)

// ReportTaskResult reports the result of a task execution and handles post-processing.
// This is the event-driven entry point that replaces separate Complete()/Fail() calls.
// After processing the result, it checks if we need to pull more tasks or advance rounds.
func (q *Queue) ReportTaskResult(task *TaskBase, result TaskExecutionResult) {
	// Calculate execution time delta (lease to complete/fail)
	var executionDelta time.Duration
	if !task.LeaseTime.IsZero() {
		executionDelta = time.Since(task.LeaseTime)
	}

	switch result {
	case TaskExecutionResultSuccessful:
		q.completeTask(task, executionDelta)
	case TaskExecutionResultFailed:
		q.failTask(task, executionDelta)
	default:
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("ReportTaskResult called with unknown result: %s", result),
				"queue", q.name, q.name)
		}
		return
	}

	// Event-driven post-processing: check if we need to pull more tasks
	pendingCount := q.getPendingCount()
	lastPullWasPartial := q.getLastPullWasPartial()
	state := q.getState()

	// Only process if queue is running
	if state != QueueStateRunning && state != QueueStateWaiting {
		return
	}

	// Check if we need to pull more tasks (if count is below threshold and last pull wasn't partial)
	pullLowWM := q.getPullLowWM()
	if pendingCount <= pullLowWM && !lastPullWasPartial {
		q.PullTasksIfNeeded(false)
	}
}

// Complete is a public wrapper for backward compatibility.
// New code should use ReportTaskResult instead.
func (q *Queue) Complete(task *TaskBase) {
	q.ReportTaskResult(task, TaskExecutionResultSuccessful)
}

// completeTask is the internal implementation for successful task completion.
func (q *Queue) completeTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round

	if q.boltDB == nil {
		return
	}

	// Handle exclusion tasks differently
	if task.Type == TaskTypeExclusion {
		q.completeExclusionTask(task)
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

	nodeID := state.ID
	queueType := getQueueType(q.name)
	nextRound := currentRound + 1

	// Remove the ULID from leased set (can do this early, doesn't affect pending count)
	q.removeLeasedKey(nodeID)

	task.Locked = false
	task.Status = "successful"

	// Increment completed count (even if failed, this is a "processed" counter)
	q.incrementRoundStatsCompleted(currentRound)

	// Record task completion in RoundInfo
	q.recordTaskCompletion(currentRound, true)

	// Note: We'll check for completion again at the end after all writes are done
	// This early check helps catch completion as soon as possible

	// Prepare child nodes for insertion
	parentPath := types.NormalizeLocationPath(task.LocationPath())
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

	// 2. Build map of existing children (ServiceID -> ULID) to avoid creating duplicates
	existingChildrenMap := make(map[string]string) // ServiceID -> ULID
	boltDB := q.getBoltDB()
	if boltDB != nil {
		if existingChildren, err := db.GetChildrenStatesByParentID(boltDB, queueType, nodeID); err == nil {
			for _, existingChild := range existingChildren {
				if existingChild != nil && existingChild.ServiceID != "" {
					existingChildrenMap[existingChild.ServiceID] = existingChild.ID
				}
			}
		}
	}

	// 3. Collect discovered children for insertion
	for _, child := range task.DiscoveredChildren {
		// For DST queues, skip folder children here - they'll be handled separately
		if queueType == "DST" && !child.IsFile {
			continue
		}

		// Get ServiceID from child
		var childServiceID string
		if child.IsFile {
			childServiceID = child.File.ServiceID
		} else {
			childServiceID = child.Folder.ServiceID
		}

		// Check if child already exists - if so, reuse its ULID
		existingULID, exists := existingChildrenMap[childServiceID]

		childState := childResultToNodeStateWithID(child, parentPath, nextRound, queueType, nodeID, existingULID, exists)
		if childState == nil {
			continue
		}

		// Populate traversal status in the NodeState metadata
		childState.TraversalStatus = child.Status

		childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
			QueueType: queueType,
			Level:     nextRound,
			Status:    child.Status,
			State:     childState,
		})

		// Queue path-to-ulid mapping for this child (for API path-based queries)
		outputBuffer := q.getOutputBuffer()
		if outputBuffer != nil && childState.Path != "" {
			outputBuffer.AddPathToULIDMapping(queueType, childState.Path, childState.ID)
		}

		// For DST queue: queue lookup mapping if this child has a matching SRC node
		if queueType == "DST" && child.SrcID != "" {
			if outputBuffer != nil {
				// Queue bidirectional lookup mapping: SrcID <-> DST node ID
				outputBuffer.AddLookupMapping(child.SrcID, childState.ID)
			}
		}

		// Increment expected count for folder children (only folders need traversal)
		if !child.IsFile {
			q.incrementRoundStatsExpected(nextRound)
		}
	}

	// 3. Handle DST queue special case: create tasks for child folders
	// Note: ExpectedFolders/ExpectedFiles will be loaded later during PullTasks() batch loading
	if q.name == "dst" {
		type dstChildFolder struct {
			folder types.Folder
			srcID  string
		}
		var childFolders []dstChildFolder
		totalFolders := 0
		pendingFolders := 0
		for _, child := range task.DiscoveredChildren {
			if !child.IsFile {
				totalFolders++
				if child.Status == db.StatusPending {
					pendingFolders++
					f := child.Folder
					f.DepthLevel = nextRound
					childFolders = append(childFolders, dstChildFolder{
						folder: f,
						srcID:  child.SrcID, // Preserve SrcID from ChildResult
					})
				}
			}
		}

		tasksCreated := 0
		for _, child := range childFolders {
			// Check if this folder already exists (by ServiceID) and reuse its ULID
			existingFolderULID, folderExists := existingChildrenMap[child.folder.ServiceID]

			// Create task state for DST child (without ExpectedFolders/ExpectedFiles - loaded in PullTasks)
			taskState := taskToNodeState(&TaskBase{
				Type:   TaskTypeDstTraversal,
				Folder: child.folder,
				Round:  nextRound,
			})
			if taskState != nil {
				// Reuse existing ULID if folder already exists
				if folderExists && existingFolderULID != "" {
					taskState.ID = existingFolderULID
				}

				// Set parent's ULID for database relationships
				taskState.ParentID = nodeID
				// Populate traversal status in the NodeState metadata
				taskState.TraversalStatus = db.StatusPending
				// Store SrcID in NodeState temporarily for BatchInsertNodes to create lookup mappings
				// (BatchInsertNodes will handle storing in lookup tables)
				if child.srcID != "" {
					taskState.SrcID = child.srcID
				}

				childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
					QueueType: queueType,
					Level:     nextRound,
					Status:    db.StatusPending,
					State:     taskState,
				})

				// Queue path-to-ulid mapping for DST child folder (for API path-based queries)
				outputBuffer := q.getOutputBuffer()
				if outputBuffer != nil && taskState.Path != "" {
					outputBuffer.AddPathToULIDMapping(queueType, taskState.Path, taskState.ID)
				}

				// Queue lookup mapping if this child has a matching SRC node
				if child.srcID != "" {
					if outputBuffer != nil {
						// Queue bidirectional lookup mapping: SrcID <-> DST node ID
						outputBuffer.AddLookupMapping(child.srcID, taskState.ID)
					}
				}

				q.incrementRoundStatsExpected(nextRound)
				tasksCreated++
			}
		}
	}

	// Write to buffer instead of directly to database
	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
		// Add all related operations atomically to prevent flushes between them
		// This ensures status update and batch insert are written together
		atomicOps := make([]db.WriteOperation, 0, 2)

		// Add parent status update operation
		atomicOps = append(atomicOps, &db.StatusUpdateOperation{
			QueueType: queueType,
			Level:     currentRound,
			OldStatus: db.StatusPending,
			NewStatus: db.StatusSuccessful,
			NodeID:    nodeID,
		})

		// Add child inserts operation (if any)
		// Lookup mappings are queued above when creating child states
		if len(childNodesToInsert) > 0 {
			atomicOps = append(atomicOps, &db.BatchInsertOperation{
				Operations: childNodesToInsert,
			})
		}

		// Add all operations atomically to prevent partial flushes
		outputBuffer.AddMultiple(atomicOps)

		// For SRC FOLDER tasks in retry mode: Queue DST cleanup (mark parent as pending, delete children)
		// Only folders have children to clean up - files don't need this
		if q.name == "src" && q.getMode() == QueueModeRetry && q.boltDB != nil && task.IsFolder() {
			dstID, err := db.GetDstIDFromSrcID(q.boltDB, nodeID)
			if err == nil && dstID != "" {
				// Get DST node state to determine its depth and current status
				dstState, err := db.GetNodeState(q.boltDB, "DST", dstID)
				if err == nil && dstState != nil {
					// Queue status update to mark DST parent as pending
					oldStatus := dstState.TraversalStatus
					if oldStatus == "" {
						oldStatus = db.StatusSuccessful // Default assumption
					}
					outputBuffer.AddStatusUpdate("DST", dstState.Depth, oldStatus, db.StatusPending, dstID)

					// Queue deletion of DST children using batch deletion
					childIDs, err := db.GetChildrenIDsByParentID(q.boltDB, "DST", dstID)
					if err == nil && len(childIDs) > 0 {
						for _, childID := range childIDs {
							// Get child state to know its depth and status for deletion
							childState, err := db.GetNodeState(q.boltDB, "DST", childID)
							if err == nil && childState != nil {
								childStatus := childState.TraversalStatus
								if childStatus == "" {
									childStatus = db.StatusSuccessful
								}
								outputBuffer.AddNodeDeletion("DST", childID, childState.Depth, childStatus)
							}
						}
					}
				}
			}
		}
	}

	// CRITICAL: Remove from in-progress LAST, after all buffer operations are queued.
	// This prevents the Run() polling loop from seeing inProgressCount == 0 before
	// the buffer operations are flushed, which could cause premature round advancement.
	q.removeInProgress(nodeID)

	// Run() polling loop will check round completion directly - no soft flags needed
}

// completeExclusionTask handles completion of exclusion tasks.
// Workers have already done the DB operations, so this just updates queue state.
func (q *Queue) completeExclusionTask(task *TaskBase) {
	currentRound := task.Round
	nodeID := task.ID

	// Remove ULID from leased set (can do this early, doesn't affect pending count)
	if nodeID != "" {
		q.removeLeasedKey(nodeID)
	}
	task.Locked = false
	task.Status = "successful"

	// Increment completed count
	q.incrementRoundStatsCompleted(currentRound)

	// Record task completion in RoundInfo
	q.recordTaskCompletion(currentRound, true)

	// CRITICAL: Remove from in-progress LAST, after all operations are complete.
	// Even though exclusion workers handle buffer operations, we still want to ensure
	// the Run() polling loop doesn't see inProgressCount == 0 prematurely.
	q.removeInProgress(nodeID)
}

// childResultToNodeStateWithID converts a ChildResult to NodeState, reusing existing ULID if provided.
func childResultToNodeStateWithID(child ChildResult, parentPath string, depth int, queueType string, parentID string, existingULID string, useExisting bool) *db.NodeState {
	// Use existing ULID if provided, otherwise generate new one
	var nodeID string
	if useExisting && existingULID != "" {
		nodeID = existingULID
	} else {
		nodeID = db.GenerateNodeID()
		if nodeID == "" {
			// If ULID generation fails, we can't proceed
			return nil
		}
	}

	// Store SrcID temporarily in NodeState for BatchInsertNodes to create lookup mappings
	// (BatchInsertNodes will handle storing in lookup tables, then SrcID can be removed from NodeState)
	var srcID string
	if queueType == "DST" {
		srcID = child.SrcID
	}

	if child.IsFile {
		file := child.File
		return &db.NodeState{
			ID:              nodeID,         // ULID for database keys (reused if exists)
			ServiceID:       file.ServiceID, // FS identifier
			ParentID:        parentID,       // Parent's ULID for database relationships
			ParentServiceID: file.ParentId,  // Parent's FS identifier
			ParentPath:      parentPath,
			Name:            file.DisplayName,
			Path:            types.NormalizeLocationPath(file.LocationPath),
			Type:            types.NodeTypeFile,
			Size:            file.Size,
			MTime:           file.LastUpdated,
			Depth:           depth,
			CopyNeeded:      false,
			Status:          child.Status,
			SrcID:           srcID, // Temporarily stored for BatchInsertNodes to create lookup mappings
		}
	}

	folder := child.Folder
	return &db.NodeState{
		ID:              nodeID,           // ULID for database keys (reused if exists)
		ServiceID:       folder.ServiceID, // FS identifier
		ParentID:        parentID,         // Parent's ULID for database relationships
		ParentServiceID: folder.ParentId,  // Parent's FS identifier
		ParentPath:      parentPath,
		Name:            folder.DisplayName,
		Path:            types.NormalizeLocationPath(folder.LocationPath),
		Type:            types.NodeTypeFolder,
		Size:            0,
		MTime:           folder.LastUpdated,
		Depth:           depth,
		CopyNeeded:      false,
		Status:          child.Status,
		SrcID:           srcID, // Temporarily stored for BatchInsertNodes to create lookup mappings
	}
}

func (q *Queue) PullTasksIfNeeded(force bool) {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Don't pull if queue is paused or completed (waiting doesn't block pulls - rounds continue once started)
	state := q.getState()
	if state == QueueStatePaused || state == QueueStateCompleted {
		return
	}

	// Note: firstPullForRound will be set to false in PullTasks() after successful pull
	// We don't set it here because we need to know if it was the first pull when checking completion

	if force {
		q.pullTasksForMode(true)
		return
	}

	// Only pull if: queue is running, buffer is low, not already pulling, and last pull wasn't partial
	// If last pull was partial, we might have exhausted this round - don't pull again until round advances
	lastPullWasPartial := q.getLastPullWasPartial()
	pendingCount := q.getPendingCount()
	pullLowWM := q.getPullLowWM()
	pulling := q.getPulling()
	needPull := state == QueueStateRunning && pendingCount <= pullLowWM && !pulling && !lastPullWasPartial
	if needPull {
		q.pullTasksForMode(false)
	}
}

// pullTasksForMode selects the appropriate pull method based on queue mode.
func (q *Queue) pullTasksForMode(force bool) {
	mode := q.getMode()

	switch mode {
	case QueueModeRetry:
		q.PullRetryTasks(force)
	default: // QueueModeTraversal
		q.PullTraversalTasks(force)
	}
}

// Pull methods are now organized in separate files:
// - PullTraversalTasks: mode_traversal.go
// - PullExclusionTasks: mode_exclusion.go
// - PullRetryTasks: mode_retry.go

// enqueuePendingLocked and dequeuePendingLocked are now replaced by enqueuePending and dequeuePending
// which use getters/setters internally. These functions are kept for backward compatibility
// but should not be used directly - use enqueuePending and dequeuePending instead.

// Fail is a public wrapper for backward compatibility.
// New code should use ReportTaskResult instead.
// Returns true if the task will be retried, false if max retries exceeded.
func (q *Queue) Fail(task *TaskBase) bool {
	q.ReportTaskResult(task, TaskExecutionResultFailed)
	// Check if task was retried by checking if it's still in pending
	nodeID := task.ID
	willRetry := q.isInPendingSet(nodeID)
	return willRetry
}

// failTask is the internal implementation for failed task handling.
func (q *Queue) failTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta (even for failures)
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID // Use ULID for tracking
	maxRetries := q.getMaxRetries()

	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Failing task: id=%s path=%s round=%d type=%s currentAttempts=%d maxRetries=%d",
				nodeID, task.LocationPath(), currentRound, task.Type, task.Attempts, maxRetries),
			"queue", q.name, q.name)
	}

	task.Attempts++

	// Check if we should retry
	if task.Attempts < maxRetries {
		task.Locked = false
		// CRITICAL: Remove from in-progress BEFORE re-enqueuing to pending
		// This ensures the task moves from in-progress to pending atomically
		q.removeInProgress(nodeID)
		q.enqueuePending(task) // Re-adds to tracked automatically
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Retrying task: id=%s path=%s round=%d attempt=%d/%d",
					nodeID, task.LocationPath(), currentRound, task.Attempts, maxRetries),
				"queue", q.name, q.name)
		}
		return // Will retry - ReportTaskResult will handle pulling tasks
	}

	// Max retries reached - task is truly done
	// Remove the ULID from leased set
	if nodeID != "" {
		q.removeLeasedKey(nodeID)
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("error",
			fmt.Sprintf("Failed to traverse folder %s (id=%s) after %d attempts (max retries exceeded) round=%d",
				task.LocationPath(), nodeID, task.Attempts, currentRound),
			"queue", q.name, q.name)
	}

	task.Locked = false
	task.Status = "failed"

	// Increment completed count
	q.incrementRoundStatsCompleted(currentRound)

	// Record task completion in RoundInfo (failed)
	q.recordTaskCompletion(currentRound, false)

	// Update traversal status to failed so leasing stops retrying this node.
	outputBuffer := q.getOutputBuffer()
	if nodeID != "" && outputBuffer != nil {
		queueType := getQueueType(q.name)
		outputBuffer.AddStatusUpdate(queueType, currentRound, db.StatusPending, db.StatusFailed, nodeID)
	}

	// CRITICAL: Remove from in-progress LAST, after all buffer operations are queued.
	// This prevents the Run() polling loop from seeing inProgressCount == 0 before
	// the buffer operations are flushed, which could cause premature round advancement.
	q.removeInProgress(nodeID)
}

// InProgressCount returns the number of tasks currently being executed.
func (q *Queue) InProgressCount() int {
	return q.getInProgressCount()
}

// TotalTracked returns the total number of tasks across all rounds (pending + in-progress).
func (q *Queue) TotalTracked() int {
	return q.getPendingCount() + q.getInProgressCount()
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
	// Stop existing ticker if any
	statsTick := q.getStatsTick()
	if statsTick != nil {
		statsTick.Stop()
	}

	q.setStatsChan(ch)
	if ch != nil {
		// Start publishing loop
		newTick := time.NewTicker(1 * time.Second)
		q.setStatsTick(newTick)
		go q.publishStatsLoop()
	} else {
		q.setStatsTick(nil)
	}
}

// SetObserver registers this queue with an observer for BoltDB stats publishing.
// The observer will poll this queue directly for statistics.
func (q *Queue) SetObserver(observer *QueueObserver) {
	if observer != nil {
		observer.RegisterQueue(q.name, q)
	}
}

// publishStatsLoop periodically publishes queue statistics to the stats channel.
// Exits when the queue is completed/stopped or the channel/ticker is cleared.
func (q *Queue) publishStatsLoop() {
	statsChan := q.getStatsChan()
	statsTick := q.getStatsTick()
	if statsChan == nil || statsTick == nil {
		return
	}

	for range statsTick.C {
		state := q.getState()
		// Exit if queue is completed or stopped
		if state == QueueStateCompleted || state == QueueStateStopped {
			return
		}
		stats := q.Stats()
		chanRef := q.getStatsChan()
		tickRef := q.getStatsTick()

		// Check if channel/ticker were cleared
		if tickRef == nil {
			return
		}

		// Send to stats channel (non-blocking)
		if chanRef != nil {
			select {
			case chanRef <- stats:
			default:
			}
		}
	}
}

// Shutdown stops the stats publishing loop and cleans up resources.
func (q *Queue) Shutdown() {
	statsTick := q.getStatsTick()
	if statsTick != nil {
		statsTick.Stop()
		q.setStatsTick(nil)
	}
	q.setStatsChan(nil)
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
		TotalTracked: len(q.pendingBuff) + len(q.inProgress),
		Workers:      len(q.workers),
	}
}

// RoundStats returns the statistics for a specific round.
// Returns nil if the round has no stats yet.
func (q *Queue) RoundStats(round int) *RoundStats {
	return q.getRoundStats(round)
}

// IncrementExpected increments the expected task count for a specific round.
// This is used to initialize Expected count (e.g., for root tasks).
func (q *Queue) IncrementExpected(round int, count int) {
	for i := 0; i < count; i++ {
		q.incrementRoundStatsExpected(round)
	}
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
	// Quick check - if already completed, nothing to do (queues clean themselves up)
	state := q.getState()
	hasStatsTick := q.getStatsTick() != nil
	outputBuffer := q.getOutputBuffer()

	if state == QueueStateCompleted {
		// Still need to clean up stats ticker and buffer if they exist
		if hasStatsTick {
			statsTick := q.getStatsTick()
			if statsTick != nil {
				statsTick.Stop()
				q.setStatsTick(nil)
			}
			q.setStatsChan(nil)
		}
		// Ensure buffer is stopped
		if outputBuffer != nil {
			outputBuffer.Stop()
		}
		return
	}

	// Queue not completed - clean up normally
	statsTick := q.getStatsTick()
	if statsTick != nil {
		statsTick.Stop()
		q.setStatsTick(nil)
	}
	q.setStatsChan(nil)
	closeOutputBuffer := q.getOutputBuffer()
	if state != QueueStateCompleted && state != QueueStateStopped {
		q.setState(QueueStateStopped)
	}

	// Ensure buffer is stopped and flushed
	if closeOutputBuffer != nil {
		closeOutputBuffer.Stop()
	}
}

// recordExecutionTime adds an execution time delta to the buffer and periodically calculates averages.
func (q *Queue) recordExecutionTime(delta time.Duration) {
	if delta <= 0 {
		return // Skip invalid deltas
	}

	q.appendExecutionTimeDelta(delta)

	// Check if it's time to calculate average
	now := time.Now()
	lastAvgTime := q.getLastAvgTime()
	avgInterval := q.getAvgInterval()
	if now.Sub(lastAvgTime) >= avgInterval {
		q.calculateAverage()
		q.setLastAvgTime(now)
	}
}

// calculateAverage calculates the average execution time and clears the buffer.
func (q *Queue) calculateAverage() {
	deltas := q.getExecutionTimeDeltas()
	if len(deltas) == 0 {
		q.setAvgExecutionTime(0)
		return
	}

	var sum time.Duration
	for _, delta := range deltas {
		sum += delta
	}
	avg := sum / time.Duration(len(deltas))
	q.setAvgExecutionTime(avg)

	// Clear buffer after calculating average
	q.clearExecutionTimeDeltas()
}

// GetAverageExecutionTime returns the current average task execution time.
func (q *Queue) GetAverageExecutionTime() time.Duration {
	return q.getAvgExecutionTime()
}

// GetExecutionTimeBufferSize returns the current size of the execution time buffer.
func (q *Queue) GetExecutionTimeBufferSize() int {
	return len(q.getExecutionTimeDeltas())
}

// GetTotalCompleted returns the total number of completed tasks across all rounds.
func (q *Queue) GetTotalCompleted() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	total := 0
	for _, roundStats := range q.roundStats {
		if roundStats != nil {
			total += roundStats.Completed
		}
	}
	return total
}

// Run is the main queue coordination loop. It has an outer loop for rounds and an inner loop
// for each round. The outer loop checks coordinator gates before starting each round (DST only).
// The inner loop processes tasks until the round is complete.
func (q *Queue) Run() {
	// DST queue only operates in traversal mode - retry/exclusion/unexclusion are SRC-only
	mode := q.getMode()
	if q.name == "dst" && !(mode == QueueModeTraversal || mode == QueueModeRetry) {
		// Mark as completed immediately and exit
		q.setState(QueueStateCompleted)
		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				fmt.Sprintf("DST queue skipping non-traversal mode (%s) - marking as completed", mode),
				"queue", q.name, q.name)
		}
		return
	}

	// OUTER LOOP: Iterate through rounds
	for {
		// Check for shutdown
		shutdownCtx := q.getShutdownCtx()
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}
		}

		state := q.getState()
		boltDB := q.getBoltDB()

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
			// Stop output buffer when queue completes to prevent goroutine leak
			outputBuffer := q.getOutputBuffer()
			if outputBuffer != nil {
				outputBuffer.Stop()
			}
			return
		}

		// Get current round
		currentRound := q.getRound()
		coordinator := q.getCoordinator()

		// GATE CHECK: Can we start processing this round? (DST only - SRC has no gates)
		// This check happens IMMEDIATELY at the start of each iteration, before any task processing.
		if q.name == "dst" && coordinator != nil {
			canStartRound := coordinator.CanDstStartRound(currentRound)

			if !canStartRound {
				// Still waiting - set state and sleep
				if state != QueueStateWaiting {
					q.setState(QueueStateWaiting)
				}
				time.Sleep(50 * time.Millisecond)
				continue // Loop back and check again
			}

			// Green light - can start this round, ensure state is running
			if state == QueueStateWaiting {
				q.setState(QueueStateRunning)
			}
		}

		// Polling loop: Single source of truth for round advancement and completion
		// Poll conditions directly - no soft flags needed
		for {
			// Check for shutdown
			if q.shutdownCtx != nil {
				select {
				case <-q.shutdownCtx.Done():
					return
				default:
				}
			}

			innerState := q.getState()

			// If paused, block here
			if innerState == QueueStatePaused {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// If stopped or completed, exit
			if innerState == QueueStateStopped || innerState == QueueStateCompleted {
				outputBuffer := q.getOutputBuffer()
				if outputBuffer != nil {
					outputBuffer.Stop()
				}
				return
			}

			// Get current state snapshot
			inProgressCount := q.getInProgressCount()
			pendingCount := q.getPendingCount()
			lastPullWasPartial := q.getLastPullWasPartial()
			pullCount := q.getCurrentRoundPullCount()
			itemsYielded := q.getCurrentRoundItemsYielded()
			roundToCheck := q.getRound()

			// 1. Check queue completion (mode-specific)
			// Check for final completion when: pullCount > 0 && itemsYielded == 0
			if pullCount > 0 && itemsYielded == 0 && inProgressCount == 0 && pendingCount == 0 {
				completed := q.checkCompletion(roundToCheck, CompletionCheckOptions{
					CheckFinalCompletion: true,
					WasFirstPull:         true,
					FlushBuffer:          true,
				})
				if completed {
					outputBuffer := q.getOutputBuffer()
					if outputBuffer != nil {
						outputBuffer.Stop()
					}
					return
				}
			}

			// 2. Check round completion (universal)
			// Round complete when: no in-progress, no pending, last pull was partial
			if inProgressCount == 0 && pendingCount == 0 && lastPullWasPartial {
				q.checkCompletion(roundToCheck, CompletionCheckOptions{
					CheckRoundComplete:     true,
					AdvanceRoundIfComplete: true,
					FlushBuffer:            true,
				})
			}

			// Check if round advanced
			if q.getRound() != currentRound {
				break // Round advanced, continue outer loop
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

// advanceToNextRound advances the queue to the next round and cleans up old round queues.
// Note: Round advancement is now free - gating only happens when STARTING a round (checked in Run()).
func (q *Queue) advanceToNextRound() {
	// No gating here - rounds advance freely
	// Gating only happens when STARTING a round (checked in Run() outer loop)

	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

	// Ensure state is running if it was waiting
	state := q.getState()
	if state == QueueStateWaiting {
		q.setState(QueueStateRunning)
	}

	// Advance round
	currentRound := q.getRound()
	newRound := currentRound + 1

	// Get stats for logging
	q.setRound(newRound)

	// Reset lastPullWasPartial since we're advancing to a new round
	q.setLastPullWasPartial(false)
	// Initialize RoundInfo for the new round (will be created on first pull)
	q.getRoundInfo(newRound) // Ensure it exists
	// Initialize stats for the new round (if not already exists)
	// Update coordinator when rounds advance
	coordinator := q.getCoordinator()
	if coordinator != nil {
		switch q.name {
		case "src":
			coordinator.UpdateSrcRound(newRound)
		case "dst":
			coordinator.UpdateDstRound(newRound)
		}
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.name, q.name)
	}

	// Pull tasks for the new round
	// pullTasksForMode() will flush buffer first, then pull tasks
	// Round completion will be detected naturally by checkCompletion() when:
	// - inProgress == 0
	// - pendingBuff == 0
	// - lastPullWasPartial == true
	// Then it will flush buffer and advance to next round
	q.PullTasksIfNeeded(true)
}
