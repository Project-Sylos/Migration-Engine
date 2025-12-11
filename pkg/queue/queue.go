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
	QueueModeExclusion QueueMode = "exclusion" // Exclusion propagation sweep
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
	inProgress         map[string]*TaskBase // Tasks currently being executed (keyed by path)
	pendingBuff        []*TaskBase          // Local task buffer fetched from BoltDB
	pendingSet         map[string]struct{}  // Fast lookup for pending buffer dedupe
	leasedKeys         map[string]struct{}  // ULIDs already pulled/leased - prevents duplicate pulls from stale views
	pulling            bool                 // Indicates a pull operation is active
	pullLowWM          int                  // Low watermark threshold for pulling more work
	lastPullWasPartial bool                 // True if last pull returned fewer tasks than requested (partial batch)
	firstPullForRound  bool                 // True if we haven't done the first pull for the current round yet (used for DST gate check)
	maxRetries         int                  // Maximum retry attempts per task
	round              int                  // Current BFS round/depth level
	workers            []Worker             // Workers associated with this queue (for reference only)
	boltDB             *db.DB               // BoltDB for operational queue storage
	srcQueue           *Queue               // For dst queue: reference to src queue for BoltDB lookups
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
		coordinator:         coordinator,
		firstPullForRound:   true,                          // Start with true so first pull checks gate
		executionTimeDeltas: make([]time.Duration, 0, 100), // Buffer capacity 100
		avgInterval:         5 * time.Second,               // Calculate average every 5 seconds
		lastAvgTime:         time.Now(),
		maxKnownDepth:       -1, // -1 means not set yet
	}
}

// SetMode sets the queue operation mode.
func (q *Queue) SetMode(mode QueueMode) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mode = mode
}

// SetMaxKnownDepth sets the maximum known depth for retry sweeps.
// Only applicable when mode is QueueModeRetry.
// Set to -1 to auto-detect from database, or a non-negative value to use a specific depth.
func (q *Queue) SetMaxKnownDepth(depth int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxKnownDepth = depth
}

// GetMode returns the current queue mode (implements worker.QueueAccessor).
func (q *Queue) GetMode() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return string(q.mode)
}

// GetBoltDB returns the BoltDB instance (implements worker.QueueAccessor).
func (q *Queue) GetBoltDB() interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.boltDB
}

// GetOutputBuffer returns the output buffer (implements worker.QueueAccessor).
func (q *Queue) GetOutputBuffer() interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.outputBuffer
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
// For dst queues, srcQueue should be provided for BoltDB expected children lookups.
// shutdownCtx is optional - if provided, workers will check for cancellation and exit on shutdown.
func (q *Queue) InitializeWithContext(boltInstance *db.DB, adapter types.FSAdapter, srcQueue *Queue, shutdownCtx context.Context) {
	q.mu.Lock()
	q.boltDB = boltInstance
	q.srcQueue = srcQueue
	q.shutdownCtx = shutdownCtx
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.Unlock()

	// Initialize output buffer for batched writes
	// Default: 1000 operations or 1 second interval
	q.outputBuffer = db.NewOutputBuffer(boltInstance, 1000, 1*time.Second)

	// Create and start workers - they manage themselves
	// Worker type depends on queue mode
	q.mu.RLock()
	mode := q.mode
	q.mu.RUnlock()

	for i := 0; i < workerCount; i++ {
		var w Worker
		if mode == QueueModeExclusion {
			w = NewExclusionWorker(
				fmt.Sprintf("%s-exclusion-worker-%d", q.name, i),
				q,
				boltInstance,
				q.name,
				shutdownCtx,
			)
		} else {
			// Traversal or retry mode use traversal workers
			w = NewTraversalWorker(
				fmt.Sprintf("%s-worker-%d", q.name, i),
				q,
				boltInstance,
				adapter,
				q.name,
				shutdownCtx,
			)
		}
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
func (q *Queue) Initialize(boltInstance *db.DB, adapter types.FSAdapter, srcQueue *Queue) {
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
	q.state = QueueStatePaused
	outputBuffer := q.outputBuffer
	q.mu.Unlock()
	// Pause buffer (which will force-flush before pausing)
	if outputBuffer != nil {
		outputBuffer.Pause()
	}
}

// Resume resumes the queue after a pause.
func (q *Queue) Resume() {
	q.mu.Lock()
	q.state = QueueStateRunning
	outputBuffer := q.outputBuffer
	q.mu.Unlock()
	// Resume buffer
	if outputBuffer != nil {
		outputBuffer.Resume()
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

	q.PullTasksIfNeeded(false)

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
			task.LeaseTime = time.Now() // Record lease time for execution tracking
			q.inProgress[path] = task
			q.mu.Unlock()
			return task
		}
		q.mu.Unlock()

		// Check if we need to pull more tasks (only if buffer is low and last pull wasn't partial)
		q.PullTasksIfNeeded(false)

	}

	return nil
}

// CompletionCheckOptions configures what actions to take during completion checks.
type CompletionCheckOptions struct {
	CheckRoundComplete     bool // Check if current round is complete
	CheckDstComplete       bool // Check if DST queue is complete (DST only)
	AdvanceRoundIfComplete bool // Advance to next round if current round is complete
	MarkDstCompleteIfDone  bool // Mark DST as complete if done (DST only)
	RequireFirstPull       bool // Only mark DST complete on first pull (DST only)
	FlushBuffer            bool // Flush buffer before checking DB
}

// checkCompletion performs completion checks based on the provided options.
// Returns true if queue was marked as completed, false otherwise.
// Thread-safe - caller must NOT hold the mutex when calling this.
func (q *Queue) checkCompletion(currentRound int, opts CompletionCheckOptions) bool {
	// Flush buffer first (if requested) to ensure all writes are persisted before checking
	outputBuffer := q.getOutputBuffer()
	if opts.FlushBuffer && outputBuffer != nil {
		outputBuffer.Flush()
	}

	// For DST completion check
	if opts.CheckDstComplete && opts.MarkDstCompleteIfDone {
		if q.name != "dst" || q.coordinator == nil {
			return false
		}

		// Check state and conditions after flush
		inProgressCount := q.getInProgressCount()
		pendingBuffCount := q.getPendingCount()
		wasFirstPull := q.getFirstPullForRound()
		queueState := q.getState()
		boltDB := q.getBoltDB()

		// If we have tasks in progress or in buffer, we're definitely not done
		if inProgressCount > 0 || pendingBuffCount > 0 {
			return false
		}

		// If requireFirstPull is true, only mark complete on first pull
		if opts.RequireFirstPull && !wasFirstPull {
			return false
		}

		// No in-memory tasks - check DB for pending tasks in current round
		if boltDB == nil {
			return false
		}

		fmt.Printf("Checking for completion for round %d\n", currentRound)

		// Check DB for pending items (buffer already flushed above)
		hasPending, err := boltDB.HasStatusBucketItems("DST", currentRound, db.StatusPending)
		if err != nil {
			return false
		}

		if !hasPending {
			// No pending tasks in current round
			q.mu.Lock()
			if queueState == QueueStateRunning || queueState == QueueStateWaiting {
				if wasFirstPull {
					// Traversal is complete ONLY if this is the first pull of the round and there are no pendings
					if logservice.LS != nil {
						_ = logservice.LS.Log("info",
							fmt.Sprintf("No pending tasks found for round %d - traversal complete (first pull)", currentRound),
							"queue", q.name, q.name)
					}
					q.state = QueueStateCompleted
					if q.coordinator != nil {
						fmt.Printf("Marking DST as completed for round %d\n", currentRound)
						q.coordinator.MarkDstCompleted()
					}
					q.mu.Unlock()
					return true
				} else {
					// No pending tasks, but not first pull - just advance the round if needed.
					if logservice.LS != nil {
						_ = logservice.LS.Log("info",
							fmt.Sprintf("No pending tasks found for round %d (not first pull) - advancing round", currentRound),
							"queue", q.name, q.name)
					}
					q.mu.Unlock()
					// Advance round if requested
					if opts.AdvanceRoundIfComplete {
						q.advanceToNextRound()
					}
					return false
				}
			}
			q.mu.Unlock()
			return false
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
	q.mu.RLock()
	pendingCount := len(q.pendingBuff)
	lastPullWasPartial := q.lastPullWasPartial
	state := q.state
	q.mu.RUnlock()

	// Only process if queue is running
	if state != QueueStateRunning && state != QueueStateWaiting {
		return
	}

	// Check if we need to pull more tasks (if count is below threshold and last pull wasn't partial)
	if pendingCount <= q.pullLowWM && !lastPullWasPartial {
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

	// 3. Handle DST queue special case: create tasks for child folders
	// Note: ExpectedFolders/ExpectedFiles will be loaded later during PullTasks() batch loading
	if q.name == "dst" && q.srcQueue != nil {
		var childFolders []types.Folder
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
				}
			}
		}

		for _, childFolder := range childFolders {
			// Create task state for DST child (without ExpectedFolders/ExpectedFiles - loaded in PullTasks)
			taskState := taskToNodeState(&TaskBase{
				Type:   TaskTypeDstTraversal,
				Folder: childFolder,
				Round:  nextRound,
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

	// Write to buffer instead of directly to database
	if q.outputBuffer != nil {
		// Add parent status update to buffer
		q.outputBuffer.AddStatusUpdate(queueType, currentRound, db.StatusPending, db.StatusSuccessful, path)

		// Add child inserts to buffer
		// For DST nodes, we need to match them to SRC nodes and populate join-lookup
		var joinLookupMappings map[string]string
		if q.name == "dst" && len(childNodesToInsert) > 0 {
			joinLookupMappings = q.buildJoinLookupMappings(childNodesToInsert)
		}
		if len(childNodesToInsert) > 0 {
			q.outputBuffer.AddBatchInsert(childNodesToInsert, joinLookupMappings)
		}
	}
}

// completeExclusionTask handles completion of exclusion tasks.
// Workers have already done the DB operations, so this just updates queue state.
func (q *Queue) completeExclusionTask(task *TaskBase) {
	currentRound := task.Round
	path := task.LocationPath()

	q.mu.Lock()
	// Remove from in-progress
	delete(q.inProgress, path)
	// Remove ULID from leased set
	if q.boltDB != nil {
		state, err := db.GetNodeStateByPath(q.boltDB, getQueueType(q.name), path)
		if err == nil && state != nil {
			delete(q.leasedKeys, state.ID)
		}
	}
	task.Locked = false
	task.Status = "successful"

	roundStats := q.getOrCreateRoundStats(currentRound)
	roundStats.Completed++
	q.mu.Unlock()

	// Workers have already:
	// 1. Read children from children bucket
	// 2. Written children to exclusion-holding bucket
	// 3. Queued exclusion update via output buffer
	// Queue just needs to track completion stats
}

func childResultToNodeState(child ChildResult, parentPath string, depth int) *db.NodeState {
	// Generate ULID for internal ID
	nodeID := db.GenerateNodeID()
	if nodeID == "" {
		// If ULID generation fails, we can't proceed
		return nil
	}

	if child.IsFile {
		file := child.File
		return &db.NodeState{
			ID:              nodeID,         // ULID for database keys
			ServiceID:       file.ServiceID, // FS identifier
			ParentID:        "",             // Will be looked up by parent path if needed
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
		}
	}

	folder := child.Folder
	return &db.NodeState{
		ID:              nodeID,           // ULID for database keys
		ServiceID:       folder.ServiceID, // FS identifier
		ParentID:        "",               // Will be looked up by parent path if needed
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
	}
}

func (q *Queue) PullTasksIfNeeded(force bool) {
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

	// Note: firstPullForRound will be set to false in PullTasks() after successful pull
	// We don't set it here because we need to know if it was the first pull when checking completion

	if force {
		q.pullTasksForMode(true)
		return
	}

	q.mu.RLock()
	// Only pull if: queue is running, buffer is low, not already pulling, and last pull wasn't partial
	// If last pull was partial, we might have exhausted this round - don't pull again until round advances
	lastPullWasPartial := q.lastPullWasPartial
	needPull := q.state == QueueStateRunning && len(q.pendingBuff) <= q.pullLowWM && !q.pulling && !lastPullWasPartial
	q.mu.RUnlock()
	if needPull {
		q.pullTasksForMode(false)
	}
}

// pullTasksForMode selects the appropriate pull method based on queue mode.
func (q *Queue) pullTasksForMode(force bool) {
	q.mu.RLock()
	mode := q.mode
	q.mu.RUnlock()

	switch mode {
	case QueueModeExclusion:
		q.PullExclusionTasks(force)
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

// Fail is a public wrapper for backward compatibility.
// New code should use ReportTaskResult instead.
// Returns true if the task will be retried, false if max retries exceeded.
func (q *Queue) Fail(task *TaskBase) bool {
	q.ReportTaskResult(task, TaskExecutionResultFailed)
	// Check if task was retried by checking if it's still in pending
	q.mu.RLock()
	path := task.LocationPath()
	_, willRetry := q.pendingSet[path]
	q.mu.RUnlock()
	return willRetry
}

// failTask is the internal implementation for failed task handling.
func (q *Queue) failTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta (even for failures)
	q.recordExecutionTime(executionDelta)
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
		return // Will retry - ReportTaskResult will handle pulling tasks
	}

	// Max retries reached - task is truly done
	// Remove the ULID from leased set
	if q.boltDB != nil {
		state, err := db.GetNodeStateByPath(q.boltDB, getQueueType(q.name), path)
		if err == nil && state != nil {
			delete(q.leasedKeys, state.ID)
		}
	}

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

	q.mu.Unlock()

	// Update traversal status to failed so leasing stops retrying this node.
	state := taskToNodeState(task)
	if state != nil && q.outputBuffer != nil {
		queueType := getQueueType(q.name)
		q.outputBuffer.AddStatusUpdate(queueType, currentRound, db.StatusPending, db.StatusFailed, state.Path)
	}
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

	// Quick check - if already completed, nothing to do (queues clean themselves up)
	q.mu.RLock()
	state := q.state
	hasStatsTick := q.statsTick != nil
	outputBuffer := q.outputBuffer
	q.mu.RUnlock()

	if state == QueueStateCompleted {
		// Still need to clean up stats ticker and buffer if they exist
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
			case <-time.After(500 * time.Millisecond):
			}
		}
		// Ensure buffer is stopped
		if outputBuffer != nil {
			outputBuffer.Stop()
		}
		return
	}

	// Queue not completed - acquire lock normally (should be quick since workers/Run() should have exited)
	q.mu.Lock()
	if q.statsTick != nil {
		q.statsTick.Stop()
		q.statsTick = nil
	}
	q.statsChan = nil
	closeOutputBuffer := q.outputBuffer
	if q.state != QueueStateCompleted && q.state != QueueStateStopped {
		q.state = QueueStateStopped
	}
	q.mu.Unlock()

	// Ensure buffer is stopped and flushed
	if closeOutputBuffer != nil {
		closeOutputBuffer.Stop()
	}
}

// WorkerCount returns the number of workers associated with this queue.
func (q *Queue) WorkerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.workers)
}

// recordExecutionTime adds an execution time delta to the buffer and periodically calculates averages.
func (q *Queue) recordExecutionTime(delta time.Duration) {
	if delta <= 0 {
		return // Skip invalid deltas
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Add to buffer (max 100 items)
	if len(q.executionTimeDeltas) >= 100 {
		// Remove oldest item (FIFO)
		q.executionTimeDeltas = q.executionTimeDeltas[1:]
	}
	q.executionTimeDeltas = append(q.executionTimeDeltas, delta)

	// Check if it's time to calculate average
	now := time.Now()
	if now.Sub(q.lastAvgTime) >= q.avgInterval {
		q.calculateAverageLocked()
		q.lastAvgTime = now
	}
}

// calculateAverageLocked calculates the average execution time and clears the buffer.
// Must be called with q.mu.Lock() held.
func (q *Queue) calculateAverageLocked() {
	if len(q.executionTimeDeltas) == 0 {
		q.avgExecutionTime = 0
		return
	}

	var sum time.Duration
	for _, delta := range q.executionTimeDeltas {
		sum += delta
	}
	q.avgExecutionTime = sum / time.Duration(len(q.executionTimeDeltas))

	// Clear buffer after calculating average
	q.executionTimeDeltas = q.executionTimeDeltas[:0]
}

// GetAverageExecutionTime returns the current average task execution time.
func (q *Queue) GetAverageExecutionTime() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgExecutionTime
}

// GetExecutionTimeBufferSize returns the current size of the execution time buffer.
func (q *Queue) GetExecutionTimeBufferSize() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.executionTimeDeltas)
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

		q.mu.RLock()
		mode := q.mode
		q.mu.RUnlock()

		// GATE CHECK: Can we start processing this round? (DST only - SRC has no gates)
		// Exclusion mode has no coordinator gating (single queue operation)
		// This check happens IMMEDIATELY at the start of each iteration, before any task processing.
		if q.name == "dst" && q.coordinator != nil && mode != QueueModeExclusion {
			canStartRound := q.coordinator.CanDstStartRound(currentRound)

			if !canStartRound {

				// Still waiting - set state and sleep
				q.mu.Lock()
				if q.state != QueueStateWaiting {
					q.state = QueueStateWaiting
				}
				q.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				continue // Loop back and check again
			}

			// Green light - can start this round, ensure state is running
			q.mu.Lock()
			if q.state == QueueStateWaiting {
				q.state = QueueStateRunning
			}
			q.mu.Unlock()
		}

		// Event-driven: Workers will call ReportTaskResult which handles:
		// - Pulling more tasks when needed (when buffer is low)
		// - Advancing rounds when complete
		// No need to pull tasks here - ReportTaskResult handles it

		// Wait for round to complete (event-driven by ReportTaskResult)
		// Check periodically if we should advance or if queue is done
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

			// If stopped or completed, exit
			if innerState == QueueStateStopped || innerState == QueueStateCompleted {
				return
			}

			// Check periodically if round advanced (for outer loop to continue)
			if q.getRound() != currentRound {
				break // Round advanced, continue outer loop
			}

			// Periodic completion check: check if round is complete or queue is done
			// This replaces event-driven checks from ReportTaskResult to avoid race conditions
			q.mu.RLock()
			pendingCount := len(q.pendingBuff)
			inProgressCount := len(q.inProgress)
			lastPullWasPartial := q.lastPullWasPartial
			state := q.state
			q.mu.RUnlock()

			// Only check if queue is running
			if state == QueueStateRunning || state == QueueStateWaiting {

				// Check if round is complete (inProgress + pendingBuff == 0 AND lastPullWasPartial == true)
				// Only check if we truly have no work left
				if inProgressCount == 0 && pendingCount == 0 && lastPullWasPartial {
					q.mu.RLock()
					roundToCheck := q.round
					q.mu.RUnlock()
					q.checkCompletion(roundToCheck, CompletionCheckOptions{
						CheckRoundComplete:     true,
						AdvanceRoundIfComplete: true,
						FlushBuffer:            true,
					})
				}
			}

			// After checking completion, re-check if round advanced (it might have advanced)
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

	q.mu.RLock()
	outputBuffer := q.outputBuffer
	q.mu.RUnlock()
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

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

	if logservice.LS != nil {
		q.mu.RLock()
		mode := q.mode
		q.mu.RUnlock()
		if mode == QueueModeExclusion {
			_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d (exclusion mode)", newRound), "queue", q.name, q.name)
		} else {
			_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.name, q.name)
		}
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

// buildJoinLookupMappings matches DST nodes to SRC nodes by path and returns a map of DST ULID -> SRC ULID.
// This is used to populate the join-lookup table during DST bulk inserts.
func (q *Queue) buildJoinLookupMappings(dstOps []db.InsertOperation) map[string]string {
	if q.boltDB == nil {
		return nil
	}

	mappings := make(map[string]string)

	// For each DST node, find the corresponding SRC node by path
	for _, op := range dstOps {
		if op.QueueType != "DST" || op.State == nil {
			continue
		}

		// Look up SRC node by path
		srcState, err := db.GetNodeStateByPath(q.boltDB, "SRC", op.State.Path)
		if err == nil && srcState != nil {
			// Found matching SRC node - add to mappings
			mappings[op.State.ID] = srcState.ID
		}
		// If no match found, skip (node doesn't exist on SRC)
	}

	return mappings
}
