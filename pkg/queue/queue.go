// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// RoundInfo tracks statistics and metadata for a specific BFS round.
type RoundInfo struct {
	Round           int       // Round number
	PullCount       int       // Number of pull operations (queries) performed this round
	ItemsYielded    int       // Pulled amount: total items actually returned from DB queries this round (like completed count but for pulls)
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
	QueueModeCopy      QueueMode = "copy"      // Copy phase (folders then files)
)

const (
	defaultLeaseBatchSize = 10_000
	maxLeaseBatchSize     = 100_000 // Upper bound for pull (lease) batch size
	// MaxSrcDstGap is the maximum allowed round gap (src - dst). When src exceeds dst + MaxSrcDstGap, src pauses pulling (Phase 6).
	MaxSrcDstGap = 2
)

// effectiveLeaseBatchSize returns the lease batch size capped by maxLeaseBatchSize.
func effectiveLeaseBatchSize() int {
	if defaultLeaseBatchSize <= maxLeaseBatchSize {
		return defaultLeaseBatchSize
	}
	return maxLeaseBatchSize
}

// getQueueType returns "SRC" for "src", "DST" for "dst", "SRC" for "copy" (copy phase uses SRC table). Returns "" for unknown.
func getQueueType(queueName string) string {
	switch strings.ToLower(queueName) {
	case "src":
		return "SRC"
	case "dst":
		return "DST"
	case "copy":
		return "SRC"
	default:
		return ""
	}
}

// taskToNodeState converts a TaskBase to NodeState for DB writes (e.g. child inserts).
func taskToNodeState(task *TaskBase) *db.NodeState {
	if task == nil {
		return nil
	}
	path := task.LocationPath()
	state := &db.NodeState{
		ID:              task.ID,
		Path:            path,
		ParentPath:      "",
		Depth:           task.Round,
		TraversalStatus: db.StatusSuccessful,
		Status:          db.StatusSuccessful,
		Name:            "",
	}
	if task.IsFolder() {
		state.Type = types.NodeTypeFolder
		state.ServiceID = task.Folder.ServiceID
		state.ParentServiceID = task.Folder.ParentId
		state.ParentPath = task.Folder.ParentPath
		state.MTime = task.Folder.LastUpdated
		state.Name = task.Folder.DisplayName
	} else {
		state.Type = types.NodeTypeFile
		state.ServiceID = task.File.ServiceID
		state.ParentServiceID = task.File.ParentId
		state.ParentPath = task.File.ParentPath
		state.MTime = task.File.LastUpdated
		state.Size = task.File.Size
		state.Name = task.File.DisplayName
	}
	return state
}

// nodeStateToTask converts a NodeState to a traversal TaskBase (src-traversal or dst-traversal).
func nodeStateToTask(state *db.NodeState, taskType string) *TaskBase {
	if state == nil {
		return nil
	}
	task := &TaskBase{
		ID:        state.ID,
		Type:      taskType,
		Round:     state.Depth,
		Attempts:  0,
		Status:    "",
		Locked:    false,
		LeaseTime: time.Now(),
	}
	if state.Type == types.NodeTypeFolder {
		task.Folder = types.Folder{
			ServiceID:    state.ServiceID,
			ParentId:     state.ParentServiceID,
			ParentPath:   state.ParentPath,
			DisplayName:  state.Name,
			LocationPath: state.Path,
			LastUpdated:  state.MTime,
			DepthLevel:   state.Depth,
			Type:         state.Type,
		}
	} else {
		task.File = types.File{
			ServiceID:    state.ServiceID,
			ParentId:     state.ParentServiceID,
			ParentPath:   state.ParentPath,
			DisplayName:  state.Name,
			LocationPath: state.Path,
			LastUpdated:  state.MTime,
			Size:         state.Size,
			DepthLevel:   state.Depth,
			Type:         state.Type,
		}
	}
	return task
}

// Queue maintains round-based task queues for BFS traversal coordination.
// It handles task leasing, retry logic, and cross-queue task propagation.
// All operational state lives in BoltDB, flushed via per-queue buffers.
type Queue struct {
	name               string               // Queue name ("src" or "dst")
	mode               QueueMode            // Operation mode (traversal/retry/copy)
	mu                 sync.RWMutex         // Protects all internal state
	state              QueueState           // Lifecycle state (running/paused/stopped/completed/waiting)
	inProgress         map[string]*TaskBase // Tasks currently being executed (keyed by ULID)
	pendingBuff        []*TaskBase          // Local task buffer fetched from BoltDB
	pendingSet         map[string]struct{}  // Fast lookup for pending buffer dedupe (keyed by ULID)
	leasedKeys         map[string]struct{}  // ULIDs already pulled/leased - prevents duplicate pulls from stale views
	pulling            bool                 // Indicates a pull operation is active
	lastPullWasPartial bool                 // True if last pull returned fewer tasks than requested (partial batch)
	firstPullForRound  bool                 // True if we haven't done the first pull for the current round
	maxRetries         int                  // Maximum retry attempts per task
	round              int                  // Current BFS round/depth level
	roundInfoMap       map[int]*RoundInfo   // Per-round statistics and metadata (key: round number)
	workers            []Worker             // Workers associated with this queue (for reference only)
	database            *db.DB               // Database for operational queue storage
	coordinator        *QueueCoordinator    // Coordinator for round advancement gates (DST only)
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
	// Copy phase specific fields
	copyPass int // Current copy pass (1 for folders, 2 for files)
	// Discovery tracking for metrics
	filesDiscoveredTotal   int64 // Total files discovered (monotonic counter)
	foldersDiscoveredTotal int64 // Total folders discovered (monotonic counter)
	// Copy phase metrics tracking
	bytesTransferredTotal int64 // Total bytes transferred (monotonic counter)
	foldersCreatedTotal   int64 // Total folders created (monotonic counter)
	filesCreatedTotal     int64 // Total files created (monotonic counter)
	// Tasks completed total: incremented on every success or final failure, pushed to stats on flush
	tasksCompletedTotal int64
	// Keyset cursors for pagination (id > cursor ORDER BY id LIMIT n). Strictly round-scoped per queue; see resetThisQueueKeysetCursor.
	srcKeysetCursor  string // SRC traversal/retry pull
	dstKeysetCursor  string // DST traversal/retry pull
	copyKeysetCursor string // Copy phase pull
}

// NewQueue creates a new Queue instance.
func NewQueue(name string, maxRetries int, workerCount int, coordinator *QueueCoordinator) *Queue {
	return &Queue{
		name:                name,
		mode:                QueueModeTraversal, // Default to traversal mode
		state:               QueueStateRunning,
		inProgress:          make(map[string]*TaskBase),
		pendingBuff:         make([]*TaskBase, 0, effectiveLeaseBatchSize()),
		pendingSet:          make(map[string]struct{}),
		leasedKeys:          make(map[string]struct{}),
		maxRetries:          maxRetries,
		round:               0,
		workers:             make([]Worker, 0, workerCount),
		roundStats:          make(map[int]*RoundStats),
		roundInfoMap:        make(map[int]*RoundInfo), // Initialize round info map
		coordinator:         coordinator,
		firstPullForRound:   true,
		executionTimeDeltas: make([]time.Duration, 0, 100), // Buffer capacity 100
		avgInterval:         5 * time.Second,               // Calculate average every 5 seconds
		lastAvgTime:         time.Now(),
		maxKnownDepth:       -1, // -1 means not set yet
	}
}

// InitializeWithContext sets up the queue with BoltDB, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// shutdownCtx is optional - if provided, workers will check for cancellation and exit on shutdown.
// For copy mode, InitializeCopyWithContext should be used instead to provide both adapters.
func (q *Queue) InitializeWithContext(database *db.DB, adapter types.FSAdapter, shutdownCtx context.Context) {
	// Set database and shutdownCtx using setters
	q.setDatabase(database)
	q.SetShutdownContext(shutdownCtx)

	// Get worker count from capacity (workers haven't been added yet, so length is 0)
	q.mu.RLock()
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.RUnlock()

	// Register flush callback for leased-key removal (DB owns buffers)
	database.SetOnFlush(getQueueType(q.name), func(nodeIDs []string) {
		for _, nodeID := range nodeIDs {
			q.removeLeasedKey(nodeID)
		}
	})

	// Create and start workers - they manage themselves
	for i := 0; i < workerCount; i++ {
		w := NewTraversalWorker(
			fmt.Sprintf("%s-worker-%d", q.name, i),
			q,
			database,
			adapter,
			q.name,
			shutdownCtx,
		)
		q.AddWorker(w)
		go w.Run()
	}

	// Start the queue's Run() method to coordinate pulling tasks and advancing rounds
	go q.Run()

	// Queues are initialized - tasks will be seeded externally or propagated through Complete()
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("%s queue initialized", strings.ToUpper(q.name)), "queue", q.name, q.name)
	}
}

// InitializeCopyWithContext sets up a copy queue with both source and destination adapters.
// This is specifically for copy mode which requires both adapters.
func (q *Queue) InitializeCopyWithContext(database *db.DB, srcAdapter, dstAdapter types.FSAdapter, shutdownCtx context.Context) {
	// Set database and shutdownCtx using setters
	q.setDatabase(database)
	q.SetShutdownContext(shutdownCtx)

	// Get worker count from capacity
	q.mu.RLock()
	workerCount := cap(q.workers)
	q.mu.RUnlock()

	// Register flush callback for leased-key removal (DB owns buffers)
	database.SetOnFlush(getQueueType(q.name), func(nodeIDs []string) {
		for _, nodeID := range nodeIDs {
			q.removeLeasedKey(nodeID)
		}
	})

	// Create and start copy workers
	for i := 0; i < workerCount; i++ {
		w := NewCopyWorker(
			fmt.Sprintf("%s-worker-%d", q.name, i),
			q,
			database,
			srcAdapter,
			dstAdapter,
			shutdownCtx,
		)
		q.AddWorker(w)
		go w.Run()
	}

	// Start the queue's Run() method to coordinate pulling tasks and advancing rounds
	go q.Run()

	// Queues are initialized - tasks will be pulled from copy status buckets
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("%s copy queue initialized", strings.ToUpper(q.name)), "queue", q.name, q.name)
	}
}

// Name returns the queue's name.
func (q *Queue) Name() string {
	return q.name
}

// IsExhausted returns true if the queue has finished all traversal or has been stopped.
func (q *Queue) IsExhausted() bool {
	state := q.State()
	return state == QueueStateCompleted || state == QueueStateStopped
}

// IsPaused returns true if the queue is paused.
func (q *Queue) IsPaused() bool {
	return q.State() == QueueStatePaused
}

// Pause pauses the queue (workers will not lease new tasks).
func (q *Queue) Pause() {
	q.SetState(QueueStatePaused)
}

// Resume resumes the queue after a pause.
func (q *Queue) Resume() {
	q.SetState(QueueStateRunning)
}

// Lease attempts to lease a task for execution atomically.
// Returns nil if no tasks are available, queue is paused, or completed.
func (q *Queue) Lease() *TaskBase {
	// Check if queue is completed before attempting to pull tasks
	if q.State() == QueueStateCompleted {
		return nil
	}

	q.PullTasksIfNeeded(false)

	for attempt := 0; attempt < 2; attempt++ {
		// Block if paused, completed, or no DB (waiting doesn't block - rounds continue once started)
		state := q.State()
		database := q.getDatabase()
		if state == QueueStatePaused || state == QueueStateCompleted || database == nil {
			return nil
		}

		task := q.dequeuePending()
		if task != nil {
			nodeID := task.ID
			if nodeID == "" {
				// With deterministic IDs, task.ID should always be pre-computed
				// This is a programming error if we reach here
				if logservice.LS != nil {
					_ = logservice.LS.Log("error",
						"Lease found task with empty ID - this indicates a bug in ID generation",
						"queue", q.name, q.name)
				}
				continue
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
	if opts.FlushBuffer {
		if database := q.getDatabase(); database != nil {
			database.FlushTablesForQueue(getQueueType(q.name))
		}
	}

	// For traversal/sweep completion check (handles all modes)
	// Called when first pull returns 0 entries - decides if we're completely done
	if opts.CheckFinalCompletion {
		// Skip check if queue is in waiting state (DST gating)
		queueState := q.State()
		if queueState == QueueStateWaiting {
			return false
		}

		// Check state and conditions after flush
		inProgressCount := q.InProgressCount()
		pendingBuffCount := q.GetPendingCount()
		wasFirstPull := opts.WasFirstPull
		database := q.getDatabase()

		// If we have tasks in progress or in buffer, we're definitely not done
		if inProgressCount > 0 || pendingBuffCount > 0 {
			return false
		}

		// Only check completion on first pull (prevents premature completion mid-round)

		if database == nil {
			return false
		}

		mode := q.GetMode()

		// Mode-specific completion conditions
		switch mode {
		case QueueModeTraversal, QueueModeRetry:
			// Delegate to traversal/retry-specific completion check in mode_traversal.go
			return q.CheckTraversalCompletion(currentRound, wasFirstPull)

		case QueueModeCopy:
			// Delegate to copy-specific completion check in mode_copy.go
			return q.CheckCopyCompletion(currentRound, wasFirstPull)
		}

		return false
	}

	// For round completion check
	if opts.CheckRoundComplete {
		// Check state first
		if q.State() != QueueStateRunning {
			return false
		}

		// Soft check: verify in-memory state (after flush)
		inProgressCount := q.InProgressCount()
		pendingBuffCount := q.GetPendingCount()
		lastPullWasPartial := q.getLastPullWasPartial()

		// Round is complete if: no in-progress, no pending, and last pull was partial
		if inProgressCount > 0 || pendingBuffCount > 0 || !lastPullWasPartial {
			return false
		}

		// Hard check: verify BoltDB buckets are empty (mode-specific)
		mode := q.GetMode()
		if mode == QueueModeCopy {
			database := q.getDatabase()
			if database != nil {
				copyPass := q.GetCopyPass()
				// Determine node type for current pass
				nodeType := db.NodeTypeFolder
				if copyPass == 2 {
					nodeType = db.NodeTypeFile
				}

				// For copy mode: check if pending OR in-progress buckets have items for this round and node type
				// Buckets are now split by node type, so no filtering needed!
				hasPendingForPass := false
				hasInProgressForPass := false

				// Check pending bucket for this node type
				c1, err1 := database.GetCopyCountAtDepth(currentRound, nodeType, db.CopyStatusPending)
				if err1 == nil && c1 > 0 {
					hasPendingForPass = true
				}

				// Check in-progress bucket for this node type
				c2, err2 := database.GetCopyCountAtDepth(currentRound, nodeType, db.CopyStatusInProgress)
				hasInProgress := err2 == nil && c2 > 0
				if err2 == nil && hasInProgress {
					hasInProgressForPass = true
				}

				if err1 == nil && err2 == nil {
					if hasPendingForPass || hasInProgressForPass {
						// Hard check failed: still have pending or in-progress tasks for this pass
						// Reset lastPullWasPartial to false so normal pull logic can trigger
						q.setLastPullWasPartial(false)
						return false
					}
				} else {
					// Error checking buckets - be conservative and don't advance
					// Errors logged via logservice if available
					return false
				}
			} else {
				// No BoltDB - can't do hard check
				return false
			}
		}

		// Both soft and hard checks passed - round is complete
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
	state := q.State()
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

	q.SetState(QueueStateCompleted)

	// Create node table indexes after hot path (traversal/copy) so writes are not slowed. Each index is O(n).
	if database := q.getDatabase(); database != nil {
		switch q.name {
		case "src":
			_ = db.EnsureNodeTableIndexes(database, "src_nodes")
		case "dst":
			_ = db.EnsureNodeTableIndexes(database, "dst_nodes")
		case "copy":
			_ = db.EnsureNodeTableIndexes(database, "src_nodes")
			_ = db.EnsureNodeTableIndexes(database, "dst_nodes")
		}
	}

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
	pendingCount := q.GetPendingCount()
	lastPullWasPartial := q.getLastPullWasPartial()
	state := q.State()

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

// completeTask is the internal implementation for successful task completion.
func (q *Queue) completeTask(task *TaskBase, executionDelta time.Duration) {
	mode := q.GetMode()

	// Delegate to mode-specific implementation
	if mode == QueueModeCopy {
		q.CompleteCopyTask(task, executionDelta)
		return
	}

	// Traversal and retry modes use the same completion logic
	q.CompleteTraversalTask(task, executionDelta)

}

// childResultToNodeState converts a ChildResult to NodeState using deterministic ID generation.
// parentPath is the root-relative path of the parent (e.g., "/items").
// The child's path is computed from parentPath + child name to ensure it's always root-relative,
// regardless of what the filesystem adapter returns in LocationPath.
// Node ID is deterministically computed from (queueType, nodeType, path) for race-safe deduplication.
func childResultToNodeState(child ChildResult, parentPath string, depth int, queueType string, parentID string) *db.NodeState {
	// Compute root-relative path from parent path and child name first
	// (needed for deterministic ID generation)
	var rootRelativePath string
	var childName string
	var nodeType string

	if child.IsFile {
		childName = child.File.DisplayName
		nodeType = types.NodeTypeFile
	} else {
		childName = child.Folder.DisplayName
		nodeType = types.NodeTypeFolder
	}

	if parentPath == "/" {
		// Child of root folder
		rootRelativePath = "/" + childName
	} else {
		// Child of non-root folder
		rootRelativePath = types.NormalizeLocationPath(parentPath + "/" + childName)
	}

	// Generate deterministic ID from logical identity (queueType, nodeType, path)
	// This eliminates duplicate logical nodes and makes traversal race-safe
	nodeID := db.DeterministicNodeID(queueType, nodeType, rootRelativePath)

	// Store SrcID temporarily in NodeState for BatchInsertNodes to create lookup mappings
	// (BatchInsertNodes will handle storing in lookup tables, then SrcID can be removed from NodeState)
	var srcID string
	if queueType == "DST" {
		srcID = child.SrcID
	}

	// Set CopyStatus to "pending" for all SRC children (will be updated during DST comparison)
	// This ensures ALL SRC items start as pending, and DST comparison will update them to successful
	// if they exist on both sides (and DST is newer for files)
	var copyStatus string
	if queueType == "SRC" {
		copyStatus = db.CopyStatusPending
	} else {
		copyStatus = "" // DST nodes don't have copy status
	}

	if child.IsFile {
		file := child.File
		return &db.NodeState{
			ID:              nodeID,         // Deterministic ID for database keys
			ServiceID:       file.ServiceID, // FS identifier
			ParentID:        parentID,       // Parent's deterministic ID for database relationships
			ParentServiceID: file.ParentId,  // Parent's FS identifier
			ParentPath:      parentPath,
			Name:            file.DisplayName,
			Path:            rootRelativePath, // Use computed root-relative path
			Type:            types.NodeTypeFile,
			Size:            file.Size,
			MTime:           file.LastUpdated,
			Depth:           depth,
			CopyStatus:      copyStatus, // Set to "pending" for SRC, empty for DST
			Status:          child.Status,
			SrcID:           srcID, // Temporarily stored for BatchInsertNodes to create lookup mappings
		}
	}

	folder := child.Folder
	return &db.NodeState{
		ID:              nodeID,           // Deterministic ID for database keys
		ServiceID:       folder.ServiceID, // FS identifier
		ParentID:        parentID,         // Parent's deterministic ID for database relationships
		ParentServiceID: folder.ParentId,  // Parent's FS identifier
		ParentPath:      parentPath,
		Name:            folder.DisplayName,
		Path:            rootRelativePath, // Use computed root-relative path
		Type:            types.NodeTypeFolder,
		Size:            0,
		MTime:           folder.LastUpdated,
		Depth:           depth,
		CopyStatus:      copyStatus, // Set to "pending" for SRC, empty for DST
		Status:          child.Status,
		SrcID:           srcID, // Temporarily stored for BatchInsertNodes to create lookup mappings
	}
}

func (q *Queue) PullTasksIfNeeded(force bool) {
		database := q.getDatabase()
		if database == nil {
		return
	}

	// Don't pull if queue is paused or completed (waiting doesn't block pulls - rounds continue once started)
	state := q.State()
	if state == QueueStatePaused || state == QueueStateCompleted {
		return
	}

	// Note: firstPullForRound will be set to false in PullTasks() after successful pull
	// We don't set it here because we need to know if it was the first pull when checking completion

	if force {
		mode := q.GetMode()
		switch mode {
		case QueueModeRetry:
			q.PullRetryTasks(true)
		case QueueModeCopy:
			q.PullCopyTasks(true)
		default: // QueueModeTraversal
			q.PullTraversalTasks(true)
		}
		return
	}

	// Only pull if: queue is running, buffer is low, not already pulling, and last pull wasn't partial
	// If last pull was partial, we might have exhausted this round - don't pull again until round advances
	lastPullWasPartial := q.getLastPullWasPartial()
	pendingCount := q.GetPendingCount()
	pullLowWM := q.getPullLowWM()
	pulling := q.getPulling()
	needPull := state == QueueStateRunning && pendingCount <= pullLowWM && !pulling && !lastPullWasPartial
	if needPull {
		mode := q.GetMode()
		switch mode {
		case QueueModeRetry:
			q.PullRetryTasks(false)
		case QueueModeCopy:
			q.PullCopyTasks(false)
		default: // QueueModeTraversal
			q.PullTraversalTasks(false)
		}
	}
}

// failTask is the internal implementation for failed task handling.
func (q *Queue) failTask(task *TaskBase, executionDelta time.Duration) {
	mode := q.GetMode()

	// Delegate to mode-specific implementation
	if mode == QueueModeCopy {
		q.FailCopyTask(task, executionDelta)
		return
	}

	// Traversal and retry modes use the same failure logic
	q.FailTraversalTask(task, executionDelta)
}

// TotalTracked returns the total number of tasks across all rounds (pending + in-progress).
func (q *Queue) TotalTracked() int {
	return q.GetPendingCount() + q.InProgressCount()
}

// Clear removes all tasks from BoltDB and resets in-progress tracking.
// Note: This is a destructive operation - use with caution.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Clear in-progress tracking
	q.inProgress = make(map[string]*TaskBase)
	q.pendingBuff = make([]*TaskBase, 0, effectiveLeaseBatchSize())
	q.pendingSet = make(map[string]struct{})
	q.pulling = false

	// BoltDB clearing would require deleting all buckets - typically not needed
	// (DuckDB-only: no ETL.)
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
		state := q.State()
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
	Failed    int // Tasks failed in this round
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

// EnsureRoundExpectedFromStats sets Expected for the current round from the stats bucket (O(1) lookup).
// Call after SetRound (e.g. on init or resume) so Expected reflects actual pending count and survives restarts.
func (q *Queue) EnsureRoundExpectedFromStats() {
	q.setExpectedFromStatsBucket(q.GetRound())
}

// AddWorker registers a worker with this queue for reference.
// Workers manage their own lifecycle - this is just for tracking/debugging.
func (q *Queue) AddWorker(worker Worker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers = append(q.workers, worker)
}

// Close stops the queue and cleans up resources: stats publishing loop, output buffer, and sets state to Stopped if not already Completed or Stopped.
func (q *Queue) Close() {
	state := q.State()
	statsTick := q.getStatsTick()
	if statsTick != nil {
		statsTick.Stop()
		q.setStatsTick(nil)
	}
	q.setStatsChan(nil)
	if state != QueueStateCompleted && state != QueueStateStopped {
		q.SetState(QueueStateStopped)
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
	deltas := q.GetExecutionTimeDeltas()
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

// GetExecutionTimeBufferSize returns the current size of the execution time buffer.
func (q *Queue) GetExecutionTimeBufferSize() int {
	return len(q.GetExecutionTimeDeltas())
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
	// DST queue runs traversal or retry; copy is the only mode DST skips
	mode := q.GetMode()
	if q.name == "dst" && !(mode == QueueModeTraversal || mode == QueueModeRetry) {
		q.SetState(QueueStateCompleted)
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

		state := q.State()
		database := q.getDatabase()

		if database == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// If queue is completed or stopped, exit
		if state == QueueStateCompleted || state == QueueStateStopped {
			if logservice.LS != nil {
				currentRound := q.GetRound()
				_ = logservice.LS.Log("info",
					fmt.Sprintf("Run() exiting - queue %s (round %d)", state, currentRound),
					"queue", q.name, q.name)
			}
			return
		}

		// Get current round
		currentRound := q.GetRound()
		coordinator := q.getCoordinator()

		// GATE CHECK: Can we start processing this round? (DST only)
		// This check happens IMMEDIATELY at the start of each iteration, before any task processing.
		if q.name == "dst" && coordinator != nil {
			canStartRound := coordinator.CanDstStartRound(currentRound)

			if !canStartRound {
				// Still waiting - set state and sleep
				if state != QueueStateWaiting {
					q.SetState(QueueStateWaiting)
				}
				time.Sleep(50 * time.Millisecond)
				continue // Loop back and check again
			}

			// Green light - can start this round, ensure state is running
			if state == QueueStateWaiting {
				q.SetState(QueueStateRunning)
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

			innerState := q.State()

			// If paused, block here
			if innerState == QueueStatePaused {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// If stopped or completed, exit
			if innerState == QueueStateStopped || innerState == QueueStateCompleted {
				return
			}

			// Get current state snapshot
			inProgressCount := q.InProgressCount()
			pendingCount := q.GetPendingCount()
			lastPullWasPartial := q.getLastPullWasPartial()
			pullCount := q.getCurrentRoundPullCount()
			pulledAmount := q.getCurrentRoundPulledAmount() // items actually returned from our DB queries this round
			roundToCheck := q.GetRound()

			// 1. Check queue completion (mode-specific)
			// Done when we've queried at least once (pullCount > 0) and pulled amount is 0 (queries returned nothing).
			mode := q.GetMode()
			if mode != QueueModeCopy {
				// Traversal/retry: mark complete only when we queried and got 0 items, and first pull of round (prevents premature completion mid-round).
				if pullCount > 0 && pulledAmount == 0 && inProgressCount == 0 && pendingCount == 0 {
					wasFirstPull := (pullCount == 1) // first pull of this round returned 0 items
					completed := q.checkCompletion(roundToCheck, CompletionCheckOptions{
						CheckFinalCompletion: true,
						WasFirstPull:         wasFirstPull,
						FlushBuffer:          true,
					})
					if completed {
						// Don't call Stop() here - let the outer loop handle cleanup
						// The outer loop will detect QueueStateCompleted and stop the buffer
						return
					}
				}
			}

			// 2. Check round completion (universal)
			// Soft check: in-memory state only (fast, no DB access)
			// Round complete when: no in-progress, no pending, last pull was partial
			roundCompleteSoft := inProgressCount == 0 && pendingCount == 0 && lastPullWasPartial

			// For copy mode, also consider round complete if we queried and pulled amount is 0 (no tasks for this pass at this round)
			if mode == QueueModeCopy && pullCount > 0 && pulledAmount == 0 && inProgressCount == 0 && pendingCount == 0 {
				roundCompleteSoft = true
			}

			// If soft check passes, do hard check (DB verification) in checkCompletion
			if roundCompleteSoft {
				q.checkCompletion(roundToCheck, CompletionCheckOptions{
					CheckRoundComplete:     true,
					AdvanceRoundIfComplete: true,
					FlushBuffer:            true,
				})
			}

			// Check if round advanced
			if q.GetRound() != currentRound {
				break // Round advanced, continue outer loop
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

// advanceToNextRound advances the queue to the next round and cleans up old round queues.
// Note: Round advancement is now free - gating only happens when STARTING a round (checked in Run()).
// For copy mode, advances to the next round that has pending tasks for the current pass.
func (q *Queue) advanceToNextRound() {
	// No gating here - rounds advance freely
	// Gating only happens when STARTING a round (checked in Run() outer loop)

	database := q.getDatabase()
	if database != nil {
		// Flush buffer before seal so all status updates are in staging
		database.FlushTablesForQueue(getQueueType(q.name))
	}

	// Phase 5: onLevelSeal - merge staging to live, update stats, write completed count, drop staging, create new staging.
	if database != nil {
		round := q.GetRound()
		completed := int64(0)
		if stats := q.GetRoundStats(round); stats != nil {
			completed = int64(stats.Completed)
		}
		_ = database.RunUpdateWriterTx(func(w *db.Writer) error {
			return w.ApplyStatusStagingAndDrop(round, getQueueType(q.name), completed)
		})
		_ = database.Checkpoint()
	}
	// This queue's cursor must not survive its seal. Reset only this queue's cursor; other queues are independent.
	q.resetThisQueueKeysetCursor()

	// Ensure state is running if it was waiting
	state := q.State()
	if state == QueueStateWaiting {
		q.SetState(QueueStateRunning)
	}

	// Delegate to mode-specific round advancement
	mode := q.GetMode()
	if mode == QueueModeCopy {
		q.AdvanceCopyRound()
		return
	}

	// For traversal and retry modes, use the same round advancement (increment round, reset flags, pull)
	q.AdvanceTraversalRound()
}
