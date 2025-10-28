// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
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

// Queue maintains pending, in-progress task lists and uses a Buffer for batch writes.
// It handles task leasing, retry logic, task pulling from DB, and coordinated flushing.
type Queue struct {
	name                string                     // Queue name ("src" or "dst")
	mu                  sync.RWMutex               // Protects all internal state
	state               QueueState                 // Lifecycle state (running/paused/stopped/completed)
	pending             []*TaskBase                // Tasks waiting to be leased
	inProgress          map[string]*TaskBase       // Tasks currently being executed (keyed by identifier)
	trackedIDs          map[string]bool            // All task IDs currently tracked (prevents re-querying)
	maxRetries          int                        // Maximum retry attempts per task
	round               int                        // Current BFS round/depth level
	batchSize           int                        // Number of tasks to fetch from DB per batch
	workers             []Worker                   // Workers associated with this queue (for reference only)
	pulling             bool                       // True when a pull is in progress (prevents concurrent pulls)
	pendingRoundAdvance bool                       // True when partial batch detected, waiting for tasks to complete before advancing
	db                  *db.DB                     // Database handle for task pulling
	tableName           string                     // "src_nodes" or "dst_nodes"
	srcCtx              *fsservices.ServiceContext // For dst queue: needed to query src nodes
	srcQueue            *Queue                     // For dst queue: reference to src queue for round coordination
}

// NewQueue creates a new Queue instance.
func NewQueue(name string, maxRetries int, batchSize int, workerCount int) *Queue {
	return &Queue{
		name:       name,
		state:      QueueStateRunning,
		pending:    make([]*TaskBase, 0),
		inProgress: make(map[string]*TaskBase),
		trackedIDs: make(map[string]bool),
		maxRetries: maxRetries,
		round:      0,
		batchSize:  batchSize,
		workers:    make([]Worker, 0, workerCount),
	}
}

// Initialize sets up the queue with database, context, and filesystem adapter references.
// Creates and starts workers immediately - they'll poll for tasks autonomously.
// For dst queues, srcContext and srcQueue are required for round coordination.
func (q *Queue) Initialize(database *db.DB, tableName string, adapter fsservices.FSAdapter, srcContext *fsservices.ServiceContext, srcQueue *Queue) {
	q.mu.Lock()
	q.db = database
	q.tableName = tableName
	q.srcCtx = srcContext
	q.srcQueue = srcQueue
	workerCount := cap(q.workers) // Get the worker count we preallocated for
	q.mu.Unlock()

	// Create and start workers - they manage themselves
	for i := 0; i < workerCount; i++ {
		worker := NewTraversalWorker(
			i,
			q,
			database,
			adapter,
			tableName,
			q.name,
		)
		q.AddWorker(worker)
		go worker.Run()

	}

	// Scenario 2: Initial pull when queue is created
	// DST queues should NOT pull on initialization - they wait for SRC to be ahead
	if q.name != "dst" {
		q.pullTasks()
	} else {
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "DST queue initialized - waiting for SRC to advance before pulling tasks", "queue", q.name)
		}
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

// BatchSize returns the configured batch size for DB queries.
func (q *Queue) BatchSize() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.batchSize
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

// Add enqueues a new task if it's not already tracked.
func (q *Queue) Add(task *TaskBase) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := task.Identifier()
	if q.trackedIDs[id] {
		return false // Already tracked
	}

	q.pending = append(q.pending, task)
	q.trackedIDs[id] = true
	return true
}

// AddBatch enqueues multiple tasks atomically.
func (q *Queue) AddBatch(tasks []*TaskBase) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	added := 0
	for _, task := range tasks {
		id := task.Identifier()
		if !q.trackedIDs[id] {
			q.pending = append(q.pending, task)
			q.trackedIDs[id] = true
			added++
		}
	}
	return added
}

// Lease attempts to lease a task for execution.
// Returns nil if no tasks are available, queue is paused, or completed.
func (q *Queue) Lease() *TaskBase {
	q.mu.Lock()

	// Don't lease if paused or completed
	if q.state == QueueStatePaused || q.state == QueueStateCompleted {
		q.mu.Unlock()
		return nil
	}

	if len(q.pending) == 0 {
		q.mu.Unlock()
		return nil
	}

	// Pop from front (FIFO)
	task := q.pending[0]
	q.pending = q.pending[1:]

	// Mark as locked and move to in-progress
	task.Locked = true
	id := task.Identifier()
	q.inProgress[id] = task

	// Scenario 1: Running low on tasks - pull more (if not completed)
	totalActive := len(q.pending) + len(q.inProgress)
	shouldPull := totalActive < q.batchSize && q.state == QueueStateRunning
	q.mu.Unlock()

	if shouldPull {
		go q.pullTasks()
	}

	return task
}

// Complete marks a task as successfully completed.
func (q *Queue) Complete(task *TaskBase) {
	q.mu.Lock()
	id := task.Identifier()
	delete(q.inProgress, id)
	delete(q.trackedIDs, id)

	task.Locked = false
	task.Status = "successful"

	// Check if we're waiting for round advance and all tasks are done
	if q.pendingRoundAdvance && len(q.pending) == 0 && len(q.inProgress) == 0 {
		q.pendingRoundAdvance = false // Reset flag - round will advance and reset pulling
		q.mu.Unlock()
		q.advanceToNextRound() // This will reset pulling flag
		return
	}

	// Normal round completion check (for edge cases where we didn't detect partial batch)
	if q.state == QueueStateRunning && len(q.pending) == 0 && len(q.inProgress) == 0 && !q.pendingRoundAdvance {
		q.mu.Unlock()
		q.markRoundComplete()
		return
	}
	q.mu.Unlock()
}

// Fail handles a failed task. If retry limit is not exceeded, re-queues the task.
// Otherwise, adds it to buffer with failed status.
func (q *Queue) Fail(task *TaskBase) bool {
	q.mu.Lock()
	id := task.Identifier()
	task.Attempts++

	// Remove from in-progress
	delete(q.inProgress, id)

	// Check if we should retry
	if task.Attempts < q.maxRetries {
		// Re-queue for retry
		task.Locked = false
		q.pending = append(q.pending, task)
		q.mu.Unlock()
		return true // Will retry
	}

	// Max retries exceeded
	delete(q.trackedIDs, id)
	q.mu.Unlock()

	task.Locked = false
	task.Status = "failed"

	return false // Will not retry
}

// PendingCount returns the number of tasks waiting to be leased.
func (q *Queue) PendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pending)
}

// InProgressCount returns the number of tasks currently being executed.
func (q *Queue) InProgressCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inProgress)
}

// TotalTracked returns the total number of tracked task IDs.
func (q *Queue) TotalTracked() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.trackedIDs)
}

// IsEmpty returns whether the queue has no pending or in-progress tasks.
func (q *Queue) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pending) == 0 && len(q.inProgress) == 0
}

// TrackedIDs returns a copy of all currently tracked task identifiers.
// This is used by the coordinator for SQL exclusion (WHERE path NOT IN ...).
func (q *Queue) TrackedIDs() []string {
	q.mu.RLock()
	defer q.mu.RUnlock()

	ids := make([]string, 0, len(q.trackedIDs))
	for id := range q.trackedIDs {
		ids = append(ids, id)
	}
	return ids
}

// Clear removes all tasks from the queue and resets tracking.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pending = make([]*TaskBase, 0)
	q.inProgress = make(map[string]*TaskBase)
	q.trackedIDs = make(map[string]bool)
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

	return QueueStats{
		Name:         q.name,
		Round:        q.round,
		Pending:      len(q.pending),
		InProgress:   len(q.inProgress),
		TotalTracked: len(q.trackedIDs),
		Workers:      len(q.workers),
	}
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

// pullTasks queries the database for new tasks and adds them to the pending queue.
func (q *Queue) pullTasks() {
	if q.db == nil {
		return
	}

	// Check if already pulling, waiting for round advance, or completed (prevent concurrent pulls)
	q.mu.Lock()
	if q.pulling || q.pendingRoundAdvance || q.state == QueueStateCompleted {
		q.mu.Unlock()
		return
	}
	q.pulling = true
	requestedBatchSize := q.batchSize // Capture immutable batch size for comparison
	q.mu.Unlock()

	// Ensure we clear the pulling flag when done (unless pendingRoundAdvance is set)
	defer func() {
		q.mu.Lock()
		if !q.pendingRoundAdvance {
			q.pulling = false
		}
		// If pendingRoundAdvance is true, pulling stays true to block more pulls
		q.mu.Unlock()
	}()

	// Pull tasks based on queue type
	if q.name == "src" {
		q.pullSrcTasks(requestedBatchSize)
	} else {
		q.pullDstTasks(requestedBatchSize)
	}
}

// markRoundComplete marks the current round as complete and automatically advances to next round.
func (q *Queue) markRoundComplete() {
	q.mu.Lock()
	currentRound := q.round
	q.mu.Unlock()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Round %d complete", currentRound), "queue", q.name)
	}

	// Auto-advance to next round
	q.advanceToNextRound()
}

// advanceToNextRound advances the queue to the next round and checks for completion.
// For dst queues, this will wait for src queue to be at least one round ahead.
func (q *Queue) advanceToNextRound() {
	// If this is a dst queue, wait for src to be ahead
	if q.name == "dst" && q.srcQueue != nil {
		q.waitForSrcRound()
	}

	q.mu.Lock()
	q.round++
	newRound := q.round
	// Reset pulling flag so we can pull tasks for the new round
	q.pulling = false
	q.mu.Unlock()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.name)
	}

	// Scenario 3: Round advanced - pull tasks for new round
	// This will call checkForCompletion if no tasks are found
	q.pullTasks()
}

// waitForSrcRound blocks until src queue is at least one round ahead.
// This ensures dst always stays behind src in the BFS traversal.
func (q *Queue) waitForSrcRound() {
	if q.srcQueue == nil {
		return
	}

	q.mu.RLock()
	myRound := q.round
	q.mu.RUnlock()

	targetRound := myRound // dst round N needs src to complete round N (be on N+1)

	for {
		srcRound := q.srcQueue.Round()

		// Src needs to be at least one round ahead (src finished targetRound, now on targetRound+1)
		if srcRound > targetRound {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug",
					fmt.Sprintf("Src round %d ready, advancing dst to round %d", srcRound, targetRound+1),
					"queue",
					q.name)
			}
			return
		}

		// Check if src is completed - if so, dst can proceed
		if q.srcQueue.IsExhausted() {
			return
		}

		// Src not ready yet, sleep and check again
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Waiting for src (currently round %d) before advancing dst to round %d", srcRound, targetRound+1),
				"queue",
				q.name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// pullSrcTasks queries src_nodes for pending folders at current round.
func (q *Queue) pullSrcTasks(requestedBatchSize int) {
	trackedIDs := q.TrackedIDs()
	round := q.Round()

	exclusionClause := ""
	if len(trackedIDs) > 0 {
		exclusionClause = " AND id NOT IN ("
		for i, id := range trackedIDs {
			if i > 0 {
				exclusionClause += ", "
			}
			exclusionClause += fmt.Sprintf("'%s'", id)
		}
		exclusionClause += ")"
	}

	query := fmt.Sprintf(`
		SELECT id, parent_id, name, path, type, depth_level, last_updated
		FROM %s
		WHERE traversal_status = 'Pending'
		  AND depth_level = %d
		  AND type = 'folder'
		  %s
		LIMIT %d
	`, q.tableName, round, exclusionClause, requestedBatchSize)

	rows, err := q.db.Query(query)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to pull src tasks: %v", err), "queue", q.name)
		}
		return
	}
	defer rows.Close()

	var tasks []*TaskBase
	for rows.Next() {
		var id, parentId, name, path, nodeType, lastUpdated string
		var depthLevel int

		err := rows.Scan(&id, &parentId, &name, &path, &nodeType, &depthLevel, &lastUpdated)
		if err != nil {
			continue
		}

		tasks = append(tasks, &TaskBase{
			Type: TaskTypeSrcTraversal,
			Folder: fsservices.Folder{
				Id:           id,
				ParentId:     parentId,
				DisplayName:  name,
				LocationPath: path,
				LastUpdated:  lastUpdated,
				DepthLevel:   depthLevel,
				Type:         nodeType,
			},
		})
	}

	pulledCount := len(tasks)

	// Update state based on results
	if pulledCount > 0 {
		added := q.AddBatch(tasks)
		if logservice.LS != nil && added > 0 {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Pulled %d src tasks for round %d", added, round), "queue", q.name)
		}

		// Check if we got a partial batch (fewer than requested)
		if pulledCount < requestedBatchSize {
			q.mu.Lock()
			q.pendingRoundAdvance = true
			// pulling flag stays true (set in defer of pullTasks) until round advances
			q.mu.Unlock()
			if logservice.LS != nil {
				_ = logservice.LS.Log("info",
					fmt.Sprintf("Partial batch detected (%d < %d) - will advance round after current tasks complete", pulledCount, requestedBatchSize),
					"queue",
					q.name)
			}
		}
	} else {
		// No tasks found - check for completion
		q.checkForCompletion(round)
	}
}

// pullDstTasks queries dst_nodes and populates expected children from src_nodes.
func (q *Queue) pullDstTasks(requestedBatchSize int) {
	if q.srcCtx == nil {
		return // Need src context to query expected children
	}

	trackedIDs := q.TrackedIDs()
	round := q.Round()

	exclusionClause := ""
	if len(trackedIDs) > 0 {
		exclusionClause = " AND id NOT IN ("
		for i, id := range trackedIDs {
			if i > 0 {
				exclusionClause += ", "
			}
			exclusionClause += fmt.Sprintf("'%s'", id)
		}
		exclusionClause += ")"
	}

	// Query 1: Get dst parent nodes
	query1 := fmt.Sprintf(`
		SELECT id, parent_id, name, path, type, depth_level, last_updated
		FROM %s
		WHERE traversal_status = 'Pending'
		  AND depth_level = %d
		  AND type = 'folder'
		  %s
		LIMIT %d
	`, q.tableName, round, exclusionClause, requestedBatchSize)

	rows, err := q.db.Query(query1)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to pull dst tasks: %v", err), "queue", q.name)
		}
		return
	}

	type dstNode struct {
		id          string
		parentId    string
		name        string
		path        string
		nodeType    string
		depthLevel  int
		lastUpdated string
	}

	var dstNodes []dstNode
	dstPaths := make([]string, 0)

	for rows.Next() {
		var node dstNode
		err := rows.Scan(&node.id, &node.parentId, &node.name, &node.path, &node.nodeType, &node.depthLevel, &node.lastUpdated)
		if err != nil {
			continue
		}
		dstNodes = append(dstNodes, node)
		dstPaths = append(dstPaths, node.path)
	}
	rows.Close()

	if len(dstNodes) == 0 {
		return
	}

	// Query 2: Get corresponding src parent nodes by path
	pathsClause := ""
	for i, path := range dstPaths {
		if i > 0 {
			pathsClause += ", "
		}
		pathsClause += fmt.Sprintf("'%s'", path)
	}

	query2 := fmt.Sprintf(`
		SELECT id, path
		FROM src_nodes
		WHERE path IN (%s)
	`, pathsClause)

	rows, err = q.db.Query(query2)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to query src parents: %v", err), "queue", q.name)
		}
		return
	}

	srcParentIDs := make([]string, 0)
	pathToSrcID := make(map[string]string)

	for rows.Next() {
		var srcID, srcPath string
		err := rows.Scan(&srcID, &srcPath)
		if err != nil {
			continue
		}
		srcParentIDs = append(srcParentIDs, srcID)
		pathToSrcID[srcPath] = srcID
	}
	rows.Close()

	if len(srcParentIDs) == 0 {
		return
	}

	// Query 3: Get all src children for these parents
	parentIDsClause := ""
	for i, id := range srcParentIDs {
		if i > 0 {
			parentIDsClause += ", "
		}
		parentIDsClause += fmt.Sprintf("'%s'", id)
	}

	query3 := fmt.Sprintf(`
		SELECT id, parent_id, name, path, type, depth_level, size, last_updated
		FROM src_nodes
		WHERE parent_id IN (%s)
	`, parentIDsClause)

	rows, err = q.db.Query(query3)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to query src children: %v", err), "queue", q.name)
		}
		return
	}

	// Group children by parent_id
	childrenByParent := make(map[string][]any)

	for rows.Next() {
		var id, parentId, name, path, nodeType, lastUpdated string
		var depthLevel int
		var size *int64

		err := rows.Scan(&id, &parentId, &name, &path, &nodeType, &depthLevel, &size, &lastUpdated)
		if err != nil {
			continue
		}

		if nodeType == fsservices.NodeTypeFolder {
			folder := fsservices.Folder{
				Id:           id,
				ParentId:     parentId,
				DisplayName:  name,
				LocationPath: path,
				LastUpdated:  lastUpdated,
				DepthLevel:   depthLevel,
				Type:         nodeType,
			}
			childrenByParent[parentId] = append(childrenByParent[parentId], folder)
		} else {
			file := fsservices.File{
				Id:           id,
				ParentId:     parentId,
				DisplayName:  name,
				LocationPath: path,
				LastUpdated:  lastUpdated,
				DepthLevel:   depthLevel,
				Size:         0,
				Type:         nodeType,
			}
			if size != nil {
				file.Size = *size
			}
			childrenByParent[parentId] = append(childrenByParent[parentId], file)
		}
	}
	rows.Close()

	// Build tasks with expected children
	var tasks []*TaskBase
	for _, dstNode := range dstNodes {
		srcParentID, ok := pathToSrcID[dstNode.path]
		if !ok {
			continue
		}

		children := childrenByParent[srcParentID]

		var expectedFolders []fsservices.Folder
		var expectedFiles []fsservices.File

		for _, child := range children {
			switch v := child.(type) {
			case fsservices.Folder:
				expectedFolders = append(expectedFolders, v)
			case fsservices.File:
				expectedFiles = append(expectedFiles, v)
			}
		}

		tasks = append(tasks, &TaskBase{
			Type: TaskTypeDstTraversal,
			Folder: fsservices.Folder{
				Id:           dstNode.id,
				ParentId:     dstNode.parentId,
				DisplayName:  dstNode.name,
				LocationPath: dstNode.path,
				LastUpdated:  dstNode.lastUpdated,
				DepthLevel:   dstNode.depthLevel,
				Type:         dstNode.nodeType,
			},
			ExpectedFolders: expectedFolders,
			ExpectedFiles:   expectedFiles,
		})
	}

	pulledCount := len(tasks)

	// Update state based on results
	if pulledCount > 0 {
		added := q.AddBatch(tasks)
		if logservice.LS != nil && added > 0 {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Pulled %d dst tasks for round %d", added, round), "queue", q.name)
		}

		// Check if we got a partial batch (fewer than requested)
		if pulledCount < requestedBatchSize {
			q.mu.Lock()
			q.pendingRoundAdvance = true
			// pulling flag stays true (set in defer of pullTasks) until round advances
			q.mu.Unlock()
			if logservice.LS != nil {
				_ = logservice.LS.Log("info",
					fmt.Sprintf("Partial batch detected (%d < %d) - will advance round after current tasks complete", pulledCount, requestedBatchSize),
					"queue",
					q.name)
			}
		}
	} else {
		// No tasks found - check for completion
		q.checkForCompletion(round)
	}
}

// checkForCompletion marks the queue as completed if no tasks were found after advancing rounds.
// This ensures we've done at least one round of work before declaring completion.
func (q *Queue) checkForCompletion(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Only mark as completed if we're currently running
	if q.state == QueueStateRunning {
		q.state = QueueStateCompleted
		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				fmt.Sprintf("No tasks found for round %d - traversal complete (max depth reached)", round),
				"queue",
				q.name)
		}
	}
}
