// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PullTraversalTasks pulls traversal tasks from BoltDB for the current round.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullTraversalTasks(force bool) {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	if q.getPulling() {
		return
	}

	// Don't pull if queue is completed (prevents deadlock on coordinator gate)
	if q.getState() == QueueStateCompleted {
		return
	}

	// Set pulling flag early and defer clearing it
	// This ensures only one thread can execute the pull logic at a time
	q.setPulling(true)
	outputBuffer := q.getOutputBuffer()

	// Always clear pulling flag when done
	defer func() {
		q.setPulling(false)
	}()

	// Force-flush buffer before pulling tasks to ensure we don't pull tasks
	// that are waiting in the buffer to be written
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

	queueType := getQueueType(q.name)
	taskType := TaskTypeSrcTraversal
	if q.name == "dst" {
		taskType = TaskTypeDstTraversal
	}

	// Get state snapshot
	snapshot := q.getStateSnapshot()

	if !force {
		// Only pull if queue is running (not paused or completed)
		if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return
		}
	} else {
		// Even when forcing, don't pull if paused
		if snapshot.State == QueueStatePaused {
			return
		}
	}

	currentRound := snapshot.Round
	coordinator := q.getCoordinator()

	// For DST: Check coordinator gate before pulling
	if q.name == "dst" && coordinator != nil {
		canStartRound := coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - wait for coordinator gate
			return
		}
	}

	batchSize := effectiveLeaseBatchSize()
	batch, err := db.BatchFetchWithKeys(boltDB, queueType, currentRound, db.StatusPending, batchSize)

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch batch from BoltDB: %v", err), "queue", q.name, q.name)
		}
		// On error, don't change lastPullWasPartial - keep existing state
		return
	}

	// Batch-load expected children for DST tasks
	var expectedFoldersMap map[string][]types.Folder
	var expectedFilesMap map[string][]types.File
	var srcIDMap map[string]map[string]string // DST ULID -> (Type+Name -> SRC node ID)
	var srcIDToMeta map[string]SrcNodeMeta   // SRC ID -> Depth/CopyStatus (for copy-status updates at completion)
	if q.name == "dst" {
		// First pass: collect all valid (not already leased) folder tasks and their DST parent ULIDs
		var dstParentIDs []string
		dstIDToPath := make(map[string]string) // DST ULID -> path (for mapping results back)
		for _, item := range batch {
			// Skip ULIDs we've already leased (prevents duplicate pulls from stale views)
			if q.isLeased(item.Key) {
				continue
			}

			task := nodeStateToTask(item.State, taskType)
			if task.IsFolder() {
				dstParentIDs = append(dstParentIDs, item.State.ID) // DST parent ULID
				dstIDToPath[item.State.ID] = task.Folder.LocationPath
			}
		}

		// Batch-load expected children for all folder tasks in one DB operation
		// Uses lookup table to get SrcID from DST parent IDs, then loads SRC children
		if len(dstParentIDs) > 0 {
			var err error
			expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta, err = BatchLoadExpectedChildrenByDSTIDs(boltDB, dstParentIDs, dstIDToPath)
			if err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to batch load expected children: %v", err), "queue", q.name, q.name)
				}
				// Continue anyway - tasks will have empty expected children
				expectedFoldersMap = make(map[string][]types.Folder)
				expectedFilesMap = make(map[string][]types.File)
				srcIDMap = make(map[string]map[string]string)
				srcIDToMeta = make(map[string]SrcNodeMeta)
			}
		} else {
			srcIDMap = make(map[string]map[string]string)
			srcIDToMeta = make(map[string]SrcNodeMeta)
		}
	}

	for _, item := range batch {
		// Skip ULIDs we've already leased (prevents duplicate pulls from stale views)
		if q.isLeased(item.Key) {
			continue
		}

		task := nodeStateToTask(item.State, taskType)
		// Ensure task has the ULID from the database
		if task != nil && task.ID == "" {
			task.ID = item.State.ID
		}

		// For DST folder tasks, populate ExpectedFolders/ExpectedFiles, SRC ID map, and SRC node meta from batch-loaded results
		if q.name == "dst" && task.IsFolder() {
			// Use DST ULID to look up expected children
			dstID := item.State.ID
			expectedFolders := expectedFoldersMap[dstID]
			expectedFiles := expectedFilesMap[dstID]
			task.ExpectedFolders = expectedFolders
			task.ExpectedFiles = expectedFiles
			if srcIDMap != nil {
				task.ExpectedSrcIDMap = srcIDMap[dstID]
			}
			// Subset of srcIDToMeta for this task's expected SRC children (avoids per-child GetNodeState at completion)
			if srcIDToMeta != nil && task.ExpectedSrcIDMap != nil {
				task.ExpectedSrcNodeMeta = make(map[string]SrcNodeMeta)
				for _, srcID := range task.ExpectedSrcIDMap {
					if meta, ok := srcIDToMeta[srcID]; ok {
						task.ExpectedSrcNodeMeta[srcID] = meta
					}
				}
			}
		}

		// Enqueue task - only mark as leased if enqueue succeeds
		if q.enqueuePending(task) {
			q.addLeasedKey(item.Key)
		}
	}

	// Track if this pull was partial (fewer tasks than requested)
	// This signals we might have exhausted the current round
	wasPartial := len(batch) < batchSize
	q.setLastPullWasPartial(wasPartial)

	// Record pull in RoundInfo
	q.recordPull(currentRound, len(batch), wasPartial)

}

// CompleteTraversalTask handles successful completion of traversal/retry tasks.
// This includes child discovery, status updates, and buffer operations.
func (q *Queue) CompleteTraversalTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round

	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	queueType := getQueueType(q.name)

	// Convert task to NodeState for BoltDB
	state := taskToNodeState(task, queueType)
	if state == nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Complete() called with task that couldn't be converted to NodeState: %v", task),
				"queue", q.name, q.name)
		}
		return
	}

	nodeID := state.ID
	nextRound := currentRound + 1

	task.Locked = false
	task.Status = "successful"

	// Increment completed count (even if failed, this is a "processed" counter)
	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()

	// Record task completion in RoundInfo
	q.recordTaskCompletion(currentRound, true)

	// Prepare child nodes for insertion
	parentPath := types.NormalizeLocationPath(task.LocationPath())
	var childNodesToInsert []db.InsertOperation

	// Log folder discovery with details and update discovery counters
	totalChildren := len(task.DiscoveredChildren)
	foldersCount := 0
	filesCount := 0
	if totalChildren > 0 {
		for _, child := range task.DiscoveredChildren {
			if child.IsFile {
				filesCount++
			} else {
				foldersCount++
			}
		}
		// Increment discovery counters (thread-safe)
		q.mu.Lock()
		q.filesDiscoveredTotal += int64(filesCount)
		q.foldersDiscoveredTotal += int64(foldersCount)
		q.mu.Unlock()
	}

	// Collect discovered children for insertion
	// Deterministic IDs based on (queueType, nodeType, path) ensure no duplicates -
	// the same logical node will always get the same ID, making this race-safe
	for _, child := range task.DiscoveredChildren {
		// For DST queues, skip folder children here - they'll be handled separately
		if queueType == "DST" && !child.IsFile {
			continue
		}

		// Create NodeState with deterministic ID (no need to check for existing children -
		// deterministic IDs naturally dedupe at all layers)
		childState := childResultToNodeState(child, parentPath, nextRound, queueType, nodeID)
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

		outputBuffer := q.getOutputBuffer()
		// For DST queue: queue lookup mapping if this child has a matching SRC node
		if queueType == "DST" && child.SrcID != "" {
			if outputBuffer != nil {
				// Queue bidirectional lookup mapping: SrcID <-> DST node ID (at nextRound level)
				outputBuffer.AddLookupMapping(nextRound, child.SrcID, childState.ID)
			}

			// Update SRC node's CopyStatus if worker determined an update is needed (use meta from pull, no DB lookup)
			if child.SrcCopyStatus != "" && task.ExpectedSrcNodeMeta != nil {
				if meta, ok := task.ExpectedSrcNodeMeta[child.SrcID]; ok && outputBuffer != nil {
					oldCopyStatus := meta.CopyStatus
					if oldCopyStatus == "" {
						oldCopyStatus = db.CopyStatusPending
					}
					nodeType := "file"
					if !child.IsFile {
						nodeType = "folder"
					}
					outputBuffer.AddCopyStatusUpdate("SRC", meta.Depth, nodeType, oldCopyStatus, child.SrcID, child.SrcCopyStatus)
				}
			}
		}
	}

	// Handle DST queue special case: create tasks for child folders
	if q.name == "dst" {
		type dstChildFolder struct {
			folder        types.Folder
			srcID         string
			status        string
			srcCopyStatus string
		}
		var childFolders []dstChildFolder
		for _, child := range task.DiscoveredChildren {
			if !child.IsFile {
				f := child.Folder
				f.DepthLevel = nextRound
				childFolders = append(childFolders, dstChildFolder{
					folder:        f,
					srcID:         child.SrcID,
					status:        child.Status,
					srcCopyStatus: child.SrcCopyStatus,
				})
			}
		}

		for _, child := range childFolders {
			// Compute root-relative path first (needed for deterministic ID generation)
			var rootRelativePath string
			if parentPath == "/" {
				// Child of root folder
				rootRelativePath = "/" + child.folder.DisplayName
			} else {
				// Child of non-root folder
				rootRelativePath = types.NormalizeLocationPath(parentPath + "/" + child.folder.DisplayName)
			}

			// Generate deterministic ID from logical identity (queueType, nodeType, path)
			// This eliminates duplicate logical nodes and makes traversal race-safe
			deterministicID := db.DeterministicNodeID(queueType, types.NodeTypeFolder, rootRelativePath)

			// Create task state for DST child folder
			taskState := &db.NodeState{
				ID:              deterministicID,
				ServiceID:       child.folder.ServiceID,
				ParentID:        nodeID,
				ParentServiceID: child.folder.ParentId,
				ParentPath:      parentPath,
				Name:            child.folder.DisplayName,
				Path:            rootRelativePath,
				Type:            types.NodeTypeFolder,
				Size:            0,
				MTime:           child.folder.LastUpdated,
				Depth:           nextRound,
				TraversalStatus: child.status,
			}
			if child.srcID != "" {
				taskState.SrcID = child.srcID
			}

			childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
				QueueType: queueType,
				Level:     nextRound,
				Status:    child.status,
				State:     taskState,
			})

			outputBuffer := q.getOutputBuffer()
			// Queue lookup mapping if this child has a matching SRC node
			if child.srcID != "" {
				if outputBuffer != nil {
					outputBuffer.AddLookupMapping(nextRound, child.srcID, taskState.ID)
				}

				// Update SRC node's CopyStatus if worker determined an update is needed (use meta from pull, no DB lookup)
				if child.srcCopyStatus != "" && task.ExpectedSrcNodeMeta != nil {
					if meta, ok := task.ExpectedSrcNodeMeta[child.srcID]; ok && outputBuffer != nil {
						oldCopyStatus := meta.CopyStatus
						if oldCopyStatus == "" {
							oldCopyStatus = db.CopyStatusPending
						}
						// childFolders only contains folder children
						outputBuffer.AddCopyStatusUpdate("SRC", meta.Depth, "folder", oldCopyStatus, child.srcID, child.srcCopyStatus)
					}
				}
			}
		}
	}

	// Write to buffer
	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
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
		if len(childNodesToInsert) > 0 {
			atomicOps = append(atomicOps, &db.BatchInsertOperation{
				Operations: childNodesToInsert,
			})
		}

		// Add all operations atomically
		outputBuffer.AddMultiple(atomicOps)

		// For SRC FOLDER tasks in retry mode: Queue DST cleanup (use preloaded RetryDstCleanup when present)
		if q.name == "src" && q.getMode() == QueueModeRetry && task.IsFolder() {
			if task.RetryDstCleanup != nil {
				c := task.RetryDstCleanup
				outputBuffer.AddStatusUpdate("DST", c.DstDepth, c.DstOldStatus, db.StatusPending, c.DstID)
				for _, ch := range c.Children {
					outputBuffer.AddNodeDeletion("DST", ch.ID, ch.Depth, ch.TraversalStatus)
				}
			} else {
				// Fallback when RetryDstCleanup was not populated (e.g. legacy path)
				// Join lookup is per level; get SRC node level then lookup
				srcState, _ := db.GetNodeState(boltDB, "SRC", nodeID)
				if srcState != nil {
					dstID, err := db.GetDstIDFromSrcID(boltDB, srcState.Depth, nodeID)
					if err == nil && dstID != "" {
					dstState, err := db.GetNodeState(boltDB, "DST", dstID)
					if err == nil && dstState != nil {
						oldStatus := dstState.TraversalStatus
						if oldStatus == "" {
							oldStatus = db.StatusSuccessful
						}
						outputBuffer.AddStatusUpdate("DST", dstState.Depth, oldStatus, db.StatusPending, dstID)
						childIDs, err := db.GetChildrenIDsByParentID(boltDB, "DST", dstState.Depth, dstID)
						if err == nil && len(childIDs) > 0 {
							for _, childID := range childIDs {
								childState, err := db.GetNodeState(boltDB, "DST", childID)
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
		}
	}

	// Remove from in-progress LAST
	q.removeInProgress(nodeID)
}

// FailTraversalTask handles failure of traversal/retry tasks.
// Retries up to maxRetries, then marks as failed.
func (q *Queue) FailTraversalTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta (even for failures)
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID
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
		// Remove from in-progress BEFORE re-enqueuing to pending
		q.removeInProgress(nodeID)
		q.enqueuePending(task) // Re-adds to tracked automatically
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Retrying task: id=%s path=%s round=%d attempt=%d/%d",
					nodeID, task.LocationPath(), currentRound, task.Attempts, maxRetries),
				"queue", q.name, q.name)
		}
		return // Will retry
	}

	// Max retries reached - task is truly done
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
	q.incrementTasksCompletedTotal()

	// Record task completion in RoundInfo (failed)
	q.recordTaskCompletion(currentRound, false)

	// Update traversal status to failed
	outputBuffer := q.getOutputBuffer()
	if nodeID != "" && outputBuffer != nil {
		queueType := getQueueType(q.name)
		outputBuffer.AddStatusUpdate(queueType, currentRound, db.StatusPending, db.StatusFailed, nodeID)
	}

	// Remove from in-progress LAST
	q.removeInProgress(nodeID)
}

// CheckTraversalCompletion checks if traversal/retry phase should complete.
// Returns true if the queue should mark as complete, false otherwise.
func (q *Queue) CheckTraversalCompletion(currentRound int, wasFirstPull bool) bool {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return false
	}

	queueType := getQueueType(q.name)
	mode := q.getMode()

	switch mode {
	case QueueModeTraversal:
		// Traversal: first pull with 0 entries → complete (exhausted all depths)
		// Check if any pending tasks exist at current round
		hasPending, err := boltDB.HasStatusBucketItems(queueType, currentRound, db.StatusPending)
		if err != nil {
			return false
		}

		// Only end if we don't have any pending items and this was our first pull of the round
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

// AdvanceTraversalRound handles traversal/retry-specific round advancement logic.
// For traversal/retry modes, simply increments the round by 1.
func (q *Queue) AdvanceTraversalRound() {
	// Flush buffer before advancing
	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

	// Ensure state is running if it was waiting
	state := q.getState()
	if state == QueueStateWaiting {
		q.setState(QueueStateRunning)
	}

	// Advance round by 1 for traversal/retry modes
	currentRound := q.getRound()
	newRound := currentRound + 1

	// Get stats for logging
	q.setRound(newRound)
	q.setExpectedFromStatsBucket(newRound)

	// Reset lastPullWasPartial since we're advancing to a new round
	q.setLastPullWasPartial(false)
	// Initialize RoundInfo for the new round (will be created on first pull)
	q.getRoundInfo(newRound) // Ensure it exists

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
	q.PullTasksIfNeeded(true)
}
