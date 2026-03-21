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

// PullTraversalTasks pulls traversal tasks from DuckDB for the current round.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullTraversalTasks(force bool) {
	database := q.getDatabase()
	if database == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	if q.getPulling() {
		return
	}

	// Don't pull if queue is completed (prevents deadlock on coordinator gate)
	if q.State() == QueueStateCompleted {
		return
	}

	// Set pulling flag early and defer clearing it
	// This ensures only one thread can execute the pull logic at a time
	q.setPulling(true)

	// Always clear pulling flag when done
	defer func() {
		q.setPulling(false)
	}()

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
			return
		}
	}

	batchSize := effectiveLeaseBatchSize()
	nc := q.NodeCache()

	// Memory-first: try pull from cache. Always record the pull when we attempt (check), even if we get 0 items.
	if nc != nil {
		level := nc.GetLevel(currentRound)
		if level != nil {
			cursor := q.getSrcKeysetCursor()
			if q.name == "dst" {
				cursor = q.getDstKeysetCursor()
			}
			list := level.ListPending(cursor, batchSize)
			if len(list) > 0 {
				lastKey := list[len(list)-1].ID
				if q.name == "dst" {
					q.setDstKeysetCursor(lastKey)
				} else {
					q.setSrcKeysetCursor(lastKey)
				}
				expectedFoldersMap := make(map[string][]types.Folder)
				expectedFilesMap := make(map[string][]types.File)
				srcIDMap := make(map[string]map[string]string)
				srcIDToMeta := make(map[string]SrcNodeMeta)
				if q.name == "dst" {
					other := q.OtherNodeCache()
					nextLevel := other.GetLevel(currentRound + 1)
					for _, state := range list {
						if state.Type != types.NodeTypeFolder {
							continue
						}
						dstID := state.ID
						var children []*db.NodeState
						if nextLevel != nil {
							children = nextLevel.ListChildrenByParentPath(state.Path)
						}
						folders, files, idMap, meta := buildExpectedMapsFromChildren(children)
						expectedFoldersMap[dstID], expectedFilesMap[dstID], srcIDMap[dstID] = folders, files, idMap
						for k, v := range meta {
							srcIDToMeta[k] = v
						}
					}
				}
				for _, state := range list {
					if q.isLeased(state.ID) {
						continue
					}
					task := nodeStateToTask(state, taskType)
					if task != nil && task.ID == "" {
						task.ID = state.ID
					}
					if q.name == "dst" && task != nil && task.IsFolder() {
						task.ExpectedFolders = expectedFoldersMap[state.ID]
						task.ExpectedFiles = expectedFilesMap[state.ID]
						task.ExpectedSrcIDMap = srcIDMap[state.ID]
						if task.ExpectedSrcIDMap != nil {
							task.ExpectedSrcNodeMeta = make(map[string]SrcNodeMeta)
							for _, srcID := range task.ExpectedSrcIDMap {
								if meta, ok := srcIDToMeta[srcID]; ok {
									task.ExpectedSrcNodeMeta[srcID] = meta
								}
							}
						}
					}
					if task != nil && q.Add(task) {
						q.addLeasedKey(state.ID)
					}
				}
			}
			// Record pull whether we got items or not; we checked and that counts as an attempt.
			wasPartial := len(list) < batchSize
			q.setLastPullWasPartial(wasPartial)
			q.recordPull(currentRound, len(list), wasPartial)
			q.setFirstPullForRound(false)
			return
		}
		// Level is nil for this round (e.g. cache not populated yet, or no nodes at this depth). We attempted but didn't read from cache.
		// Set lastPullWasPartial=false so we retry pull once cache is populated; otherwise we'd never pull again this round.
		q.setLastPullWasPartial(false)
		q.recordPull(currentRound, 0, true)
		q.setFirstPullForRound(false)
	}
}

// CompleteTraversalTask handles successful completion of traversal/retry tasks.
// This includes child discovery, status updates, and buffer operations.
func (q *Queue) CompleteTraversalTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round

	database := q.getDatabase()
	if database == nil {
		return
	}

	queueType := getQueueType(q.name)

	// Convert task to NodeState for DuckDB
	state := taskToNodeState(task)
	if state == nil {
		if logservice.LS != nil {
			err := logservice.LS.Log("error",
				fmt.Sprintf("Complete() called with task that couldn't be converted to NodeState: %v", task),
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
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

		}
	}

	nc := q.NodeCache()
	if nc != nil {
		// Memory-first: update cache only
		level := nc.EnsureLevel(currentRound)
		prevStatus := ""
		if prev := level.Get(nodeID); prev != nil {
			prevStatus = prev.TraversalStatus
			if prevStatus == "" {
				prevStatus = prev.Status
			}
		}
		level.UpdateStatus(nodeID, db.StatusSuccessful, "")
		if prevStatus == db.StatusPending {
			nc.RecordTraversalTransition(currentRound, db.StatusPending, db.StatusSuccessful)
		}
		nc.IncrementCompleted(currentRound)
		nextLevel := nc.EnsureLevel(nextRound)
		for _, op := range childNodesToInsert {
			if op.State != nil {
				nextLevel.Put(op.State.ID, op.State)
				if op.State.TraversalStatus == db.StatusPending {
					nc.IncrementPending(nextRound)
				}
			}
		}
		// DST: update SRC copy status in other cache for children that have SrcID
		if queueType == "DST" {
			other := q.OtherNodeCache()
			if other != nil {
				otherNext := other.EnsureLevel(nextRound)
				for _, child := range task.DiscoveredChildren {
					if child.SrcID != "" && child.SrcCopyStatus != "" && task.ExpectedSrcNodeMeta != nil {
						if _, ok := task.ExpectedSrcNodeMeta[child.SrcID]; ok {
							otherNext.UpdateStatus(child.SrcID, "", child.SrcCopyStatus)
						}
					}
				}
			}
		}
	}

	// For SRC FOLDER tasks in retry mode: Queue DST cleanup only when RetryDstCleanup was populated at pull (no DB reads here).
	if q.name == "src" && q.GetMode() == QueueModeRetry && task.IsFolder() && task.RetryDstCleanup != nil {
		c := task.RetryDstCleanup
		dstCache := q.OtherNodeCache()
		if dstCache != nil {
			dstCache.EnsureLevel(task.Round).Put(c.DstID, &db.NodeState{ID: c.DstID, Depth: task.Round, TraversalStatus: db.StatusPending})
			dstCache.IncrementPending(task.Round)
		}
		deletions := make([]db.NodeDeletion, 0, len(c.Children))
		for _, ch := range c.Children {
			deletions = append(deletions, db.NodeDeletion{Table: "DST", NodeID: ch.ID})
		}
		if err := database.AddNodeDeletions(deletions); err != nil {
			fmt.Println("error adding node deletions", err)
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
		err := logservice.LS.Log("debug",
			fmt.Sprintf("Failing task: id=%s path=%s round=%d type=%s currentAttempts=%d maxRetries=%d",
				nodeID, task.LocationPath(), currentRound, task.Type, task.Attempts, maxRetries),
			"queue", q.name, q.name)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	task.Attempts++

	// Check if we should retry
	if task.Attempts < maxRetries {
		task.Locked = false
		// Remove from in-progress BEFORE re-enqueuing to pending
		q.removeInProgress(nodeID)
		q.Add(task) // Re-adds to tracked automatically
		if logservice.LS != nil {
			err := logservice.LS.Log("debug",
				fmt.Sprintf("Retrying task: id=%s path=%s round=%d attempt=%d/%d",
					nodeID, task.LocationPath(), currentRound, task.Attempts, maxRetries),
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return // Will retry
	}

	// Max retries reached - task is truly done
	if logservice.LS != nil {
		err := logservice.LS.Log("error",
			fmt.Sprintf("Failed to traverse folder %s (id=%s) after %d attempts (max retries exceeded) round=%d",
				task.LocationPath(), nodeID, task.Attempts, currentRound),
			"queue", q.name, q.name)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	task.Locked = false
	task.Status = "failed"

	// Increment completed count
	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()

	// Record task completion in RoundInfo (failed)
	q.recordTaskCompletion(currentRound, false)

	// Update traversal status to failed
	if nodeID != "" {
		nc := q.NodeCache()
		if nc != nil {
			level := nc.GetLevel(currentRound)
			prevStatus := ""
			if level != nil {
				if prev := level.Get(nodeID); prev != nil {
					prevStatus = prev.TraversalStatus
					if prevStatus == "" {
						prevStatus = prev.Status
					}
				}
				level.UpdateStatus(nodeID, db.StatusFailed, "")
			}
			if prevStatus == db.StatusPending {
				nc.RecordTraversalTransition(currentRound, db.StatusPending, db.StatusFailed)
			}
			nc.IncrementCompleted(currentRound)
		}
	}

	// Remove from in-progress LAST
	q.removeInProgress(nodeID)
}

// CheckTraversalCompletion checks if traversal/retry phase should complete.
// All traversal-specific completion logic lives here: cache loaded, attempted pull, first pull returned 0, no pending in cache.
// Returns true if the queue should mark as complete, false otherwise.
func (q *Queue) CheckTraversalCompletion(currentRound int) bool {
	nc := q.NodeCache()
	if nc == nil {
		return false
	}
	if !q.getTraversalCacheLoaded() {
		return false
	}

	pullCount := q.getCurrentRoundPullCount()
	pulledAmount := q.getCurrentRoundPulledAmount()
	attemptedPull := pullCount > 0
	wasFirstPull := pullCount == 1
	if !attemptedPull {
		return false
	}

	mode := q.GetMode()
	hasPending := false
	level := nc.GetLevel(currentRound)
	if level != nil {
		hasPending = len(level.ListPending("", 1)) > 0
	}
	if hasPending {
		return false
	}

	switch mode {
	case QueueModeTraversal:
		// Only complete when the first pull of this round actually returned 0 items (we checked and found nothing).
		if !wasFirstPull || pulledAmount != 0 {
			return false
		}
		return q.markComplete("No pending tasks found for round %d - traversal complete (first pull)", currentRound)
	case QueueModeRetry:
		maxKnownDepth := q.getMaxKnownDepth()
		if maxKnownDepth >= 0 && currentRound > maxKnownDepth {
			return q.markComplete("Retry sweep complete - past maxKnownDepth (%d), no pending at round %d", maxKnownDepth, currentRound)
		}
	}
	return false
}

// AdvanceTraversalRound handles traversal/retry-specific round advancement logic.
// For traversal/retry modes, simply increments the round by 1.
func (q *Queue) AdvanceTraversalRound() {
	// Ensure state is running if it was waiting
	state := q.State()
	if state == QueueStateWaiting {
		q.SetState(QueueStateRunning)
	}

	// Advance round by 1 for traversal/retry modes
	currentRound := q.GetRound()
	newRound := currentRound + 1

	// Get stats for logging
	q.SetRound(newRound)
	q.setExpectedFromStatsBucket(newRound)

	// Reset lastPullWasPartial since we're advancing to a new round
	q.setLastPullWasPartial(false)
	// firstPullForRound is set to true in SetRound (above) so the new round gets "first pull" semantics for completion.
	// This queue's keyset cursor is reset in setRound and after this queue's seal (resetThisQueueKeysetCursor)
	// Initialize RoundInfo for the new round (will be created on first pull)
	q.getRoundInfo(newRound) // Ensure it exists

	// Update coordinator when rounds advance
	coordinator := q.getCoordinator()
	if coordinator != nil {
		switch q.name {
		case "src":
			coordinator.UpdateRound("src", newRound)
		case "dst":
			coordinator.UpdateRound("dst", newRound)
		}
	}

	if logservice.LS != nil {
		err := logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.name, q.name)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	// Pull tasks for the new round
	q.PullTasksIfNeeded(true)
}
