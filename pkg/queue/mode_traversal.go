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

// PullTraversalTasks refills the queue from DuckDB for the current round (ID-offset pagination, ~10K batch).
// SRC: ListNodesByDepthKeyset; DST: ListDstBatchWithSrcChildren (join for expected children). Pushed directly to queue.
func (q *Queue) PullTraversalTasks(force bool) {
	database := q.getDatabase()
	if database == nil {
		return
	}
	if q.getPulling() {
		return
	}
	if q.State() == QueueStateCompleted {
		return
	}
	// Allow first refill attempt without prior cache hydration (DB-backed frontier).
	if !q.getTraversalCacheLoaded() {
		q.SetTraversalCacheLoaded(true)
	}
	q.setPulling(true)
	defer q.setPulling(false)

	taskType := TaskTypeSrcTraversal
	if q.name == "dst" {
		taskType = TaskTypeDstTraversal
	}
	snapshot := q.getStateSnapshot()
	if !force {
		if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return
		}
	} else {
		if snapshot.State == QueueStatePaused {
			return
		}
	}
	currentRound := snapshot.Round
	coordinator := q.getCoordinator()
	if q.name == "dst" && coordinator != nil {
		if !coordinator.CanDstStartRound(currentRound) {
			return
		}
	}

	batchSize := refillFromDBBatchSize
	var count int
	if q.name == "dst" {
		afterID := q.getDstKeysetCursor()
		dstBatch, childrenByDstID, err := db.ListDstBatchWithSrcChildren(database, currentRound, afterID, batchSize, db.StatusPending)
		if err != nil {
			q.setLastPullWasPartial(true)
			q.recordPull(currentRound, 0, true)
			q.setFirstPullForRound(false)
			return
		}
		expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta := BuildExpectedMapsFromDstWithChildren(dstBatch, childrenByDstID)
		for _, fr := range dstBatch {
			task := nodeStateToTask(fr.State, taskType)
			if task != nil {
				if task.ID == "" {
					task.ID = fr.Key
				}
				if task.IsFolder() {
					task.ExpectedFolders = expectedFoldersMap[fr.Key]
					task.ExpectedFiles = expectedFilesMap[fr.Key]
					task.ExpectedSrcIDMap = srcIDMap[fr.Key]
					task.ExpectedSrcNodeMeta = srcIDToMeta
				}
				_ = q.Add(task)
				count++
			}
		}
		if len(dstBatch) > 0 {
			q.setDstKeysetCursor(dstBatch[len(dstBatch)-1].Key)
		}
	} else {
		afterID := q.getSrcKeysetCursor()
		queueType := getQueueType(q.name)
		results, err := db.ListNodesByDepthKeyset(database, queueType, currentRound, afterID, db.StatusPending, batchSize)
		if err != nil {
			q.setLastPullWasPartial(true)
			q.recordPull(currentRound, 0, true)
			q.setFirstPullForRound(false)
			return
		}
		for _, fr := range results {
			task := nodeStateToTask(fr.State, taskType)
			if task != nil {
				if task.ID == "" {
					task.ID = fr.Key
				}
				_ = q.Add(task)
				count++
			}
		}
		if len(results) > 0 {
			q.setSrcKeysetCursor(results[len(results)-1].Key)
		}
	}
	wasPartial := count < batchSize
	q.setLastPullWasPartial(wasPartial)
	q.recordPull(currentRound, count, wasPartial)
	q.setFirstPullForRound(false)
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

	// Push discovered children and completed-node status to appender buffer (async flush until round advance).
	if len(childNodesToInsert) > 0 {
		database.AppendDiscoveredNodes(childNodesToInsert)
	}
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:               nodeID,
		TraversalStatus:  db.StatusSuccessful,
		CopyStatus:       state.CopyStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            currentRound,
	})

	// For SRC FOLDER tasks in retry mode: Re-queue DST task via status event and schedule DST child deletions.
	if q.name == "src" && q.GetMode() == QueueModeRetry && task.IsFolder() && task.RetryDstCleanup != nil {
		c := task.RetryDstCleanup
		database.AppendStatusEvent("DST", db.StatusEvent{
			ID:              c.DstID,
			TraversalStatus: db.StatusPending,
			EventTime:       time.Now().UnixNano(),
			Depth:           task.Round,
		})
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
	q.removeLeasedKey(nodeID)
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
		q.removeInProgress(nodeID)
		q.Add(task)
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

	// Persist failed status to appender buffer
	database := q.getDatabase()
	if database != nil && nodeID != "" {
		database.AppendStatusEvent(getQueueType(q.name), db.StatusEvent{
			ID:              nodeID,
			TraversalStatus: db.StatusFailed,
			EventTime:       time.Now().UnixNano(),
			Depth:           currentRound,
		})
	}
	// Remove from in-progress LAST
	q.removeInProgress(nodeID)
	q.removeLeasedKey(nodeID)
}

// CheckTraversalCompletion checks if traversal/retry phase should complete.
// DB-backed: no pending at depth (from GetPendingTraversalCountAtDepthFromLive), attempted pull, first pull returned 0.
func (q *Queue) CheckTraversalCompletion(currentRound int) bool {
	if q.State() == QueueStateWaiting {
		return false
	}
	if coordinator := q.getCoordinator(); coordinator != nil {
		if !coordinator.CanDstStartRound(currentRound) && q.name == "dst" {
			return false
		}
	}
	if !q.getTraversalCacheLoaded() {
		return false
	}

	info := q.getRoundInfoReadOnly(currentRound)
	pullCount := 0
	pulledAmount := 0
	if info != nil {
		pullCount = info.PullCount
		pulledAmount = info.ItemsYielded
	}
	attemptedPull := pullCount > 0
	wasFirstPull := pullCount == 1
	if !attemptedPull {
		q.PullTasksIfNeeded(true)
		info = q.getRoundInfoReadOnly(currentRound)
		pullCount = 0
		pulledAmount = 0
		if info != nil {
			pullCount = info.PullCount
			pulledAmount = info.ItemsYielded
		}
		attemptedPull = pullCount > 0
		wasFirstPull = pullCount == 1
		if !attemptedPull {
			return false
		}
	}

	mode := q.GetMode()

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
