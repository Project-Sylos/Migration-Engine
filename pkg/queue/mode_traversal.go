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
	requestLimit := batchSize + 1
	var count int
	if q.name == "dst" {
		afterID := q.getDstKeysetCursor()
		dstBatch, childrenByDstID, err := db.ListDstBatchWithSrcChildren(database, currentRound, afterID, requestLimit, db.StatusPending)
		if err != nil {
			q.setLastPullWasPartial(true)
			q.recordPull(currentRound, 0, true)
			q.setFirstPullForRound(false)
			return
		}
		processLimit := batchSize
		if len(dstBatch) <= batchSize {
			processLimit = len(dstBatch)
		}
		expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta := BuildExpectedMapsFromDstWithChildren(dstBatch[:processLimit], childrenByDstID)
		for i := 0; i < processLimit; i++ {
			fr := dstBatch[i]
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
			cursorIdx := processLimit - 1
			if cursorIdx < 0 {
				cursorIdx = 0
			}
			q.setDstKeysetCursor(dstBatch[cursorIdx].Key)
		}
		q.setLastPullWasPartial(len(dstBatch) <= batchSize)
	} else {
		afterID := q.getSrcKeysetCursor()
		queueType := getQueueType(q.name)
		results, err := db.ListNodesByDepthKeyset(database, queueType, currentRound, afterID, db.StatusPending, requestLimit)
		if err != nil {
			q.setLastPullWasPartial(true)
			q.recordPull(currentRound, 0, true)
			q.setFirstPullForRound(false)
			return
		}
		processLimit := batchSize
		if len(results) <= batchSize {
			processLimit = len(results)
		}
		for i := 0; i < processLimit; i++ {
			fr := results[i]
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
			cursorIdx := processLimit - 1
			if cursorIdx < 0 {
				cursorIdx = 0
			}
			q.setSrcKeysetCursor(results[cursorIdx].Key)
		}
		q.setLastPullWasPartial(len(results) <= batchSize)
	}
	q.recordPull(currentRound, count, q.GetLastPullWasPartial())
	q.setFirstPullForRound(false)
}

// CompleteTraversalTask handles successful completion of traversal/retry tasks.
// This includes child discovery, status updates, and buffer operations.
func (q *Queue) CompleteTraversalTask(task *TaskBase, executionDelta time.Duration) {
	// Record execution time delta
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID

	task.Locked = false
	task.Status = "successful"

	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()
	q.recordTaskCompletion(currentRound, true)

	// Update discovery counters (thread-safe)
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
		q.mu.Lock()
		q.filesDiscoveredTotal += int64(filesCount)
		q.foldersDiscoveredTotal += int64(foldersCount)
		q.mu.Unlock()
	}

	database := q.getDatabase()
	if database == nil {
		q.removeInProgress(nodeID)
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
		q.removeInProgress(nodeID)
		return
	}

	nextRound := currentRound + 1

	// Prepare child nodes for insertion
	parentPath := types.NormalizeLocationPath(task.LocationPath())
	var childNodesToInsert []db.InsertOperation

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
	fromRetry := q.GetMode() == QueueModeRetry
	completionCopyStatus := state.CopyStatus
	if q.name == "src" {
		// Traversal completion should not overwrite copy_status; DST comparison/copy flows own that field.
		completionCopyStatus = ""
	}
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:                  nodeID,
		TraversalStatus:     db.StatusSuccessful,
		CopyStatus:          completionCopyStatus,
		EventTime:           time.Now().UnixNano(),
		Depth:               currentRound,
		PrevTraversalStatus: state.TraversalStatus,
		PrevCopyStatus:      state.CopyStatus,
	}, fromRetry)

	// DST comparison: persist SRC copy status for each matched child so path review shows correct copy_status.
	if q.name == "dst" {
		eventTime := time.Now().UnixNano()
		for _, child := range task.DiscoveredChildren {
			if child.SrcID == "" || child.SrcCopyStatus == "" {
				continue
			}
			meta := task.ExpectedSrcNodeMeta[child.SrcID]
			if child.SrcCopyStatus == meta.CopyStatus {
				continue // No SRC copy-status change, so no event is needed.
			}
			prevTrav := meta.TraversalStatus
			if prevTrav == "" {
				prevTrav = db.StatusSuccessful
			}
			database.AppendStatusEvent("SRC", db.StatusEvent{
				ID:                  child.SrcID,
				TraversalStatus:     prevTrav,
				CopyStatus:          child.SrcCopyStatus,
				EventTime:           eventTime,
				Depth:               nextRound,
				PrevTraversalStatus: prevTrav,
				PrevCopyStatus:      meta.CopyStatus, // old copy status before this comparison update
			}, false)
		}
	}

	// For SRC FOLDER tasks in retry mode: Re-queue DST task via status event and schedule DST child deletions.
	if q.name == "src" && fromRetry && task.IsFolder() && task.RetryDstCleanup != nil {
		c := task.RetryDstCleanup
		database.AppendStatusEvent("DST", db.StatusEvent{
			ID:                  c.DstID,
			TraversalStatus:     db.StatusPending,
			EventTime:           time.Now().UnixNano(),
			Depth:               task.Round,
			PrevTraversalStatus: c.DstOldStatus,
		}, false)
		deletions := make([]db.NodeDeletion, 0, len(c.Children))
		for _, ch := range c.Children {
			deletions = append(deletions, db.NodeDeletion{Table: "DST", NodeID: ch.ID})
		}
		if err := database.AddNodeDeletions(deletions); err != nil {
			fmt.Println("error adding node deletions", err)
		}
	}

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
		q.removeInProgress(nodeID)
		if !q.Add(task) {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error",
					fmt.Sprintf("retry re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
					"queue", q.name, q.name)
			}
		}
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
	task.Locked = false
	task.Status = "failed"

	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()
	q.recordTaskCompletion(currentRound, false)

	if logservice.LS != nil {
		err := logservice.LS.Log("error",
			fmt.Sprintf("Failed to traverse folder %s (id=%s) after %d attempts (max retries exceeded) round=%d",
				task.LocationPath(), nodeID, task.Attempts, currentRound),
			"queue", q.name, q.name)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	database := q.getDatabase()
	if database == nil {
		q.removeInProgress(nodeID)
		return
	}

	queueType := getQueueType(q.name)

	// Record task error if present
	if task.LastError != "" {
		database.AppendTaskError(queueType, "traversal", nodeID, task.LastError, task.Attempts, task.LocationPath())
	}

	if nodeID != "" {
		database.AppendStatusEvent(queueType, db.StatusEvent{
			ID:                  nodeID,
			TraversalStatus:     db.StatusFailed,
			EventTime:           time.Now().UnixNano(),
			Depth:               currentRound,
			PrevTraversalStatus: db.StatusPending,
		}, q.GetMode() == QueueModeRetry)
	}

	q.removeInProgress(nodeID)
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
		// Per algorithms.md: only apply "pull -> see nothing -> end" when currentRound >= maxKnownDepth.
		// Otherwise a round may have 0 retry items while deeper levels still do; return false so we advance the round.
		if !wasFirstPull || pulledAmount != 0 {
			return false
		}
		maxKnownDepth := q.getMaxKnownDepth()
		if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
			return false
		}
		if maxKnownDepth >= 0 && currentRound > maxKnownDepth {
			return q.markComplete("Retry sweep complete - past maxKnownDepth (%d), no pending at round %d", maxKnownDepth, currentRound)
		}
		return q.markComplete("Retry sweep complete - no pending at round %d", currentRound)
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
