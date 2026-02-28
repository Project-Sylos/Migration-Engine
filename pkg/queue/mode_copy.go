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

// CheckCopyCompletion checks if the copy phase should switch passes or complete.
// Returns true if the queue should mark as complete, false otherwise.
// This is called when advanceToNextRound can't find a next round for the current pass.
func (q *Queue) CheckCopyCompletion(currentRound int, wasFirstPull bool) bool {
	database := q.getDatabase()
	if database == nil {
		return false
	}

	// Copy: must progress through all rounds up to maxKnownDepth in each pass
	// Only switch from pass 1 to pass 2 after reaching maxKnownDepth
	// This is similar to retry mode's pattern
	copyPass := q.GetCopyPass()
	maxKnownDepth := q.getMaxKnownDepth()

	// If we haven't reached maxKnownDepth yet, don't switch passes
	// Even if current round has no folders/files, we need to progress through all rounds
	if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
		return false // Let advanceToNextRound handle progression
	}

	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	var levels []int
	nc := q.NodeCache()
	if nc != nil {
		levels = nc.LevelDepths()
	} else {
		var err error
		levels, err = db.GetAllLevels(database, "SRC")
		if err != nil {
			return false
		}
	}

	hasAnyPendingForPass := false
	hasAnyInProgressForPass := false
	inProgressLevels := []int{}
	for _, level := range levels {
		if level == 0 {
			continue
		}
		if nc != nil {
			ls := nc.GetLevelStats(level)
			if ls != nil && ls.CopyPending > 0 {
				hasAnyPendingForPass = true
			}
		} else {
			c, err := database.GetCopyCountAtDepth(level, nodeType, db.CopyStatusPending)
			if err == nil && c > 0 {
				hasAnyPendingForPass = true
			}
			c2, err := database.GetCopyCountAtDepth(level, nodeType, db.CopyStatusInProgress)
			if err == nil && c2 > 0 {
				hasAnyInProgressForPass = true
				inProgressLevels = append(inProgressLevels, level)
			}
		}
		if hasAnyPendingForPass && (hasAnyInProgressForPass || (nc != nil && q.InProgressCount() > 0)) {
			break
		}
	}
	if nc != nil {
		hasAnyInProgressForPass = q.InProgressCount() > 0
	}
	if hasAnyInProgressForPass && logservice.LS != nil {
		_ = logservice.LS.Log("warn", fmt.Sprintf("Found in-progress tasks for pass %d (nodeType=%s) at levels: %v", copyPass, nodeType, inProgressLevels), "queue", q.name, q.name)
	}

	// If no pending tasks in DuckDB, no in-progress tasks in DuckDB, no pending in memory,
	// no in-progress in memory, and this was first pull, switch passes or complete
	// CRITICAL: Must check both DuckDB AND memory state to avoid premature completion
	// Tasks retrying are in-progress in DuckDB but pending in memory
	if !hasAnyPendingForPass && !hasAnyInProgressForPass && q.GetPendingCount() == 0 && q.InProgressCount() == 0 && wasFirstPull {
		if copyPass == 1 {
			q.SetCopyPass(2)

			minLevel := -1
			for _, level := range levels {
				if level == 0 {
					continue
				}
				if nc != nil {
					lvl := nc.GetLevel(level)
					if lvl != nil && len(lvl.ListPendingCopy("", 1, db.NodeTypeFile)) > 0 {
						if minLevel == -1 || level < minLevel {
							minLevel = level
						}
						break
					}
				} else {
					c, err := database.GetCopyCountAtDepth(level, db.NodeTypeFile, db.CopyStatusPending)
					if err == nil && c > 0 {
						if minLevel == -1 || level < minLevel {
							minLevel = level
						}
						break
					}
				}
			}

			if minLevel == -1 {
				// No file tasks found - pass 2 is also complete
				return q.markComplete("Copy phase complete - both passes finished (no files to copy)")
			}

			q.SetRound(minLevel) // Set to minimum pending level for pass 2
			q.setExpectedFromStatsBucket(minLevel)

			if logservice.LS != nil {
				err := logservice.LS.Log("info", fmt.Sprintf("Copy pass 1 (folders) complete, switching to pass 2 (files) at round %d", minLevel), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			return false // Not complete yet, just switching passes
		} else if copyPass == 2 {
			// Pass 2 (files) complete - copy phase is done
			return q.markComplete("Copy phase complete - both passes finished")
		}
	}

	// Still have tasks for current pass - rounds will advance naturally
	return false
}

// AdvanceCopyRound handles copy-specific round advancement logic.
// Checks for pending tasks matching the current pass and advances to the next applicable round.
func (q *Queue) AdvanceCopyRound() {
	database := q.getDatabase()
	if database == nil {
		return
	}

	nc := q.NodeCache()
	if nc == nil {
		return
	}

	currentRound := q.GetRound()
	copyPass := q.GetCopyPass()

	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	levels := nc.LevelDepths()

	currentRoundHasPending := false
	if currentRound > 0 {
		lvl := nc.GetLevel(currentRound)
		if lvl != nil && len(lvl.ListPendingCopy("", 1, nodeType)) > 0 {
			currentRoundHasPending = true
		}
	}

	var newRound int
	if currentRoundHasPending {
		newRound = currentRound
	} else {
		newRound = -1
		for _, level := range levels {
			if level <= currentRound || level == 0 {
				continue
			}
			lvl := nc.GetLevel(level)
			if lvl != nil && len(lvl.ListPendingCopy("", 1, nodeType)) > 0 {
				newRound = level
				break
			}
		}
	}

	// If no next round found with tasks, check if we should advance sequentially or switch passes
	if newRound == -1 {
		maxKnownDepth := q.getMaxKnownDepth()

		// If we're below maxKnownDepth, advance sequentially even if no tasks exist
		// This maintains BFS progression through all levels
		if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
			newRound = currentRound + 1
		} else {
			// At or past maxKnownDepth - check if we should switch passes or complete
			if logservice.LS != nil {
				err := logservice.LS.Log("info", fmt.Sprintf("No more rounds with pending tasks for pass %d, checking for completion", copyPass), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			// Check for final completion (will switch passes or mark complete)
			completed := q.checkCompletion(currentRound, CompletionCheckOptions{
				CheckFinalCompletion: true,
				WasFirstPull:         true,
			})
			if completed {
				// Queue is complete - state is set to QueueStateCompleted
				return
			}
			// If not completed, we switched passes - round was reset to minimum for new pass
			// Pull tasks for the new pass
			q.PullTasksIfNeeded(true)
			return
		}
	}

	// Get stats for logging
	q.SetRound(newRound)
	q.setExpectedFromStatsBucket(newRound)

	passName := "folders"
	if copyPass == 2 {
		passName = "files"
	}

	if logservice.LS != nil {
		err := logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d (pass %d: %s)", newRound, copyPass, passName), "queue", q.name, q.name)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	// Pull tasks for the new round
	q.PullTasksIfNeeded(true)
}

// PullCopyTasks pulls copy tasks from DuckDB for the current round.
// Pulls from SRC copy status buckets, filters by pass (folders vs files), and skips round 0.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullCopyTasks(force bool) {
	database := q.getDatabase()
	if database == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	if q.getPulling() {
		return
	}

	// Don't pull if queue is completed
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

	snapshot := q.getStateSnapshot()
	if snapshot.State == QueueStatePaused {
		return
	}
	if !force && snapshot.PendingCount > snapshot.PullLowWM {
		return
	}

	currentRound := snapshot.Round
	if currentRound == 0 {
		return
	}

	copyPass := q.GetCopyPass()
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	batchSize := effectiveLeaseBatchSize()
	var matchedBatch []db.FetchResult
	var hitEndOfBucket bool

	nc := q.NodeCache()
	if nc == nil {
		return
	}
	level := nc.GetLevel(currentRound)
	if level == nil {
		return
	}
	pendingNodes := level.ListPendingCopy(q.getCopyKeysetCursor(), batchSize, nodeType)
	for _, n := range pendingNodes {
		if n == nil || q.isLeased(n.ID) {
			continue
		}
		matchedBatch = append(matchedBatch, db.FetchResult{Key: n.ID, State: n})
		if len(matchedBatch) >= batchSize {
			break
		}
	}
	if len(matchedBatch) > 0 {
		q.setCopyKeysetCursor(matchedBatch[len(matchedBatch)-1].Key)
		for _, r := range matchedBatch {
			level.UpdateStatus(r.State.ID, "", db.CopyStatusInProgress)
			nc.RecordCopyTransition(currentRound, db.CopyStatusPending, db.CopyStatusInProgress)
		}
	}
	hitEndOfBucket = len(pendingNodes) < batchSize

	// Batch resolve parent SRC ID -> DST ID -> DST node (ServiceID). No per-item DB reads.
	parentIDSet := make(map[string]struct{})
	for _, item := range matchedBatch {
		if item.State.ParentID != "" {
			parentIDSet[item.State.ParentID] = struct{}{}
		}
	}
	parentIDs := make([]string, 0, len(parentIDSet))
	for pid := range parentIDSet {
		parentIDs = append(parentIDs, pid)
	}
	dstIDBySrcID, err := db.BatchGetDstIDsFromSrcIDs(database, parentIDs)
	if err != nil {
		if logservice.LS != nil {
			err := logservice.LS.Log("error", fmt.Sprintf("Batch parent lookup failed: %v", err), "queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return
	}
	dstParentIDs := make([]string, 0, len(dstIDBySrcID))
	for _, dstID := range dstIDBySrcID {
		dstParentIDs = append(dstParentIDs, dstID)
	}
	dstNodesByID, err := db.BatchGetNodesByID(database, "DST", dstParentIDs)
	if err != nil {
		if logservice.LS != nil {
			err := logservice.LS.Log("error", fmt.Sprintf("Batch DST node lookup failed: %v", err), "queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return
	}

	// Move tasks to in-progress status and create tasks
	enqueueSuccessCount := 0
	for _, item := range matchedBatch {

		// Determine task type based on copy pass (not just node type, to ensure consistency)
		// We're pulling from a bucket filtered by nodeType, so this should match item.State.Type
		taskType := TaskTypeCopyFile
		if copyPass == 1 {
			taskType = TaskTypeCopyFolder
		}

		// Verify node type matches what we're pulling (sanity check)
		expectedType := types.NodeTypeFile
		if copyPass == 1 {
			expectedType = types.NodeTypeFolder
		}
		if item.State.Type != expectedType {
			if logservice.LS != nil {
				err := logservice.LS.Log("error", fmt.Sprintf("Type mismatch for %s - pass=%d expects %s but node has %s - skipping", item.State.Path, copyPass, expectedType, item.State.Type), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue // Skip mismatched items
		}

		task := nodeStateToCopyTask(item.State, taskType, copyPass)
		// Ensure task has the ULID from the database
		if task != nil && task.ID == "" {
			task.ID = item.State.ID
		}

		// Resolve destination parent ServiceID from batch lookups
		if item.State.ParentID == "" {
			if logservice.LS != nil {
				err := logservice.LS.Log("error", fmt.Sprintf("Item at round %d has empty ParentID (path=%s) - this should not happen", item.State.Depth, item.State.Path), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue
		}
		dstParentULID := dstIDBySrcID[item.State.ParentID]
		if dstParentULID == "" {
			if logservice.LS != nil {
				err := logservice.LS.Log("warn", fmt.Sprintf("No join-lookup for parent %s of %s", item.State.ParentID, item.State.Path), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue
		}
		dstParentNode := dstNodesByID[dstParentULID]
		if dstParentNode == nil {
			if logservice.LS != nil {
				err := logservice.LS.Log("error", fmt.Sprintf("DST node not found for ULID %s (parent of %s)", dstParentULID, item.State.Path), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue
		}
		task.DstParentID = dstParentNode.ServiceID

		if q.Add(task) {
			q.addLeasedKey(item.Key)
			enqueueSuccessCount++
		}
	}

	// Track if this pull was partial
	// wasPartial = true when we hit the end of the bucket (couldn't collect enough matching items)
	// This accurately signals when we've exhausted the current round for this pass
	wasPartial := hitEndOfBucket
	q.setLastPullWasPartial(wasPartial)

	// Record pull in RoundInfo; after first pull we're no longer "first pull for round"
	q.recordPull(currentRound, len(matchedBatch), wasPartial)
	q.setFirstPullForRound(false)
}

// nodeStateToCopyTask converts a NodeState to a copy TaskBase.
func nodeStateToCopyTask(state *db.NodeState, taskType string, copyPass int) *TaskBase {
	if state == nil {
		return nil
	}

	task := &TaskBase{
		ID:          state.ID,
		Type:        taskType,
		Round:       state.Depth,
		CopyPass:    copyPass,
		Attempts:    0,
		Status:      "",
		Locked:      false,
		LeaseTime:   time.Now(),
		DstParentID: "",
	}

	// Populate folder or file based on node type
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

// CompleteCopyTask handles successful completion of copy tasks.
// Updates copy status, creates DST node entry, and updates join-lookup mapping.
func (q *Queue) CompleteCopyTask(task *TaskBase, executionDelta time.Duration) {
	q.recordExecutionTime(executionDelta)

	currentRound := task.Round
	nodeID := task.ID

	database := q.getDatabase()
	if database == nil {
		return
	}

	task.Locked = false
	task.Status = "successful"

	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()
	q.recordTaskCompletion(currentRound, true)

	taskType := types.NodeTypeFile
	taskPath := task.LocationPath()
	taskName := task.File.DisplayName
	taskSize := task.File.Size
	taskMTime := task.File.LastUpdated
	if task.IsFolder() {
		taskType = types.NodeTypeFolder
		taskName = task.Folder.DisplayName
		taskMTime = task.Folder.LastUpdated
	}

	nc := q.NodeCache()
	if nc != nil {
		nc.EnsureLevel(currentRound).UpdateStatus(nodeID, "", db.CopyStatusSuccessful)
		nc.RecordCopyTransition(currentRound, db.CopyStatusInProgress, db.CopyStatusSuccessful)
		dstNodeID := db.DeterministicNodeID("DST", taskType, taskPath)
		var dstServiceID string
		if task.IsFolder() {
			dstServiceID = task.Folder.ServiceID
		} else {
			dstServiceID = task.File.ServiceID
		}
		dstNode := &db.NodeState{
			ID:              dstNodeID,
			ServiceID:       dstServiceID,
			ParentID:        "",
			ParentServiceID: task.DstParentID,
			Name:            taskName,
			Path:            taskPath,
			Type:            taskType,
			Size:            taskSize,
			MTime:           taskMTime,
			Depth:           currentRound,
			TraversalStatus: db.StatusSuccessful,
			Status:          db.StatusSuccessful,
		}
		other := q.OtherNodeCache()
		if other != nil {
			other.EnsureLevel(currentRound).Put(dstNodeID, dstNode)
		}
	}

	q.mu.Lock()
	if task.IsFolder() {
		q.foldersCreatedTotal++
	} else if task.IsFile() {
		q.filesCreatedTotal++
		q.bytesTransferredTotal += task.BytesTransferred
	}
	q.mu.Unlock()

	q.removeInProgress(nodeID)
}

// FailCopyTask handles failure of copy tasks.
// Updates copy status to failed if max retries exceeded, or back to pending if retrying.
func (q *Queue) FailCopyTask(task *TaskBase, executionDelta time.Duration) {
	database := q.getDatabase()
	if database == nil {
		return
	}

	// Record execution time delta (even for failures)
	q.recordExecutionTime(executionDelta)

	currentRound := task.Round
	nodeID := task.ID
	maxRetries := q.getMaxRetries()

	if logservice.LS != nil {
		err := logservice.LS.Log("debug",
			fmt.Sprintf("Failing copy task: id=%s path=%s round=%d pass=%d attempts=%d maxRetries=%d",
				nodeID, task.LocationPath(), currentRound, task.CopyPass, task.Attempts, maxRetries),
			"queue", q.name, q.name)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	task.Attempts++

	// Use only task fields for status update (no DB read)
	// Check if we should retry
	if task.Attempts < maxRetries {
		// Retry: re-enqueue to memory, DON'T write to DuckDB (stays as in-progress)
		// This avoids unnecessary DuckDB writes and keeps the queue fast
		task.Locked = false
		q.removeInProgress(nodeID)

		// Remove from leased set so it can be pulled again
		q.removeLeasedKey(nodeID)

		// Re-enqueue to memory pending buffer (append, not insert at 0)
		// The task stays as in-progress in DuckDB to avoid unnecessary writes
		// It will be pulled from memory on the next worker cycle
		q.Add(task)

		if logservice.LS != nil {
			err := logservice.LS.Log("debug",
				fmt.Sprintf("Copy task will retry: path=%s attempts=%d", task.LocationPath(), task.Attempts),
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
	} else {
		task.Locked = false
		task.Status = "failed"

		q.incrementRoundStatsFailed(currentRound)
		q.incrementTasksCompletedTotal()
		q.recordTaskCompletion(currentRound, false)

		if nc := q.NodeCache(); nc != nil {
			nc.EnsureLevel(currentRound).UpdateStatus(nodeID, "", db.CopyStatusFailed)
			nc.RecordCopyTransition(currentRound, db.CopyStatusInProgress, db.CopyStatusFailed)
		}

		q.removeInProgress(nodeID)

		if logservice.LS != nil {
			err := logservice.LS.Log("error",
				fmt.Sprintf("Copy task failed (max retries): path=%s", task.LocationPath()),
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
	}
}
