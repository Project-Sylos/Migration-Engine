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

	// We've reached or passed maxKnownDepth for current pass
	// Now check if there are any pending or in-progress tasks for the current pass at any level
	levels, err := db.GetAllLevels(database, "SRC")
	if err != nil {
		return false
	}

	// Determine node type for current pass
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	hasAnyPendingForPass := false
	hasAnyInProgressForPass := false
	inProgressLevels := []int{}
	for _, level := range levels {
		if level == 0 {
			continue // Skip round 0
		}
		c, err := database.GetCopyCountAtDepth(level, nodeType, db.CopyStatusPending)
		if err == nil && c > 0 {
			hasAnyPendingForPass = true
		}
		c2, err := database.GetCopyCountAtDepth(level, nodeType, db.CopyStatusInProgress)
		hasInProgress := err == nil && c2 > 0
		if err == nil && hasInProgress {
			hasAnyInProgressForPass = true
			inProgressLevels = append(inProgressLevels, level)
		}
		if hasAnyPendingForPass && hasAnyInProgressForPass {
			break // Found both, no need to continue
		}
	}

	// Log in-progress tasks if found
	if hasAnyInProgressForPass {
		if logservice.LS != nil {
			_ = logservice.LS.Log("warn", fmt.Sprintf("Found in-progress tasks for pass %d (nodeType=%s) at levels: %v", copyPass, nodeType, inProgressLevels), "queue", q.name, q.name)
		}
	}

	// If no pending tasks in BoltDB, no in-progress tasks in BoltDB, no pending in memory,
	// no in-progress in memory, and this was first pull, switch passes or complete
	// CRITICAL: Must check both BoltDB AND memory state to avoid premature completion
	// Tasks retrying are in-progress in BoltDB but pending in memory
	if !hasAnyPendingForPass && !hasAnyInProgressForPass && q.GetPendingCount() == 0 && q.InProgressCount() == 0 && wasFirstPull {
		if copyPass == 1 {
			// Pass 1 (folders) complete - switch to pass 2 (files)
			q.SetCopyPass(2)

			// Find minimum level with pending file tasks for pass 2
			minLevel := -1
			for _, level := range levels {
				if level == 0 {
					continue // Skip round 0
				}
				c, err := database.GetCopyCountAtDepth(level, db.NodeTypeFile, db.CopyStatusPending)
				if err == nil && c > 0 {
					if minLevel == -1 || level < minLevel {
						minLevel = level
					}
					break
				}
			}

			if minLevel == -1 {
				// No file tasks found - pass 2 is also complete
				return q.markComplete("Copy phase complete - both passes finished (no files to copy)")
			}

			q.SetRound(minLevel) // Set to minimum pending level for pass 2
			q.setExpectedFromStatsBucket(minLevel)

			if logservice.LS != nil {
				_ = logservice.LS.Log("info", fmt.Sprintf("Copy pass 1 (folders) complete, switching to pass 2 (files) at round %d", minLevel), "queue", q.name, q.name)
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

	// Flush buffer before advancing to ensure all pending writes (like DST node creation)
	// are persisted before the next round's tasks try to read them
	database.FlushTablesForQueue(getQueueType(q.name))

	currentRound := q.GetRound()
	copyPass := q.GetCopyPass()

	// Get all levels to check
	levels, err := db.GetAllLevels(database, "SRC")
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Error getting levels: %v", err), "queue", q.name, q.name)
		}
		return
	}

	// Determine node type for current pass
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	// Check if current round still has pending tasks matching the current pass
	currentRoundHasPending := false
	if currentRound > 0 {
		c, err := database.GetCopyCountAtDepth(currentRound, nodeType, db.CopyStatusPending)
		if err == nil && c > 0 {
			currentRoundHasPending = true
		}
	}

	var newRound int

	// If current round still has pending tasks, stay on it
	if currentRoundHasPending {
		newRound = currentRound
	} else {
		// Find the next round (after currentRound) that has pending tasks matching the current pass
		newRound = -1
		for _, level := range levels {
			if level <= currentRound || level == 0 {
				continue // Skip current round, previous rounds, and round 0
			}
			c, err := database.GetCopyCountAtDepth(level, nodeType, db.CopyStatusPending)
			if err == nil && c > 0 {
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
				_ = logservice.LS.Log("info", fmt.Sprintf("No more rounds with pending tasks for pass %d, checking for completion", copyPass), "queue", q.name, q.name)
			}
			// Check for final completion (will switch passes or mark complete)
			completed := q.checkCompletion(currentRound, CompletionCheckOptions{
				CheckFinalCompletion: true,
				WasFirstPull:         true,
				FlushBuffer:          true,
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
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d (pass %d: %s)", newRound, copyPass, passName), "queue", q.name, q.name)
	}

	// Pull tasks for the new round
	q.PullTasksIfNeeded(true)
}

// PullCopyTasks pulls copy tasks from BoltDB for the current round.
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

	// Force-flush buffer before pulling tasks to ensure we don't pull tasks
	// that are waiting in the buffer to be written
	database.FlushTablesForQueue(getQueueType(q.name))

	// Get state snapshot
	snapshot := q.getStateSnapshot()

	// Even when forcing, don't pull if paused
	if snapshot.State == QueueStatePaused {
		return
	}

	if !force {
		// Only pull if queue is running and buffer is low
		if snapshot.PendingCount > snapshot.PullLowWM {
			return
		}
	}

	currentRound := snapshot.Round

	// Skip round 0 (root already exists)
	if currentRound == 0 {
		return
	}

	// Get current copy pass (1 for folders, 2 for files)
	copyPass := q.GetCopyPass()

	// Determine node type for current pass
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	// Fetch pending copy tasks via keyset (copy_status = 'pending' in query; cursor is round-scoped).
	batchSize := effectiveLeaseBatchSize()
	results, err := db.ListNodesCopyKeyset(database, currentRound, nodeType, q.getCopyKeysetCursor(), batchSize)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to fetch copy tasks: round=%d, error=%v", currentRound, err), "queue", q.name, q.name)
		}
		return
	}
	var matchedBatch []db.FetchResult
	for _, r := range results {
		if q.isLeased(r.Key) {
			continue
		}
		matchedBatch = append(matchedBatch, r)
		if len(matchedBatch) >= batchSize {
			break
		}
	}
	if len(matchedBatch) > 0 {
		q.setCopyKeysetCursor(matchedBatch[len(matchedBatch)-1].Key)
	}
	hitEndOfBucket := len(results) < batchSize

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
			_ = logservice.LS.Log("error", fmt.Sprintf("Batch parent lookup failed: %v", err), "queue", q.name, q.name)
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
			_ = logservice.LS.Log("error", fmt.Sprintf("Batch DST node lookup failed: %v", err), "queue", q.name, q.name)
		}
		return
	}

	// Move tasks to in-progress status and create tasks
	enqueueSuccessCount := 0
	for _, item := range matchedBatch {
		// Already filtered for leased items during scan, so no need to check again

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
				_ = logservice.LS.Log("error", fmt.Sprintf("Type mismatch for %s - pass=%d expects %s but node has %s - skipping", item.State.Path, copyPass, expectedType, item.State.Type), "queue", q.name, q.name)
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
				_ = logservice.LS.Log("error", fmt.Sprintf("Item at round %d has empty ParentID (path=%s) - this should not happen", item.State.Depth, item.State.Path), "queue", q.name, q.name)
			}
			continue
		}
		dstParentULID := dstIDBySrcID[item.State.ParentID]
		if dstParentULID == "" {
			if logservice.LS != nil {
				_ = logservice.LS.Log("warn", fmt.Sprintf("No join-lookup for parent %s of %s", item.State.ParentID, item.State.Path), "queue", q.name, q.name)
			}
			continue
		}
		dstParentNode := dstNodesByID[dstParentULID]
		if dstParentNode == nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error", fmt.Sprintf("DST node not found for ULID %s (parent of %s)", dstParentULID, item.State.Path), "queue", q.name, q.name)
			}
			continue
		}
		task.DstParentID = dstParentNode.ServiceID

		// Enqueue task first - only proceed if enqueue succeeds
		if q.Add(task) {
			q.addLeasedKey(item.Key)
			enqueueSuccessCount++

			// ONLY queue status update if we successfully enqueued the task
			// This prevents queueing status updates for already-leased or duplicate tasks
			database.AddCopyToStaging(item.State.ID, db.CopyStatusInProgress)
		}
	}

	// This ensures the status updates are written to BoltDB before any hard checks run
	// Without this flush, hard checks will still see tasks in the pending bucket
	// Only flush if we actually enqueued tasks (enqueueSuccessCount > 0)
	if enqueueSuccessCount > 0 {
		database.FlushTablesForQueue(getQueueType(q.name))
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
	// Record execution time delta
	q.recordExecutionTime(executionDelta)

	currentRound := task.Round
	nodeID := task.ID

	database := q.getDatabase()
	if database == nil {
		return
	}

	task.Locked = false
	task.Status = "successful"

	// Increment completed count
	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()

	// Record task completion in RoundInfo
	q.recordTaskCompletion(currentRound, true)

	// Use only task fields (no DB read). Derive type/path/name/size/mtime from task.
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

	// Update copy status: in-progress -> successful
	database.AddCopyToStaging(nodeID, db.CopyStatusSuccessful)


	// Create DST node entry and update join-lookup (deterministic ID from path)
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
		ParentServiceID: "",
		Name:            taskName,
		Path:            taskPath,
		Type:            taskType,
		Size:            taskSize,
		MTime:           taskMTime,
		Depth:           currentRound,
	}

	// Queue the creation of the DST node (DB-owned buffer)
	// DST nodes use traversal status (not copy status) - mark as "successful" since it was just created
	database.AddNode("DST", dstNode, db.StatusSuccessful)


	// Track metrics based on task type
	q.mu.Lock()
	if task.IsFolder() {
		q.foldersCreatedTotal++
	} else if task.IsFile() {
		q.filesCreatedTotal++
		q.bytesTransferredTotal += task.BytesTransferred
	}
	q.mu.Unlock()

	// Remove from in-progress
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
		_ = logservice.LS.Log("debug",
			fmt.Sprintf("Failing copy task: id=%s path=%s round=%d pass=%d attempts=%d maxRetries=%d",
				nodeID, task.LocationPath(), currentRound, task.CopyPass, task.Attempts, maxRetries),
			"queue", q.name, q.name)
	}

	task.Attempts++

	// Use only task fields for status update (no DB read)
	// Check if we should retry
	if task.Attempts < maxRetries {
		// Retry: re-enqueue to memory, DON'T write to BoltDB (stays as in-progress)
		// This avoids unnecessary BoltDB writes and keeps the queue fast
		task.Locked = false
		q.removeInProgress(nodeID)

		// Remove from leased set so it can be pulled again
		q.removeLeasedKey(nodeID)

		// Re-enqueue to memory pending buffer (append, not insert at 0)
		// The task stays as in-progress in BoltDB to avoid unnecessary writes
		// It will be pulled from memory on the next worker cycle
		q.Add(task)

		if logservice.LS != nil {
			_ = logservice.LS.Log("debug",
				fmt.Sprintf("Copy task will retry: path=%s attempts=%d", task.LocationPath(), task.Attempts),
				"queue", q.name, q.name)
		}
	} else {
		// Max retries exceeded: mark as failed
		// Only NOW do we add to buffer - this is the final failure
		task.Locked = false
		task.Status = "failed"

		// Increment failed count and completed total
		q.incrementRoundStatsFailed(currentRound)
		q.incrementTasksCompletedTotal()
		q.recordTaskCompletion(currentRound, false)

		// Add to buffer only on final failure
		database.AddCopyToStaging(nodeID, db.CopyStatusFailed)


		q.removeInProgress(nodeID)

		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Copy task failed (max retries): path=%s", task.LocationPath()),
				"queue", q.name, q.name)
		}
	}
}
