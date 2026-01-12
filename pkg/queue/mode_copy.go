// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
	bolt "go.etcd.io/bbolt"
)

// CheckCopyCompletion checks if the copy phase should switch passes or complete.
// Returns true if the queue should mark as complete, false otherwise.
// This is called when advanceToNextRound can't find a next round for the current pass.
func (q *Queue) CheckCopyCompletion(currentRound int, wasFirstPull bool) bool {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return false
	}

	// Copy: must progress through all rounds up to maxKnownDepth in each pass
	// Only switch from pass 1 to pass 2 after reaching maxKnownDepth
	// This is similar to retry mode's pattern
	copyPass := q.getCopyPass()
	maxKnownDepth := q.getMaxKnownDepth()

	// If we haven't reached maxKnownDepth yet, don't switch passes
	// Even if current round has no folders/files, we need to progress through all rounds
	if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
		return false // Let advanceToNextRound handle progression
	}

	// We've reached or passed maxKnownDepth for current pass
	// Now check if there are any pending or in-progress tasks for the current pass at any level
	levels, err := boltDB.GetAllLevels("SRC")
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
		hasPending, err := boltDB.HasCopyStatusBucketItems(level, nodeType, db.CopyStatusPending)
		if err == nil && hasPending {
			hasAnyPendingForPass = true
		}
		hasInProgress, err := boltDB.HasCopyStatusBucketItems(level, nodeType, db.CopyStatusInProgress)
		if err == nil && hasInProgress {
			hasAnyInProgressForPass = true
			inProgressLevels = append(inProgressLevels, level)
		}
		if hasAnyPendingForPass && hasAnyInProgressForPass {
			break // Found both, no need to continue
		}
	}

	// Log in-progress tasks if found - this is critical for debugging
	if hasAnyInProgressForPass {
		fmt.Printf("[Copy Completion] WARNING: Found in-progress tasks for pass %d (nodeType=%s) at levels: %v\n", copyPass, nodeType, inProgressLevels)
	}

	// If no pending tasks for current pass and this was first pull, switch passes or complete
	if !hasAnyPendingForPass && wasFirstPull {
		if copyPass == 1 {
			// Pass 1 (folders) complete - switch to pass 2 (files)
			q.setCopyPass(2)
			fmt.Printf("[Copy Completion] Reached maxKnownDepth %d, switching to pass 2\n", maxKnownDepth)

			// Find minimum level with pending file tasks for pass 2
			minLevel := -1
			for _, level := range levels {
				if level == 0 {
					continue // Skip round 0
				}
				hasPending, err := boltDB.HasCopyStatusBucketItems(level, db.NodeTypeFile, db.CopyStatusPending)
				if err == nil && hasPending {
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

			q.setRound(minLevel) // Set to minimum pending level for pass 2

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
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Flush buffer before advancing to ensure all pending writes (like DST node creation)
	// are persisted before the next round's tasks try to read them
	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

	currentRound := q.getRound()
	copyPass := q.getCopyPass()

	// Get all levels to check
	levels, err := boltDB.GetAllLevels("SRC")
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
		hasPending, err := boltDB.HasCopyStatusBucketItems(currentRound, nodeType, db.CopyStatusPending)
		if err == nil && hasPending {
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
			hasPending, err := boltDB.HasCopyStatusBucketItems(level, nodeType, db.CopyStatusPending)
			if err == nil && hasPending {
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
			fmt.Printf("[Copy Advance] No next round found for pass %d, checking for completion/pass switch\n", copyPass)
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
				fmt.Printf("[Copy Advance] Queue marked as complete\n")
				// Queue is complete - state is set to QueueStateCompleted
				return
			}
			// If not completed, we switched passes - round was reset to minimum for new pass
			newRoundAfterSwitch := q.getRound()
			newPassAfterSwitch := q.getCopyPass()
			fmt.Printf("[Copy Advance] Pass switched, new round=%d, new pass=%d\n", newRoundAfterSwitch, newPassAfterSwitch)
			// Pull tasks for the new pass
			q.PullTasksIfNeeded(true)
			return
		}
	}

	// Set new round and log
	if newRound != currentRound {
		fmt.Printf("[Copy Advance] Found next round: %d (was %d)\n", newRound, currentRound)
	}

	// Get stats for logging
	q.setRound(newRound)

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
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	if q.getPulling() {
		return
	}

	// Don't pull if queue is completed
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
	copyPass := q.getCopyPass()
	passName := "folders"
	if copyPass == 2 {
		passName = "files"
	}

	// Determine node type for current pass
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	// Only log pull attempts when actually pulling (not on every check)
	if force {
		fmt.Printf("[Copy Pull] Pulling tasks: round=%d, pass=%d (%s), nodeType=%s\n", currentRound, copyPass, passName, nodeType)
	}

	// Scan pending status bucket for the specific node type
	// Bucket is already filtered by node type, so no in-memory filtering needed!
	var matchedBatch []db.FetchResult
	var hitEndOfBucket bool

	err := boltDB.View(func(tx *bolt.Tx) error {
		// Get pending copy status bucket for this level and node type
		copyStatusBucket := db.GetCopyStatusBucket(tx, currentRound, nodeType, db.CopyStatusPending)
		if copyStatusBucket == nil {
			hitEndOfBucket = true // No bucket = no items = hit end
			return nil
		}

		nodesBucket := db.GetNodesBucket(tx, "SRC")
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for SRC")
		}

		cursor := copyStatusBucket.Cursor()
		scannedCount := 0

		for nodeIDBytes, _ := cursor.First(); nodeIDBytes != nil; nodeIDBytes, _ = cursor.Next() {
			scannedCount++

			// Skip if already leased
			if q.isLeased(string(nodeIDBytes)) {
				continue
			}

			// Get the node state
			nodeData := nodesBucket.Get(nodeIDBytes)
			if nodeData == nil {
				continue // Node was deleted
			}

			state, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid entries
			}

			// No filtering needed - bucket already contains only the correct node type!
			// Add to batch
			matchedBatch = append(matchedBatch, db.FetchResult{
				Key:   string(nodeIDBytes),
				State: state,
			})

			// Stop if we've collected enough
			if len(matchedBatch) >= defaultLeaseBatchSize {
				break
			}
		}

		// We hit end of bucket if cursor finished before we collected enough items
		hitEndOfBucket = len(matchedBatch) < defaultLeaseBatchSize

		return nil
	})

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Failed to scan copy bucket: round=%d, error=%v", currentRound, err), "queue", q.name, q.name)
		}
		return
	}

	fmt.Printf("[Copy Pull] Scanned pending bucket: matched=%d items for pass %d (%s), hitEnd=%v\n",
		len(matchedBatch), copyPass, passName, hitEndOfBucket)

	// Move tasks to in-progress status and create tasks
	enqueueSuccessCount := 0
	for _, item := range matchedBatch {
		// Already filtered for leased items during scan, so no need to check again

		// Determine task type based on node type
		taskType := TaskTypeCopyFile
		if item.State.Type == types.NodeTypeFolder {
			taskType = TaskTypeCopyFolder
		}

		task := nodeStateToCopyTask(item.State, taskType, copyPass)
		// Ensure task has the ULID from the database
		if task != nil && task.ID == "" {
			task.ID = item.State.ID
		}

		// Resolve destination parent ServiceID using join-lookup
		// Join-lookup maps SRC ULID → DST ULID, then load DST node to get ServiceID
		if item.State.ParentID != "" {
			dstParentULID, err := db.GetDstIDFromSrcID(boltDB, item.State.ParentID)
			if err != nil {
				// Only log critical errors - missing parent lookup will cause task failure
				if logservice.LS != nil {
					_ = logservice.LS.Log("error", fmt.Sprintf("Failed to join-lookup for parent %s of %s: %v", item.State.ParentID, item.State.Path, err), "queue", q.name, q.name)
				}
			} else if dstParentULID == "" {
				// Only log critical errors - missing parent lookup will cause task failure
				if logservice.LS != nil {
					_ = logservice.LS.Log("warn", fmt.Sprintf("No join-lookup for parent %s of %s", item.State.ParentID, item.State.Path), "queue", q.name, q.name)
				}
			} else {
				// Load DST parent node to get ServiceID
				dstParentNode, err := db.GetNodeState(boltDB, "DST", dstParentULID)
				if err != nil || dstParentNode == nil {
					// Only log critical errors - missing DST parent will cause task failure
					if logservice.LS != nil {
						_ = logservice.LS.Log("error", fmt.Sprintf("DST node not found for ULID %s (parent of %s)", dstParentULID, item.State.Path), "queue", q.name, q.name)
					}
				} else {
					// Extract ServiceID for adapter call
					task.DstParentID = dstParentNode.ServiceID
				}
			}
		}

		// Enqueue task first - only proceed if enqueue succeeds
		if q.enqueuePending(task) {
			q.addLeasedKey(item.Key)
			enqueueSuccessCount++

			// ONLY queue status update if we successfully enqueued the task
			// This prevents queueing status updates for already-leased or duplicate tasks
			if outputBuffer != nil {
				outputBuffer.AddCopyStatusUpdate("SRC", item.State.Depth, db.CopyStatusPending, item.State.ID, db.CopyStatusInProgress)
			}
		}
	}

	if enqueueSuccessCount > 0 {
		fmt.Printf("[Copy Pull] Enqueued %d/%d tasks\n", enqueueSuccessCount, len(matchedBatch))
	}

	// This ensures the status updates are written to BoltDB before any hard checks run
	// Without this flush, hard checks will still see tasks in the pending bucket
	// Only flush if we actually enqueued tasks (enqueueSuccessCount > 0)
	if outputBuffer != nil && enqueueSuccessCount > 0 {
		outputBuffer.Flush()
	}

	// Track if this pull was partial
	// wasPartial = true when we hit the end of the bucket (couldn't collect enough matching items)
	// This accurately signals when we've exhausted the current round for this pass
	wasPartial := hitEndOfBucket
	q.setLastPullWasPartial(wasPartial)

	// Record pull in RoundInfo
	q.recordPull(currentRound, len(matchedBatch), wasPartial)

	fmt.Printf("[Copy Pull] Pull complete: round=%d, pass=%d, items=%d, wasPartial=%v\n",
		currentRound, copyPass, len(matchedBatch), wasPartial)
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

	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Remove the ULID from leased set
	q.removeLeasedKey(nodeID)

	task.Locked = false
	task.Status = "successful"

	// Increment completed count
	q.incrementRoundStatsCompleted(currentRound)

	// Record task completion in RoundInfo
	q.recordTaskCompletion(currentRound, true)

	// Get node state to update copy status
	srcNode, err := db.GetNodeState(boltDB, "SRC", nodeID)
	if err != nil || srcNode == nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to get node state for copy task completion: %s, error: %v", nodeID, err),
				"queue", q.name, q.name)
		}
		return
	}

	// Update copy status: in-progress -> successful
	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
		outputBuffer.AddCopyStatusUpdate("SRC", srcNode.Depth, db.CopyStatusInProgress, nodeID, db.CopyStatusSuccessful)
	}

	// Create DST node entry and update join-lookup
	// This maps SRC ULID → DST ULID consistently (not mixing ServiceID)
	// Generate new ULID for DST node
	dstULID := db.GenerateNodeID()

	// Create DST node with ServiceID from created folder/file
	var dstServiceID string
	if task.IsFolder() {
		dstServiceID = task.Folder.ServiceID
	} else {
		dstServiceID = task.File.ServiceID
	}

	// Create DST node state
	dstNode := &db.NodeState{
		ID:              dstULID,
		ServiceID:       dstServiceID,
		ParentID:        "", // Will be populated later if needed
		ParentServiceID: "", // Will be populated later if needed
		Name:            srcNode.Name,
		Path:            srcNode.Path,
		Type:            srcNode.Type,
		Size:            srcNode.Size,
		MTime:           srcNode.MTime,
		Depth:           srcNode.Depth,
	}

	// Queue the creation of the DST node as an output buffer operation.
	// DST nodes use traversal status (not copy status) - mark as "successful" since it was just created
	if outputBuffer != nil {
		outputBuffer.AddCreateNode("DST", currentRound, db.StatusSuccessful, dstNode)
	}

	// Map SRC ULID → DST ULID in join-lookup
	if outputBuffer != nil {
		outputBuffer.AddLookupMapping(nodeID, dstULID)
	}

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
	boltDB := q.getBoltDB()
	if boltDB == nil {
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

	// Get node state to update copy status
	srcNode, err := db.GetNodeState(boltDB, "SRC", nodeID)
	if err != nil || srcNode == nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to get node state for copy task failure: %s, error: %v", nodeID, err),
				"queue", q.name, q.name)
		}
		return
	}

	outputBuffer := q.getOutputBuffer()

	// Check if we should retry
	if task.Attempts < maxRetries {
		// Retry: move back to pending in BoltDB
		// DON'T re-enqueue immediately - let the next pull cycle find it naturally
		// This prevents duplicate status updates and ensures the attempt count persists
		// DON'T add to buffer - let the next pull cycle handle it naturally
		// This avoids hammering the DB with writes for intermediate failures
		task.Locked = false
		q.removeInProgress(nodeID)

		// Remove from leased set so it can be pulled again in the next cycle
		q.removeLeasedKey(nodeID)

		// DO NOT add to buffer here - only on final failure
		// DO NOT call enqueuePending here - the task will be pulled from BoltDB
		// in the next cycle after the status update flushes to pending

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

		// Increment failed count
		q.incrementRoundStatsFailed(currentRound)
		q.recordTaskCompletion(currentRound, false)

		// Add to buffer only on final failure
		if outputBuffer != nil {
			outputBuffer.AddCopyStatusUpdate("SRC", srcNode.Depth, db.CopyStatusInProgress, nodeID, db.CopyStatusFailed)
		}

		q.removeInProgress(nodeID)

		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Copy task failed (max retries): path=%s", task.LocationPath()),
				"queue", q.name, q.name)
		}
	}
}
