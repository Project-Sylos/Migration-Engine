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
// Only called when we're past maxKnownDepth - the pass has exhausted itself round-by-round.
// Trust the per-round logic; no re-checking of pending/inProgress/wasFirstPull.
func (q *Queue) CheckCopyCompletion(currentRound int) bool {
	database := q.getDatabase()
	if database == nil {
		return false
	}

	copyPass := q.GetCopyPass()
	maxKnownDepth := q.getMaxKnownDepth()

	// Only consider pass switch/phase complete when we've exhausted all rounds for this pass
	if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
		return false
	}

	// if any in progress or pending in the current queue
	// just to be safe.
	if q.InProgressCount() > 0 || q.GetPendingCount() > 0 {
		return false
	}

	if copyPass == 1 {
		// Pass 1 (folders) done - switch to pass 2 (files); find starting round
		q.SetCopyPass(2)
		q.resetRoundStatsCompleted()

		q.SetRound(1)
		q.setExpectedFromStatsBucket(1)
		q.setLastPullWasPartial(false)

		return false
	}

	// Pass 2 (files) complete
	return q.markComplete("Copy phase complete - both passes finished")
}

// AdvanceCopyRound handles copy-specific round advancement logic.
// Round completion is determined by lastPullWasPartial (memory/keyset only); we never query the DB for in-round advancement.
// When called, the current round has just completed - we always advance to currentRound+1.
func (q *Queue) AdvanceCopyRound() {
	currentRound := q.GetRound()
	copyPass := q.GetCopyPass()
	maxKnownDepth := q.getMaxKnownDepth()

	// We just completed currentRound (lastPullWasPartial, pending empty, inProgress empty).
	// Advance sequentially - no DB queries for "does this round have pending?" (DB is stale during round).
	newRound := currentRound + 1

	if maxKnownDepth >= 0 && newRound > maxKnownDepth {
		// Past maxKnownDepth - check for pass switch or phase complete.
		// DB can be used here: we're at phase boundary, flush has run, deciding phase-level completion.
		if logservice.LS != nil {
			err := logservice.LS.Log("info", fmt.Sprintf("Pass %d exhausted rounds (past maxKnownDepth %d), checking for completion", copyPass, maxKnownDepth), "queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		completed := q.checkCompletion(currentRound, CompletionCheckOptions{
			CheckFinalCompletion: true,
		})
		if completed {
			return
		}
		q.PullTasksIfNeeded(true)
		return
	}

	q.SetRound(newRound)
	q.setExpectedFromStatsBucket(newRound)
	q.setLastPullWasPartial(false)

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
	copyStatusFilter := db.CopyStatusPending
	if q.GetMode() == QueueModeCopyRetry {
		copyStatusFilter = db.CopyStatusFailed
	}
	// Request limit+1 to detect keyspace exhaustion: if we get <= limit, we're done; else more exists.
	requestLimit := batchSize + 1
	results, err := db.ListNodesCopyKeyset(database, currentRound, nodeType, q.getCopyKeysetCursor(), requestLimit, copyStatusFilter)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("ListNodesCopyKeyset failed: %v", err), "queue", q.name, q.name)
		}
		return
	}
	// Process at most batchSize items this pull; the extra (+1) is only for exhaustion detection
	processLimit := batchSize
	if len(results) <= processLimit {
		processLimit = len(results)
	}
	var matchedBatch []db.FetchResult
	for i := 0; i < processLimit; i++ {
		r := results[i]
		if r.State == nil {
			continue
		}
		matchedBatch = append(matchedBatch, r)
	}
	// Cursor = last item we consumed. lastPullWasPartial = keyspace exhausted (got <= batchSize from DB).
	if len(results) > 0 {
		cursorIdx := processLimit - 1
		if cursorIdx < 0 {
			cursorIdx = 0
		}
		q.setCopyKeysetCursor(results[cursorIdx].Key)
	}
	q.setLastPullWasPartial(len(results) <= batchSize)

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
				err := logservice.LS.Log("warning", fmt.Sprintf("No join-lookup for parent %s of %s", item.State.ParentID, item.State.Path), "queue", q.name, q.name)
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
			enqueueSuccessCount++
		}
	}

	// Record pull in RoundInfo; lastPullWasPartial already set from raw DB result count
	q.recordPull(currentRound, enqueueSuccessCount, q.getLastPullWasPartial())
	q.setFirstPullForRound(false)
}

// nodeStateToCopyTask converts a NodeState to a copy TaskBase.
func nodeStateToCopyTask(state *db.NodeState, taskType string, copyPass int) *TaskBase {
	if state == nil {
		return nil
	}

	task := &TaskBase{
		ID:                 state.ID,
		Type:               taskType,
		Round:              state.Depth,
		CopyPass:           copyPass,
		Attempts:           0,
		Status:             "",
		Locked:             false,
		LeaseTime:          time.Now(),
		DstParentID:        "",
		CopyStatus:         state.CopyStatus,
		SrcTraversalStatus: state.TraversalStatus,
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
	taskPath := db.NormalizeRootRelativePath(task.LocationPath())
	taskName := task.File.DisplayName
	taskSize := task.File.Size
	taskMTime := task.File.LastUpdated
	if task.IsFolder() {
		taskType = types.NodeTypeFolder
		taskName = task.Folder.DisplayName
		taskMTime = task.Folder.LastUpdated
	}

	parentPath := task.File.ParentPath
	if task.IsFolder() {
		parentPath = task.Folder.ParentPath
	}
	parentPath = db.NormalizeRootRelativePath(parentPath)

	// DB-backed: append SRC copy status event and DST node to seal buffer
	database.AppendStatusEvent("SRC", db.StatusEvent{
		ID:              nodeID,
		TraversalStatus: task.SrcTraversalStatus,
		CopyStatus:      db.CopyStatusSuccessful,
		PrevCopyStatus:  task.CopyStatus,
		EventTime:       time.Now().UnixNano(),
		Depth:           currentRound,
	}, false)
	dstNodeID := db.DeterministicNodeID("DST", taskType, taskPath)
	var dstServiceID string
	if task.IsFolder() {
		dstServiceID = task.Folder.ServiceID
	} else {
		dstServiceID = task.File.ServiceID
	}
	dstParentID := ""
	if parentPath != "" && parentPath != "/" {
		dstParentID = db.DeterministicNodeID("DST", db.NodeTypeFolder, parentPath)
	}
	dstNode := &db.NodeState{
		ID:              dstNodeID,
		ServiceID:       dstServiceID,
		ParentID:        dstParentID,
		ParentServiceID: task.DstParentID,
		ParentPath:      parentPath,
		Name:            taskName,
		Path:            taskPath,
		Type:            taskType,
		Size:            taskSize,
		MTime:           taskMTime,
		Depth:           currentRound,
		TraversalStatus: db.StatusSuccessful,
		Status:          db.StatusSuccessful,
	}
	database.AppendDiscoveredNodes([]db.InsertOperation{
		{QueueType: "DST", Level: currentRound, Status: db.StatusSuccessful, State: dstNode},
	})

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

		q.incrementRoundStatsCompleted(currentRound)
		q.incrementRoundStatsFailed(currentRound)
		q.incrementTasksCompletedTotal()
		q.recordTaskCompletion(currentRound, false)

		database.AppendStatusEvent("SRC", db.StatusEvent{
			ID:              nodeID,
			TraversalStatus: task.SrcTraversalStatus,
			CopyStatus:      db.CopyStatusFailed,
			PrevCopyStatus:  task.CopyStatus,
			EventTime:       time.Now().UnixNano(),
			Depth:           currentRound,
		}, false)

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
