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

// copyHasPendingDBWorkAtRound reports whether src_nodes still has copy work for the current pass at depth round.
func (q *Queue) copyHasPendingDBWorkAtRound(round int) bool {
	database := q.getDatabase()
	if database == nil || round <= 0 {
		return false
	}
	nodeType := db.NodeTypeFolder
	if q.GetCopyPass() == 2 {
		nodeType = db.NodeTypeFile
	}
	copyStatus := db.CopyStatusPending
	if q.GetMode() == QueueModeCopyRetry {
		copyStatus = db.CopyStatusFailed
	}
	count, err := database.GetCopyCountAtDepth(round, nodeType, copyStatus, true)
	return err == nil && count > 0
}

// minDepthWithPendingCopyWork returns the shallowest depth (>0) with pending copy work for copyPass (1=folders, 2=files), or -1.
func (q *Queue) minDepthWithPendingCopyWork(copyPass int) int {
	database := q.getDatabase()
	if database == nil {
		return -1
	}
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}
	copyStatus := db.CopyStatusPending
	if q.GetMode() == QueueModeCopyRetry {
		copyStatus = db.CopyStatusFailed
	}
	levels, err := db.GetAllLevels(database, "SRC")
	if err != nil {
		return -1
	}
	min := -1
	for _, level := range levels {
		if level <= 0 {
			continue
		}
		c, err := database.GetCopyCountAtDepth(level, nodeType, copyStatus, true)
		if err == nil && c > 0 {
			if min == -1 || level < min {
				min = level
			}
		}
	}
	return min
}

// copyRoundAdvanceNeedsDBRetry is true when the in-memory round looks exhausted but DuckDB still has copy work at this depth.
func (q *Queue) copyRoundAdvanceNeedsDBRetry(round int) bool {
	mode := q.GetMode()
	if mode != QueueModeCopy && mode != QueueModeCopyRetry {
		return false
	}
	if q.GetPendingCount() > 0 || q.InProgressCount() > 0 || q.getPulling() {
		return false
	}
	return q.copyHasPendingDBWorkAtRound(round)
}

// retryCopyPullForRound resets the keyset cursor and re-pulls when DB still has work at this depth.
func (q *Queue) retryCopyPullForRound(round int, reason string) {
	if logservice.LS != nil {
		_ = logservice.LS.Log("warning", reason, "queue", q.name, q.name)
	}
	q.resetThisQueueKeysetCursor()
	q.setLastPullWasPartial(false)
	q.pullWithRetryIfNeeded(true)
}

// CheckCopyCompletion checks if the copy phase should switch passes or complete.
// Only called when we're past maxKnownDepth - the pass has exhausted itself round-by-round.
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
		if minDepth := q.minDepthWithPendingCopyWork(1); minDepth > 0 {
			q.retryCopyPullForRound(minDepth, fmt.Sprintf(
				"Pass 1 folder sweep finished but pending folders remain (e.g. depth %d); re-running folder pass",
				minDepth,
			))
			q.SetRound(minDepth)
			q.setExpectedFromStatsBucket(minDepth)
			return false
		}
		// Pass 1 (folders) done - switch to pass 2 (files)
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "Copy pass 1 (folders) complete — starting pass 2 (files)", "queue", q.name, q.name)
		}
		q.SetCopyPass(2)
		q.resetRoundStatsCompleted()
		q.resetThisQueueKeysetCursor()

		startRound := 1
		if minFile := q.minDepthWithPendingCopyWork(2); minFile > 0 {
			startRound = minFile
		}
		q.SetRound(startRound)
		q.setExpectedFromStatsBucket(startRound)
		q.setLastPullWasPartial(false)
		q.pullWithRetryIfNeeded(true)
		return false
	}

	if minDepth := q.minDepthWithPendingCopyWork(2); minDepth > 0 {
		q.retryCopyPullForRound(minDepth, fmt.Sprintf(
			"Pass 2 file sweep finished but pending files remain (e.g. depth %d); re-running file pass at depth %d",
			minDepth, minDepth,
		))
		q.SetRound(minDepth)
		q.setExpectedFromStatsBucket(minDepth)
		return false
	}

	// Pass 2 (files) complete
	return q.markComplete("Copy phase complete - both passes finished")
}

// AdvanceCopyRound handles copy-specific round advancement logic.
// Round completion is determined by lastPullWasPartial (memory/keyset only); we never query the DB for in-round advancement.
// When called, the current round has just completed - we always advance to currentRound+1.
func (q *Queue) AdvanceCopyRound() {
	q.noteCopyResumeDstExistenceLeavingAnchorRound()

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
		q.pullWithRetryIfNeeded(true)
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
	q.pullWithRetryIfNeeded(true)
}

// PullCopyTasks pulls copy tasks from DuckDB for the current round.
// Pulls from SRC copy status buckets, filters by pass (folders vs files), and skips round 0.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullCopyTasks(force bool) PullResult {
	database := q.getDatabase()
	if database == nil {
		return PullResult{Status: PullAborted}
	}
	if q.getPulling() {
		return PullResult{Round: q.GetRound(), Status: PullSkipped}
	}
	if q.State() == QueueStateCompleted {
		return PullResult{Status: PullAborted}
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
		return PullResult{Round: snapshot.Round, Status: PullAborted}
	}
	if !force && snapshot.PendingCount > snapshot.PullLowWM {
		return PullResult{Round: snapshot.Round, Status: PullSkipped}
	}

	currentRound := snapshot.Round
	if currentRound == 0 {
		return PullResult{Round: 0, Status: PullSkipped}
	}

	copyPass := q.GetCopyPass()
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	batchSize := q.effectiveLeaseBatch()
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
		return PullResult{Round: currentRound, Status: PullSkipped}
	}
	if q.GetRound() != currentRound {
		return PullResult{Round: currentRound, QueriedDB: true, Status: PullStaleRound}
	}
	// Process at most batchSize items this pull; the extra (+1) is only for exhaustion detection
	processLimit := min(batchSize, len(results))
	var matchedBatch []db.FetchResult
	for _, r := range results[:processLimit] {
		if r.State == nil {
			continue
		}
		matchedBatch = append(matchedBatch, r)
	}
	// Cursor = last item we consumed. lastPullWasPartial = keyspace exhausted (got <= batchSize from DB).
	if len(results) > 0 {
		cursorIdx := max(0, processLimit-1)
		q.setCopyKeysetCursor(results[cursorIdx].Key)
	}
	q.setLastPullWasPartial(len(results) <= batchSize)

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

		// DstParentServiceID from path_hash join in ListNodesCopyKeyset
		if item.State.ParentID == "" {
			if logservice.LS != nil {
				err := logservice.LS.Log("error", fmt.Sprintf("Item at round %d has empty ParentID (path=%s) - this should not happen", item.State.Depth, item.State.Path), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue
		}
		dstParentServiceID := item.DstParentServiceID
		if dstParentServiceID == "" {
			parentPath := db.NormalizeRootRelativePath(item.State.ParentPath)
			if parentPath == "/" {
				if _, rootState, ok := db.GetRootNode(database, "DST"); ok && rootState != nil && rootState.ServiceID != "" {
					dstParentServiceID = rootState.ServiceID
				}
			}
		}
		if dstParentServiceID == "" {
			if logservice.LS != nil {
				err := logservice.LS.Log("error", fmt.Sprintf("No DST parent for %s (parent_path=%s parent_id=%s) — skipping copy task", item.State.Path, item.State.ParentPath, item.State.ParentID), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue
		}
		task.DstParentID = dstParentServiceID

		if q.Add(task) {
			enqueueSuccessCount++
		}

	}

	partial := len(results) <= batchSize
	q.setLastPullWasPartial(partial)
	q.recordPull(currentRound, enqueueSuccessCount, partial)
	q.setFirstPullForRound(false)
	return PullResult{Round: currentRound, Yield: enqueueSuccessCount, Partial: partial, QueriedDB: true, Status: PullOK}
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
		q.removeInProgress(nodeID)
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

	// Check if we should retry
	if task.Attempts < maxRetries {
		// Retry: re-enqueue to memory, DON'T write to DuckDB (stays as in-progress)
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
				fmt.Sprintf("Copy task will retry: path=%s attempts=%d", task.LocationPath(), task.Attempts),
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return
	}

	// Max retries exceeded - permanent failure
	task.Locked = false
	task.Status = "failed"

	q.incrementRoundStatsCompleted(currentRound)
	q.incrementRoundStatsFailed(currentRound)
	q.incrementTasksCompletedTotal()
	q.recordTaskCompletion(currentRound, false)

	if logservice.LS != nil {
		err := logservice.LS.Log("error",
			fmt.Sprintf("Copy task failed (max retries): path=%s", task.LocationPath()),
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

	// Record task error if present
	if task.LastError != "" {
		database.AppendTaskError("SRC", "copy", nodeID, task.LastError, task.Attempts, task.LocationPath())
	}

	copyEv := db.StatusEvent{
		ID:              nodeID,
		TraversalStatus: task.SrcTraversalStatus,
		CopyStatus:      db.CopyStatusFailed,
		PrevCopyStatus:  task.CopyStatus,
		EventTime:       time.Now().UnixNano(),
		Depth:           currentRound,
	}
	db.AttachTaskFailureLog(&copyEv, "copy", q.name, nodeID, task.LocationPath(), task.Attempts, task.LastError)
	database.AppendStatusEvent("SRC", copyEv, false)

	// Folder failure cascades: mark all pending descendants as failed so they aren't
	// pulled in the file pass (they'd be skipped anyway since the DST parent won't exist).
	if task.IsFolder() {
		taskPath := db.NormalizeSubtreeRootPathForPropagation(task.LocationPath())
		if taskPath != "" && taskPath != "/" {
			database.AppendFailedSubtree(taskPath)
		}
	}

	q.removeInProgress(nodeID)
}
