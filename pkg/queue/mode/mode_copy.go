// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"context"
	"fmt"
	"path"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/worker"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// CheckCopyCompletion checks if the copy phase should switch passes or complete.
// Only called when we're past maxKnownDepth - the pass has exhausted itself round-by-round.
// Trust the per-round logic; no re-checking of pending/inProgress/wasFirstPull.
func CheckCopyCompletion(q *queue.Queue, currentRound int) bool {
	database := q.Database()
	if database == nil {
		return false
	}

	copyPass := q.GetCopyPass()
	maxKnownDepth := q.GetMaxKnownDepth()

	// Only consider pass switch/phase complete when we've exhausted all rounds for this pass
	if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
		return false
	}

	// if any in progress or pending in the current queue
	// just to be safe.
	if q.InProgressCount() > 0 || q.GetPendingCount() > 0 {
		return false
	}

	// Run() polls CheckFinalCompletion every tick, including before the first pull.
	// Do not switch passes / complete until this round has actually pulled (same gate as
	// round completion). Otherwise pass 1 is skipped when maxKnownDepth == start round.
	info := q.RoundInfoReadOnly(currentRound)
	if info == nil || info.PullCount == 0 {
		return false
	}
	if !q.GetLastPullWasPartial() {
		return false
	}

	if copyPass == 1 {
		// Pass 1 (folders) done - switch to pass 2 (files); find starting round
		q.SetCopyPass(2)
		q.ResetRoundStatsCompleted()

		q.SetRound(1)
		q.SetExpectedFromStatsBucket(1)
		q.SetLastPullWasPartial(false)

		return false
	}

	// Pass 2 (files) complete
	return q.MarkComplete("Copy phase complete - both passes finished")
}

// AdvanceCopyRound handles copy-specific round advancement logic.
// Round completion is determined by lastPullWasPartial (memory/keyset only); we never query the DB for in-round advancement.
// When called, the current round has just completed - we always advance to currentRound+1.
func AdvanceCopyRound(q *queue.Queue) {
	q.NoteCopyResumeDstExistenceLeavingAnchorRound()

	currentRound := q.GetRound()
	copyPass := q.GetCopyPass()
	maxKnownDepth := q.GetMaxKnownDepth()

	// We just completed currentRound (lastPullWasPartial, pending empty, inProgress empty).
	// Advance sequentially - no DB queries for "does this round have pending?" (DB is stale during round).
	newRound := currentRound + 1

	if maxKnownDepth >= 0 && newRound > maxKnownDepth {
		// Past maxKnownDepth - check for pass switch or phase complete.
		// DB can be used here: we're at phase boundary, flush has run, deciding phase-level completion.
		if logservice.LS != nil {
			err := logservice.LS.Log("info", fmt.Sprintf("Pass %d exhausted rounds (past maxKnownDepth %d), checking for completion", copyPass, maxKnownDepth), "queue", q.Name(), q.Name())
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		completed := q.CheckCompletion(currentRound, queue.CompletionCheckOptions{
			CheckFinalCompletion: true,
		})
		if completed {
			return
		}
		q.PullWithRetryIfNeeded(true)
		return
	}

	q.SetRound(newRound)
	q.SetExpectedFromStatsBucket(newRound)
	q.SetLastPullWasPartial(false)

	passName := "folders"
	if copyPass == 2 {
		passName = "files"
	}

	if logservice.LS != nil {
		err := logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d (pass %d: %s)", newRound, copyPass, passName), "queue", q.Name(), q.Name())
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	// Pull tasks for the new round
	q.PullWithRetryIfNeeded(true)
}

// PullCopyTasks pulls copy tasks from DuckDB for the current round.
// Pulls from SRC copy status buckets, filters by pass (folders vs files), and skips round 0.
// Uses getter/setter methods - no direct mutex access.
func PullCopyTasks(q *queue.Queue, force bool) queue.PullResult {
	database := q.Database()
	if database == nil {
		return queue.PullResult{Status: queue.PullAborted}
	}
	if !q.TryBeginPulling() {
		return queue.PullResult{Round: q.GetRound(), Status: queue.PullSkipped}
	}
	defer q.SetPulling(false)
	if q.State() == queue.QueueStateCompleted {
		return queue.PullResult{Status: queue.PullAborted}
	}

	snapshot := q.StateSnapshot()
	if snapshot.State == queue.QueueStatePaused {
		return queue.PullResult{Round: snapshot.Round, Status: queue.PullAborted}
	}
	if !force && snapshot.PendingCount > snapshot.PullLowWM {
		return queue.PullResult{Round: snapshot.Round, Status: queue.PullSkipped}
	}

	currentRound := snapshot.Round
	if currentRound == 0 {
		return queue.PullResult{Round: 0, Status: queue.PullSkipped}
	}

	copyPass := q.GetCopyPass()
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}

	batchSize := q.EffectiveLeaseBatchSize()
	copyStatusFilter := db.CopyStatusPending
	if q.GetMode() == queue.QueueModeCopyRetry {
		// Marks convert failed→pending; retry pulls those pending marks (and any leftover pending).
		copyStatusFilter = db.CopyStatusPending
	}
	// Request limit+1 to detect keyspace exhaustion: if we get <= limit, we're done; else more exists.
	requestLimit := batchSize + 1
	q.BeginDBPull()
	defer q.ReleaseDBPull()
	pullStart := time.Now()
	results, err := pull.ListNodesCopyKeyset(database, currentRound, nodeType, q.GetKeysetCursor(), requestLimit, copyStatusFilter)
	q.RecordDBPull(len(results), time.Since(pullStart))
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("ListNodesCopyKeyset failed: %v", err), "queue", q.Name(), q.Name())
		}
		return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
	}
	if q.GetRound() != currentRound {
		return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
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
		q.SetKeysetCursor(results[cursorIdx].Key)
	}
	q.SetLastPullWasPartial(len(results) <= batchSize)

	// Move tasks to in-progress status and create tasks
	enqueueSuccessCount := 0
	for _, item := range matchedBatch {

		// Determine task type based on copy pass (not just node type, to ensure consistency)
		// We're pulling from a bucket filtered by nodeType, so this should match item.State.Type
		taskType := queue.TaskTypeCopyFile
		if copyPass == 1 {
			taskType = queue.TaskTypeCopyFolder
		}

		// Verify node type matches what we're pulling (sanity check)
		expectedType := types.NodeTypeFile
		if copyPass == 1 {
			expectedType = types.NodeTypeFolder
		}
		if item.State.Type != expectedType {
			if logservice.LS != nil {
				err := logservice.LS.Log("error", fmt.Sprintf("Type mismatch for %s - pass=%d expects %s but node has %s - skipping", item.State.Path, copyPass, expectedType, item.State.Type), "queue", q.Name(), q.Name())
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			continue // Skip mismatched items
		}

		task := nodeStateToCopyTask(item.State, taskType, copyPass)
		if task == nil {
			continue
		}
		if task.ID == "" {
			task.ID = item.State.ID
		}

		if item.State.ParentID == "" && logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("Item at round %d has empty ParentID (path=%s)", item.State.Depth, item.State.Path), "queue", q.Name(), q.Name())
		}
		dstParentServiceID := item.DstParentServiceID
		if dstParentServiceID == "" {
			parentPath := db.NormalizeRootRelativePath(item.State.ParentPath)
			if parentPath == "/" {
				if _, rootState, ok := pull.GetRootNode(database, "DST"); ok && rootState != nil && rootState.ServiceID != "" {
					dstParentServiceID = rootState.ServiceID
				}
			}
		}
		if dstParentServiceID == "" && logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("No DST parent for %s (parent_path=%s parent_id=%s); enqueueing so copy still claims the task", item.State.Path, item.State.ParentPath, item.State.ParentID), "queue", q.Name(), q.Name())
		}
		task.DstParentID = dstParentServiceID
		task.DstParentNodeID = item.DstParentNodeID
		if item.DstParentNodeID == "" && (item.State.ParentPath == "" || item.State.ParentPath == "/") {
			task.DstParentNodeID = db.MintNodeID("DST", "", db.NodeTypeFolder, "/")
		}
		task.ResolvedDstName = item.ResolvedDstPath
		task.SrcParentDeleteStatus = item.SrcParentDeleteStatus

		if q.Add(task) {
			enqueueSuccessCount++
		}

	}

	partial := len(results) <= batchSize
	q.SetLastPullWasPartial(partial)
	q.RecordPull(currentRound, enqueueSuccessCount, partial)
	q.SetFirstPullForRound(false)
	return queue.PullResult{Round: currentRound, Yield: enqueueSuccessCount, Partial: partial, QueriedDB: true, Status: queue.PullOK}
}

// nodeStateToCopyTask converts a NodeState to a copy queue.TaskBase.
func nodeStateToCopyTask(state *db.NodeState, taskType string, copyPass int) *queue.TaskBase {
	if state == nil {
		return nil
	}

	logicalPath := db.NormalizeRootRelativePath(state.Path)
	logicalParent := state.ParentPath
	if state.Depth > 0 {
		logicalParent = db.NormalizeRootRelativePath(state.ParentPath)
	}
	task := &queue.TaskBase{
		ID:                   state.ID,
		Type:                 taskType,
		Round:                state.Depth,
		CopyPass:             copyPass,
		Attempts:             0,
		Status:               "",
		Locked:               false,
		LeaseTime:            time.Now(),
		DstParentID:          "",
		CopyStatus:           state.CopyStatus,
		DeleteStatus:         state.DeleteStatus,
		SrcLogicalPath:       logicalPath,
		SrcLogicalParentPath: logicalParent,
		SrcTraversalStatus:   state.TraversalStatus,
		ExclusionSource:      state.ExclusionSource,
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
func CompleteCopyTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	q.RecordExecutionTime(executionDelta)

	currentRound := task.Round
	nodeID := task.ID

	database := q.Database()
	if database == nil {
		q.RemoveInProgress(nodeID)
		return
	}

	task.Locked = false
	task.Status = "successful"

	q.IncrementRoundStatsCompleted(currentRound)
	q.IncrementTasksCompletedTotal()

	_ = q.ClearTransferCheckpoint(context.Background(), task)
	q.RecordTaskCompletion(currentRound, true)

	taskType := types.NodeTypeFile
	taskPath := worker.CopyTaskLogicalPath(task)
	parentPath := worker.CopyTaskLogicalParentPath(task)
	taskName := task.File.DisplayName
	taskSize := task.File.Size
	taskMTime := task.File.LastUpdated
	if task.IsFolder() {
		taskType = types.NodeTypeFolder
		taskName = task.Folder.DisplayName
		taskMTime = task.Folder.LastUpdated
	}
	if taskName == "" {
		taskName = path.Base(taskPath)
	}

	parentDelete := task.SrcParentDeleteStatus
	deleteStatus := db.DeleteStatusAfterCopyComplete(parentDelete, task.DeleteStatus)

	copyStatus := db.CopyStatusSuccessful
	if task.ProgressAlreadyExists {
		copyStatus = db.CopyStatusAlreadyExisted
	}
	accepted := database.AppendStatusEvent("SRC", db.StatusEvent{
		ID:               nodeID,
		CopyStatus:       copyStatus,
		PrevCopyStatus:   task.CopyStatus,
		DeleteStatus:     deleteStatus,
		PrevDeleteStatus: task.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            currentRound,
		Size:             taskSize,
		NodeType:         taskType,
		ExclusionSource:  task.ExclusionSource,
	}, false)
	dstParentID := db.MintNodeID("DST", "", db.NodeTypeFolder, "/")
	if task.DstParentNodeID != "" {
		dstParentID = task.DstParentNodeID
	}
	createName := taskName
	if task.ResolvedDstName != "" {
		createName = db.NormalizeNodeBasename(task.ResolvedDstName)
	}
	dstNodeID := db.MintNodeID("DST", dstParentID, taskType, createName)
	var dstServiceID string
	if task.IsFolder() {
		dstServiceID = task.Folder.ServiceID
	} else {
		dstServiceID = task.File.ServiceID
	}
	dstNode := &db.NodeState{
		ID:              dstNodeID,
		ServiceID:       dstServiceID,
		ParentID:        dstParentID,
		ParentServiceID: task.DstParentID,
		ParentPath:      parentPath,
		Name:            createName,
		Path:            taskPath, // display path stays SRC-original; create uses ResolvedDstName
		Type:            taskType,
		Size:            taskSize,
		MTime:           taskMTime,
		Depth:           currentRound,
		TraversalStatus: db.StatusSuccessful,
		Status:          db.StatusSuccessful,
	}
	if err := database.AppendDiscoveredNodes([]db.InsertOperation{
		{QueueType: "DST", Level: currentRound, Status: db.StatusSuccessful, State: dstNode},
	}); err != nil {
		if logservice.LS != nil {
			logErr := logservice.LS.Log("error",
				fmt.Sprintf("DST node persist failed after copy for %s (id=%s dst=%s): %v",
					task.LocationPath(), nodeID, dstNodeID, err),
				"queue", q.Name(), q.Name())
			if logErr != nil {
				fmt.Println("error logging", logErr)
			}
		}
	}
	parentDepth := currentRound - 1
	if parentDepth < 0 {
		parentDepth = 0
	}
	if err := database.AppendKidTicket(opsdb.SideDST, dstParentID, parentDepth, opsdb.KidRecord{
		ID: dstNodeID, ServiceID: dstServiceID, ParentServiceID: task.DstParentID,
		Path: taskPath, ParentPath: parentPath, Name: createName, Type: taskType,
		Size: taskSize, MTime: taskMTime, Depth: currentRound,
		TraversalStatus: db.StatusSuccessful,
	}); err != nil && logservice.LS != nil {
		_ = logservice.LS.Log("error", fmt.Sprintf("dst kids ticket failed for %s: %v", taskPath, err), "queue", q.Name(), q.Name())
	}
	database.AppendIDMapEvent(db.IDMapEvent{
		SrcInternalID: nodeID,
		DstInternalID: dstNodeID,
		Source:        db.IDMapSourceDSTCompare,
		Status:        db.IDMapStatusActive,
	})

	if !accepted {
		q.RemoveInProgress(nodeID)
		return
	}
	outcome := queue.TerminalProgressCopied
	if task.ProgressAlreadyExists {
		outcome = queue.TerminalProgressAlreadyExists
	}
	q.RecordTerminalProgress(outcome, nodeID, task.IsFolder(), task.IsFile(), task.BytesTransferred)
}

// FailCopyTask handles failure of copy tasks.
// Updates copy status to failed if max retries exceeded, or back to pending if retrying.
func FailCopyTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	// Record execution time delta (even for failures)
	q.RecordExecutionTime(executionDelta)

	currentRound := task.Round
	nodeID := task.ID
	maxRetries := q.MaxRetries()

	if logservice.LS != nil {
		err := logservice.LS.Log("debug",
			fmt.Sprintf("Failing copy task: id=%s path=%s round=%d pass=%d attempts=%d maxRetries=%d",
				nodeID, task.LocationPath(), currentRound, task.CopyPass, task.Attempts, maxRetries),
			"queue", q.Name(), q.Name())
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	task.Attempts++

	// Permanent FS errors should not retry — they block round completion and waste API quota.
	if queue.IsNonRetryableCopyError(task.LastError) {
		task.Attempts = maxRetries
	}

	// Check if we should retry
	if task.Attempts < maxRetries {
		// Retry: re-enqueue to memory, DON'T write to DuckDB (stays as in-progress)
		task.Locked = false
		q.RemoveInProgress(nodeID)
		if !q.Add(task) {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error",
					fmt.Sprintf("retry re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
					"queue", q.Name(), q.Name())
			}
		}

		if logservice.LS != nil {
			err := logservice.LS.Log("debug",
				fmt.Sprintf("Copy task will retry: path=%s attempts=%d", task.LocationPath(), task.Attempts),
				"queue", q.Name(), q.Name())
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return
	}

	// Max retries exceeded - permanent failure
	task.Locked = false
	task.Status = "failed"

	q.IncrementRoundStatsCompleted(currentRound)
	q.IncrementRoundStatsFailed(currentRound)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(currentRound, false)

	if logservice.LS != nil {
		err := logservice.LS.Log("error",
			fmt.Sprintf("Copy task failed (max retries): path=%s", task.LocationPath()),
			"queue", q.Name(), q.Name())
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	database := q.Database()
	if database == nil {
		q.RemoveInProgress(nodeID)
		return
	}

	// Record task error if present
	if task.LastError != "" {
		database.AppendTaskError("SRC", "copy", nodeID, task.LastError, task.Attempts, task.LocationPath())
	}

	failType := types.NodeTypeFile
	failSize := task.File.Size
	if task.IsFolder() {
		failType = types.NodeTypeFolder
		failSize = 0
	}
	copyEv := db.StatusEvent{
		ID:              nodeID,
		CopyStatus:      db.CopyStatusFailed,
		PrevCopyStatus:  task.CopyStatus,
		EventTime:       time.Now().UnixNano(),
		Depth:           currentRound,
		Size:            failSize,
		NodeType:        failType,
		ExclusionSource: task.ExclusionSource,
	}
	failurelog.AttachTaskFailureLog(&copyEv, "copy", q.Name(), nodeID, task.LocationPath(), task.Attempts, task.LastError)
	accepted := database.AppendStatusEvent("SRC", copyEv, false)

	// Folder failure cascades: mark pending descendants failed in the same write pass
	// that accumulates SelectedBytes (no separate recount scan for progress).
	if task.IsFolder() {
		taskPath := db.NormalizeSubtreeRootPathForPropagation(task.LocationPath())
		if taskPath != "" && taskPath != "/" {
			if mut, err := subtree.PropagateCopyFailure(database, taskPath); err == nil {
				failSize = mut.SelectedBytes
			}
		}
	}
	if !accepted {
		q.RemoveInProgress(nodeID)
		return
	}
	q.RecordTerminalProgress(queue.TerminalProgressFailed, nodeID, task.IsFolder(), task.IsFile(), failSize)
}
