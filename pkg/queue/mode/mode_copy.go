// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"context"
	"fmt"
	"path"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/worker"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PullCopyTasks pulls copy tasks from DB for the current round.
// Pulls from SRC copy status buckets, filters by pass (folders vs files), and skips round 0.
// Uses getter/setter methods - no direct mutex access.
func PullCopyTasks(q *queue.Queue, force bool) queue.PullResult {
	ps, early, ok := beginPull(q, force, pullGate{skipRoundZero: true})
	if !ok {
		return early
	}
	defer ps.end()
	database, currentRound := ps.Database, ps.Round
	copyPass := q.GetCopyPass()
	nodeType := db.NodeTypeFolder
	if copyPass == 2 {
		nodeType = db.NodeTypeFile
	}
	page, early, ok := pullKeysetPage(q, currentRound, "ListNodesCopyKeyset", func(limit int) ([]db.FetchResult, error) {
		return pull.ListNodesCopyKeyset(database, currentRound, nodeType, q.GetKeysetCursor(), limit, db.CopyStatusPending)
	})
	if !ok {
		return early
	}
	matchedBatch := page.Matched

	// Prefetch SRC→DST map + DST peer copy status / service id (batched).
	srcIDs := make([]string, 0, len(matchedBatch))
	for _, item := range matchedBatch {
		if item.State != nil && item.State.ID != "" {
			srcIDs = append(srcIDs, item.State.ID)
		}
	}
	peerBySrc := map[string]struct {
		dstID     string
		copySt    string
		serviceID string
	}{}
	if ops := database.Ops(); ops != nil && len(srcIDs) > 0 {
		maps, mapErr := ops.BatchGetMap(srcIDs, false)
		if mapErr == nil {
			dstIDs := make([]string, 0, len(maps))
			for _, m := range maps {
				if m.DstID != "" {
					dstIDs = append(dstIDs, m.DstID)
				}
			}
			dstSt, _ := ops.BatchGetStatus(opsdb.SideDST, dstIDs)
			dstNodes, _ := ops.BatchGetNode(opsdb.SideDST, dstIDs)
			for srcID, m := range maps {
				if m.DstID == "" {
					continue
				}
				peerBySrc[srcID] = struct {
					dstID     string
					copySt    string
					serviceID string
				}{
					dstID:     m.DstID,
					copySt:    dstSt[m.DstID].CopyStatus,
					serviceID: dstNodes[m.DstID].ServiceID,
				}
			}
		}
	}

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
				err := logservice.LS.Log("error", fmt.Sprintf("Type mismatch for %s - pass=%d expects %s but node has %s - skipping", item.State.DisplayPath, copyPass, expectedType, item.State.Type), "queue", q.Name(), q.Name())
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
			_ = logservice.LS.Log("error", fmt.Sprintf("Item at round %d has empty ParentID (path=%s)", item.State.Depth, item.State.DisplayPath), "queue", q.Name(), q.Name())
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
			_ = logservice.LS.Log("error", fmt.Sprintf("No DST parent for %s (parent_path=%s parent_id=%s); enqueueing so copy still claims the task", item.State.DisplayPath, item.State.ParentPath, item.State.ParentID), "queue", q.Name(), q.Name())
		}
		task.DstParentID = dstParentServiceID
		task.DstParentNodeID = item.DstParentNodeID
		if item.DstParentNodeID == "" && (item.State.ParentPath == "" || item.State.ParentPath == "/") {
			task.DstParentNodeID = db.MintNodeID("DST", "", db.NodeTypeFolder, "/")
		}
		task.ResolvedDstName = item.ResolvedDstPath
		task.SrcParentDeleteStatus = item.SrcParentDeleteStatus
		if peer, ok := peerBySrc[task.ID]; ok {
			task.DstPeerID = peer.dstID
			task.DstPeerCopyStatus = peer.copySt
			task.DstPeerServiceID = peer.serviceID
		}

		if q.Add(task) {
			enqueueSuccessCount++
		}

	}

	return finishPull(q, currentRound, enqueueSuccessCount, page.Partial)
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
		LeaseTime:            time.Now(),
		CopyStatus:           state.CopyStatus,
		DeleteStatus:         state.DeleteStatus,
		SrcLogicalPath:       logicalPath,
		SrcLogicalParentPath: logicalParent,
		SrcTraversalStatus:   state.TraversalStatus,
		ExclusionSource:      state.ExclusionSource,
	}
	task.AttachNodeState(state)
	return task
}

// CompleteCopyTask handles successful completion of copy tasks.
// Updates copy status, creates DST node entry, and updates join-lookup mapping.
func CompleteCopyTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	database := beginTerminalSuccess(q, task, executionDelta)
	if database == nil {
		return
	}
	currentRound := task.Round
	nodeID := task.ID

	_ = q.ClearTransferCheckpoint(context.Background(), task)

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
	if task.DstPeerID != "" {
		dstNodeID = task.DstPeerID
	}
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
					task.DisplayPath, nodeID, dstNodeID, err),
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
	dstCopyStatus := db.CopyStatusSuccessful
	if task.ProgressAlreadyExists {
		dstCopyStatus = db.CopyStatusAlreadyExisted
	}
	database.AppendStatusEvent("DST", db.StatusEvent{
		ID:             dstNodeID,
		CopyStatus:     dstCopyStatus,
		PrevCopyStatus: task.DstPeerCopyStatus,
		EventTime:      time.Now().UnixNano(),
		Depth:          currentRound,
		Size:           taskSize,
		NodeType:       taskType,
	}, false)

	outcome := queue.TerminalProgressCopied
	if task.ProgressAlreadyExists {
		outcome = queue.TerminalProgressAlreadyExists
	}
	q.ReleaseTaskInFlightWeight(task)
	recordTerminal(q, task, accepted, outcome, task.BytesTransferred)
}

// FailCopyTask handles failure of copy tasks.
// Updates copy status to failed if max retries exceeded, or back to pending if retrying.
func FailCopyTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	// Record execution time delta (even for failures)
	q.RecordExecutionTime(executionDelta)

	currentRound := task.Round
	nodeID := task.ID
	maxRetries := q.MaxRetries()

	logQueue(q, "debug", fmt.Sprintf("Failing copy task: id=%s path=%s round=%d pass=%d attempts=%d maxRetries=%d",
		nodeID, task.DisplayPath, currentRound, task.CopyPass, task.Attempts, maxRetries))

	if task.UserCancelRequested {
		if task.LastError == "" {
			task.LastError = "cancelled by user"
		}
		_ = q.ClearTransferCheckpoint(context.Background(), task)
		task.Attempts = maxRetries
	}

	task.Attempts++

	// Permanent FS errors should not retry; they block round completion and waste API quota.
	if queue.IsNonRetryableCopyError(task.LastError) {
		task.Attempts = maxRetries
	}

	// Retry: re-enqueue to memory, DON'T write to DB (stays as in-progress)
	if task.Attempts < maxRetries {
		if !requeueForRetry(q, task) {
			logQueue(q, "error", fmt.Sprintf("retry re-enqueue rejected for %s (id=%s) - task lost", task.DisplayPath, nodeID))
		}
		logQueue(q, "debug", fmt.Sprintf("Copy task will retry: path=%s attempts=%d", task.DisplayPath, task.Attempts))
		return
	}

	// Max retries exceeded - permanent failure
	logQueue(q, "error", fmt.Sprintf("Copy task failed (max retries): path=%s", task.DisplayPath))
	database := beginTerminalFailure(q, task)
	if database == nil {
		return
	}

	failType := types.NodeTypeFile
	failSize := task.File.Size
	if task.IsFolder() {
		failType = types.NodeTypeFolder
		failSize = 0
	}
	accepted := appendFailureEvent(q, task, database, "copy", db.StatusEvent{
		ID:              nodeID,
		CopyStatus:      db.CopyStatusFailed,
		PrevCopyStatus:  task.CopyStatus,
		EventTime:       time.Now().UnixNano(),
		Depth:           currentRound,
		Size:            failSize,
		NodeType:        failType,
		ExclusionSource: task.ExclusionSource,
	})

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
	recordTerminal(q, task, accepted, queue.TerminalProgressFailed, failSize)
}
