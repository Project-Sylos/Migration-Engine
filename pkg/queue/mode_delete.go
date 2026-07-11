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

// CheckDeleteCompletion checks if the delete phase should switch passes or complete.
// Delete uses reverse BFS (max depth → 1) with pass 1 = files, pass 2 = folders.
func (q *Queue) CheckDeleteCompletion(currentRound int) bool {
	if currentRound > 1 {
		return false
	}
	if q.InProgressCount() > 0 || q.GetPendingCount() > 0 {
		return false
	}
	deletePass := q.GetCopyPass()
	if deletePass == 1 {
		q.SetCopyPass(2)
		q.resetRoundStatsCompleted()
		q.setExpectedFromStatsBucket(currentRound)
		q.setLastPullWasPartial(false)
		return false
	}
	database := q.getDatabase()
	if database != nil {
		if counts, err := database.GetDeleteStatusCountsFromEvents(); err == nil && counts.Pending > 0 {
			return false
		}
	}
	return q.markComplete("Delete phase complete - both passes finished")
}

// AdvanceDeleteRound handles delete-specific round advancement (reverse BFS).
func (q *Queue) AdvanceDeleteRound() {
	currentRound := q.GetRound()
	deletePass := q.GetCopyPass()

	if deletePass == 1 {
		q.SetCopyPass(2)
		q.resetRoundStatsCompleted()
		q.setExpectedFromStatsBucket(currentRound)
		q.setLastPullWasPartial(false)
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", fmt.Sprintf("Delete pass 1 (files) done at depth %d, starting pass 2 (folders)", currentRound), "queue", q.name, q.name)
		}
		q.pullWithRetryIfNeeded(true)
		return
	}

	newRound := currentRound - 1
	if newRound < 1 {
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "Delete rounds exhausted at depth 1, checking completion", "queue", q.name, q.name)
		}
		completed := q.checkCompletion(currentRound, CompletionCheckOptions{CheckFinalCompletion: true})
		if completed {
			return
		}
		q.pullWithRetryIfNeeded(true)
		return
	}

	q.SetCopyPass(1)
	q.SetRound(newRound)
	q.setExpectedFromStatsBucket(newRound)
	q.setLastPullWasPartial(false)
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced delete to depth %d (pass 1: files)", newRound), "queue", q.name, q.name)
	}
	q.pullWithRetryIfNeeded(true)
}

// PullDeleteTasks pulls delete tasks from DuckDB for the current reverse-BFS round.
func (q *Queue) PullDeleteTasks(force bool) PullResult {
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

	q.setPulling(true)
	defer func() { q.setPulling(false) }()

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

	deletePass := q.GetCopyPass()
	nodeType := db.NodeTypeFile
	if deletePass == 2 {
		nodeType = db.NodeTypeFolder
	}

	batchSize := q.EffectiveLeaseBatchSize()
	statusFilter := db.DeleteStatusPending
	if q.GetMode() == QueueModeDeleteRetry {
		statusFilter = db.DeleteStatusFailed
	}
	requestLimit := batchSize + 1
	results, err := db.ListNodesDeleteKeyset(database, currentRound, nodeType, q.getCopyKeysetCursor(), requestLimit, statusFilter)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("ListNodesDeleteKeyset failed: %v", err), "queue", q.name, q.name)
		}
		return PullResult{Round: currentRound, Status: PullSkipped}
	}
	if q.GetRound() != currentRound {
		return PullResult{Round: currentRound, QueriedDB: true, Status: PullStaleRound}
	}

	processLimit := min(batchSize, len(results))
	var matchedBatch []db.FetchResult
	for _, r := range results[:processLimit] {
		if r.State == nil {
			continue
		}
		matchedBatch = append(matchedBatch, r)
	}
	if len(results) > 0 {
		cursorIdx := max(0, processLimit-1)
		q.setCopyKeysetCursor(results[cursorIdx].Key)
	}
	q.setLastPullWasPartial(len(results) <= batchSize)

	// Folder gate: block parents whose direct children are not all deleted.
	var folderIDs []string
	for _, item := range matchedBatch {
		if item.State.Type == db.NodeTypeFolder {
			folderIDs = append(folderIDs, item.State.ID)
		}
	}
	blocked := make(map[string]bool)
	if len(folderIDs) > 0 {
		blocked, err = db.FolderDeleteBlockedIDs(database, folderIDs)
		if err != nil && logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("FolderDeleteBlockedIDs failed: %v", err), "queue", q.name, q.name)
		}
	}

	enqueueSuccessCount := 0
	for _, item := range matchedBatch {
		if item.State.Type == db.NodeTypeFolder && blocked[item.State.ID] {
			q.emitDeleteBlockedFailure(item.State)
			continue
		}

		taskType := TaskTypeDeleteFile
		expectedType := types.NodeTypeFile
		if deletePass == 2 {
			taskType = TaskTypeDeleteFolder
			expectedType = types.NodeTypeFolder
		}
		if item.State.Type != expectedType {
			continue
		}

		task := nodeStateToDeleteTask(item.State, taskType, deletePass)
		if task != nil && task.ID == "" {
			task.ID = item.State.ID
		}
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

func (q *Queue) emitDeleteBlockedFailure(state *db.NodeState) {
	database := q.getDatabase()
	if database == nil || state == nil {
		return
	}
	currentRound := state.Depth
	q.incrementRoundStatsCompleted(currentRound)
	q.incrementRoundStatsFailed(currentRound)
	q.incrementTasksCompletedTotal()
	ev := db.StatusEvent{
		ID:               state.ID,
		TraversalStatus:  state.TraversalStatus,
		CopyStatus:       state.CopyStatus,
		DeleteStatus:     db.DeleteStatusFailed,
		PrevDeleteStatus: state.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            state.Depth,
		ErrorLogMessage:  "copy_blocked: not all direct children deleted",
		ErrorLogDetail:   "copy_blocked",
		ErrorLogQueue:    q.name,
	}
	db.AttachTaskFailureLog(&ev, "delete", q.name, state.ID, state.Path, 1, ev.ErrorLogMessage)
	database.AppendStatusEvent("SRC", ev, false)
	database.AppendTaskError("SRC", "delete", state.ID, ev.ErrorLogMessage, 1, state.Path)
}

func nodeStateToDeleteTask(state *db.NodeState, taskType string, deletePass int) *TaskBase {
	if state == nil {
		return nil
	}
	task := &TaskBase{
		ID:                 state.ID,
		Type:               taskType,
		Round:              state.Depth,
		CopyPass:           deletePass,
		Attempts:           0,
		CopyStatus:         state.CopyStatus,
		DeleteStatus:       state.DeleteStatus,
		SrcTraversalStatus: state.TraversalStatus,
		LeaseTime:          time.Now(),
	}
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

// CompleteDeleteTask marks a delete task successful.
func (q *Queue) CompleteDeleteTask(task *TaskBase, executionDelta time.Duration) {
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

	q.mu.Lock()
	if task.IsFolder() {
		q.foldersCreatedTotal++
	} else if task.IsFile() {
		q.filesCreatedTotal++
	}
	q.mu.Unlock()

	database.AppendStatusEvent("SRC", db.StatusEvent{
		ID:               nodeID,
		TraversalStatus:  task.SrcTraversalStatus,
		CopyStatus:       task.CopyStatus,
		DeleteStatus:     db.DeleteStatusDeleted,
		PrevDeleteStatus: task.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            currentRound,
	}, false)

	q.removeInProgress(nodeID)
}

// FailDeleteTask handles delete task failure with retries.
func (q *Queue) FailDeleteTask(task *TaskBase, executionDelta time.Duration) {
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID
	maxRetries := q.getMaxRetries()
	task.Attempts++

	if task.Attempts < maxRetries {
		task.Locked = false
		q.removeInProgress(nodeID)
		_ = q.Add(task)
		return
	}

	task.Locked = false
	task.Status = "failed"
	q.incrementRoundStatsCompleted(currentRound)
	q.incrementRoundStatsFailed(currentRound)
	q.incrementTasksCompletedTotal()
	q.recordTaskCompletion(currentRound, false)

	database := q.getDatabase()
	if database == nil {
		q.removeInProgress(nodeID)
		return
	}

	if task.LastError != "" {
		database.AppendTaskError("SRC", "delete", nodeID, task.LastError, task.Attempts, task.LocationPath())
	}

	delEv := db.StatusEvent{
		ID:               nodeID,
		TraversalStatus:  task.SrcTraversalStatus,
		CopyStatus:       task.CopyStatus,
		DeleteStatus:     db.DeleteStatusFailed,
		PrevDeleteStatus: task.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            currentRound,
	}
	db.AttachTaskFailureLog(&delEv, "delete", q.name, nodeID, task.LocationPath(), task.Attempts, task.LastError)
	database.AppendStatusEvent("SRC", delEv, false)
	q.removeInProgress(nodeID)
}
