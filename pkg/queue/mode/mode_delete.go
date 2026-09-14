// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// CheckDeleteCompletion checks if the delete phase should switch passes or complete.
// Only called when pass 1 has swept all depths up to maxKnownDepth (forward BFS), mirroring copy.
// Pass 1 = folders, pass 2 = files. Trust per-round exhaustion; no global DB pending re-check.
func CheckDeleteCompletion(q *queue.Queue, currentRound int) bool {
	maxKnownDepth := q.GetMaxKnownDepth()

	if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
		return false
	}
	if q.InProgressCount() > 0 || q.GetPendingCount() > 0 {
		return false
	}

	info := q.RoundInfoReadOnly(currentRound)
	if info == nil || info.PullCount == 0 {
		return false
	}
	if !q.GetLastPullWasPartial() {
		return false
	}

	deletePass := q.GetCopyPass()
	if deletePass == 1 {
		q.SetCopyPass(2)
		q.ResetRoundStatsCompleted()
		q.SetRound(1)
		q.SetExpectedFromStatsBucket(1)
		q.SetLastPullWasPartial(false)
		return false
	}

	return q.MarkComplete("Delete phase complete - both passes finished")
}

// AdvanceDeleteRound handles delete-specific round advancement (forward BFS).
// Round completion is determined by lastPullWasPartial (memory/keyset only); we never query the DB for in-round advancement.
// When called, the current depth has just completed — increment depth within the same pass until maxKnownDepth, then check pass switch or phase complete.
func AdvanceDeleteRound(q *queue.Queue) {
	currentRound := q.GetRound()
	deletePass := q.GetCopyPass()
	maxKnownDepth := q.GetMaxKnownDepth()

	newRound := currentRound + 1
	if maxKnownDepth >= 0 && newRound > maxKnownDepth {
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", fmt.Sprintf("Pass %d exhausted depths (past maxKnownDepth %d), checking for completion", deletePass, maxKnownDepth), "queue", q.Name(), q.Name())
		}
		completed := q.CheckCompletion(currentRound, queue.CompletionCheckOptions{CheckFinalCompletion: true})
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
	if deletePass == 2 {
		passName = "files"
	}
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Advanced delete to depth %d (pass %d: %s)", newRound, deletePass, passName), "queue", q.Name(), q.Name())
	}
	q.PullWithRetryIfNeeded(true)
}

// PullDeleteTasks pulls delete tasks from DuckDB for the current forward-BFS round.
// Only pending_explicit (or failed in retry) nodes are pulled; pending_inherited are covered by ancestor recursive deletes.
func PullDeleteTasks(q *queue.Queue, force bool) queue.PullResult {
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

	deletePass := q.GetCopyPass()
	nodeType := db.NodeTypeFolder
	if deletePass == 2 {
		nodeType = db.NodeTypeFile
	}

	batchSize := q.EffectiveLeaseBatchSize()
	statusFilter := db.DeleteStatusPendingExplicit
	if q.GetMode() == queue.QueueModeDeleteRetry {
		statusFilter = db.DeleteStatusFailed
	}
	requestLimit := batchSize + 1
	q.BeginDBPull()
	defer q.ReleaseDBPull()
	pullStart := time.Now()
	results, err := pull.ListNodesDeleteKeyset(database, currentRound, nodeType, q.GetKeysetCursor(), requestLimit, statusFilter)
	q.RecordDBPull(len(results), time.Since(pullStart))
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error", fmt.Sprintf("ListNodesDeleteKeyset failed: %v", err), "queue", q.Name(), q.Name())
		}
		return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
	}
	if q.GetRound() != currentRound {
		return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
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
		q.SetKeysetCursor(results[cursorIdx].Key)
	}
	q.SetLastPullWasPartial(len(results) <= batchSize)

	enqueueSuccessCount := 0
	for _, item := range matchedBatch {
		taskType := queue.TaskTypeDeleteFolder
		expectedType := types.NodeTypeFolder
		if deletePass == 2 {
			taskType = queue.TaskTypeDeleteFile
			expectedType = types.NodeTypeFile
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
	q.SetLastPullWasPartial(partial)
	q.RecordPull(currentRound, enqueueSuccessCount, partial)
	q.SetFirstPullForRound(false)
	return queue.PullResult{Round: currentRound, Yield: enqueueSuccessCount, Partial: partial, QueriedDB: true, Status: queue.PullOK}
}

func nodeStateToDeleteTask(state *db.NodeState, taskType string, deletePass int) *queue.TaskBase {
	if state == nil {
		return nil
	}
	task := &queue.TaskBase{
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

// CompleteDeleteTask marks a delete task successful and cascades deleted to pending* under the path.
func CompleteDeleteTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
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
	q.RecordTaskCompletion(currentRound, true)

	bytes := int64(0)
	if task.IsFile() && task.File.Size > 0 {
		bytes = task.File.Size
	}

	rootPath := task.LocationPath()
	nodeType := db.NodeTypeFile
	if task.IsFolder() {
		nodeType = db.NodeTypeFolder
	}
	accepted := database.AppendStatusEvent("SRC", db.StatusEvent{
		ID:               nodeID,
		DeleteStatus:     db.DeleteStatusDeleted,
		PrevDeleteStatus: task.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            currentRound,
		Size:             task.File.Size,
		NodeType:         nodeType,
	}, false)
	var cascadeFolders, cascadeFiles, cascadeBytes int64
	if rootPath != "" && task.IsFolder() {
		mut, err := subtree.CascadeDeleteUnderPath(database, rootPath)
		if err == nil {
			cascadeFolders = mut.Folders
			cascadeFiles = mut.Files
			cascadeBytes = mut.SelectedBytes
		}
	}

	if !accepted {
		q.RemoveInProgress(nodeID)
		return
	}
	q.RecordTerminalProgress(queue.TerminalProgressDeleted, nodeID, task.IsFolder(), task.IsFile(), bytes)
	q.RecordCascadeDeletedProgress(cascadeFolders, cascadeFiles, cascadeBytes)
}

// FailDeleteTask handles delete task failure with retries.
func FailDeleteTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	q.RecordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID
	maxRetries := q.MaxRetries()
	task.Attempts++

	if task.Attempts < maxRetries {
		task.Locked = false
		q.RemoveInProgress(nodeID)
		_ = q.Add(task)
		return
	}

	task.Locked = false
	task.Status = "failed"
	q.IncrementRoundStatsCompleted(currentRound)
	q.IncrementRoundStatsFailed(currentRound)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(currentRound, false)

	database := q.Database()
	if database == nil {
		q.RemoveInProgress(nodeID)
		return
	}

	if task.LastError != "" {
		database.AppendTaskError("SRC", "delete", nodeID, task.LastError, task.Attempts, task.LocationPath())
	}

	delEv := db.StatusEvent{
		ID:               nodeID,
		DeleteStatus:     db.DeleteStatusFailed,
		PrevDeleteStatus: task.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            currentRound,
		Size:             task.File.Size,
		NodeType:         db.NodeTypeFile,
	}
	if task.IsFolder() {
		delEv.NodeType = db.NodeTypeFolder
	}
	failurelog.AttachTaskFailureLog(&delEv, "delete", q.Name(), nodeID, task.LocationPath(), task.Attempts, task.LastError)
	accepted := database.AppendStatusEvent("SRC", delEv, false)
	if !accepted {
		q.RemoveInProgress(nodeID)
		return
	}
	failBytes := int64(0)
	if task.IsFile() {
		failBytes = task.File.Size
	}
	q.RecordTerminalProgress(queue.TerminalProgressFailed, nodeID, task.IsFolder(), task.IsFile(), failBytes)
}
