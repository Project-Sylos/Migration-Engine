// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PullDeleteTasks pulls delete tasks from DB for the current forward-BFS round.
// Only pending_explicit (or failed in retry) nodes are pulled; pending_inherited are covered by ancestor recursive deletes.
func PullDeleteTasks(q *queue.Queue, force bool) queue.PullResult {
	ps, early, ok := beginPull(q, force, pullGate{skipRoundZero: true})
	if !ok {
		return early
	}
	defer ps.end()
	database, currentRound := ps.Database, ps.Round
	deletePass := q.GetCopyPass()
	nodeType := db.NodeTypeFolder
	if deletePass == 2 {
		nodeType = db.NodeTypeFile
	}
	statusFilter := db.DeleteStatusPendingExplicit
	if q.GetMode() == queue.QueueModeDeleteRetry {
		statusFilter = db.DeleteStatusFailed
	}
	page, early, ok := pullKeysetPage(q, currentRound, "ListNodesDeleteKeyset", func(limit int) ([]db.FetchResult, error) {
		return pull.ListNodesDeleteKeyset(database, currentRound, nodeType, q.GetKeysetCursor(), limit, statusFilter)
	})
	if !ok {
		return early
	}

	enqueueSuccessCount := 0
	for _, item := range page.Matched {
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
	return finishPull(q, currentRound, enqueueSuccessCount, page.Partial)
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
		CopyStatus:         state.CopyStatus,
		DeleteStatus:       state.DeleteStatus,
		SrcTraversalStatus: state.TraversalStatus,
		LeaseTime:          time.Now(),
	}
	task.AttachNodeState(state)
	return task
}

// CompleteDeleteTask marks a delete task successful and cascades deleted to pending* under the path.
func CompleteDeleteTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	database := beginTerminalSuccess(q, task, executionDelta)
	if database == nil {
		return
	}
	currentRound := task.Round
	nodeID := task.ID

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

	q.ReleaseTaskInFlightWeight(task)
	if recordTerminal(q, task, accepted, queue.TerminalProgressDeleted, bytes) {
		q.RecordCascadeDeletedProgress(cascadeFolders, cascadeFiles, cascadeBytes)
	}
}

// FailDeleteTask handles delete task failure with retries.
func FailDeleteTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	q.RecordExecutionTime(executionDelta)
	maxRetries := q.MaxRetries()
	if task.UserCancelRequested {
		if task.LastError == "" {
			task.LastError = "cancelled by user"
		}
		task.Attempts = maxRetries
	}
	task.Attempts++

	if task.Attempts < maxRetries {
		requeueForRetry(q, task)
		return
	}

	database := beginTerminalFailure(q, task)
	if database == nil {
		return
	}

	delEv := db.StatusEvent{
		ID:               task.ID,
		DeleteStatus:     db.DeleteStatusFailed,
		PrevDeleteStatus: task.DeleteStatus,
		EventTime:        time.Now().UnixNano(),
		Depth:            task.Round,
		Size:             task.File.Size,
		NodeType:         db.NodeTypeFile,
	}
	if task.IsFolder() {
		delEv.NodeType = db.NodeTypeFolder
	}
	accepted := appendFailureEvent(q, task, database, "delete", delEv)
	failBytes := int64(0)
	if task.IsFile() {
		failBytes = task.File.Size
	}
	recordTerminal(q, task, accepted, queue.TerminalProgressFailed, failBytes)
}
