// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// PullGPLTasks pulls nodes with gpl_status=pending at the current depth (BFS cascade).
func PullGPLTasks(q *queue.Queue, force bool) queue.PullResult {
	if db.GPLDisabled {
		return queue.PullResult{Round: q.GetRound(), Status: queue.PullSkipped}
	}
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
	if !force {
		if snapshot.State != queue.QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return queue.PullResult{Round: snapshot.Round, Status: queue.PullSkipped}
		}
	} else if snapshot.State == queue.QueueStatePaused {
		return queue.PullResult{Round: snapshot.Round, Status: queue.PullAborted}
	}

	currentRound := snapshot.Round
	queueType := queue.GetQueueType(q.Name())
	batchSize := q.EffectiveLeaseBatchSize()
	requestLimit := batchSize + 1

	cursor := q.GetKeysetCursor()

	q.BeginDBPull()
	defer q.ReleaseDBPull()
	pullStart := time.Now()
	results, err := pull.ListNodesGPLKeyset(database, queueType, currentRound, cursor, db.GPLStatusPending, requestLimit)
	q.RecordDBPull(len(results), time.Since(pullStart))
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch GPL batch: %v", err), "queue", q.Name(), q.Name())
		}
		return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
	}
	if q.GetRound() != currentRound {
		return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
	}
	if len(results) == 0 {
		q.SetLastPullWasPartial(true)
		q.RecordPull(currentRound, 0, true)
		return queue.PullResult{Round: currentRound, Yield: 0, Partial: true, QueriedDB: true, Status: queue.PullOK}
	}

	processLimit := batchSize
	if len(results) <= batchSize {
		processLimit = len(results)
	}
	count := 0
	for i := 0; i < processLimit; i++ {
		fr := results[i]
		task := gplFetchToTask(fr, currentRound)
		if task == nil {
			continue
		}
		if q.Add(task) {
			count++
		}
	}
	lastKey := results[processLimit-1].Key
	q.SetKeysetCursor(lastKey)
	partial := len(results) <= batchSize
	q.SetLastPullWasPartial(partial)
	q.RecordPull(currentRound, count, partial)
	return queue.PullResult{Round: currentRound, Yield: count, Partial: partial, QueriedDB: true, Status: queue.PullOK}
}

func gplFetchToTask(fr db.FetchResult, round int) *queue.TaskBase {
	if fr.State == nil {
		return nil
	}
	st := fr.State
	task := &queue.TaskBase{
		ID:              st.ID,
		Type:            queue.TaskTypeGPL,
		Round:           round,
		Status:          st.TraversalStatus,
		CopyStatus:      st.CopyStatus,
		DeleteStatus:    st.DeleteStatus,
		GPLState:        st.GPLState,
		ParentGPLState:  fr.ParentGPLState,
		ResolvedDstName: fr.ResolvedDstPath,
	}
	name := st.Name
	if name == "" {
		name = db.NormalizeNodeBasename(st.Path)
	}
	if st.Type == db.NodeTypeFile {
		task.File.ServiceID = st.ServiceID
		task.File.DisplayName = name
		task.File.LocationPath = st.Path
		task.File.ParentId = st.ParentServiceID
		task.File.DepthLevel = st.Depth
		task.File.Size = st.Size
	} else {
		task.Folder.ServiceID = st.ServiceID
		task.Folder.DisplayName = name
		task.Folder.LocationPath = st.Path
		task.Folder.ParentId = st.ParentServiceID
		task.Folder.DepthLevel = st.Depth
	}
	return task
}

// CompleteGPLTask marks gpl_status successful/failed after path-scoped revalidation.
func CompleteGPLTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	q.RecordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID

	task.Locked = false
	task.Status = "successful"
	q.IncrementRoundStatsCompleted(currentRound)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(currentRound, true)

	database := q.Database()
	if database == nil {
		q.RemoveInProgress(nodeID)
		return
	}

	queueType := queue.GetQueueType(q.Name())
	status := db.GPLStatusSuccessful
	if task.LastError != "" {
		status = db.GPLStatusFailed
	}
	nodeType := db.NodeTypeFolder
	if task.IsFile() {
		nodeType = db.NodeTypeFile
	}
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:            nodeID,
		GPLStatus:     status,
		EventTime:     time.Now().UnixNano(),
		Depth:         task.Round,
		PrevGPLStatus: db.GPLStatusPending,
		NodeType:      nodeType,
	}, false)
	q.RemoveInProgress(nodeID)
}

// FailGPLTask records gpl_status=failed for a cascade task.
func FailGPLTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	q.RecordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID
	task.Locked = false
	task.Status = "failed"
	q.RecordTaskCompletion(currentRound, false)

	database := q.Database()
	if database != nil {
		nodeType := db.NodeTypeFolder
		if task.IsFile() {
			nodeType = db.NodeTypeFile
		}
		database.AppendStatusEvent(queue.GetQueueType(q.Name()), db.StatusEvent{
			ID:            nodeID,
			GPLStatus:     db.GPLStatusFailed,
			EventTime:     time.Now().UnixNano(),
			Depth:         task.Round,
			PrevGPLStatus: db.GPLStatusPending,
			NodeType:      nodeType,
		}, false)
	}
	q.RemoveInProgress(nodeID)
}
