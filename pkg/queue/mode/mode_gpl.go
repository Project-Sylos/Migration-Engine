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
	ps, early, ok := beginPull(q, force, pullGate{runningOnly: true})
	if !ok {
		return early
	}
	defer ps.end()
	database, currentRound := ps.Database, ps.Round
	queueType := queue.GetQueueType(q.Name())
	batchSize := q.EffectiveLeaseBatchSize()
	requestLimit := batchSize + 1

	cursor := q.GetKeysetCursor()

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

	processLimit := min(batchSize, len(results))
	count := 0
	for _, fr := range results[:processLimit] {
		task := gplFetchToTask(fr, currentRound)
		if task == nil {
			continue
		}
		if q.Add(task) {
			count++
		}
	}
	q.SetKeysetCursor(results[processLimit-1].Key)
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
		q.ReleaseTaskInFlightWeight(task)
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
	q.ReleaseTaskInFlightWeight(task)
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
