// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

const TaskTypeGPL = "gpl-revalidate"

// PullGPLTasks pulls nodes with gpl_status=pending at the current depth (BFS cascade).
func (q *Queue) PullGPLTasks(force bool) PullResult {
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
	defer q.setPulling(false)

	snapshot := q.getStateSnapshot()
	if !force {
		if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return PullResult{Round: snapshot.Round, Status: PullSkipped}
		}
	} else if snapshot.State == QueueStatePaused {
		return PullResult{Round: snapshot.Round, Status: PullAborted}
	}

	currentRound := snapshot.Round
	queueType := getQueueType(q.name)
	batchSize := q.EffectiveLeaseBatchSize()
	requestLimit := batchSize + 1

	var cursor string
	if q.name == "dst" {
		cursor = q.getDstKeysetCursor()
	} else {
		cursor = q.getSrcKeysetCursor()
	}

	results, err := db.ListNodesGPLKeyset(database, queueType, currentRound, cursor, db.GPLStatusPending, requestLimit)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch GPL batch: %v", err), "queue", q.name, q.name)
		}
		return PullResult{Round: currentRound, Status: PullSkipped}
	}
	if q.GetRound() != currentRound {
		return PullResult{Round: currentRound, QueriedDB: true, Status: PullStaleRound}
	}
	if len(results) == 0 {
		q.setLastPullWasPartial(true)
		q.recordPull(currentRound, 0, true)
		return PullResult{Round: currentRound, Yield: 0, Partial: true, QueriedDB: true, Status: PullOK}
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
	if q.name == "dst" {
		q.setDstKeysetCursor(lastKey)
	} else {
		q.setSrcKeysetCursor(lastKey)
	}
	partial := len(results) <= batchSize
	q.setLastPullWasPartial(partial)
	q.recordPull(currentRound, count, partial)
	return PullResult{Round: currentRound, Yield: count, Partial: partial, QueriedDB: true, Status: PullOK}
}

func gplFetchToTask(fr db.FetchResult, round int) *TaskBase {
	if fr.State == nil {
		return nil
	}
	st := fr.State
	task := &TaskBase{
		ID:             st.ID,
		Type:           TaskTypeGPL,
		Round:          round,
		Status:         st.TraversalStatus,
		CopyStatus:     st.CopyStatus,
		DeleteStatus:   st.DeleteStatus,
		GPLState:       st.GPLState,
		ParentGPLState: fr.ParentGPLState,
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
func (q *Queue) CompleteGPLTask(task *TaskBase, executionDelta time.Duration) {
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID

	task.Locked = false
	task.Status = "successful"
	q.incrementRoundStatsCompleted(currentRound)
	q.incrementTasksCompletedTotal()
	q.recordTaskCompletion(currentRound, true)

	database := q.getDatabase()
	if database == nil {
		q.removeInProgress(nodeID)
		return
	}

	queueType := getQueueType(q.name)
	status := db.GPLStatusSuccessful
	if task.LastError != "" {
		status = db.GPLStatusFailed
	}
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:            nodeID,
		GPLStatus:     status,
		EventTime:     time.Now().UnixNano(),
		Depth:         task.Round,
		PrevGPLStatus: db.GPLStatusPending,
	}, false)
	q.removeInProgress(nodeID)
}

// FailGPLTask records gpl_status=failed for a cascade task.
func (q *Queue) FailGPLTask(task *TaskBase, executionDelta time.Duration) {
	q.recordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID
	task.Locked = false
	task.Status = "failed"
	q.recordTaskCompletion(currentRound, false)

	database := q.getDatabase()
	if database != nil {
		database.AppendStatusEvent(getQueueType(q.name), db.StatusEvent{
			ID:            nodeID,
			GPLStatus:     db.GPLStatusFailed,
			EventTime:     time.Now().UnixNano(),
			Depth:         task.Round,
			PrevGPLStatus: db.GPLStatusPending,
		}, false)
	}
	q.removeInProgress(nodeID)
}

// ProcessGPLTaskSRC runs path-only revalidation for a SRC node and updates gpl_state / path_events.
func ProcessGPLTaskSRC(database *db.DB, target gpl.Target, task *TaskBase) error {
	if database == nil || task == nil {
		return fmt.Errorf("nil database or task")
	}
	parentParts := parseGPLParts(task.ParentGPLState)
	base := task.ResolvedDstName
	if base == "" {
		base = db.NormalizeNodeBasename(task.LocationPath())
	}
	parts := append(append([]string(nil), parentParts...), base)
	merged, pathIssues, err := evaluateGPLPathOnly(target, parts, task.GPLState, task.IsFile())
	if err != nil {
		return err
	}
	task.GPLState = merged

	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.UpdateNodeGPLState(task.ID, merged); err != nil {
				return err
			}
			if len(pathIssues) == 0 {
				return nil
			}
			issuesJSON, _ := json.Marshal(pathIssues)
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID:           task.ID,
				EventTime:    time.Now().UnixNano(),
				Category:     db.PathEventCategoryGPLClean,
				ProposedPath: base,
				Status:       db.PathEventStatusPending,
				GPLIssues:    string(issuesJSON),
			}})
		})
	})
}
