// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PullRetryTasks pulls retry tasks from failed/pending status buckets.
// Rounds at or below maxKnownDepth use the retry pending keyset pull. Rounds past that
// floor (or when maxKnownDepth is unknown) delegate to traversal pull without holding the
// retry pull lock — nested TryBeginPulling would always Skip and block completion.
//
// When the round Expected pending count is already 0, the expensive full-depth keyset walk is
// skipped (empty partial pull) so empty retry depths advance immediately. SRC pending rows are
// loaded via *_current (not scanning every node at the depth).
func PullRetryTasks(q *queue.Queue, force bool) queue.PullResult {
	database := q.Database()
	if database == nil {
		return queue.PullResult{Status: queue.PullAborted}
	}

	maxKnownDepth := q.GetMaxKnownDepth()
	if maxKnownDepth == -1 {
		if d, err := stats.GetMaxDepth(database, queue.GetQueueType(q.Name())); err == nil {
			q.SetMaxKnownDepth(d)
			maxKnownDepth = d
		}
	}

	currentRound := q.GetRound()
	if maxKnownDepth < 0 || currentRound > maxKnownDepth {
		return q.PullTasks(queue.ModeTraversal, force)
	}

	if !q.TryBeginPulling() {
		return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
	}
	defer q.SetPulling(false)
	if q.State() == queue.QueueStateCompleted {
		return queue.PullResult{Status: queue.PullAborted}
	}
	if ctx := q.ShutdownCtx(); ctx != nil && ctx.Err() != nil {
		// Soft-stop mid-sweep: do not mark an empty partial frontier; Expected already
		// records whether more work remains at this depth.
		return queue.PullResult{Round: currentRound, Status: queue.PullAborted}
	}

	snapshot := q.StateSnapshot()

	if !force {
		if snapshot.State != queue.QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return queue.PullResult{Round: snapshot.Round, Status: queue.PullSkipped}
		}
	} else if snapshot.State == queue.QueueStatePaused {
		return queue.PullResult{Round: snapshot.Round, Status: queue.PullAborted}
	}

	currentRound = snapshot.Round

	// For DST: Check coordinator gate before pulling
	coordinator := q.Coordinator()
	if q.Name() == "dst" && coordinator != nil {
		canStartRound := coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
		}
	}

	expected := q.RoundExpected(currentRound)
	if expected < 0 {
		q.SetExpectedFromStatsBucket(currentRound)
		expected = q.RoundExpected(currentRound)
	}
	if expected == 0 {
		q.SetLastPullWasPartial(true)
		q.RecordPull(currentRound, 0, true)
		q.SetFirstPullForRound(false)
		return queue.PullResult{Round: currentRound, Yield: 0, Partial: true, QueriedDB: true, Status: queue.PullOK}
	}

	queueType := queue.GetQueueType(q.Name())
	batchSize := q.EffectiveLeaseBatchSize()
	requestLimit := batchSize + 1
	var batch []db.FetchResult
	var expectedFoldersMap map[string][]types.Folder
	var expectedFilesMap map[string][]types.File
	var srcIDMap map[string]map[string]string
	var srcIDToMeta map[string]queue.SrcNodeMeta
	var srcParentDeleteByDstID map[string]string
	var err error

	rawResultCount := 0
	dstPullPartial := false
	var retryDstCleanupMap map[string]*queue.RetryDstCleanup
	q.BeginDBPull()
	defer q.ReleaseDBPull()
	if q.Name() == "dst" {
		var childrenByDstID map[string][]*db.NodeState
		var lastEnqueuedID string
		taskQuota := batchSize
		childQuota := q.EffectiveDstPullChildQuota(taskQuota)
		pullStart := time.Now()
		batch, childrenByDstID, srcParentDeleteByDstID, lastEnqueuedID, dstPullPartial, err = pull.ListDstBatchWithSrcChildrenQuota(database, currentRound, q.GetKeysetCursor(), pull.DstPullQuota{
			MaxTasks:    taskQuota,
			MaxChildren: childQuota,
		}, db.StatusPending)
		q.RecordDBPull(len(batch), time.Since(pullStart))
		if err == nil && len(batch) > 0 {
			rawResultCount = len(batch)
			if lastEnqueuedID != "" {
				q.SetKeysetCursor(lastEnqueuedID)
			}
			expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta = queue.BuildExpectedMapsFromDstWithChildren(batch, childrenByDstID)
		}
		if err != nil {
			batch = nil
		}
	} else {
		pullStart := time.Now()
		batch, err = pull.ListNodesPendingAtDepthKeyset(database, queueType, currentRound, q.GetKeysetCursor(), requestLimit, db.NodeTypeFolder)
		q.RecordDBPull(len(batch), time.Since(pullStart))
		if err == nil && len(batch) > 0 {
			rawResultCount = len(batch)
			processLimit := batchSize
			if len(batch) <= batchSize {
				processLimit = len(batch)
			}
			q.SetKeysetCursor(batch[processLimit-1].Key)
			batch = batch[:processLimit]
		}
		if err == nil && len(batch) > 0 {
			var srcFolderIDs []string
			for _, item := range batch {
				task := queue.NodeStateToTask(item.State, queue.TaskTypeSrcTraversal)
				if task != nil && task.IsFolder() {
					srcFolderIDs = append(srcFolderIDs, item.State.ID)
				}
			}
			if len(srcFolderIDs) > 0 {
				var loadErr error
				retryDstCleanupMap, loadErr = queue.BatchLoadRetryDstCleanup(database, srcFolderIDs)
				if loadErr != nil {
					if logservice.LS != nil {
						err := logservice.LS.Log("debug", fmt.Sprintf("Failed to batch load retry DST cleanup: %v", loadErr), "queue", q.Name(), q.Name())
						if err != nil {
							fmt.Println("error logging", err)
						}
					}
					retryDstCleanupMap = make(map[string]*queue.RetryDstCleanup)
				}
			} else {
				retryDstCleanupMap = make(map[string]*queue.RetryDstCleanup)
			}
		}
	}
	if err != nil {
		if logservice.LS != nil {
			err := logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch: %v", err), "queue", q.Name(), q.Name())
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
	}
	if len(batch) == 0 && q.Name() == "dst" {
		expectedFoldersMap = make(map[string][]types.Folder)
		expectedFilesMap = make(map[string][]types.File)
		srcIDMap = make(map[string]map[string]string)
		srcIDToMeta = make(map[string]queue.SrcNodeMeta)
	}

	taskType := queue.TaskTypeSrcTraversal
	if q.Name() == "dst" {
		taskType = queue.TaskTypeDstTraversal
	}

	enqueuedCount := 0
	for _, item := range batch {
		task := queue.NodeStateToTask(item.State, taskType)
		// Ensure task has the ULID from the database
		if task != nil && task.ID == "" {
			task.ID = item.State.ID
		}

		// For SRC folder tasks in retry, attach preloaded DST cleanup data
		if q.Name() == "src" && task != nil && task.IsFolder() && retryDstCleanupMap != nil {
			if c, ok := retryDstCleanupMap[task.ID]; ok {
				task.RetryDstCleanup = c
			}
		}

		// For DST folder tasks, populate ExpectedFolders/ExpectedFiles and ExpectedSrcNodeMeta
		if q.Name() == "dst" && task.IsFolder() {
			dstID := item.State.ID
			task.ExpectedFolders = expectedFoldersMap[dstID]
			task.ExpectedFiles = expectedFilesMap[dstID]
			task.SrcParentDeleteStatus = srcParentDeleteByDstID[dstID]
			if srcIDMap != nil {
				task.ExpectedSrcIDMap = srcIDMap[dstID]
			}
			if srcIDToMeta != nil && task.ExpectedSrcIDMap != nil {
				task.ExpectedSrcNodeMeta = make(map[string]queue.SrcNodeMeta)
				for _, srcID := range task.ExpectedSrcIDMap {
					if meta, ok := srcIDToMeta[srcID]; ok {
						task.ExpectedSrcNodeMeta[srcID] = meta
					}
				}
			}
		}

		if q.Add(task) {
			enqueuedCount++
		}
	}

	partial := rawResultCount <= batchSize
	if q.Name() == "dst" {
		partial = dstPullPartial
	}
	q.SetLastPullWasPartial(partial)
	q.RecordPull(currentRound, enqueuedCount, partial)
	q.SetFirstPullForRound(false)
	return queue.PullResult{Round: currentRound, Yield: enqueuedCount, Partial: partial, QueriedDB: true, Status: queue.PullOK}
}
