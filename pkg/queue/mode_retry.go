// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PullRetryTasks pulls retry tasks from failed/pending status buckets.
func (q *Queue) PullRetryTasks(force bool) PullResult {
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
	defer func() {
		q.setPulling(false)
	}()

	// Get state snapshot
	snapshot := q.getStateSnapshot()

	if !force {
		if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return PullResult{Round: snapshot.Round, Status: PullSkipped}
		}
	} else if snapshot.State == QueueStatePaused {
		return PullResult{Round: snapshot.Round, Status: PullAborted}
	}

	currentRound := snapshot.Round
	maxKnownDepth := q.getMaxKnownDepth()

	// For DST: Check coordinator gate before pulling
	coordinator := q.getCoordinator()
	if q.name == "dst" && coordinator != nil {
		canStartRound := coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			return PullResult{Round: currentRound, Status: PullSkipped}
		}
	}

	// If maxKnownDepth is not set, get it from the stats table (max depth with any stats)
	if maxKnownDepth == -1 {
		d, err := database.GetMaxDepth(getQueueType(q.name))
		if err == nil {
			q.SetMaxKnownDepth(d)
			maxKnownDepth = d
		}
	}

	// If current round <= maxKnownDepth, scan all known levels with pending status (keyset + status filter)
	if maxKnownDepth >= 0 && currentRound <= maxKnownDepth {
		queueType := getQueueType(q.name)
		batchSize := q.EffectiveLeaseBatchSize()
		requestLimit := batchSize + 1
		var batch []db.FetchResult
		var expectedFoldersMap map[string][]types.Folder
		var expectedFilesMap map[string][]types.File
		var srcIDMap map[string]map[string]string
		var srcIDToMeta map[string]SrcNodeMeta
		var err error

		rawResultCount := 0
		if q.name == "dst" {
			var childrenByDstID map[string][]*db.NodeState
			batch, childrenByDstID, err = db.ListDstBatchWithSrcChildren(database, currentRound, q.getDstKeysetCursor(), requestLimit, db.StatusPending)
			if err == nil && len(batch) > 0 {
				rawResultCount = len(batch)
				processLimit := batchSize
				if len(batch) <= batchSize {
					processLimit = len(batch)
				}
				q.setDstKeysetCursor(batch[processLimit-1].Key)
				expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta = BuildExpectedMapsFromDstWithChildren(batch[:processLimit], childrenByDstID)
				batch = batch[:processLimit]
			}
			if err != nil {
				batch = nil
			}
		} else {
			batch, err = db.ListNodesByDepthKeyset(database, queueType, currentRound, q.getSrcKeysetCursor(), db.StatusPending, requestLimit)
			if err == nil && len(batch) > 0 {
				rawResultCount = len(batch)
				processLimit := batchSize
				if len(batch) <= batchSize {
					processLimit = len(batch)
				}
				q.setSrcKeysetCursor(batch[processLimit-1].Key)
				batch = batch[:processLimit]
			}
		}
		if err != nil {
			if logservice.LS != nil {
				err := logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch: %v", err), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			return PullResult{Round: currentRound, Status: PullSkipped}
		}
		if len(batch) == 0 && q.name == "dst" {
			expectedFoldersMap = make(map[string][]types.Folder)
			expectedFilesMap = make(map[string][]types.File)
			srcIDMap = make(map[string]map[string]string)
			srcIDToMeta = make(map[string]SrcNodeMeta)
		}

		taskType := TaskTypeSrcTraversal
		if q.name == "dst" {
			taskType = TaskTypeDstTraversal
		}

		// For SRC: Batch-load retry DST cleanup (DST counterpart + children meta) for folder tasks
		var retryDstCleanupMap map[string]*RetryDstCleanup
		if q.name == "src" {
			var srcFolderIDs []string
			for _, item := range batch {
				task := nodeStateToTask(item.State, taskType)
				if task != nil && task.IsFolder() {
					srcFolderIDs = append(srcFolderIDs, item.State.ID)
				}
			}
			if len(srcFolderIDs) > 0 {
				var loadErr error
				retryDstCleanupMap, loadErr = BatchLoadRetryDstCleanup(database, srcFolderIDs)
				if loadErr != nil {
					if logservice.LS != nil {
						err := logservice.LS.Log("debug", fmt.Sprintf("Failed to batch load retry DST cleanup: %v", loadErr), "queue", q.name, q.name)
						if err != nil {
							fmt.Println("error logging", err)
						}
					}
					retryDstCleanupMap = make(map[string]*RetryDstCleanup)
				}
			} else {
				retryDstCleanupMap = make(map[string]*RetryDstCleanup)
			}
		}

		enqueuedCount := 0
		for _, item := range batch {
			task := nodeStateToTask(item.State, taskType)
			// Ensure task has the ULID from the database
			if task != nil && task.ID == "" {
				task.ID = item.State.ID
			}

			// For SRC folder tasks in retry, attach preloaded DST cleanup data
			if q.name == "src" && task != nil && task.IsFolder() && retryDstCleanupMap != nil {
				if c, ok := retryDstCleanupMap[task.ID]; ok {
					task.RetryDstCleanup = c
				}
			}

			// For DST folder tasks, populate ExpectedFolders/ExpectedFiles and ExpectedSrcNodeMeta
			if q.name == "dst" && task.IsFolder() {
				dstID := item.State.ID
				task.ExpectedFolders = expectedFoldersMap[dstID]
				task.ExpectedFiles = expectedFilesMap[dstID]
				if srcIDMap != nil {
					task.ExpectedSrcIDMap = srcIDMap[dstID]
				}
				if srcIDToMeta != nil && task.ExpectedSrcIDMap != nil {
					task.ExpectedSrcNodeMeta = make(map[string]SrcNodeMeta)
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
		q.setLastPullWasPartial(partial)
		q.recordPull(currentRound, enqueuedCount, partial)
		q.setFirstPullForRound(false)
		return PullResult{Round: currentRound, Yield: enqueuedCount, Partial: partial, QueriedDB: true, Status: PullOK}
	}

	return q.PullTraversalTasks(force)
}
