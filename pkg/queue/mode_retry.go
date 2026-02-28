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
// Checks maxKnownDepth and scans all known levels up to maxKnownDepth, then uses normal traversal logic for deeper levels.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullRetryTasks(force bool) {
	database := q.getDatabase()
	if database == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	if q.getPulling() {
		return
	}

	// Don't pull if queue is completed (prevents deadlock on coordinator gate)
	if q.State() == QueueStateCompleted {
		return
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
		// Only pull if queue is running (not paused or completed)
		if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return
		}
	} else {
		// Even when forcing, don't pull if paused
		if snapshot.State == QueueStatePaused {
			return
		}
	}

	currentRound := snapshot.Round
	maxKnownDepth := q.getMaxKnownDepth()

	// For DST: Check coordinator gate before pulling
	coordinator := q.getCoordinator()
	if q.name == "dst" && coordinator != nil {
		canStartRound := coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - wait for coordinator gate
			return
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
		batchSize := effectiveLeaseBatchSize()
		var batch []db.FetchResult
		var expectedFoldersMap map[string][]types.Folder
		var expectedFilesMap map[string][]types.File
		var srcIDMap map[string]map[string]string
		var srcIDToMeta map[string]SrcNodeMeta
		var err error

		if q.name == "dst" {
			var childrenByDstID map[string][]*db.NodeState
			batch, childrenByDstID, err = db.ListDstBatchWithSrcChildren(database, currentRound, q.getDstKeysetCursor(), batchSize, db.StatusPending)
			if err == nil && len(batch) > 0 {
				q.setDstKeysetCursor(batch[len(batch)-1].Key)
				expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta = BuildExpectedMapsFromDstWithChildren(batch, childrenByDstID)
			}
			if err != nil {
				batch = nil
			}
		} else {
			batch, err = db.ListNodesByDepthKeyset(database, queueType, currentRound, q.getSrcKeysetCursor(), db.StatusPending, batchSize)
			if err == nil && len(batch) > 0 {
				q.setSrcKeysetCursor(batch[len(batch)-1].Key)
			}
		}
		if err != nil {
			if logservice.LS != nil {
				err := logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch: %v", err), "queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			return
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
				if q.isLeased(item.Key) {
					continue
				}
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
			// Skip ULIDs we've already leased
			if q.isLeased(item.Key) {
				continue
			}

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

			// Enqueue task - only mark as leased if enqueue succeeds
			if q.Add(task) {
				q.addLeasedKey(item.Key)
				enqueuedCount++
			}
		}

		// Track if pull was partial based on actual enqueued count
		// Even if we enqueued 0 (all were leased), we still record the pull
		// so the queue can properly advance rounds and check completion
		wasPartial := len(batch) < batchSize
		q.setLastPullWasPartial(wasPartial)

		// Record pull in RoundInfo
		q.recordPull(currentRound, len(batch), wasPartial)
		q.setFirstPullForRound(false)

		return
	}

	// For rounds > maxKnownDepth, use normal traversal pull logic
	// This allows discovering new deeper levels
	// Note: PullTraversalTasks will handle incrementing counters itself
	q.PullTraversalTasks(force)
}
