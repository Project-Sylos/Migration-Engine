// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// PullRetryTasks pulls retry tasks from failed/pending status buckets.
// Checks maxKnownDepth and scans all known levels up to maxKnownDepth, then uses normal traversal logic for deeper levels.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullRetryTasks(force bool) {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	if q.getPulling() {
		return
	}

	// Don't pull if queue is completed (prevents deadlock on coordinator gate)
	if q.getState() == QueueStateCompleted {
		return
	}

	// Set pulling flag early and defer clearing it
	// This ensures only one thread can execute the pull logic at a time
	q.setPulling(true)
	defer func() {
		q.setPulling(false)
	}()

	// Force-flush buffer before pulling tasks to ensure we don't pull tasks
	// that are waiting in the buffer to be written
	outputBuffer := q.getOutputBuffer()
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

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

	// If maxKnownDepth is not set, try to compute it from existing levels
	if maxKnownDepth == -1 {
		levels, err := boltDB.GetAllLevels(getQueueType(q.name))
		if err == nil && len(levels) > 0 {
			maxKnownDepth = 0
			for _, level := range levels {
				if level > maxKnownDepth {
					maxKnownDepth = level
				}
			}
			q.setMaxKnownDepth(maxKnownDepth)
		}
	}

	// If current round <= maxKnownDepth, scan all known levels with pending/failed status
	if maxKnownDepth >= 0 && currentRound <= maxKnownDepth {
		// For retry sweep up to maxKnownDepth, pull from current round
		// All tasks should have been moved to pending before this sweep, so we only need to pull pending
		queueType := getQueueType(q.name)
		batch, err := db.BatchFetchWithKeys(boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch from BoltDB: %v", err), "queue", q.name, q.name)
			}
			return
		}

		taskType := TaskTypeSrcTraversal
		if q.name == "dst" {
			taskType = TaskTypeDstTraversal
		}

		// For DST: Batch-load expected children (same as traversal mode)
		var expectedFoldersMap map[string][]types.Folder
		var expectedFilesMap map[string][]types.File
		var srcIDMap map[string]map[string]string
		if q.name == "dst" {
			// Collect DST parent IDs for batch loading
			var dstParentIDs []string
			dstIDToPath := make(map[string]string)
			for _, item := range batch {
				if q.isLeased(item.Key) {
					continue
				}
				task := nodeStateToTask(item.State, taskType)
				if task.IsFolder() {
					dstParentIDs = append(dstParentIDs, item.State.ID)
					dstIDToPath[item.State.ID] = task.Folder.LocationPath
				}
			}

			// Batch-load expected children
			if len(dstParentIDs) > 0 {
				var err error
				expectedFoldersMap, expectedFilesMap, srcIDMap, err = BatchLoadExpectedChildrenByDSTIDs(boltDB, dstParentIDs, dstIDToPath)
				if err != nil {
					if logservice.LS != nil {
						_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to batch load expected children in retry mode: %v", err), "queue", q.name, q.name)
					}
					expectedFoldersMap = make(map[string][]types.Folder)
					expectedFilesMap = make(map[string][]types.File)
					srcIDMap = make(map[string]map[string]string)
				}
			} else {
				expectedFoldersMap = make(map[string][]types.Folder)
				expectedFilesMap = make(map[string][]types.File)
				srcIDMap = make(map[string]map[string]string)
			}
		}

		enqueuedCount := 0
		for _, item := range batch {
			// Skip ULIDs we've already leased
			if q.isLeased(item.Key) {
				continue
			}

			// Mark as leased
			q.addLeasedKey(item.Key)

			task := nodeStateToTask(item.State, taskType)
			// Ensure task has the ULID from the database
			if task != nil && task.ID == "" {
				task.ID = item.State.ID
			}

			// For DST folder tasks, populate ExpectedFolders/ExpectedFiles
			if q.name == "dst" && task.IsFolder() {
				dstID := item.State.ID
				task.ExpectedFolders = expectedFoldersMap[dstID]
				task.ExpectedFiles = expectedFilesMap[dstID]
				if srcIDMap != nil {
					task.ExpectedSrcIDMap = srcIDMap[dstID]
				}
			}

			// Enqueue task
			q.enqueuePending(task)
			enqueuedCount++
		}

		// Track if pull was partial based on actual enqueued count
		// Even if we enqueued 0 (all were leased), we still record the pull
		// so the queue can properly advance rounds and check completion
		wasPartial := len(batch) < defaultLeaseBatchSize
		q.setLastPullWasPartial(wasPartial)

		// Record pull in RoundInfo
		q.recordPull(currentRound, len(batch), wasPartial)

		return
	}

	// For rounds > maxKnownDepth, use normal traversal pull logic
	// This allows discovering new deeper levels
	// Note: PullTraversalTasks will handle incrementing counters itself
	q.PullTraversalTasks(force)
}
