// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// Buffer flush calls should eventually be replaced with store.Barrier() semantic barriers.

// PullRetryTasks pulls retry tasks from failed/pending status buckets.
// Checks maxKnownDepth and scans all known levels up to maxKnownDepth, then uses normal traversal logic for deeper levels.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullRetryTasks(force bool) {

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

	// reads conflict with buffered writes (O(1) domain metadata check)

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
		levels, err := q.getStore().GetAllLevels(getQueueType(q.name))
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
		leasedIDs, err := q.getStore().LeaseTasksAtLevel(queueType, currentRound, defaultLeaseBatchSize)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch from BoltDB: %v", err), "queue", q.name, q.name)
			}
			return
		}

		// Convert leased IDs -> NodeStates
		type leasedItem struct {
			Key   string
			State *bolt.NodeState
		}
		batch := make([]leasedItem, 0, len(leasedIDs))
		for _, idBytes := range leasedIDs {
			nodeID := string(idBytes)
			if nodeID == "" {
				continue
			}
			state, err := q.getStore().GetNode(queueType, nodeID)
			if err != nil || state == nil {
				continue
			}
			batch = append(batch, leasedItem{Key: nodeID, State: state})
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
			for _, item := range batch {
				if q.isLeased(item.Key) {
					continue
				}
				task := nodeStateToTask(item.State, taskType)
				if task.IsFolder() {
					dstParentIDs = append(dstParentIDs, item.State.ID)
				}
			}

			// Batch-load expected children
			if len(dstParentIDs) > 0 {
				expectedFoldersMap = make(map[string][]types.Folder)
				expectedFilesMap = make(map[string][]types.File)
				srcIDMap = make(map[string]map[string]string)

				// For each DST parent, find corresponding SRC parent and load its children
				for _, dstParentID := range dstParentIDs {
					// Get corresponding SRC parent ID via join mapping
					srcParentID, err := q.getStore().GetJoinMapping("dst-to-src", dstParentID)
					if err != nil || srcParentID == "" {
						// No SRC parent found - skip
						continue
					}

					// Load SRC children (expected children for this DST parent)
					childIDsResult, err := q.getStore().GetChildren("SRC", srcParentID, "ids")
					if err != nil {
						if logservice.LS != nil {
							_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to load SRC children for parent %s: %v", srcParentID, err), "queue", q.name, q.name)
						}
						continue
					}

					childIDs, ok := childIDsResult.([]string)
					if !ok {
						continue
					}

					// Convert child IDs to Folder/File objects
					var folders []types.Folder
					var files []types.File
					childSrcIDMap := make(map[string]string) // Type+Name -> SRC node ID

					for _, childID := range childIDs {
						childState, err := q.getStore().GetNode("SRC", childID)
						if err != nil {
							continue
						}

						childTask := nodeStateToTask(childState, TaskTypeSrcTraversal)
						if childTask == nil {
							continue
						}

						var matchKey string
						if childTask.IsFolder() {
							matchKey = childTask.Folder.Type + ":" + childTask.Folder.DisplayName
							folders = append(folders, childTask.Folder)
						} else if childTask.IsFile() {
							matchKey = childTask.File.Type + ":" + childTask.File.DisplayName
							files = append(files, childTask.File)
						} else {
							continue
						}

						childSrcIDMap[matchKey] = childID
					}

					expectedFoldersMap[dstParentID] = folders
					expectedFilesMap[dstParentID] = files
					srcIDMap[dstParentID] = childSrcIDMap
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

			// Enqueue task - only mark as leased if enqueue succeeds
			if q.enqueuePending(task) {
				q.addLeasedKey(item.Key)
				enqueuedCount++
			}
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
	q.PullTraversalTasks(force)
}
