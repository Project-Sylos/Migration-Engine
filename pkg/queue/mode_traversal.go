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

// PullTraversalTasks pulls traversal tasks from BoltDB for the current round.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullTraversalTasks(force bool) {
	storeInstance := q.getStore()
	if storeInstance == nil {
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

	// Always clear pulling flag when done
	defer func() {
		q.setPulling(false)
	}()

	queueType := getQueueType(q.name)
	taskType := TaskTypeSrcTraversal
	if q.name == "dst" {
		taskType = TaskTypeDstTraversal
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
	coordinator := q.getCoordinator()

	// For DST: Check coordinator gate before pulling
	if q.name == "dst" && coordinator != nil {
		canStartRound := coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - wait for coordinator gate
			return
		}
	}

	leasedIDs, err := storeInstance.LeaseTasksAtLevel(queueType, currentRound, defaultLeaseBatchSize)

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch batch from BoltDB: %v", err), "queue", q.name, q.name)
		}
		// On error, don't change lastPullWasPartial - keep existing state
		return
	}

	// Convert leased IDs -> NodeStates (no wrappers/no-ops: if we can't load state, we skip that ID)
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
		state, err := storeInstance.GetNode(queueType, nodeID)
		if err != nil || state == nil {
			continue
		}
		batch = append(batch, leasedItem{Key: nodeID, State: state})
	}

	// Batch-load expected children for DST tasks
	var expectedFoldersMap map[string][]types.Folder
	var expectedFilesMap map[string][]types.File
	var srcIDMap map[string]map[string]string // DST ULID -> (Type+Name -> SRC node ID)
	if q.name == "dst" {
		// First pass: collect all valid (not already leased) folder tasks and their DST parent ULIDs
		var dstParentIDs []string
		for _, item := range batch {
			// Skip ULIDs we've already leased (prevents duplicate pulls from stale views)
			if q.isLeased(item.Key) {
				continue
			}

			task := nodeStateToTask(item.State, taskType)
			if task.IsFolder() {
				dstParentIDs = append(dstParentIDs, item.State.ID) // DST parent ULID
			}
		}

		// Batch-load expected children for all folder tasks in one DB operation
		// Uses lookup table to get SRC parent ID from DST parent IDs, then loads SRC children
		if len(dstParentIDs) > 0 {
			expectedFoldersMap = make(map[string][]types.Folder)
			expectedFilesMap = make(map[string][]types.File)
			srcIDMap = make(map[string]map[string]string)

			// For each DST parent, find corresponding SRC parent and load its children
			for _, dstParentID := range dstParentIDs {
				// Get corresponding SRC parent ID via join mapping
				srcParentID, err := q.getStore().GetJoinMapping("dst-to-src", dstParentID)
				if err != nil || srcParentID == "" {
					// No SRC parent found - skip (this DST folder has no corresponding SRC)
					continue
				}

				// Load SRC children (these are the expected children for this DST parent)
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
					// Get child NodeState from SRC
					childState, err := q.getStore().GetNode("SRC", childID)
					if err != nil {
						continue
					}

					// Convert NodeState to Folder/File using nodeStateToTask helper
					childTask := nodeStateToTask(childState, TaskTypeSrcTraversal)
					if childTask == nil {
						continue
					}

					// Build match key (Type:Name) for srcIDMap
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

					// Map Type+Name to SRC node ID
					childSrcIDMap[matchKey] = childID
				}

				// Store results keyed by DST parent ID
				expectedFoldersMap[dstParentID] = folders
				expectedFilesMap[dstParentID] = files
				srcIDMap[dstParentID] = childSrcIDMap
			}
		} else {
			srcIDMap = make(map[string]map[string]string)
		}
	}

	for _, item := range batch {
		// Skip ULIDs we've already leased (prevents duplicate pulls from stale views)
		if q.isLeased(item.Key) {
			continue
		}

		task := nodeStateToTask(item.State, taskType)
		// Ensure task has the ULID from the database
		if task != nil && task.ID == "" {
			task.ID = item.State.ID
		}

		// For DST folder tasks, populate ExpectedFolders/ExpectedFiles and SRC ID map from batch-loaded results
		if q.name == "dst" && task.IsFolder() {
			// Use DST ULID to look up expected children
			dstID := item.State.ID
			expectedFolders := expectedFoldersMap[dstID]
			expectedFiles := expectedFilesMap[dstID]
			task.ExpectedFolders = expectedFolders
			task.ExpectedFiles = expectedFiles
			if srcIDMap != nil {
				task.ExpectedSrcIDMap = srcIDMap[dstID]
			}
		}

		// Enqueue task - only mark as leased if enqueue succeeds
		if q.enqueuePending(task) {
			q.addLeasedKey(item.Key)
		}
	}

	// Track if this pull was partial (fewer tasks than requested)
	// This signals we might have exhausted the current round
	wasPartial := len(batch) < defaultLeaseBatchSize
	q.setLastPullWasPartial(wasPartial)

	// Record pull in RoundInfo
	q.recordPull(currentRound, len(batch), wasPartial)

}
