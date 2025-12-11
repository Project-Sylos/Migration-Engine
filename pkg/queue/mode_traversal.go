// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// PullTraversalTasks pulls traversal tasks from BoltDB for the current round.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullTraversalTasks(force bool) {
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
	outputBuffer := q.getOutputBuffer()

	// Always clear pulling flag when done
	defer func() {
		q.setPulling(false)
	}()

	// Force-flush buffer before pulling tasks to ensure we don't pull tasks
	// that are waiting in the buffer to be written
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

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

	// Track if this is the first pull for the round (before we potentially set it to false)
	wasFirstPullForRound := snapshot.FirstPullForRound
	currentRound := snapshot.Round
	coordinator := q.getCoordinator()

	// For DST: Check coordinator gate before pulling (but don't check completion yet)
	// We'll check completion AFTER pulling to ensure workers have finished adding tasks
	if q.name == "dst" && coordinator != nil {
		canStartRound := coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - wait for coordinator gate
			return
		}
		// Gate passed - we'll mark firstPullForRound = false only after actually fetching tasks
	}

	batch, err := db.BatchFetchWithKeys(boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch batch from BoltDB: %v", err), "queue", q.name, q.name)
		}
		// On error, don't change lastPullWasPartial - keep existing state
		// Also don't set firstPullForRound = false since pull failed
		return
	}

	// Pull was successful - set firstPullForRound = false only if we actually fetched tasks
	// This ensures we only mark completion on the actual first pull that fetched tasks
	if wasFirstPullForRound && len(batch) > 0 {
		q.setFirstPullForRound(false)
	}

	// For DST: Check completion AFTER pulling to ensure workers have finished adding tasks
	// This prevents race conditions where workers are still completing tasks that add pending items
	if q.name == "dst" && coordinator != nil && currentRound > 0 && wasFirstPullForRound && len(batch) == 0 {
		// First pull returned empty - check if we're truly complete (no in-progress, no pending buffer, no DB pending)
		if q.checkCompletion(currentRound, CompletionCheckOptions{
			CheckDstComplete:      true,
			MarkDstCompleteIfDone: true,
			RequireFirstPull:      true,
			FlushBuffer:           true,
		}) {
			return
		}
		// If checkCompletion returned false, there might be tasks in progress or buffer
		// Don't mark as complete yet - let workers finish and check again later
	}

	// we've exhausted max depth - mark queue as completed (only if not DST, or if DST check above passed)
	if wasFirstPullForRound && len(batch) == 0 {
		// For DST, completion was already checked above, so if we reach here it means checkCompletion returned false
		// For SRC, we can mark as complete directly
		if q.name != "dst" {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info",
					fmt.Sprintf("First pull for round %d returned empty batch - traversal complete (max depth exhausted)", currentRound),
					"queue", q.name, q.name)
			}
			q.setState(QueueStateCompleted)
			// Update coordinator
			if coordinator != nil {
				coordinator.MarkSrcCompleted()
			}
			return
		}
	}

	// Batch-load expected children for DST tasks
	var expectedFoldersMap map[string][]types.Folder
	var expectedFilesMap map[string][]types.File
	var srcIDMap map[string]map[string]string // DST ULID -> (Type+Name -> SRC node ID)
	if q.name == "dst" {
		// First pass: collect all valid (not already leased) folder tasks and their DST parent ULIDs
		var dstParentIDs []string
		dstIDToPath := make(map[string]string) // DST ULID -> path (for mapping results back)
		for _, item := range batch {
			// Skip ULIDs we've already leased (prevents duplicate pulls from stale views)
			if q.isLeased(item.Key) {
				continue
			}

			task := nodeStateToTask(item.State, taskType)
			if task.IsFolder() {
				dstParentIDs = append(dstParentIDs, item.State.ID) // DST parent ULID
				dstIDToPath[item.State.ID] = task.Folder.LocationPath
			}
		}

		// Batch-load expected children for all folder tasks in one DB operation
		// Uses SrcID from DST NodeState to find corresponding SRC parents, then loads their children
		if len(dstParentIDs) > 0 {
			var err error
			expectedFoldersMap, expectedFilesMap, srcIDMap, err = BatchLoadExpectedChildrenByDSTIDs(boltDB, dstParentIDs, dstIDToPath)
			if err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to batch load expected children: %v", err), "queue", q.name, q.name)
				}
				// Continue anyway - tasks will have empty expected children
				expectedFoldersMap = make(map[string][]types.Folder)
				expectedFilesMap = make(map[string][]types.File)
				srcIDMap = make(map[string]map[string]string)
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

		// Mark this ULID as leased
		q.addLeasedKey(item.Key)

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

		// Enqueue task
		q.enqueuePending(task)
	}

	// Track if this pull was partial (fewer tasks than requested)
	// This signals we might have exhausted the current round
	q.setLastPullWasPartial(len(batch) < defaultLeaseBatchSize)
}
