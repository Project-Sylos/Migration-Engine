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
func (q *Queue) PullTraversalTasks(force bool) {
	if q.boltDB == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	// This prevents multiple threads from executing pull logic concurrently
	q.mu.Lock()
	if q.pulling {
		q.mu.Unlock()
		return
	}

	// Don't pull if queue is completed (prevents deadlock on coordinator gate)
	if q.state == QueueStateCompleted {
		q.mu.Unlock()
		return
	}

	// Set pulling flag early and defer clearing it
	// This ensures only one thread can execute the pull logic at a time
	q.pulling = true
	outputBuffer := q.outputBuffer
	q.mu.Unlock()

	// Always clear pulling flag when done
	defer func() {
		q.mu.Lock()
		q.pulling = false
		q.mu.Unlock()
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

	q.mu.Lock()
	queueState := q.state
	pendingCount := len(q.pendingBuff)

	if !force {
		// Only pull if queue is running (not paused or completed)
		if queueState != QueueStateRunning || pendingCount > q.pullLowWM {
			q.mu.Unlock()
			return
		}
	} else {
		// Even when forcing, don't pull if paused
		if queueState == QueueStatePaused {
			q.mu.Unlock()
			return
		}
	}

	// Track if this is the first pull for the round (before we potentially set it to false)
	wasFirstPullForRound := q.firstPullForRound
	currentRound := q.round

	// For DST: Check if there are any pending tasks in current round BEFORE checking coordinator gate
	// This allows us to exit early if there's no work, avoiding unnecessary waiting at the gate
	// Skip this check for round 0 (bootstrap round) - we need to process at least one round before checking completion
	if q.name == "dst" && q.coordinator != nil && currentRound > 0 {
		q.mu.Unlock()

		// Check if DST is complete (requires first pull to avoid false positives)
		if q.checkCompletion(currentRound, CompletionCheckOptions{
			CheckDstComplete:      true,
			MarkDstCompleteIfDone: true,
			RequireFirstPull:      true,
			FlushBuffer:           true,
		}) {
			return
		}

		// There are tasks (in-memory or in DB) - check coordinator gate
		q.mu.Lock()
		canStartRound := q.coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - wait for coordinator gate
			q.mu.Unlock()
			return
		}
		// Gate passed - we'll mark firstPullForRound = false only after actually fetching tasks
	} else if q.name == "dst" && q.coordinator != nil && currentRound == 0 {
		// For round 0, just check the coordinator gate (no completion check)
		q.mu.Unlock()
		q.mu.Lock()
		canStartRound := q.coordinator.CanDstStartRound(currentRound)
		if !canStartRound {
			// Can't start this round yet - wait for coordinator gate
			q.mu.Unlock()
			return
		}
		// Gate passed - we'll mark firstPullForRound = false only after actually fetching tasks
	}

	q.mu.Unlock()

	batch, err := db.BatchFetchWithKeys(q.boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)

	q.mu.Lock()
	defer q.mu.Unlock()

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
		q.firstPullForRound = false
	}

	// we've exhausted max depth - mark queue as completed
	if wasFirstPullForRound && len(batch) == 0 {
		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				fmt.Sprintf("First pull for round %d returned empty batch - traversal complete (max depth exhausted)", currentRound),
				"queue", q.name, q.name)
		}
		q.state = QueueStateCompleted
		// Update coordinator
		if q.coordinator != nil {
			switch q.name {
			case "src":
				q.coordinator.MarkSrcCompleted()
			case "dst":
				q.coordinator.MarkDstCompleted()
			}
		}
		return
	}

	// Batch-load expected children for DST tasks
	var expectedFoldersMap map[string][]types.Folder
	var expectedFilesMap map[string][]types.File
	if q.name == "dst" {
		// First pass: collect all valid (not already leased) folder tasks and their parent paths
		var parentPaths []string
		for _, item := range batch {
			// Skip path hashes we've already leased (prevents duplicate pulls from stale views)
			if _, exists := q.leasedKeys[item.Key]; exists {
				continue
			}

			task := nodeStateToTask(item.State, taskType)
			if task.IsFolder() {
				parentPaths = append(parentPaths, task.Folder.LocationPath)
			}
		}

		// Batch-load expected children for all folder tasks in one DB operation
		if len(parentPaths) > 0 {
			var err error
			expectedFoldersMap, expectedFilesMap, err = BatchLoadExpectedChildren(q.boltDB, parentPaths)
			if err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to batch load expected children: %v", err), "queue", q.name, q.name)
				}
				// Continue anyway - tasks will have empty expected children
				expectedFoldersMap = make(map[string][]types.Folder)
				expectedFilesMap = make(map[string][]types.File)
			}
		}
	}

	added := 0
	for _, item := range batch {
		// Skip path hashes we've already leased (prevents duplicate pulls from stale views)
		if _, exists := q.leasedKeys[item.Key]; exists {
			continue
		}

		// Mark this path hash as leased
		q.leasedKeys[item.Key] = struct{}{}

		task := nodeStateToTask(item.State, taskType)

		// For DST folder tasks, populate ExpectedFolders/ExpectedFiles from batch-loaded results
		if q.name == "dst" && task.IsFolder() {
			parentPath := task.Folder.LocationPath
			expectedFolders := expectedFoldersMap[parentPath]
			expectedFiles := expectedFilesMap[parentPath]
			task.ExpectedFolders = expectedFolders
			task.ExpectedFiles = expectedFiles
		}

		beforeCount := len(q.pendingBuff)
		q.enqueuePendingLocked(task)
		if len(q.pendingBuff) > beforeCount {
			added++
		}
	}

	// Track if this pull was partial (fewer tasks than requested)
	// This signals we might have exhausted the current round
	q.lastPullWasPartial = len(batch) < defaultLeaseBatchSize
}
