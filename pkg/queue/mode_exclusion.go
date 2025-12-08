// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// PullExclusionTasks pulls exclusion tasks from the exclusion-holding bucket for the current round.
// Scans the bucket O(n) and filters by current round level. Breaks when finding first node with level > current round.
func (q *Queue) PullExclusionTasks(force bool) {
	if q.boltDB == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	q.mu.Lock()
	if q.pulling {
		q.mu.Unlock()
		return
	}

	if q.state == QueueStateCompleted {
		q.mu.Unlock()
		return
	}

	q.pulling = true
	outputBuffer := q.outputBuffer
	q.mu.Unlock()

	defer func() {
		q.mu.Lock()
		q.pulling = false
		q.mu.Unlock()
	}()

	// Force-flush buffer before pulling tasks
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

	queueType := getQueueType(q.name)

	q.mu.Lock()
	queueState := q.state
	pendingCount := len(q.pendingBuff)
	currentRound := q.round
	wasFirstPullForRound := q.firstPullForRound
	q.mu.Unlock()

	if !force {
		if queueState != QueueStateRunning || pendingCount > q.pullLowWM {
			return
		}
	} else {
		if queueState == QueueStatePaused {
			return
		}
	}

	// Scan exclusion-holding bucket for current round
	entries, hasHigherLevels, err := db.ScanExclusionHoldingBucketByLevel(q.boltDB, queueType, currentRound, defaultLeaseBatchSize)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to scan exclusion-holding bucket: %v", err), "queue", q.name, q.name)
		}
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Set firstPullForRound = false if we got tasks
	if wasFirstPullForRound && len(entries) > 0 {
		q.firstPullForRound = false
	}

	// Check if exclusion-holding bucket is empty (no entries found and no higher levels)
	if wasFirstPullForRound && len(entries) == 0 && !hasHigherLevels {
		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				"Exclusion-holding bucket is empty - exclusion sweep complete",
				"queue", q.name, q.name)
		}
		q.state = QueueStateCompleted
		q.mu.Unlock()
		// Clear exclusion-holding bucket after completion
		if err := db.ClearExclusionHoldingBucket(q.boltDB, queueType); err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("warning", fmt.Sprintf("Failed to clear exclusion-holding bucket: %v", err), "queue", q.name, q.name)
			}
		}
		return
	}

	// If no entries found at current level but higher levels exist, we need to advance round
	// This will be handled by round completion check when task count hits 0
	if len(entries) == 0 && hasHigherLevels {
		// No tasks for this round, but more work in future rounds
		// Set lastPullWasPartial to trigger round advancement
		q.lastPullWasPartial = true
		return
	}

	added := 0
	// Pop entries from exclusion-holding bucket as we pull them
	pathHashesToRemove := make([]string, 0, len(entries))

	for _, entry := range entries {
		// Skip path hashes we've already leased
		if _, exists := q.leasedKeys[entry.PathHash]; exists {
			continue
		}

		// Mark as leased
		q.leasedKeys[entry.PathHash] = struct{}{}

		// Get node state to determine exclusion mode
		nodeState, err := db.GetNodeState(q.boltDB, queueType, []byte(entry.PathHash))
		if err != nil || nodeState == nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to get node state for exclusion task %s: %v", entry.PathHash, err), "queue", q.name, q.name)
			}
			continue
		}

		// Determine exclusion mode based on explicit_excluded flag
		// If explicitly excluded, mode is "exclude", otherwise "unexclude"
		exclusionMode := "exclude"
		if !nodeState.ExplicitExcluded {
			exclusionMode = "unexclude"
		}

		// Convert to task
		task := &TaskBase{
			Type:          TaskTypeExclusion,
			Round:         currentRound,
			ExclusionMode: exclusionMode,
		}

		if nodeState.Type == "folder" {
			task.Folder = types.Folder{
				Id:           nodeState.ID,
				ParentId:     nodeState.ParentID,
				ParentPath:   types.NormalizeParentPath(nodeState.ParentPath),
				DisplayName:  nodeState.Name,
				LocationPath: types.NormalizeLocationPath(nodeState.Path),
				LastUpdated:  nodeState.MTime,
				DepthLevel:   nodeState.Depth,
				Type:         nodeState.Type,
			}
		} else {
			task.File = types.File{
				Id:           nodeState.ID,
				ParentId:     nodeState.ParentID,
				ParentPath:   types.NormalizeParentPath(nodeState.ParentPath),
				DisplayName:  nodeState.Name,
				LocationPath: types.NormalizeLocationPath(nodeState.Path),
				LastUpdated:  nodeState.MTime,
				DepthLevel:   nodeState.Depth,
				Size:         nodeState.Size,
				Type:         nodeState.Type,
			}
		}

		beforeCount := len(q.pendingBuff)
		q.enqueuePendingLocked(task)
		if len(q.pendingBuff) > beforeCount {
			added++
			// Track path hashes to remove from exclusion-holding bucket
			pathHashesToRemove = append(pathHashesToRemove, entry.PathHash)
		}
	}

	// Pop entries from exclusion-holding bucket
	q.mu.Unlock()
	if len(pathHashesToRemove) > 0 {
		for _, pathHash := range pathHashesToRemove {
			if err := db.RemoveExclusionHoldingEntry(q.boltDB, queueType, pathHash); err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to remove exclusion-holding entry %s: %v", pathHash, err), "queue", q.name, q.name)
				}
			}
		}
	}
	q.mu.Lock()

	// Set lastPullWasPartial: true if we found fewer entries than limit OR if there are higher levels
	// If hasHigherLevels is true, we know there are more tasks in future rounds
	q.lastPullWasPartial = len(entries) < defaultLeaseBatchSize || hasHigherLevels
}
