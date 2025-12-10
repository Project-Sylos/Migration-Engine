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
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullExclusionTasks(force bool) {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	// Check pulling flag FIRST before any other logic
	if q.getPulling() {
		return
	}

	if q.getState() == QueueStateCompleted {
		return
	}

	// Set pulling flag and get output buffer
	q.setPulling(true)
	outputBuffer := q.getOutputBuffer()

	defer func() {
		q.setPulling(false)
	}()

	// Force-flush buffer before pulling tasks
	if outputBuffer != nil {
		outputBuffer.Flush()
	}

	queueType := getQueueType(q.name)

	// Get state snapshot
	snapshot := q.getStateSnapshot()

	if !force {
		if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return
		}
	} else {
		if snapshot.State == QueueStatePaused {
			return
		}
	}

	// Scan exclusion-holding bucket for current round
	entries, hasHigherLevels, err := db.ScanExclusionHoldingBucketByLevel(boltDB, queueType, snapshot.Round, defaultLeaseBatchSize)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to scan exclusion-holding bucket: %v", err), "queue", q.name, q.name)
		}
		return
	}

	// Debug: Log pull stats
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug", fmt.Sprintf("PullExclusionTasks: Round %d, found %d entries, hasHigherLevels=%v", snapshot.Round, len(entries), hasHigherLevels), "queue", q.name, q.name)
	}

	// Set firstPullForRound = false if we got tasks
	if snapshot.FirstPullForRound && len(entries) > 0 {
		q.setFirstPullForRound(false)
	}

	// Check if both holding buckets are empty (no entries found and no higher levels)
	if snapshot.FirstPullForRound && len(entries) == 0 && !hasHigherLevels {
		// Flush output buffer before completing to ensure all file exclusion updates are written
		outputBuffer := q.getOutputBuffer()
		if outputBuffer != nil {
			outputBuffer.Flush()
		}

		if logservice.LS != nil {
			_ = logservice.LS.Log("info",
				"Both exclusion and unexclusion holding buckets are empty - exclusion sweep complete",
				"queue", q.name, q.name)
		}
		q.setState(QueueStateCompleted)
		return
	}

	// If no entries found at current level but higher levels exist, we need to advance round
	// This will be handled by round completion check when task count hits 0
	if len(entries) == 0 && hasHigherLevels {
		// No tasks for this round, but more work in future rounds
		// Set lastPullWasPartial to trigger round advancement
		q.setLastPullWasPartial(true)
		return
	}

	// Pop entries from holding buckets as we pull them
	type entryToRemove struct {
		pathHash string
		mode     string
	}
	pathHashesToRemove := make([]entryToRemove, 0, len(entries))

	for _, entry := range entries {
		// Skip path hashes we've already leased
		if q.isLeased(entry.PathHash) {
			continue
		}

		// Mark as leased
		q.addLeasedKey(entry.PathHash)

		// Get node state to determine exclusion mode
		nodeState, err := db.GetNodeState(boltDB, queueType, []byte(entry.PathHash))
		if err != nil || nodeState == nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to get node state for exclusion task %s: %v", entry.PathHash, err), "queue", q.name, q.name)
			}
			continue
		}

		// Use the mode from the entry (which bucket it came from)
		// Entry.Mode is set by ScanExclusionHoldingBucketByLevel based on which bucket the entry was found in
		exclusionMode := entry.Mode
		if exclusionMode == "" {
			// Fallback: determine mode based on explicit_excluded flag
			if nodeState.ExplicitExcluded {
				exclusionMode = "exclude"
			} else {
				exclusionMode = "unexclude"
			}
		}

		// Convert to task
		task := &TaskBase{
			Type:          TaskTypeExclusion,
			Round:         snapshot.Round,
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

		// Enqueue task and track if it was added
		_, _, wasAdded := q.getPendingCountAndEnqueue(task)
		if wasAdded {
			// Track path hashes to remove from appropriate holding bucket (with mode)
			pathHashesToRemove = append(pathHashesToRemove, entryToRemove{
				pathHash: entry.PathHash,
				mode:     exclusionMode,
			})
		}
	}

	// Pop entries from appropriate holding buckets (outside lock)
	if len(pathHashesToRemove) > 0 {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Removing %d entries from holding buckets (Round %d)", len(pathHashesToRemove), snapshot.Round), "queue", q.name, q.name)
		}
		for _, entryToRemove := range pathHashesToRemove {
			if err := db.RemoveHoldingEntry(boltDB, queueType, entryToRemove.pathHash, entryToRemove.mode); err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to remove holding entry %s from %s bucket: %v", entryToRemove.pathHash, entryToRemove.mode, err), "queue", q.name, q.name)
				}
			}
		}
	}

	// Set lastPullWasPartial: true if we found fewer entries than limit OR if there are higher levels
	// If hasHigherLevels is true, we know there are more tasks in future rounds
	q.setLastPullWasPartial(len(entries) < defaultLeaseBatchSize || hasHigherLevels)
}
