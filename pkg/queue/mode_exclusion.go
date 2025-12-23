// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

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
	entries, _, err := db.ScanExclusionHoldingBucketByLevel(boltDB, queueType, snapshot.Round, defaultLeaseBatchSize)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to scan exclusion-holding bucket: %v", err), "queue", q.name, q.name)
		}
		return
	}

	// Debug: Log pull stats
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug", fmt.Sprintf("PullExclusionTasks: Round %d, found %d entries", snapshot.Round, len(entries)), "queue", q.name, q.name)
	}

	// Pop entries from holding buckets as we pull them
	type entryToRemove struct {
		nodeID string
		mode   string
	}
	nodeIDsToRemove := make([]entryToRemove, 0, len(entries))

	for _, entry := range entries {
		// Skip ULIDs we've already leased (these are legitimately in progress)
		if q.isLeased(entry.NodeID) {
			continue
		}

		// Mark as leased
		q.addLeasedKey(entry.NodeID)

		// Get node state to determine exclusion mode
		nodeState, err := db.GetNodeState(boltDB, queueType, entry.NodeID)
		if err != nil || nodeState == nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to get node state for exclusion task %s: %v - removing from holding bucket", entry.NodeID, err), "queue", q.name, q.name)
			}
			// Node doesn't exist or lookup failed - remove from holding bucket to prevent infinite retries
			q.removeLeasedKey(entry.NodeID) // Un-lease since we're skipping
			nodeIDsToRemove = append(nodeIDsToRemove, entryToRemove{
				nodeID: entry.NodeID,
				mode:   entry.Mode, // Use mode from entry
			})
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

		// Query status-lookup bucket to get current status (for moving between status buckets)
		previousStatus := db.StatusPending // Default to pending if not found
		_ = boltDB.View(func(tx *bolt.Tx) error {
			lookupBucket := db.GetStatusLookupBucket(tx, queueType, nodeState.Depth)
			if lookupBucket != nil {
				statusBytes := lookupBucket.Get([]byte(entry.NodeID))
				if statusBytes != nil {
					previousStatus = string(statusBytes)
				}
			}
			return nil
		})

		// For unexclusion tasks, verify the node is actually in the excluded status bucket
		// (mirroring how exclusion verifies nodes are in their previous status bucket)
		if exclusionMode == "unexclude" {
			var isInExcludedBucket bool
			_ = boltDB.View(func(tx *bolt.Tx) error {
				excludedBucket := db.GetStatusBucket(tx, queueType, nodeState.Depth, db.StatusExcluded)
				if excludedBucket != nil {
					isInExcludedBucket = excludedBucket.Get([]byte(entry.NodeID)) != nil
				}
				return nil
			})

			// If node is not in excluded bucket, skip it (may have been processed already)
			// Remove from holding bucket to prevent infinite retries
			if !isInExcludedBucket {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Skipping unexclusion task %s: not in excluded status bucket - removing from holding bucket", entry.NodeID), "queue", q.name, q.name)
				}
				q.removeLeasedKey(entry.NodeID) // Un-lease since we're skipping
				nodeIDsToRemove = append(nodeIDsToRemove, entryToRemove{
					nodeID: entry.NodeID,
					mode:   exclusionMode,
				})
				continue
			}

			// Ensure PreviousStatus is set to excluded for unexclusion tasks
			if previousStatus != db.StatusExcluded {
				previousStatus = db.StatusExcluded
			}
		}

		// Convert to task
		task := &TaskBase{
			ID:             entry.NodeID,
			Type:           TaskTypeExclusion,
			Round:          snapshot.Round,
			ExclusionMode:  exclusionMode,
			PreviousStatus: previousStatus,
		}

		if nodeState.Type == "folder" {
			task.Folder = types.Folder{
				ServiceID:    nodeState.ServiceID,
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
				ServiceID:    nodeState.ServiceID,
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
			// Track ULIDs to remove from appropriate holding bucket (with mode)
			nodeIDsToRemove = append(nodeIDsToRemove, entryToRemove{
				nodeID: entry.NodeID,
				mode:   exclusionMode,
			})
		} else {
			// Enqueue failed - remove from leased set to allow future attempts
			q.removeLeasedKey(entry.NodeID)
		}
	}

	// Pop entries from appropriate holding buckets (outside lock)
	if len(nodeIDsToRemove) > 0 {
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Removing %d entries from holding buckets (Round %d)", len(nodeIDsToRemove), snapshot.Round), "queue", q.name, q.name)
		}
		for _, entryToRemove := range nodeIDsToRemove {
			if err := db.RemoveHoldingEntry(boltDB, queueType, entryToRemove.nodeID, entryToRemove.mode); err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to remove holding entry %s from %s bucket: %v", entryToRemove.nodeID, entryToRemove.mode, err), "queue", q.name, q.name)
				}
			}
		}
	}

	// Set lastPullWasPartial based on batch size (same as traversal mode)
	// If we got fewer entries than the batch limit, this round is exhausted
	// Workers may add more entries to future rounds, which we'll discover when we advance
	wasPartial := len(entries) < defaultLeaseBatchSize
	q.setLastPullWasPartial(wasPartial)

	// Record pull in RoundInfo
	q.recordPull(snapshot.Round, len(entries), wasPartial)
}
