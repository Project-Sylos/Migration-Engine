// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// PullRetryTasks pulls retry tasks from failed/pending status buckets.
// Checks maxKnownDepth and scans all known levels up to maxKnownDepth, then uses normal traversal logic for deeper levels.
// Uses getter/setter methods - no direct mutex access.
func (q *Queue) PullRetryTasks(force bool) {
	boltDB := q.getBoltDB()
	if boltDB == nil {
		return
	}

	currentRound := q.getRound()
	maxKnownDepth := q.getMaxKnownDepth()

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
		// Round advancement will handle moving through all levels
		// Pull from current round with failed status (retry failed tasks)
		queueType := getQueueType(q.name)
		batch, err := db.BatchFetchWithKeys(boltDB, queueType, currentRound, db.StatusFailed, defaultLeaseBatchSize)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch from BoltDB: %v", err), "queue", q.name, q.name)
			}
			return
		}

		// Also pull pending tasks from current round (in case some were marked pending)
		pendingBatch, err := db.BatchFetchWithKeys(boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)
		if err == nil {
			// Merge batches (avoid duplicates)
			batchMap := make(map[string]db.FetchResult)
			for _, item := range batch {
				batchMap[item.Key] = item
			}
			for _, item := range pendingBatch {
				if _, exists := batchMap[item.Key]; !exists {
					batch = append(batch, item)
				}
			}
		}

		// For SRC queue, handle DST node cleanup when retrying SRC nodes
		if q.name == "src" && len(batch) > 0 {
			// Collect SRC node IDs being retried
			srcNodeIDs := make([]string, 0, len(batch))
			for _, item := range batch {
				if item.State != nil && item.State.ID != "" {
					srcNodeIDs = append(srcNodeIDs, item.State.ID)
				}
			}

			// Query SrcToDst lookup table for each SRC ID to get corresponding DST parent IDs
			dstParentIDs := make(map[string]bool) // Use map to deduplicate
			for _, srcID := range srcNodeIDs {
				dstID, err := db.GetDstIDFromSrcID(boltDB, srcID)
				if err == nil && dstID != "" {
					dstParentIDs[dstID] = true
				}
			}

			// For each DST parent ID found, mark as pending and delete its children
			for dstParentID := range dstParentIDs {
				// Get DST parent node state to determine its level
				dstParentState, err := db.GetNodeState(boltDB, "DST", dstParentID)
				if err != nil || dstParentState == nil {
					continue // DST parent not found, skip
				}

				// Direct DB write: Mark DST parent as pending status
				oldStatus := dstParentState.TraversalStatus
				if oldStatus == "" {
					// Try to get status from status-lookup bucket
					var lookupStatus string
					_ = boltDB.View(func(tx *bolt.Tx) error {
						lookupBucket := db.GetStatusLookupBucket(tx, "DST", dstParentState.Depth)
						if lookupBucket != nil {
							statusData := lookupBucket.Get([]byte(dstParentID))
							if statusData != nil {
								lookupStatus = string(statusData)
							}
						}
						return nil
					})
					if lookupStatus != "" {
						oldStatus = lookupStatus
					} else {
						oldStatus = db.StatusSuccessful // Default assumption
					}
				}
				_, err = db.UpdateNodeStatusByID(boltDB, "DST", dstParentState.Depth, oldStatus, db.StatusPending, dstParentID)
				if err != nil {
					if logservice.LS != nil {
						_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to mark DST parent %s as pending: %v", dstParentID, err), "queue", q.name, q.name)
					}
					continue
				}

				// Query DST children bucket to get child node IDs for this parent
				childIDs, err := db.GetChildrenIDsByParentID(boltDB, "DST", dstParentID)
				if err != nil || len(childIDs) == 0 {
					continue // No children or error
				}

				// Delete all DST child nodes using batch deletion (direct write)
				if err := db.BatchDeleteNodes(boltDB, "DST", childIDs); err != nil {
					if logservice.LS != nil {
						_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to delete DST children for parent %s: %v", dstParentID, err), "queue", q.name, q.name)
					}
					// Continue anyway - deletion failure shouldn't block retry
				}
			}
		}

		taskType := TaskTypeSrcTraversal
		if q.name == "dst" {
			taskType = TaskTypeDstTraversal
		}

		for _, item := range batch {
			// Skip ULIDs we've already leased
			if q.isLeased(item.Key) {
				continue
			}

			// Mark as leased
			q.addLeasedKey(item.Key)

			task := nodeStateToTask(item.State, taskType)
			// Enqueue task
			q.enqueuePending(task)
		}

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
