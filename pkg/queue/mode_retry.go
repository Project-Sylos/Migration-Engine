// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// PullRetryTasks pulls retry tasks from failed/pending status buckets.
// Checks maxKnownDepth and scans all known levels up to maxKnownDepth, then uses normal traversal logic for deeper levels.
func (q *Queue) PullRetryTasks(force bool) {
	if q.boltDB == nil {
		return
	}

	q.mu.Lock()
	currentRound := q.round
	maxKnownDepth := q.maxKnownDepth
	q.mu.Unlock()

	// If maxKnownDepth is not set, try to compute it from existing levels
	if maxKnownDepth == -1 {
		levels, err := q.boltDB.GetAllLevels(getQueueType(q.name))
		if err == nil && len(levels) > 0 {
			maxKnownDepth = 0
			for _, level := range levels {
				if level > maxKnownDepth {
					maxKnownDepth = level
				}
			}
			q.mu.Lock()
			q.maxKnownDepth = maxKnownDepth
			q.mu.Unlock()
		}
	}

	// If current round <= maxKnownDepth, scan all known levels with pending/failed status
	if maxKnownDepth >= 0 && currentRound <= maxKnownDepth {
		// For retry sweep up to maxKnownDepth, pull from current round
		// Round advancement will handle moving through all levels
		// Pull from current round with failed status (retry failed tasks)
		queueType := getQueueType(q.name)
		batch, err := db.BatchFetchWithKeys(q.boltDB, queueType, currentRound, db.StatusFailed, defaultLeaseBatchSize)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", fmt.Sprintf("Failed to fetch retry batch from BoltDB: %v", err), "queue", q.name, q.name)
			}
			return
		}

		// Also pull pending tasks from current round (in case some were marked pending)
		pendingBatch, err := db.BatchFetchWithKeys(q.boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)
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

		q.mu.Lock()
		defer q.mu.Unlock()

		taskType := TaskTypeSrcTraversal
		if q.name == "dst" {
			taskType = TaskTypeDstTraversal
		}

		added := 0
		for _, item := range batch {
			if _, exists := q.leasedKeys[item.Key]; exists {
				continue
			}

			q.leasedKeys[item.Key] = struct{}{}

			task := nodeStateToTask(item.State, taskType)
			beforeCount := len(q.pendingBuff)
			q.enqueuePendingLocked(task)
			if len(q.pendingBuff) > beforeCount {
				added++
			}
		}

		q.lastPullWasPartial = len(batch) < defaultLeaseBatchSize

		// Check if we've exhausted all known levels
		if len(batch) == 0 && currentRound >= maxKnownDepth {
			// Check if there are any more failed/pending tasks in deeper levels
			// If not, retry sweep is complete
			// For now, advance and let normal logic handle deeper discovery
			q.lastPullWasPartial = true
		}

		return
	}

	// For rounds > maxKnownDepth, use normal traversal pull logic
	// This allows discovering new deeper levels
	q.PullTraversalTasks(force)
}
