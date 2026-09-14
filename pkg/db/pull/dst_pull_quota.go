// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

const dstPullSchedChunkSize = 100

// DstPullQuota limits DST traversal pull batch size by folder count and expected SRC children.
// MaxChildren 0 means no child cap (task cap only).
type DstPullQuota struct {
	MaxTasks    int
	MaxChildren int
}

// DstPullChildQuota returns taskQuota * multiplier (0 when either arg is non-positive).
func DstPullChildQuota(taskQuota, multiplier int) int {
	if taskQuota <= 0 || multiplier <= 0 {
		return 0
	}
	return taskQuota * multiplier
}

// ListDstBatchWithSrcChildrenQuota pulls DST traversal folders and hydrates SRC expected children
// until task or child quota is reached. partial matches SRC pull semantics: true when the depth
// frontier is exhausted after this pull (round may advance once buffers drain). lastID is the
// last enqueued folder id (cursor for the next pull).
func ListDstBatchWithSrcChildrenQuota(d *db.DB, depth int, afterID string, quota DstPullQuota, traversalStatus string) ([]db.FetchResult, map[string][]*db.NodeState, map[string]string, string, bool, error) {
	return listDstBatchWithSrcChildrenQuotaOps(d, depth, afterID, quota, traversalStatus)
}

func listDstBatchWithSrcChildrenQuotaOps(d *db.DB, depth int, afterID string, quota DstPullQuota, traversalStatus string) ([]db.FetchResult, map[string][]*db.NodeState, map[string]string, string, bool, error) {
	if traversalStatus == "" {
		traversalStatus = db.StatusPending
	}
	maxTasks := quota.MaxTasks
	if maxTasks <= 0 {
		maxTasks = 1000
	}
	maxChildren := quota.MaxChildren

	start := time.Now()
	cursor := afterID
	results := make([]db.FetchResult, 0, maxTasks)
	childrenByDstID := make(map[string][]*db.NodeState)
	srcParentDelete := make(map[string]string)
	lastEnqueued := afterID
	taskCount := 0
	childCount := 0
	quotaHit := false

	for taskCount < maxTasks {
		remainingTasks := maxTasks - taskCount
		chunkLimit := dstPullSchedChunkSize
		if chunkLimit > remainingTasks {
			chunkLimit = remainingTasks
		}
		if chunkLimit <= 0 {
			break
		}

		ids, chunk, err := listSchedResults(d, opsSide("DST"), opsdb.PhaseTrav, depth, db.NodeTypeFolder, cursor, traversalStatus, chunkLimit)
		if err != nil {
			return nil, nil, nil, "", false, fmt.Errorf("list dst sched: %w", err)
		}
		if len(chunk) == 0 {
			break
		}

		chunkChildren, chunkDelete, err := hydrateSrcChildrenForDstBatch(d, chunk)
		if err != nil {
			return nil, nil, nil, "", false, err
		}

		stoppedEarly := false
		for _, fr := range chunk {
			if fr.State == nil {
				continue
			}
			dstID := fr.Key
			folderChildren := chunkChildren[dstID]
			folderChildCount := len(folderChildren)

			if taskCount > 0 && maxChildren > 0 && childCount+folderChildCount > maxChildren {
				quotaHit = true
				stoppedEarly = true
				break
			}

			results = append(results, fr)
			if len(folderChildren) > 0 {
				childrenByDstID[dstID] = folderChildren
			}
			if st := chunkDelete[dstID]; st != "" {
				srcParentDelete[dstID] = st
			}
			taskCount++
			childCount += folderChildCount
			lastEnqueued = dstID

			if taskCount >= maxTasks {
				if hasMoreDstPendingFolders(d, depth, lastEnqueued) {
					quotaHit = true
				}
				stoppedEarly = true
				break
			}
		}

		if stoppedEarly {
			break
		}

		cursor = ids[len(ids)-1]
		if len(chunk) < chunkLimit {
			break
		}
	}

	partial := dstPullExhausted(d, depth, lastEnqueued, taskCount, quotaHit)
	d.RecordOp(db.OpDstPullHydrate, fmt.Sprintf("quota tasks=%d children=%d partial=%v", taskCount, childCount, partial), int64(taskCount+childCount), time.Since(start), nil)
	return results, childrenByDstID, srcParentDelete, lastEnqueued, partial, nil
}

// dstPullExhausted mirrors SRC ListNodesPendingAtDepthKeyset partial semantics (partial=true when exhausted).
func dstPullExhausted(d *db.DB, depth int, lastEnqueued string, taskCount int, quotaHit bool) bool {
	if quotaHit {
		return false
	}
	if taskCount == 0 {
		return true
	}
	return !hasMoreDstPendingFolders(d, depth, lastEnqueued)
}

func hasMoreDstPendingFolders(d *db.DB, depth int, afterID string) bool {
	ids, _, err := listSchedResults(d, opsSide("DST"), opsdb.PhaseTrav, depth, db.NodeTypeFolder, afterID, db.StatusPending, 1)
	if err != nil || len(ids) == 0 {
		return false
	}
	return true
}
