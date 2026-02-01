// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// InsertNodeWithIndex atomically inserts a node into the nodes bucket, adds it to a status bucket,
// and updates the parent's children list in the children bucket.
func InsertNodeWithIndex(db *DB, queueType string, level int, status string, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	nodeID := []byte(state.ID)
	var parentID []byte

	return db.Update(func(tx *bolt.Tx) error {
		// ParentID can be empty for root nodes, otherwise must be set
		if state.ParentID != "" {
			parentID = []byte(state.ParentID)
		}

		// 1. Insert into nodes bucket (level-sharded)
		nodesBucket := GetNodesBucket(tx, queueType, level)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		// Ensure TraversalStatus is set
		if state.TraversalStatus == "" {
			state.TraversalStatus = status
		}

		nodeData, err := state.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		if err := nodesBucket.Put(nodeID, nodeData); err != nil {
			return fmt.Errorf("failed to insert node: %w", err)
		}

		// 2. Add to status bucket
		statusBucket, err := GetOrCreateStatusBucket(tx, queueType, level, status)
		if err != nil {
			return fmt.Errorf("failed to get status bucket: %w", err)
		}

		if err := statusBucket.Put(nodeID, []byte{}); err != nil {
			return fmt.Errorf("failed to add to status bucket: %w", err)
		}

		// 3. Update status-lookup index
		if err := UpdateStatusLookup(tx, queueType, level, nodeID, status); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		// 4. Update parent's children list in children bucket (parent is at level-1)
		if state.ParentID != "" && level > 0 {
			parentLevel := level - 1
			childrenBucket := GetChildrenBucket(tx, queueType, parentLevel)
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", queueType)
			}

			// Get existing children list
			var children []string
			childrenData := childrenBucket.Get(parentID)
			if childrenData != nil {
				if err := json.Unmarshal(childrenData, &children); err != nil {
					return fmt.Errorf("failed to unmarshal children list: %w", err)
				}
			}

			// Add this child's ULID if not already present
			found := false
			for _, c := range children {
				if c == state.ID {
					found = true
					break
				}
			}

			if !found {
				children = append(children, state.ID)

				// Save updated children list
				childrenData, err := json.Marshal(children)
				if err != nil {
					return fmt.Errorf("failed to marshal children list: %w", err)
				}

				if err := childrenBucket.Put(parentID, childrenData); err != nil {
					return fmt.Errorf("failed to update children list: %w", err)
				}
			}
		}

		return nil
	})
}

// DeleteNodeWithIndex atomically deletes a node from the nodes bucket, removes it from status buckets,
// and updates the parent's children list.
func DeleteNodeWithIndex(db *DB, queueType string, level int, status string, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	nodeID := []byte(state.ID)
	var parentID []byte

	return db.Update(func(tx *bolt.Tx) error {
		// ParentID must be set - no path-based lookup
		if state.ParentID == "" {
			// Skip parent operations if no ParentID
			parentID = nil
		} else {
			parentID = []byte(state.ParentID)
		}

		// 1. Delete from nodes bucket (level-sharded)
		nodesBucket := GetNodesBucket(tx, queueType, level)
		if nodesBucket != nil {
			nodesBucket.Delete(nodeID) // Ignore errors
		}

		// 2. Remove from status bucket
		statusBucket := GetStatusBucket(tx, queueType, level, status)
		if statusBucket != nil {
			statusBucket.Delete(nodeID) // Ignore errors
		}

		// 3. Remove from status-lookup index
		lookupBucket := GetStatusLookupBucket(tx, queueType, level)
		if lookupBucket != nil {
			lookupBucket.Delete(nodeID) // Ignore errors
		}

		// 4. Remove from parent's children list (parent is at level-1)
		if state.ParentID != "" && level > 0 {
			parentLevel := level - 1
			childrenBucket := GetChildrenBucket(tx, queueType, parentLevel)
			if childrenBucket != nil {
				var children []string
				childrenData := childrenBucket.Get(parentID)
				if childrenData != nil {
					if err := json.Unmarshal(childrenData, &children); err == nil {
						// Remove this child's ULID
						filtered := make([]string, 0, len(children))
						for _, c := range children {
							if c != state.ID {
								filtered = append(filtered, c)
							}
						}

						// Save updated list
						if len(filtered) > 0 {
							childrenData, err := json.Marshal(filtered)
							if err == nil {
								childrenBucket.Put(parentID, childrenData)
							}
						} else {
							// No children left, remove entry
							childrenBucket.Delete(parentID)
						}
					}
				}
			}
		}

		return nil
	})
}

// GetChildrenIDsByParentID retrieves the list of child ULIDs for a given parent ULID at the given parent level.
func GetChildrenIDsByParentID(db *DB, queueType string, parentLevel int, parentID string) ([]string, error) {
	var children []string

	err := db.View(func(tx *bolt.Tx) error {
		childrenBucket := GetChildrenBucket(tx, queueType, parentLevel)
		if childrenBucket == nil {
			return fmt.Errorf("children bucket not found for %s level %d", queueType, parentLevel)
		}

		childrenData := childrenBucket.Get([]byte(parentID))
		if childrenData == nil {
			return nil // No children
		}

		if err := json.Unmarshal(childrenData, &children); err != nil {
			return fmt.Errorf("failed to unmarshal children list: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return children, nil
}

// BatchGetChildrenIDsByParentIDs retrieves child ID lists for multiple parent IDs at the given level in one transaction.
// Returns map[parentID][]childID; parents with no children have an empty slice.
func BatchGetChildrenIDsByParentIDs(db *DB, queueType string, parentLevel int, parentIDs []string) (map[string][]string, error) {
	result := make(map[string][]string)
	if len(parentIDs) == 0 {
		return result, nil
	}
	err := db.View(func(tx *bolt.Tx) error {
		childrenBucket := GetChildrenBucket(tx, queueType, parentLevel)
		if childrenBucket == nil {
			return fmt.Errorf("children bucket not found for %s level %d", queueType, parentLevel)
		}
		for _, parentID := range parentIDs {
			childrenData := childrenBucket.Get([]byte(parentID))
			if childrenData == nil {
				result[parentID] = nil
				continue
			}
			var children []string
			if err := json.Unmarshal(childrenData, &children); err != nil {
				result[parentID] = nil
				continue
			}
			result[parentID] = children
		}
		return nil
	})
	return result, err
}

// GetChildrenStatesByParentID retrieves the full NodeState for all children of a parent by parent ULID.
// parentLevel is the level of the parent; children are at parentLevel+1.
func GetChildrenStatesByParentID(db *DB, queueType string, parentLevel int, parentID string) ([]*NodeState, error) {
	childIDs, err := GetChildrenIDsByParentID(db, queueType, parentLevel, parentID)
	if err != nil {
		return nil, err
	}

	if len(childIDs) == 0 {
		return []*NodeState{}, nil
	}

	childLevel := parentLevel + 1
	var children []*NodeState

	err = db.View(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType, childLevel)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s level %d", queueType, childLevel)
		}

		for _, childID := range childIDs {
			nodeData := nodesBucket.Get([]byte(childID))
			if nodeData == nil {
				continue // Child may have been deleted
			}

			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize child node: %w", err)
			}

			children = append(children, ns)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return children, nil
}

// BatchInsertNodes inserts multiple nodes with their indices in a single transaction.
type InsertOperation struct {
	QueueType string
	Level     int
	Status    string
	State     *NodeState
}

// computeBatchInsertStatsDeltas analyzes insert operations and computes stats deltas from op fields only (no bucket lookups).
// Returns a map of bucket path (as string) -> delta count. Uses in-batch deduplication. Level-sharded buckets keyed by path including level.
func computeBatchInsertStatsDeltas(_ *bolt.Tx, ops []InsertOperation) map[string]int64 {
	deltas := make(map[string]int64)
	seenNodes := make(map[string]struct{})    // "queueType/level:nodeID"
	seenStatus := make(map[string]struct{})   // "queueType/level/status:nodeID"
	seenChildren := make(map[string]struct{}) // "queueType/parentLevel:parentID"
	seenSrcToDst := make(map[string]struct{}) // "level:srcID"
	seenDstToSrc := make(map[string]struct{}) // "level:dstID"

	nodesCounts := make(map[string]int64)    // key "queueType/level"
	statusCounts := make(map[string]int64)  // "queueType/level/status"
	childrenCounts := make(map[string]int64) // key "queueType/parentLevel"
	srcToDstCountByLevel := make(map[int]int64)
	dstToSrcCountByLevel := make(map[int]int64)

	for _, op := range ops {
		if op.State == nil || op.State.ID == "" {
			continue
		}
		nodeIDStr := op.State.ID

		nodeKey := fmt.Sprintf("%s/%d:%s", op.QueueType, op.Level, nodeIDStr)
		if _, seen := seenNodes[nodeKey]; !seen {
			seenNodes[nodeKey] = struct{}{}
			k := fmt.Sprintf("%s/%d", op.QueueType, op.Level)
			nodesCounts[k]++
		}

		statusKey := fmt.Sprintf("%s/%d/%s", op.QueueType, op.Level, op.Status)
		statusNodeKey := statusKey + ":" + nodeIDStr
		if _, seen := seenStatus[statusNodeKey]; !seen {
			seenStatus[statusNodeKey] = struct{}{}
			statusCounts[statusKey]++
		}

		if op.State.ParentID != "" && op.Level > 0 {
			parentLevel := op.Level - 1
			childrenKey := fmt.Sprintf("%s/%d:%s", op.QueueType, parentLevel, op.State.ParentID)
			if _, seen := seenChildren[childrenKey]; !seen {
				seenChildren[childrenKey] = struct{}{}
				k := fmt.Sprintf("%s/%d", op.QueueType, parentLevel)
				childrenCounts[k]++
			}
		}

		if op.QueueType == "DST" && op.State.SrcID != "" {
			dstKey := fmt.Sprintf("%d:%s", op.Level, nodeIDStr)
			if _, seen := seenDstToSrc[dstKey]; !seen {
				seenDstToSrc[dstKey] = struct{}{}
				dstToSrcCountByLevel[op.Level]++
			}
			srcKey := fmt.Sprintf("%d:%s", op.Level, op.State.SrcID)
			if _, seen := seenSrcToDst[srcKey]; !seen {
				seenSrcToDst[srcKey] = struct{}{}
				srcToDstCountByLevel[op.Level]++
			}
		}
	}

	// Convert to bucket path strings (level-sharded)
	for key, count := range nodesCounts {
		parts := strings.Split(key, "/")
		if len(parts) == 2 {
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			path := strings.Join(GetNodesBucketPath(queueType, level), "/")
			deltas[path] += count
		}
	}

	for statusKey, count := range statusCounts {
		parts := strings.Split(statusKey, "/")
		if len(parts) == 3 {
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			status := parts[2]
			path := strings.Join(GetStatusBucketPath(queueType, level, status), "/")
			deltas[path] += count
		}
	}

	for key, count := range childrenCounts {
		parts := strings.Split(key, "/")
		if len(parts) == 2 {
			queueType := parts[0]
			level, _ := strconv.Atoi(parts[1])
			path := strings.Join(GetChildrenBucketPath(queueType, level), "/")
			deltas[path] += count
		}
	}

	for level, count := range srcToDstCountByLevel {
		if count > 0 {
			path := strings.Join(GetSrcToDstBucketPath(level), "/")
			deltas[path] += count
		}
	}
	for level, count := range dstToSrcCountByLevel {
		if count > 0 {
			path := strings.Join(GetDstToSrcBucketPath(level), "/")
			deltas[path] += count
		}
	}

	return deltas
}

// BatchInsertNodes inserts multiple nodes with their indices in a single transaction.
// SrcID is already populated in NodeState during matching, so no join-lookup needed.
func BatchInsertNodes(db *DB, ops []InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
		// Ensure stats bucket exists
		if _, err := getStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		// Compute stats deltas BEFORE executing writes (check what exists first)
		statsDeltas := computeBatchInsertStatsDeltas(tx, ops)

		// Execute all inserts
		for _, op := range ops {
			if op.State == nil || op.State.ID == "" {
				return fmt.Errorf("node state must have ID (ULID)")
			}

			// Ensure level shard exists (e.g. level 0 for root seeding; created on demand)
			if err := EnsureLevelBucket(tx, op.QueueType, op.Level); err != nil {
				return fmt.Errorf("ensure level %d for %s: %w", op.Level, op.QueueType, err)
			}

			nodeID := []byte(op.State.ID)
			var parentID []byte

			// ParentID must be set - no path-based lookup
			if op.State.ParentID != "" {
				parentID = []byte(op.State.ParentID)
			}

			// Get nodes bucket for this level (level-sharded)
			nodesBucket := GetNodesBucket(tx, op.QueueType, op.Level)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found for %s level %d", op.QueueType, op.Level)
			}

			if op.State.TraversalStatus == "" {
				op.State.TraversalStatus = op.Status
			}

			// 1. Insert into nodes bucket
			nodeData, err := op.State.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize node: %w", err)
			}

			if err := nodesBucket.Put(nodeID, nodeData); err != nil {
				return fmt.Errorf("failed to insert node: %w", err)
			}

			// 2. Add to status bucket
			statusBucket, err := GetOrCreateStatusBucket(tx, op.QueueType, op.Level, op.Status)
			if err != nil {
				return fmt.Errorf("failed to get status bucket: %w", err)
			}

			if err := statusBucket.Put(nodeID, []byte{}); err != nil {
				return fmt.Errorf("failed to add to status bucket: %w", err)
			}

			// 3. Update status-lookup index
			if err := UpdateStatusLookup(tx, op.QueueType, op.Level, nodeID, op.Status); err != nil {
				return fmt.Errorf("failed to update status-lookup: %w", err)
			}

			// 4. Update children index (parent is at level op.Level-1)
			if op.State.ParentID != "" && op.Level > 0 {
				parentLevel := op.Level - 1
				childrenBucket := GetChildrenBucket(tx, op.QueueType, parentLevel)
				if childrenBucket == nil {
					return fmt.Errorf("children bucket not found for %s level %d", op.QueueType, parentLevel)
				}

				var children []string
				childrenData := childrenBucket.Get(parentID)
				if childrenData != nil {
					json.Unmarshal(childrenData, &children)
				}

				found := false
				for _, c := range children {
					if c == op.State.ID {
						found = true
						break
					}
				}

				if !found {
					children = append(children, op.State.ID)
					childrenData, _ := json.Marshal(children)
					childrenBucket.Put(parentID, childrenData)
				}
			}

			// Store lookup mappings if SrcID is present in NodeState (for backward compatibility)
			// For DST nodes, store DST→SRC and SRC→DST mappings
			// Note: Stats deltas are already computed in computeBatchInsertStatsDeltas
			if op.QueueType == "DST" && op.State.SrcID != "" {
				// Store DST→SRC mapping at this level
				dstToSrcBucket, err := GetOrCreateDstToSrcBucket(tx, op.Level)
				if err == nil {
					dstToSrcBucket.Put(nodeID, []byte(op.State.SrcID))
				}
				// Store SRC→DST mapping at this level (SRC node at same depth)
				srcToDstBucket, err := GetOrCreateSrcToDstBucket(tx, op.Level)
				if err == nil {
					srcIDBytes := []byte(op.State.SrcID)
					srcToDstBucket.Put(srcIDBytes, nodeID)
				}
			}

			// Note: Path-to-ULID mappings are queued via OutputBuffer.AddPathToULIDMapping()
			// during task completion (queue.go), not here. This avoids duplicates and ensures
			// proper ordering with other buffered operations.
		}

		// Apply all stats updates in one batch
		for bucketPathStr, delta := range statsDeltas {
			// Convert string path back to []string for UpdateBucketStatsInTx
			bucketPath := strings.Split(bucketPathStr, "/")
			if err := UpdateBucketStatsInTx(tx, bucketPath, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", bucketPathStr, err)
			}
		}

		return nil
	})
}

// BatchDeleteNodes deletes multiple nodes by their IDs in a single transaction.
// For each node ID, it retrieves the node state to determine level and status,
// then deletes from all relevant buckets (nodes, status, status-lookup, children).
func BatchDeleteNodes(db *DB, queueType string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	return db.Update(func(tx *bolt.Tx) error {
		// Track parent updates per level: level -> parentID -> remaining children
		parentUpdatesByLevel := make(map[int]map[string][]string)
		statsDeltas := make(map[string]int64)

		for _, nodeIDStr := range nodeIDs {
			nodeID := []byte(nodeIDStr)

			// We need to find the node to get its level - try levels 0..maxKnownDepth or scan
			// For simplicity: get max depth and scan levels, or require level to be passed in.
			// BatchDeleteNodes is called with nodeIDs - we don't have level. So we must find the node first.
			// Option: iterate level buckets and look for nodeID in each level's nodes bucket.
			var ns *NodeState
			var nodeLevel int
			var found bool
			maxDepth := db.GetMaxKnownDepth(queueType)
			for level := 0; level <= maxDepth && !found; level++ {
				nodesBucket := GetNodesBucket(tx, queueType, level)
				if nodesBucket == nil {
					continue
				}
				nodeData := nodesBucket.Get(nodeID)
				if nodeData != nil {
					var err error
					ns, err = DeserializeNodeState(nodeData)
					if err != nil {
						return fmt.Errorf("failed to deserialize node state for %s: %w", nodeIDStr, err)
					}
					nodeLevel = level
					found = true
					break
				}
			}
			if !found || ns == nil {
				continue // Node already deleted or not found, skip
			}

			nodesBucket := GetNodesBucket(tx, queueType, nodeLevel)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found for %s level %d", queueType, nodeLevel)
			}
			childrenBucket := GetChildrenBucket(tx, queueType, nodeLevel)
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s level %d", queueType, nodeLevel)
			}

			// Determine current status from status-lookup
			lookupBucket := GetStatusLookupBucket(tx, queueType, nodeLevel)
			var currentStatus string
			if lookupBucket != nil {
				statusData := lookupBucket.Get(nodeID)
				if statusData != nil {
					currentStatus = string(statusData)
				}
			}

			// 1. Delete from nodes bucket
			if err := nodesBucket.Delete(nodeID); err != nil {
				return fmt.Errorf("failed to delete from nodes bucket: %w", err)
			}
			// Decrement nodes bucket count
			nodesPath := GetNodesBucketPath(queueType, nodeLevel)
			statsDeltas[strings.Join(nodesPath, "/")]--

			// 2. Delete from status bucket
			if currentStatus != "" {
				statusBucket := GetStatusBucket(tx, queueType, nodeLevel, currentStatus)
				if statusBucket != nil {
					statusBucket.Delete(nodeID) // Ignore errors
					// Decrement status bucket count
					statusPath := GetStatusBucketPath(queueType, nodeLevel, currentStatus)
					statsDeltas[strings.Join(statusPath, "/")]--
				}
			}

			// 3. Delete from status-lookup bucket
			if lookupBucket != nil {
				lookupBucket.Delete(nodeID) // Ignore errors
			}

			// 4. Track parent for children list update (parent is at nodeLevel-1)
			if ns.ParentID != "" && nodeLevel > 0 {
				parentLevel := nodeLevel - 1
				parentChildrenBucket := GetChildrenBucket(tx, queueType, parentLevel)
				if parentChildrenBucket == nil {
					continue
				}
				if parentUpdatesByLevel[parentLevel] == nil {
					parentUpdatesByLevel[parentLevel] = make(map[string][]string)
				}
				if _, exists := parentUpdatesByLevel[parentLevel][ns.ParentID]; !exists {
					// Load current children list
					parentID := []byte(ns.ParentID)
					childrenData := parentChildrenBucket.Get(parentID)
					if childrenData != nil {
						var children []string
						if err := json.Unmarshal(childrenData, &children); err == nil {
							parentUpdatesByLevel[parentLevel][ns.ParentID] = children
						}
					} else {
						parentUpdatesByLevel[parentLevel][ns.ParentID] = []string{}
					}
				}
				// Remove this child from the list
				children := parentUpdatesByLevel[parentLevel][ns.ParentID]
				filtered := make([]string, 0, len(children))
				for _, c := range children {
					if c != nodeIDStr {
						filtered = append(filtered, c)
					}
				}
				parentUpdatesByLevel[parentLevel][ns.ParentID] = filtered
			}

			// 5. Delete node's own children list (if folder) - children at this node's level
			if ns.Type == "folder" {
				if childrenBucket.Get(nodeID) != nil {
					childrenBucket.Delete(nodeID)
					// Decrement children bucket count
					childrenPath := GetChildrenBucketPath(queueType, nodeLevel)
					statsDeltas[strings.Join(childrenPath, "/")]--
				}
			}

			// 6. Delete from join-lookup tables (at this level)
			switch queueType {
			case "SRC":
				srcToDstBucket := GetSrcToDstBucket(tx, nodeLevel)
				if srcToDstBucket != nil {
					srcToDstBucket.Delete(nodeID)
				}
			case "DST":
				dstToSrcBucket := GetDstToSrcBucket(tx, nodeLevel)
				if dstToSrcBucket != nil {
					dstToSrcBucket.Delete(nodeID)
				}
			}
		}

		// Apply all parent children list updates (per level)
		for parentLevel, parentUpdates := range parentUpdatesByLevel {
			childrenBucket := GetChildrenBucket(tx, queueType, parentLevel)
			if childrenBucket == nil {
				continue
			}
			for parentIDStr, children := range parentUpdates {
				parentID := []byte(parentIDStr)
				if len(children) > 0 {
					childrenData, err := json.Marshal(children)
					if err != nil {
						return fmt.Errorf("failed to marshal children list: %w", err)
					}
					if err := childrenBucket.Put(parentID, childrenData); err != nil {
						return fmt.Errorf("failed to update children list: %w", err)
					}
				} else {
					// No children left, remove entry
					childrenBucket.Delete(parentID)
				}
			}
		}

		// Update stats bucket for all deletions
		statsBucket, err := getStatsBucket(tx)
		if err == nil && statsBucket != nil {
			for pathStr, delta := range statsDeltas {
				keyBytes := []byte(pathStr)

				// Get current count
				var currentCount int64
				existingValue := statsBucket.Get(keyBytes)
				if existingValue != nil {
					currentCount = int64(binary.BigEndian.Uint64(existingValue))
				}

				// Compute new count
				newCount := currentCount + delta
				if newCount < 0 {
					newCount = 0
				}

				if newCount == 0 {
					// Remove stats entry if count is zero
					statsBucket.Delete(keyBytes)
				} else {
					// Store new count as 8-byte big-endian int64
					valueBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(valueBytes, uint64(newCount))
					statsBucket.Put(keyBytes, valueBytes)
				}
			}
		}

		return nil
	})
}
