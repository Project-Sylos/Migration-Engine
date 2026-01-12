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

		// 1. Insert into nodes bucket
		nodesBucket := GetNodesBucket(tx, queueType)
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

		// 4. Update parent's children list in children bucket
		if state.ParentID != "" {
			childrenBucket := GetChildrenBucket(tx, queueType)
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

		// 1. Delete from nodes bucket
		nodesBucket := GetNodesBucket(tx, queueType)
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

		// 4. Remove from parent's children list
		if state.ParentID != "" {
			childrenBucket := GetChildrenBucket(tx, queueType)
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

// GetChildrenIDsByParentID retrieves the list of child ULIDs for a given parent ULID.
func GetChildrenIDsByParentID(db *DB, queueType string, parentID string) ([]string, error) {
	var children []string

	err := db.View(func(tx *bolt.Tx) error {
		childrenBucket := GetChildrenBucket(tx, queueType)
		if childrenBucket == nil {
			return fmt.Errorf("children bucket not found for %s", queueType)
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

// GetChildrenStatesByParentID retrieves the full NodeState for all children of a parent by parent ULID.
func GetChildrenStatesByParentID(db *DB, queueType string, parentID string) ([]*NodeState, error) {
	childIDs, err := GetChildrenIDsByParentID(db, queueType, parentID)
	if err != nil {
		return nil, err
	}

	if len(childIDs) == 0 {
		return []*NodeState{}, nil
	}

	var children []*NodeState

	err = db.View(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
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

// computeBatchInsertStatsDeltas analyzes insert operations and computes stats deltas.
// Returns a map of bucket path (as string) -> delta count.
// Simply counts all inserts and groups by bucket - one update per bucket.
func computeBatchInsertStatsDeltas(tx *bolt.Tx, ops []InsertOperation) map[string]int64 {
	deltas := make(map[string]int64)

	// Group by bucket path and count
	nodesCounts := make(map[string]int64)    // queueType -> count
	statusCounts := make(map[string]int64)   // "queueType/level/status" -> count
	childrenCounts := make(map[string]int64) // queueType -> count of new parent entries
	srcToDstCount := int64(0)                // Count of new src-to-dst entries
	dstToSrcCount := int64(0)                // Count of new dst-to-src entries

	for _, op := range ops {
		if op.State == nil || op.State.ID == "" {
			continue
		}

		nodeID := []byte(op.State.ID)
		var parentID []byte

		// ParentID must be set - no path-based lookup
		if op.State.ParentID != "" {
			parentID = []byte(op.State.ParentID)
		}

		// Check if node already exists - only count new nodes
		nodesBucket := GetNodesBucket(tx, op.QueueType)
		if nodesBucket != nil && nodesBucket.Get(nodeID) == nil {
			nodesCounts[op.QueueType]++
		}

		// Check if status entry already exists - only count new entries
		statusBucket := GetStatusBucket(tx, op.QueueType, op.Level, op.Status)
		if statusBucket == nil || statusBucket.Get(nodeID) == nil {
			statusKey := fmt.Sprintf("%s/%d/%s", op.QueueType, op.Level, op.Status)
			statusCounts[statusKey]++
		}

		// Check if children entry needs to be created
		if op.State.ParentID != "" {
			childrenBucket := GetChildrenBucket(tx, op.QueueType)
			if childrenBucket != nil && childrenBucket.Get(parentID) == nil {
				childrenCounts[op.QueueType]++
			}
		}

		// Check if lookup mappings need to be created (for DST nodes with SrcID)
		if op.QueueType == "DST" && op.State.SrcID != "" {
			// Check dst-to-src bucket
			dstToSrcBucket := GetDstToSrcBucket(tx)
			if dstToSrcBucket != nil && dstToSrcBucket.Get(nodeID) == nil {
				dstToSrcCount++
			}

			// Check src-to-dst bucket
			srcToDstBucket := GetSrcToDstBucket(tx)
			if srcToDstBucket != nil {
				srcIDBytes := []byte(op.State.SrcID)
				if srcToDstBucket.Get(srcIDBytes) == nil {
					srcToDstCount++
				}
			}
		}
	}

	// Convert to bucket path strings
	for queueType, count := range nodesCounts {
		path := strings.Join(GetNodesBucketPath(queueType), "/")
		deltas[path] += count
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

	for queueType, count := range childrenCounts {
		path := strings.Join(GetChildrenBucketPath(queueType), "/")
		deltas[path] += count
	}

	// Add src-to-dst and dst-to-src stats deltas
	if srcToDstCount > 0 {
		srcToDstPath := GetSrcToDstBucketPath()
		deltas[strings.Join(srcToDstPath, "/")] += srcToDstCount
	}
	if dstToSrcCount > 0 {
		dstToSrcPath := GetDstToSrcBucketPath()
		deltas[strings.Join(dstToSrcPath, "/")] += dstToSrcCount
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
		var nodesBucket *bolt.Bucket
		var currentQueueType string

		for _, op := range ops {
			if op.State == nil || op.State.ID == "" {
				return fmt.Errorf("node state must have ID (ULID)")
			}

			nodeID := []byte(op.State.ID)
			var parentID []byte

			// ParentID must be set - no path-based lookup
			if op.State.ParentID != "" {
				parentID = []byte(op.State.ParentID)
			}

			// Get or cache nodes bucket
			if currentQueueType != op.QueueType {
				nodesBucket = GetNodesBucket(tx, op.QueueType)
				if nodesBucket == nil {
					return fmt.Errorf("nodes bucket not found for %s", op.QueueType)
				}
				currentQueueType = op.QueueType
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

			// 4. Update children index
			if op.State.ParentID != "" {
				childrenBucket := GetChildrenBucket(tx, op.QueueType)
				if childrenBucket == nil {
					return fmt.Errorf("children bucket not found for %s", op.QueueType)
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
				// Store DST→SRC mapping
				dstToSrcBucket, err := GetOrCreateDstToSrcBucket(tx)
				if err == nil {
					dstToSrcBucket.Put(nodeID, []byte(op.State.SrcID))
				}
				// Store SRC→DST mapping
				srcToDstBucket, err := GetOrCreateSrcToDstBucket(tx)
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
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		childrenBucket := GetChildrenBucket(tx, queueType)
		if childrenBucket == nil {
			return fmt.Errorf("children bucket not found for %s", queueType)
		}

		// Get join-lookup buckets
		var srcToDstBucket, dstToSrcBucket *bolt.Bucket
		switch queueType {
		case "SRC":
			srcToDstBucket = GetSrcToDstBucket(tx)
		case "DST":
			dstToSrcBucket = GetDstToSrcBucket(tx)
		}

		// Track parent updates and stats deltas
		parentUpdates := make(map[string][]string) // parentID -> remaining children
		statsDeltas := make(map[string]int64)      // bucket path -> delta

		for _, nodeIDStr := range nodeIDs {
			nodeID := []byte(nodeIDStr)

			// Get node state to determine level and status
			nodeData := nodesBucket.Get(nodeID)
			if nodeData == nil {
				continue // Node already deleted, skip
			}

			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state for %s: %w", nodeIDStr, err)
			}

			// Determine current status from status-lookup
			lookupBucket := GetStatusLookupBucket(tx, queueType, ns.Depth)
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
			nodesPath := GetNodesBucketPath(queueType)
			statsDeltas[strings.Join(nodesPath, "/")]--

			// 2. Delete from status bucket
			if currentStatus != "" {
				statusBucket := GetStatusBucket(tx, queueType, ns.Depth, currentStatus)
				if statusBucket != nil {
					statusBucket.Delete(nodeID) // Ignore errors
					// Decrement status bucket count
					statusPath := GetStatusBucketPath(queueType, ns.Depth, currentStatus)
					statsDeltas[strings.Join(statusPath, "/")]--
				}
			}

			// 3. Delete from status-lookup bucket
			if lookupBucket != nil {
				lookupBucket.Delete(nodeID) // Ignore errors
			}

			// 4. Track parent for children list update
			if ns.ParentID != "" {
				if _, exists := parentUpdates[ns.ParentID]; !exists {
					// Load current children list
					parentID := []byte(ns.ParentID)
					childrenData := childrenBucket.Get(parentID)
					if childrenData != nil {
						var children []string
						if err := json.Unmarshal(childrenData, &children); err == nil {
							parentUpdates[ns.ParentID] = children
						}
					} else {
						parentUpdates[ns.ParentID] = []string{}
					}
				}
				// Remove this child from the list
				children := parentUpdates[ns.ParentID]
				filtered := make([]string, 0, len(children))
				for _, c := range children {
					if c != nodeIDStr {
						filtered = append(filtered, c)
					}
				}
				parentUpdates[ns.ParentID] = filtered
			}

			// 5. Delete node's own children list (if folder)
			if ns.Type == "folder" {
				if childrenBucket.Get(nodeID) != nil {
					childrenBucket.Delete(nodeID)
					// Decrement children bucket count
					childrenPath := GetChildrenBucketPath(queueType)
					statsDeltas[strings.Join(childrenPath, "/")]--
				}
			}

			// 6. Delete from join-lookup tables
			if queueType == "SRC" && srcToDstBucket != nil {
				srcToDstBucket.Delete(nodeID)
			} else if queueType == "DST" && dstToSrcBucket != nil {
				dstToSrcBucket.Delete(nodeID)
			}

			// 7. Delete from path-to-ulid lookup table
			if ns.Path != "" {
				DeletePathToULIDMapping(tx, queueType, ns.Path) // Ignore errors
			}
		}

		// Apply all parent children list updates
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
