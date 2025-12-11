// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// getNodeIDByPath looks up a node by its path and returns its ULID.
// This is used when we need to find a parent's ULID from its path.
func getNodeIDByPath(tx *bolt.Tx, queueType string, path string) (string, error) {
	if path == "" {
		return "", nil
	}

	// We need to iterate through all nodes to find one with matching path
	// This is inefficient but necessary during transition
	// TODO: Consider adding a path->ULID index if this becomes a bottleneck
	nodesBucket := GetNodesBucket(tx, queueType)
	if nodesBucket == nil {
		return "", fmt.Errorf("nodes bucket not found for %s", queueType)
	}

	cursor := nodesBucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		state, err := DeserializeNodeState(value)
		if err != nil {
			continue
		}
		if state.Path == path {
			return state.ID, nil
		}
	}

	return "", fmt.Errorf("node not found for path: %s", path)
}

// InsertNodeWithIndex atomically inserts a node into the nodes bucket, adds it to a status bucket,
// and updates the parent's children list in the children bucket.
func InsertNodeWithIndex(db *DB, queueType string, level int, status string, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node ID (ULID) cannot be empty")
	}

	nodeID := []byte(state.ID)
	var parentID []byte

	return db.Update(func(tx *bolt.Tx) error {
		// Look up parent's ULID if we have parent path but not parent ID
		if state.ParentPath != "" && state.ParentID == "" {
			parentULID, err := getNodeIDByPath(tx, queueType, state.ParentPath)
			if err != nil {
				return fmt.Errorf("failed to find parent node for path %s: %w", state.ParentPath, err)
			}
			state.ParentID = parentULID
			parentID = []byte(parentULID)
		} else if state.ParentID != "" {
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
		// Look up parent's ULID if we have parent path but not parent ID
		if state.ParentPath != "" && state.ParentID == "" {
			parentULID, err := getNodeIDByPath(tx, queueType, state.ParentPath)
			if err == nil {
				state.ParentID = parentULID
				parentID = []byte(parentULID)
			}
		} else if state.ParentID != "" {
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

// GetChildrenIDs retrieves the list of child ULIDs for a given parent path.
// This function looks up the parent by path first, then gets its children.
func GetChildrenIDs(db *DB, queueType string, parentPath string) ([]string, error) {
	var children []string

	err := db.View(func(tx *bolt.Tx) error {
		// First, find the parent's ULID by path
		parentID, err := getNodeIDByPath(tx, queueType, parentPath)
		if err != nil {
			return fmt.Errorf("parent not found for path %s: %w", parentPath, err)
		}

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

// GetChildrenHashes is deprecated - use GetChildrenIDs instead.
// Kept for backward compatibility during migration.
func GetChildrenHashes(db *DB, queueType string, parentPath string) ([]string, error) {
	return GetChildrenIDs(db, queueType, parentPath)
}

// GetChildrenStates retrieves the full NodeState for all children of a parent.
func GetChildrenStates(db *DB, queueType string, parentPath string) ([]*NodeState, error) {
	childIDs, err := GetChildrenIDs(db, queueType, parentPath)
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

// FetchChildrenByParentPath is a compatibility function that fetches children for a parent.
func FetchChildrenByParentPath(db *DB, queueType string, parentPath string, levels []int) ([]*NodeState, error) {
	// Note: In the new Bolt structure, we don't filter by level in the children bucket
	// The children bucket stores all children regardless of level
	return GetChildrenStates(db, queueType, parentPath)
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

	for _, op := range ops {
		if op.State == nil || op.State.ID == "" {
			continue
		}

		nodeID := []byte(op.State.ID)
		var parentID []byte

		// Look up parent ID if needed
		if op.State.ParentPath != "" && op.State.ParentID == "" {
			parentULID, err := getNodeIDByPath(tx, op.QueueType, op.State.ParentPath)
			if err == nil {
				op.State.ParentID = parentULID
				parentID = []byte(parentULID)
			}
		} else if op.State.ParentID != "" {
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

	return deltas
}

// BatchInsertNodes inserts multiple nodes with their indices in a single transaction.
// joinLookupMappings is optional - if provided, maps DST node ULIDs to SRC node ULIDs for join-lookup table.
// Format: map[dstNodeID]srcNodeID
func BatchInsertNodes(db *DB, ops []InsertOperation, joinLookupMappings map[string]string) error {
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

			// Look up parent ID if needed
			if op.State.ParentPath != "" && op.State.ParentID == "" {
				parentULID, err := getNodeIDByPath(tx, op.QueueType, op.State.ParentPath)
				if err != nil {
					return fmt.Errorf("failed to find parent node for path %s: %w", op.State.ParentPath, err)
				}
				op.State.ParentID = parentULID
				parentID = []byte(parentULID)
			} else if op.State.ParentID != "" {
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

			// 5. Populate join-lookup for DST nodes
			if op.QueueType == BucketDst && joinLookupMappings != nil {
				if srcNodeID, exists := joinLookupMappings[op.State.ID]; exists {
					joinLookupBucket, err := GetOrCreateJoinLookupBucket(tx)
					if err != nil {
						return fmt.Errorf("failed to get join-lookup bucket: %w", err)
					}
					if err := joinLookupBucket.Put([]byte(op.State.ID), []byte(srcNodeID)); err != nil {
						return fmt.Errorf("failed to insert join-lookup entry: %w", err)
					}
				}
			}
		}

		// Apply all stats updates in one batch
		for bucketPathStr, delta := range statsDeltas {
			// Convert string path back to []string for updateBucketStats
			bucketPath := strings.Split(bucketPathStr, "/")
			if err := updateBucketStats(tx, bucketPath, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", bucketPathStr, err)
			}
		}

		return nil
	})
}
