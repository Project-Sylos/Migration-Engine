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

// InsertNodeWithIndex atomically inserts a node into the nodes bucket, adds it to a status bucket,
// and updates the parent's children list in the children bucket.
func InsertNodeWithIndex(db *DB, queueType string, level int, status string, state *NodeState) error {
	pathHash := []byte(HashPath(state.Path))
	parentHash := []byte(HashPath(state.ParentPath))

	return db.Update(func(tx *bolt.Tx) error {
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

		if err := nodesBucket.Put(pathHash, nodeData); err != nil {
			return fmt.Errorf("failed to insert node: %w", err)
		}

		// 2. Add to status bucket
		statusBucket, err := GetOrCreateStatusBucket(tx, queueType, level, status)
		if err != nil {
			return fmt.Errorf("failed to get status bucket: %w", err)
		}

		if err := statusBucket.Put(pathHash, []byte{}); err != nil {
			return fmt.Errorf("failed to add to status bucket: %w", err)
		}

		// 3. Update status-lookup index
		if err := UpdateStatusLookup(tx, queueType, level, pathHash, status); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		// 4. Update parent's children list in children bucket
		if state.ParentPath != "" {
			childrenBucket := GetChildrenBucket(tx, queueType)
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", queueType)
			}

			// Get existing children list
			var children []string
			childrenData := childrenBucket.Get(parentHash)
			if childrenData != nil {
				if err := json.Unmarshal(childrenData, &children); err != nil {
					return fmt.Errorf("failed to unmarshal children list: %w", err)
				}
			}

			// Add this child's hash if not already present
			childHash := HashPath(state.Path)
			found := false
			for _, c := range children {
				if c == childHash {
					found = true
					break
				}
			}

			if !found {
				children = append(children, childHash)

				// Save updated children list
				childrenData, err := json.Marshal(children)
				if err != nil {
					return fmt.Errorf("failed to marshal children list: %w", err)
				}

				if err := childrenBucket.Put(parentHash, childrenData); err != nil {
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
	pathHash := []byte(HashPath(state.Path))
	parentHash := []byte(HashPath(state.ParentPath))

	return db.Update(func(tx *bolt.Tx) error {
		// 1. Delete from nodes bucket
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket != nil {
			nodesBucket.Delete(pathHash) // Ignore errors
		}

		// 2. Remove from status bucket
		statusBucket := GetStatusBucket(tx, queueType, level, status)
		if statusBucket != nil {
			statusBucket.Delete(pathHash) // Ignore errors
		}

		// 3. Remove from status-lookup index
		lookupBucket := GetStatusLookupBucket(tx, queueType, level)
		if lookupBucket != nil {
			lookupBucket.Delete(pathHash) // Ignore errors
		}

		// 4. Remove from parent's children list
		if state.ParentPath != "" {
			childrenBucket := GetChildrenBucket(tx, queueType)
			if childrenBucket != nil {
				var children []string
				childrenData := childrenBucket.Get(parentHash)
				if childrenData != nil {
					if err := json.Unmarshal(childrenData, &children); err == nil {
						// Remove this child's hash
						childHash := HashPath(state.Path)
						filtered := make([]string, 0, len(children))
						for _, c := range children {
							if c != childHash {
								filtered = append(filtered, c)
							}
						}

						// Save updated list
						if len(filtered) > 0 {
							childrenData, err := json.Marshal(filtered)
							if err == nil {
								childrenBucket.Put(parentHash, childrenData)
							}
						} else {
							// No children left, remove entry
							childrenBucket.Delete(parentHash)
						}
					}
				}
			}
		}

		return nil
	})
}

// GetChildrenHashes retrieves the list of child path hashes for a given parent path.
func GetChildrenHashes(db *DB, queueType string, parentPath string) ([]string, error) {
	parentHash := []byte(HashPath(parentPath))
	var children []string

	err := db.View(func(tx *bolt.Tx) error {
		childrenBucket := GetChildrenBucket(tx, queueType)
		if childrenBucket == nil {
			return fmt.Errorf("children bucket not found for %s", queueType)
		}

		childrenData := childrenBucket.Get(parentHash)
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

// GetChildrenStates retrieves the full NodeState for all children of a parent.
func GetChildrenStates(db *DB, queueType string, parentPath string) ([]*NodeState, error) {
	childHashes, err := GetChildrenHashes(db, queueType, parentPath)
	if err != nil {
		return nil, err
	}

	if len(childHashes) == 0 {
		return []*NodeState{}, nil
	}

	var children []*NodeState

	err = db.View(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		for _, childHash := range childHashes {
			nodeData := nodesBucket.Get([]byte(childHash))
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
		if op.State == nil {
			continue
		}

		pathHash := []byte(HashPath(op.State.Path))
		parentHash := []byte(HashPath(op.State.ParentPath))

		// Check if node already exists - only count new nodes
		nodesBucket := GetNodesBucket(tx, op.QueueType)
		if nodesBucket != nil && nodesBucket.Get(pathHash) == nil {
			nodesCounts[op.QueueType]++
		}

		// Check if status entry already exists - only count new entries
		statusBucket := GetStatusBucket(tx, op.QueueType, op.Level, op.Status)
		if statusBucket == nil || statusBucket.Get(pathHash) == nil {
			statusKey := fmt.Sprintf("%s/%d/%s", op.QueueType, op.Level, op.Status)
			statusCounts[statusKey]++
		}

		// Check if children entry needs to be created
		if op.State.ParentPath != "" {
			childrenBucket := GetChildrenBucket(tx, op.QueueType)
			if childrenBucket != nil && childrenBucket.Get(parentHash) == nil {
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
			pathHash := []byte(HashPath(op.State.Path))
			parentHash := []byte(HashPath(op.State.ParentPath))

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

			if err := nodesBucket.Put(pathHash, nodeData); err != nil {
				return fmt.Errorf("failed to insert node: %w", err)
			}

			// 2. Add to status bucket
			statusBucket, err := GetOrCreateStatusBucket(tx, op.QueueType, op.Level, op.Status)
			if err != nil {
				return fmt.Errorf("failed to get status bucket: %w", err)
			}

			if err := statusBucket.Put(pathHash, []byte{}); err != nil {
				return fmt.Errorf("failed to add to status bucket: %w", err)
			}

			// 3. Update status-lookup index
			if err := UpdateStatusLookup(tx, op.QueueType, op.Level, pathHash, op.Status); err != nil {
				return fmt.Errorf("failed to update status-lookup: %w", err)
			}

			// 4. Update children index
			if op.State.ParentPath != "" {
				childrenBucket := GetChildrenBucket(tx, op.QueueType)
				if childrenBucket == nil {
					return fmt.Errorf("children bucket not found for %s", op.QueueType)
				}

				var children []string
				childrenData := childrenBucket.Get(parentHash)
				if childrenData != nil {
					json.Unmarshal(childrenData, &children)
				}

				childHash := HashPath(op.State.Path)
				found := false
				for _, c := range children {
					if c == childHash {
						found = true
						break
					}
				}

				if !found {
					children = append(children, childHash)
					childrenData, _ := json.Marshal(children)
					childrenBucket.Put(parentHash, childrenData)
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
