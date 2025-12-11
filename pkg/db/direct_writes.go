// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// UpdateNodeStatusInTx updates a node's status within an existing transaction.
// This is used by queue.Complete() and queue.Fail() to write directly to BoltDB.
func UpdateNodeStatusInTx(tx *bolt.Tx, queueType string, level int, oldStatus, newStatus, path string) error {
	// Look up node's ULID by path
	nodeIDStr, err := getNodeIDByPath(tx, queueType, path)
	if err != nil {
		return fmt.Errorf("node not found for path %s: %w", path, err)
	}
	nodeID := []byte(nodeIDStr)

	// Get the node data from nodes bucket
	nodesBucket := GetNodesBucket(tx, queueType)
	if nodesBucket == nil {
		return fmt.Errorf("nodes bucket not found for %s", queueType)
	}

	nodeData := nodesBucket.Get(nodeID)
	if nodeData == nil {
		return fmt.Errorf("node not found in nodes bucket: %s", path)
	}

	// Deserialize and update traversal status
	ns, err := DeserializeNodeState(nodeData)
	if err != nil {
		return fmt.Errorf("failed to deserialize node state: %w", err)
	}
	ns.TraversalStatus = newStatus

	// Serialize updated state back to nodes bucket
	updatedData, err := ns.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	if err := nodesBucket.Put(nodeID, updatedData); err != nil {
		return fmt.Errorf("failed to update node in nodes bucket: %w", err)
	}

	// Update status bucket membership
	// Delete from old status bucket
	oldBucket := GetStatusBucket(tx, queueType, level, oldStatus)
	if oldBucket != nil {
		if err := oldBucket.Delete(nodeID); err != nil {
			return fmt.Errorf("failed to delete from old status bucket: %w", err)
		}
	}

	// Add to new status bucket
	newBucket, err := GetOrCreateStatusBucket(tx, queueType, level, newStatus)
	if err != nil {
		return fmt.Errorf("failed to get new status bucket: %w", err)
	}

	if err := newBucket.Put(nodeID, []byte{}); err != nil {
		return fmt.Errorf("failed to add to new status bucket: %w", err)
	}

	// Update status-lookup index
	if err := UpdateStatusLookup(tx, queueType, level, nodeID, newStatus); err != nil {
		return fmt.Errorf("failed to update status-lookup: %w", err)
	}

	// Stats updates are handled by batch processing in output buffer flush
	return nil
}

// BatchInsertNodesInTx inserts multiple nodes within an existing transaction.
// This is used by queue.Complete() to insert discovered children atomically.
// Stats updates are handled separately by batch processing in output buffer flush.
// joinLookupMappings is optional - if provided, maps DST node ULIDs to SRC node ULIDs for join-lookup table.
func BatchInsertNodesInTx(tx *bolt.Tx, operations []InsertOperation, joinLookupMappings map[string]string) error {
	var nodesBucket *bolt.Bucket
	var currentQueueType string

	for _, op := range operations {
		if op.State == nil || op.State.ID == "" {
			return fmt.Errorf("node state must have ID (ULID)")
		}

		// Ensure NodeState has the status field populated
		if op.State.TraversalStatus == "" {
			op.State.TraversalStatus = op.Status
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

		// 1. Insert into nodes bucket
		nodeData, err := op.State.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
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

			// Get existing children list
			var children []string
			childrenData := childrenBucket.Get(parentID)
			if childrenData != nil {
				if err := DeserializeStringSlice(childrenData, &children); err != nil {
					return fmt.Errorf("failed to unmarshal children list: %w", err)
				}
			}

			// Add this child's ULID if not already present
			found := false
			for _, c := range children {
				if c == op.State.ID {
					found = true
					break
				}
			}

			if !found {
				children = append(children, op.State.ID)
				childrenData, err := SerializeStringSlice(children)
				if err != nil {
					return fmt.Errorf("failed to marshal children list: %w", err)
				}

				if err := childrenBucket.Put(parentID, childrenData); err != nil {
					return fmt.Errorf("failed to update children list: %w", err)
				}
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

	return nil
}
