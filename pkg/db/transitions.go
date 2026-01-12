// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// UpdateNodeStatusByID updates a node's traversal status by moving it between status buckets using ULID.
// The node data remains in the nodes bucket; only the status membership changes.
// Returns the updated NodeState.
func UpdateNodeStatusByID(db *DB, queueType string, level int, oldStatus, newStatus string, nodeID string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.Update(func(tx *bolt.Tx) error {
		nodeIDBytes := []byte(nodeID)

		// Get the node data from nodes bucket
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		nodeData := nodesBucket.Get(nodeIDBytes)
		if nodeData == nil {
			return fmt.Errorf("node not found in nodes bucket: %s", nodeID)
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

		if err := nodesBucket.Put(nodeIDBytes, updatedData); err != nil {
			return fmt.Errorf("failed to update node in nodes bucket: %w", err)
		}

		nodeState = ns

		// Update status bucket membership
		// Delete from old status bucket
		oldBucket := GetStatusBucket(tx, queueType, level, oldStatus)
		if oldBucket != nil {
			if err := oldBucket.Delete(nodeIDBytes); err != nil {
				return fmt.Errorf("failed to delete from old status bucket: %w", err)
			}
		}

		// Add to new status bucket
		newBucket, err := GetOrCreateStatusBucket(tx, queueType, level, newStatus)
		if err != nil {
			return fmt.Errorf("failed to get new status bucket: %w", err)
		}

		if err := newBucket.Put(nodeIDBytes, []byte{}); err != nil {
			return fmt.Errorf("failed to add to new status bucket: %w", err)
		}

		// Update status-lookup index
		if err := UpdateStatusLookup(tx, queueType, level, nodeIDBytes, newStatus); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// UpdateNodeCopyStatusByID updates a node's copy status in the metadata (in nodes bucket) using ULID.
// This only updates the CopyStatus field in NodeState without changing status bucket membership.
func UpdateNodeCopyStatusByID(db *DB, queueType string, level int, status string, nodeID string, newCopyStatus string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.Update(func(tx *bolt.Tx) error {
		nodeIDBytes := []byte(nodeID)

		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		nodeData := nodesBucket.Get(nodeIDBytes)
		if nodeData == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		// Update copy status and copy_needed flag
		ns.CopyStatus = newCopyStatus

		// Serialize and save
		updatedData, err := ns.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		if err := nodesBucket.Put(nodeIDBytes, updatedData); err != nil {
			return fmt.Errorf("failed to update node: %w", err)
		}

		nodeState = ns
		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// UpdateNodeCopyStatusTransitionByID updates a node's copy status and moves it between copy status buckets using ULID.
// This is similar to UpdateNodeStatusByID but for copy status (SRC only).
// The node data remains in the nodes bucket; only the copy status membership changes.
// Returns the updated NodeState.
func UpdateNodeCopyStatusTransitionByID(db *DB, level int, oldCopyStatus, newCopyStatus string, nodeID string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.Update(func(tx *bolt.Tx) error {
		nodeIDBytes := []byte(nodeID)

		// Get the node data from nodes bucket (SRC only for copy status)
		nodesBucket := GetNodesBucket(tx, BucketSrc)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for SRC")
		}

		nodeData := nodesBucket.Get(nodeIDBytes)
		if nodeData == nil {
			return fmt.Errorf("node not found in nodes bucket: %s", nodeID)
		}

		// Deserialize and update copy status
		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}
		ns.CopyStatus = newCopyStatus

		// Serialize updated state back to nodes bucket
		updatedData, err := ns.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		if err := nodesBucket.Put(nodeIDBytes, updatedData); err != nil {
			return fmt.Errorf("failed to update node in nodes bucket: %w", err)
		}

		nodeState = ns

		// Determine node type from NodeState for bucket routing
		nodeType := NodeTypeFile
		if ns.Type == "folder" {
			nodeType = NodeTypeFolder
		}

		// Update copy status bucket membership
		// Delete from old copy status bucket
		oldBucket := GetCopyStatusBucket(tx, level, nodeType, oldCopyStatus)
		if oldBucket != nil {
			if err := oldBucket.Delete(nodeIDBytes); err != nil {
				return fmt.Errorf("failed to delete from old copy status bucket: %w", err)
			}
		}

		// Add to new copy status bucket
		newBucket, err := GetOrCreateCopyStatusBucket(tx, level, nodeType, newCopyStatus)
		if err != nil {
			return fmt.Errorf("failed to get new copy status bucket: %w", err)
		}

		if err := newBucket.Put(nodeIDBytes, []byte{}); err != nil {
			return fmt.Errorf("failed to add to new copy status bucket: %w", err)
		}

		// Update copy status-lookup index (no node type needed in lookup)
		if err := UpdateCopyStatusLookup(tx, level, nodeIDBytes, newCopyStatus); err != nil {
			return fmt.Errorf("failed to update copy status-lookup: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// SetNodeState stores a NodeState in the nodes bucket.
// This is used for initial insertion or updates without state transitions.
// nodeID is the ULID of the node (as []byte).
func SetNodeState(db *DB, queueType string, nodeID []byte, state *NodeState) error {
	if state.ID == "" {
		return fmt.Errorf("node state must have ID (ULID)")
	}
	// Ensure the nodeID parameter matches the state's ID
	if string(nodeID) != state.ID {
		return fmt.Errorf("nodeID parameter must match state.ID")
	}

	value, err := state.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	return db.Update(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}
		return nodesBucket.Put(nodeID, value)
	})
}

// GetNodeState retrieves a NodeState from the nodes bucket by ULID.
// GetNodeState retrieves a NodeState by ULID (as string).
func GetNodeState(db *DB, queueType string, nodeID string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.View(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		nodeData := nodesBucket.Get([]byte(nodeID))
		if nodeData == nil {
			return nil // Not found
		}

		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		nodeState = ns
		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// BatchUpdateNodeStatusByID updates multiple nodes from one status to another in a single transaction using ULIDs.
func BatchUpdateNodeStatusByID(db *DB, queueType string, level int, oldStatus, newStatus string, nodeIDs []string) (map[string]*NodeState, error) {
	results := make(map[string]*NodeState)

	err := db.Update(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		oldBucket := GetStatusBucket(tx, queueType, level, oldStatus)
		newBucket, err := GetOrCreateStatusBucket(tx, queueType, level, newStatus)
		if err != nil {
			return fmt.Errorf("failed to get new status bucket: %w", err)
		}

		for _, nodeIDStr := range nodeIDs {
			nodeID := []byte(nodeIDStr)

			// Get node data
			nodeData := nodesBucket.Get(nodeID)
			if nodeData == nil {
				// Skip if not found (may have been processed by another worker)
				continue
			}

			// Deserialize and update
			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state: %w", err)
			}
			ns.TraversalStatus = newStatus

			// Serialize and save
			updatedData, err := ns.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize node state: %w", err)
			}

			if err := nodesBucket.Put(nodeID, updatedData); err != nil {
				return fmt.Errorf("failed to update node: %w", err)
			}

			// Update status bucket membership
			if oldBucket != nil {
				oldBucket.Delete(nodeID) // Ignore errors
			}

			if err := newBucket.Put(nodeID, []byte{}); err != nil {
				return fmt.Errorf("failed to add to new status bucket: %w", err)
			}

			// Update status-lookup index
			if err := UpdateStatusLookup(tx, queueType, level, nodeID, newStatus); err != nil {
				return fmt.Errorf("failed to update status-lookup: %w", err)
			}

			results[nodeIDStr] = ns
		}

		return nil
	})

	return results, err
}

// BatchUpdateNodeCopyStatusByID updates copy status for multiple nodes in one transaction using ULIDs.
func BatchUpdateNodeCopyStatusByID(db *DB, queueType string, level int, status string, newCopyStatus string, nodeIDs []string) (map[string]*NodeState, error) {
	results := make(map[string]*NodeState)

	err := db.Update(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		for _, nodeIDStr := range nodeIDs {
			nodeID := []byte(nodeIDStr)

			nodeData := nodesBucket.Get(nodeID)
			if nodeData == nil {
				continue
			}

			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state: %w", err)
			}

			ns.CopyStatus = newCopyStatus

			updatedData, err := ns.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize node state: %w", err)
			}

			if err := nodesBucket.Put(nodeID, updatedData); err != nil {
				return fmt.Errorf("failed to update node: %w", err)
			}

			results[nodeIDStr] = ns
		}

		return nil
	})

	return results, err
}
