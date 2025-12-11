// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// UpdateNodeStatus updates a node's traversal status by moving it between status buckets.
// The node data remains in the nodes bucket; only the status membership changes.
// Returns the updated NodeState.
func UpdateNodeStatus(db *DB, queueType string, level int, oldStatus, newStatus, path string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.Update(func(tx *bolt.Tx) error {
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

		nodeState = ns

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

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// UpdateNodeCopyStatus updates a node's copy status in the metadata (in nodes bucket).
// This only updates the CopyStatus field in NodeState without changing status bucket membership.
func UpdateNodeCopyStatus(db *DB, queueType string, level int, status, path string, newCopyStatus string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.Update(func(tx *bolt.Tx) error {
		// Look up node's ULID by path
		nodeIDStr, err := getNodeIDByPath(tx, queueType, path)
		if err != nil {
			return fmt.Errorf("node not found for path %s: %w", path, err)
		}
		nodeID := []byte(nodeIDStr)

		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		nodeData := nodesBucket.Get(nodeID)
		if nodeData == nil {
			return fmt.Errorf("node not found: %s", path)
		}

		ns, err := DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		// Update copy status and copy_needed flag
		ns.CopyStatus = newCopyStatus
		ns.CopyNeeded = (newCopyStatus == CopyStatusPending)

		// Serialize and save
		updatedData, err := ns.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		if err := nodesBucket.Put(nodeID, updatedData); err != nil {
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

// GetNodeStateByPath retrieves a NodeState by path.
// This function looks up the node's ULID by path first, then retrieves the node.
func GetNodeStateByPath(db *DB, queueType string, path string) (*NodeState, error) {
	var nodeState *NodeState

	err := db.View(func(tx *bolt.Tx) error {
		nodeID, err := getNodeIDByPath(tx, queueType, path)
		if err != nil {
			return err
		}

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

// BatchUpdateNodeStatus updates multiple nodes from one status to another in a single transaction.
func BatchUpdateNodeStatus(db *DB, queueType string, level int, oldStatus, newStatus string, paths []string) (map[string]*NodeState, error) {
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

		for _, path := range paths {
			// Look up node's ULID by path
			nodeIDStr, err := getNodeIDByPath(tx, queueType, path)
			if err != nil {
				// Skip if not found (may have been processed by another worker)
				continue
			}
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

			results[path] = ns
		}

		return nil
	})

	return results, err
}

// BatchUpdateNodeCopyStatus updates copy status for multiple nodes in one transaction.
func BatchUpdateNodeCopyStatus(db *DB, queueType string, level int, status string, newCopyStatus string, paths []string) (map[string]*NodeState, error) {
	results := make(map[string]*NodeState)

	err := db.Update(func(tx *bolt.Tx) error {
		nodesBucket := GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for %s", queueType)
		}

		for _, path := range paths {
			// Look up node's ULID by path
			nodeIDStr, err := getNodeIDByPath(tx, queueType, path)
			if err != nil {
				continue // Skip if not found
			}
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
			ns.CopyNeeded = (newCopyStatus == CopyStatusPending)

			updatedData, err := ns.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize node state: %w", err)
			}

			if err := nodesBucket.Put(nodeID, updatedData); err != nil {
				return fmt.Errorf("failed to update node: %w", err)
			}

			results[path] = ns
		}

		return nil
	})

	return results, err
}
