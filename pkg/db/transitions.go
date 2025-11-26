// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// UpdateNodeStatus updates a node's traversal status by updating the key in place.
// The old key is deleted and a new key with the updated status is created.
// Format: {queueType}:{level}:{oldStatus}:{copyStatus}:{path_hash} -> {queueType}:{level}:{newStatus}:{copyStatus}:{path_hash}
func UpdateNodeStatus(db *DB, queueType string, level int, oldStatus, newStatus, copyStatus, path string) (*NodeState, error) {
	oldKey := KeyNode(queueType, level, oldStatus, copyStatus, path)
	newKey := KeyNode(queueType, level, newStatus, copyStatus, path)

	var nodeState *NodeState

	err := db.Update(func(txn *badger.Txn) error {
		// Get the value from old key
		item, err := txn.Get(oldKey)
		if err != nil {
			return fmt.Errorf("key not found with status %s: %w", oldStatus, err)
		}

		var value []byte
		err = item.Value(func(val []byte) error {
			value = make([]byte, len(val))
			copy(value, val)
			return nil
		})
		if err != nil {
			return err
		}

		// Deserialize to verify it's valid
		ns, err := DeserializeNodeState(value)
		if err != nil {
			return err
		}
		nodeState = ns

		// Delete old key and set new key atomically
		if err := txn.Delete(oldKey); err != nil {
			return err
		}
		if err := txn.Set(newKey, value); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// UpdateNodeCopyStatus updates a node's copy status by updating the key in place.
// Format: {queueType}:{level}:{traversalStatus}:{oldCopyStatus}:{path_hash} -> {queueType}:{level}:{traversalStatus}:{newCopyStatus}:{path_hash}
func UpdateNodeCopyStatus(db *DB, queueType string, level int, traversalStatus, oldCopyStatus, newCopyStatus, path string) (*NodeState, error) {
	oldKey := KeyNode(queueType, level, traversalStatus, oldCopyStatus, path)
	newKey := KeyNode(queueType, level, traversalStatus, newCopyStatus, path)

	var nodeState *NodeState

	err := db.Update(func(txn *badger.Txn) error {
		// Get the value from old key
		item, err := txn.Get(oldKey)
		if err != nil {
			return fmt.Errorf("key not found with copy status %s: %w", oldCopyStatus, err)
		}

		var value []byte
		err = item.Value(func(val []byte) error {
			value = make([]byte, len(val))
			copy(value, val)
			return nil
		})
		if err != nil {
			return err
		}

		// Deserialize to update copy_needed flag
		ns, err := DeserializeNodeState(value)
		if err != nil {
			return err
		}

		// Update copy_needed based on new copy status
		ns.CopyNeeded = (newCopyStatus == CopyStatusPending)

		// Serialize updated state
		value, err = ns.Serialize()
		if err != nil {
			return err
		}
		nodeState = ns

		// Delete old key and set new key atomically
		if err := txn.Delete(oldKey); err != nil {
			return err
		}
		if err := txn.Set(newKey, value); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodeState, nil
}

// SetNodeState stores a NodeState at the specified key.
// This is used for initial insertion or updates without state transitions.
func SetNodeState(db *DB, key []byte, state *NodeState) error {
	value, err := state.Serialize()
	if err != nil {
		return err
	}
	return db.Set(key, value)
}

// GetNodeState retrieves a NodeState by key.
func GetNodeState(db *DB, key []byte) (*NodeState, error) {
	value, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil // Key not found
	}
	return DeserializeNodeState(value)
}

// GetNodeStateByStatus retrieves a NodeState by path, level, and status.
func GetNodeStateByStatus(db *DB, queueType string, level int, traversalStatus, copyStatus, path string) (*NodeState, error) {
	key := KeyNode(queueType, level, traversalStatus, copyStatus, path)
	return GetNodeState(db, key)
}

// BatchUpdateNodeStatus updates multiple nodes from one status to another in a single transaction.
func BatchUpdateNodeStatus(db *DB, queueType string, level int, oldStatus, newStatus, copyStatus string, paths []string) (map[string]*NodeState, error) {
	results := make(map[string]*NodeState)

	err := db.Update(func(txn *badger.Txn) error {
		for _, path := range paths {
			oldKey := KeyNode(queueType, level, oldStatus, copyStatus, path)
			newKey := KeyNode(queueType, level, newStatus, copyStatus, path)

			// Get the value from old key
			item, err := txn.Get(oldKey)
			if err != nil {
				// Skip if not found (may have been processed by another worker)
				continue
			}

			var value []byte
			err = item.Value(func(val []byte) error {
				value = make([]byte, len(val))
				copy(value, val)
				return nil
			})
			if err != nil {
				return err
			}

			// Deserialize to verify it's valid
			ns, err := DeserializeNodeState(value)
			if err != nil {
				return err
			}

			// Delete old key and set new key atomically
			if err := txn.Delete(oldKey); err != nil {
				return err
			}
			if err := txn.Set(newKey, value); err != nil {
				return err
			}

			results[path] = ns
		}

		return nil
	})

	return results, err
}

// BatchUpdateNodeCopyStatus updates copy status for multiple nodes in one transaction.
func BatchUpdateNodeCopyStatus(db *DB, queueType string, level int, traversalStatus, oldCopyStatus, newCopyStatus string, paths []string) (map[string]*NodeState, error) {
	results := make(map[string]*NodeState)

	err := db.Update(func(txn *badger.Txn) error {
		for _, path := range paths {
			oldKey := KeyNode(queueType, level, traversalStatus, oldCopyStatus, path)
			newKey := KeyNode(queueType, level, traversalStatus, newCopyStatus, path)

			item, err := txn.Get(oldKey)
			if err != nil {
				continue
			}

			var value []byte
			err = item.Value(func(val []byte) error {
				value = make([]byte, len(val))
				copy(value, val)
				return nil
			})
			if err != nil {
				return err
			}

			ns, err := DeserializeNodeState(value)
			if err != nil {
				return err
			}
			ns.CopyNeeded = (newCopyStatus == CopyStatusPending)

			value, err = ns.Serialize()
			if err != nil {
				return err
			}

			if err := txn.Delete(oldKey); err != nil {
				return err
			}
			if err := txn.Set(newKey, value); err != nil {
				return err
			}

			results[path] = ns
		}
		return nil
	})

	return results, err
}
