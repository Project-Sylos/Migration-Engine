// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Index prefixes for parent→child relationships
const (
	IndexPrefixSrcChildren = "src:children"
	IndexPrefixDstChildren = "dst:children"
)

// KeyChildIndex generates a secondary index key for parent→child relationship.
// Format: {prefix}:children:{parent_path_hash}:{child_path_hash}
// Value is empty (key encodes the relationship).
func KeyChildIndex(prefix string, parentPath, childPath string) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s:%s", prefix, "children", HashPath(parentPath), HashPath(childPath)))
}

// PrefixChildIndex generates a prefix for iterating all children of a given parent.
// Format: {prefix}:children:{parent_path_hash}:
func PrefixChildIndex(prefix string, parentPath string) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s:", prefix, "children", HashPath(parentPath)))
}

// InsertNodeWithIndex atomically inserts a node into both primary store and children index.
// This ensures both entries are written together (or neither is written).
func InsertNodeWithIndex(db *DB, primaryKey []byte, state *NodeState, indexPrefix string) error {
	// Serialize the node state
	value, err := state.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	// Build index key (parent_path → child_path relationship)
	indexKey := KeyChildIndex(indexPrefix, state.ParentPath, state.Path)

	return db.Update(func(txn *badger.Txn) error {
		// Write primary entry
		if err := txn.Set(primaryKey, value); err != nil {
			return fmt.Errorf("failed to set primary key: %w", err)
		}

		// Write index entry (empty value - key encodes relationship)
		if err := txn.Set(indexKey, []byte{}); err != nil {
			return fmt.Errorf("failed to set index key: %w", err)
		}

		return nil
	})
}

// DeleteNodeWithIndex atomically deletes a node from both primary store and children index.
func DeleteNodeWithIndex(db *DB, primaryKey []byte, state *NodeState, indexPrefix string) error {
	// Build index key
	indexKey := KeyChildIndex(indexPrefix, state.ParentPath, state.Path)

	return db.Update(func(txn *badger.Txn) error {
		// Delete primary entry
		if err := txn.Delete(primaryKey); err != nil {
			return fmt.Errorf("failed to delete primary key: %w", err)
		}

		// Delete index entry
		if err := txn.Delete(indexKey); err != nil {
			return fmt.Errorf("failed to delete index key: %w", err)
		}

		return nil
	})
}

// MoveNodeWithIndex atomically moves a node from one state to another, updating both primary and index.
// This handles state transitions (pending → processing → visited) while maintaining the index.
// Note: The index entry doesn't need to change during state transitions (parent→child relationship is stable).
func MoveNodeWithIndex(db *DB, oldKey, newKey []byte, state *NodeState, indexPrefix string) error {
	// Serialize the node state
	value, err := state.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize node state: %w", err)
	}

	return db.Update(func(txn *badger.Txn) error {
		// Verify old key exists
		_, err := txn.Get(oldKey)
		if err != nil {
			return fmt.Errorf("old key not found: %w", err)
		}

		// Delete old primary entry
		if err := txn.Delete(oldKey); err != nil {
			return fmt.Errorf("failed to delete old primary key: %w", err)
		}

		// Set new primary entry
		if err := txn.Set(newKey, value); err != nil {
			return fmt.Errorf("failed to set new primary key: %w", err)
		}

		// Index entry remains unchanged (parent→child relationship doesn't change with state)

		return nil
	})
}

// FetchChildrenByParentPath fetches all children of a given parent path using the secondary index.
// Returns a map of child_path -> NodeState (keyed by actual path, not hash).
// This is the efficient O(k) lookup where k = number of children.
//
// primaryPrefixes should be a slice of prefixes to search (e.g., ["src:visited", "src:processing"]).
// This allows finding children regardless of their current state.
func FetchChildrenByParentPath(db *DB, indexPrefix string, parentPath string, primaryPrefixes []string, level int) (map[string]*NodeState, error) {
	// Build index prefix for this parent
	indexPrefixBytes := PrefixChildIndex(indexPrefix, parentPath)

	// Map to store child_path -> NodeState (keyed by actual path for easy lookup)
	children := make(map[string]*NodeState)

	// First pass: collect all child path hashes from index
	childHashes := make([]string, 0)

	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = indexPrefixBytes

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// Extract child hash from index key
			// Format: {prefix}:children:{parent_hash}:{child_hash}
			keyStr := string(key)
			parts := strings.Split(keyStr, ":")
			if len(parts) >= 4 {
				childHash := parts[len(parts)-1] // Last part is child hash
				childHashes = append(childHashes, childHash)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate index: %w", err)
	}

	if len(childHashes) == 0 {
		return children, nil // No children found
	}

	// Build a set of child hashes for fast lookup
	childHashSet := make(map[string]bool)
	for _, hash := range childHashes {
		childHashSet[hash] = true
	}

	// Second pass: fetch primary entries for each child
	// Search across all provided prefixes and all levels (children are typically at level+1)
	err = db.View(func(txn *badger.Txn) error {
		// Search each primary prefix
		for _, primaryPrefix := range primaryPrefixes {
			// Search expected level first (level+1 for children)
			expectedLevel := level + 1
			primaryPrefixBytes := PrefixForLevel(primaryPrefix, expectedLevel)

			opts := badger.DefaultIteratorOptions
			opts.Prefix = primaryPrefixBytes

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()

				// Extract hash from key: {prefix}:{level}:{hash}
				keyStr := string(key)
				parts := strings.Split(keyStr, ":")
				if len(parts) >= 3 {
					keyHash := parts[len(parts)-1]

					// Check if this is one of our children
					if childHashSet[keyHash] {
						err := item.Value(func(val []byte) error {
							state, err := DeserializeNodeState(val)
							if err != nil {
								return err
							}
							// Key by actual path for easy lookup
							children[state.Path] = state
							return nil
						})
						if err != nil {
							return err
						}
					}
				}
			}
			it.Close()

			// If we found all children, we're done
			if len(children) >= len(childHashes) {
				break
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to fetch primary entries: %w", err)
	}

	return children, nil
}

// DeleteChildrenIndexPrefix deletes all index entries with the given prefix.
// Used during pruning operations to clean up index entries.
func DeleteChildrenIndexPrefix(db *DB, indexPrefix string) error {
	prefixBytes := []byte(indexPrefix + ":")
	return db.DeletePrefix(prefixBytes)
}

// InsertOperation represents a pending node insert.
type InsertOperation struct {
	QueueType       string
	Level           int
	TraversalStatus string
	CopyStatus      string
	State           *NodeState
	IndexPrefix     string
}

// BatchInsertNodes inserts multiple nodes (and their children index entries) in one transaction.
func BatchInsertNodes(db *DB, ops []InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}
	return db.Update(func(txn *badger.Txn) error {
		for _, op := range ops {
			if op.State == nil {
				continue
			}
			key := KeyNode(op.QueueType, op.Level, op.TraversalStatus, op.CopyStatus, op.State.Path)
			value, err := op.State.Serialize()
			if err != nil {
				return err
			}
			indexKey := KeyChildIndex(op.IndexPrefix, op.State.ParentPath, op.State.Path)

			if err := txn.Set(key, value); err != nil {
				return err
			}
			if err := txn.Set(indexKey, []byte{}); err != nil {
				return err
			}
		}
		return nil
	})
}
