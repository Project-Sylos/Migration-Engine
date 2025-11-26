// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// IteratorOptions configures how keys are iterated.
type IteratorOptions struct {
	// Prefix restricts iteration to keys with this prefix
	Prefix []byte
	// Limit is the maximum number of items to return (0 = no limit)
	Limit int
	// Reverse iterates in reverse order
	Reverse bool
}

// IterateKeys iterates over keys matching the options and calls fn for each key.
// If fn returns an error, iteration stops and that error is returned.
func (db *DB) IterateKeys(opts IteratorOptions, fn func(key []byte) error) error {
	return db.View(func(txn *badger.Txn) error {
		badgerOpts := badger.DefaultIteratorOptions
		badgerOpts.Prefix = opts.Prefix
		badgerOpts.Reverse = opts.Reverse

		it := txn.NewIterator(badgerOpts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if opts.Limit > 0 && count >= opts.Limit {
				break
			}

			item := it.Item()
			key := item.Key()
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)

			if err := fn(keyCopy); err != nil {
				return err
			}

			count++
		}

		return nil
	})
}

// IterateKeyValues iterates over key-value pairs matching the options and calls fn for each pair.
func (db *DB) IterateKeyValues(opts IteratorOptions, fn func(key []byte, value []byte) error) error {
	return db.View(func(txn *badger.Txn) error {
		badgerOpts := badger.DefaultIteratorOptions
		badgerOpts.Prefix = opts.Prefix
		badgerOpts.Reverse = opts.Reverse

		it := txn.NewIterator(badgerOpts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if opts.Limit > 0 && count >= opts.Limit {
				break
			}

			item := it.Item()
			key := item.Key()
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)

			err := item.Value(func(val []byte) error {
				valueCopy := make([]byte, len(val))
				copy(valueCopy, val)
				return fn(keyCopy, valueCopy)
			})
			if err != nil {
				return err
			}

			count++
		}

		return nil
	})
}

// IterateNodeStates iterates over NodeStates matching the options and calls fn for each state.
func (db *DB) IterateNodeStates(opts IteratorOptions, fn func(key []byte, state *NodeState) error) error {
	return db.IterateKeyValues(opts, func(key []byte, value []byte) error {
		state, err := DeserializeNodeState(value)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state at key %s: %w", string(key), err)
		}
		return fn(key, state)
	})
}

// BatchFetchByPrefix fetches a batch of nodes matching the given prefix.
// Returns up to batchSize nodes.
func BatchFetchByPrefix(db *DB, prefix []byte, batchSize int) ([]*NodeState, error) {
	var results []*NodeState

	opts := IteratorOptions{
		Prefix: prefix,
		Limit:  batchSize,
	}

	err := db.IterateNodeStates(opts, func(key []byte, state *NodeState) error {
		results = append(results, state)
		return nil
	})

	return results, err
}

// CountByPrefix counts the number of keys with the given prefix.
func (db *DB) CountByPrefix(prefix []byte) (int, error) {
	count := 0
	err := db.IterateKeys(IteratorOptions{Prefix: prefix}, func(key []byte) error {
		count++
		return nil
	})
	return count, err
}

// FetchByParentPaths fetches all NodeStates where parent_path is in the provided list.
// This is used for DST task packaging - finding src children for dst parent nodes.
// Uses the secondary index for efficient O(k) lookup per parent.
//
// parentPaths should be normalized paths (ParentPath) of the parent nodes.
// primaryPrefixes should be prefixes to search (e.g., ["src:visited", "src:processing"]).
// Returns a map of parent_path -> []NodeState (children of that parent).
func FetchByParentPaths(db *DB, indexPrefix string, parentPaths []string, primaryPrefixes []string, level int) (map[string][]*NodeState, error) {
	results := make(map[string][]*NodeState)

	// For each parent path, fetch its children using the index
	for _, parentPath := range parentPaths {
		children, err := FetchChildrenByParentPath(db, indexPrefix, parentPath, primaryPrefixes, level)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch children for parent %s: %w", parentPath, err)
		}

		// Convert map to slice
		childrenList := make([]*NodeState, 0, len(children))
		for _, child := range children {
			childrenList = append(childrenList, child)
		}

		results[parentPath] = childrenList
	}

	return results, nil
}
