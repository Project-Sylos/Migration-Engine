// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// EnsureExclusionHoldingBuckets ensures that the exclusion-holding and unexclusion-holding buckets exist for both SRC and DST.
// This should be called when traversal completes to ensure the buckets are ready for exclusion intent queuing.
// The buckets are created during DB initialization, but this provides a safety check.
func EnsureExclusionHoldingBuckets(db *DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
		if traversalBucket == nil {
			return fmt.Errorf("Traversal-Data bucket not found")
		}

		for _, queueType := range []string{BucketSrc, BucketDst} {
			queueBucket := traversalBucket.Bucket([]byte(queueType))
			if queueBucket == nil {
				return fmt.Errorf("queue bucket %s not found in Traversal-Data", queueType)
			}

			// Create exclusion-holding bucket if it doesn't exist
			if _, err := queueBucket.CreateBucketIfNotExists([]byte(SubBucketExclusionHolding)); err != nil {
				return fmt.Errorf("failed to create exclusion-holding bucket for %s: %w", queueType, err)
			}

			// Create unexclusion-holding bucket if it doesn't exist
			if _, err := queueBucket.CreateBucketIfNotExists([]byte(SubBucketUnexclusionHolding)); err != nil {
				return fmt.Errorf("failed to create unexclusion-holding bucket for %s: %w", queueType, err)
			}
		}

		return nil
	})
}

// ExclusionEntry represents an entry in the exclusion-holding or unexclusion-holding bucket.
type ExclusionEntry struct {
	NodeID string // ULID of the node (key)
	Depth  int    // Depth level (value)
	Mode   string // "exclude" or "unexclude" - indicates which bucket this came from
}

// ScanExclusionHoldingBucketByLevel scans both exclusion-holding and unexclusion-holding buckets and returns entries matching the specified level.
// Returns entries for the current level, and true if any entries were found with level > currentLevel.
// Scans entire buckets O(n) but only collects entries matching currentLevel up to limit.
func ScanExclusionHoldingBucketByLevel(db *DB, queueType string, currentLevel int, limit int) ([]ExclusionEntry, bool, error) {
	var entries []ExclusionEntry
	var hasHigherLevels bool

	err := db.View(func(tx *bolt.Tx) error {
		// Scan both exclusion and unexclusion buckets
		for _, mode := range []string{"exclude", "unexclude"} {
			holdingBucket := GetHoldingBucket(tx, queueType, mode)
			if holdingBucket == nil {
				continue // Bucket doesn't exist, skip
			}

			cursor := holdingBucket.Cursor()
			entriesCollected := 0

			// Scan entire bucket O(n)
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				if len(v) != 8 {
					continue // Skip invalid entries
				}

				// Decode depth level (int64 stored as 8 bytes, big-endian)
				depth := int(binary.BigEndian.Uint64(v))

				if depth == currentLevel {
					if entriesCollected < limit {
						entries = append(entries, ExclusionEntry{
							NodeID: string(k), // k is the ULID
							Depth:  depth,
							Mode:   mode,
						})
						entriesCollected++
					}
					// Continue scanning to check for higher levels
				} else if depth > currentLevel {
					hasHigherLevels = true
					// Continue scanning - may have more entries at current level
				}
				// Skip entries with depth < currentLevel (already processed in earlier rounds)
			}
		}

		return nil
	})

	return entries, hasHigherLevels, err
}

// AddHoldingEntry adds a ULID to the appropriate holding bucket (exclusion or unexclusion) with its depth level.
func AddHoldingEntry(db *DB, queueType string, nodeID string, depth int, mode string) error {
	return db.Update(func(tx *bolt.Tx) error {
		holdingBucket, err := GetOrCreateHoldingBucket(tx, queueType, mode)
		if err != nil {
			return err
		}

		// Encode depth level as 8 bytes, big-endian
		depthBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(depthBytes, uint64(depth))

		return holdingBucket.Put([]byte(nodeID), depthBytes)
	})
}

// AddExclusionHoldingEntry adds a ULID to the exclusion-holding bucket with its depth level.
// Deprecated: Use AddHoldingEntry with mode "exclude" instead.
func AddExclusionHoldingEntry(db *DB, queueType string, nodeID string, depth int) error {
	return AddHoldingEntry(db, queueType, nodeID, depth, "exclude")
}

// RemoveHoldingEntry removes a ULID from the appropriate holding bucket (exclusion or unexclusion).
func RemoveHoldingEntry(db *DB, queueType string, nodeID string, mode string) error {
	return db.Update(func(tx *bolt.Tx) error {
		holdingBucket := GetHoldingBucket(tx, queueType, mode)
		if holdingBucket == nil {
			return nil // Bucket doesn't exist, nothing to remove
		}

		return holdingBucket.Delete([]byte(nodeID))
	})
}

// RemoveExclusionHoldingEntry removes a ULID from the exclusion-holding bucket.
// Deprecated: Use RemoveHoldingEntry with mode "exclude" instead.
func RemoveExclusionHoldingEntry(db *DB, queueType string, nodeID string) error {
	return RemoveHoldingEntry(db, queueType, nodeID, "exclude")
}

// CheckHoldingEntry checks if a ULID exists in either holding bucket (O(1) lookup).
// Returns (exists, mode) where mode is "exclude", "unexclude", or "" if not found.
func CheckHoldingEntry(db *DB, queueType string, nodeID string) (bool, string, error) {
	var exists bool
	var mode string

	err := db.View(func(tx *bolt.Tx) error {
		// Check exclusion bucket first
		exclusionBucket := GetExclusionHoldingBucket(tx, queueType)
		if exclusionBucket != nil && exclusionBucket.Get([]byte(nodeID)) != nil {
			exists = true
			mode = "exclude"
			return nil
		}

		// Check unexclusion bucket
		unexclusionBucket := GetUnexclusionHoldingBucket(tx, queueType)
		if unexclusionBucket != nil && unexclusionBucket.Get([]byte(nodeID)) != nil {
			exists = true
			mode = "unexclude"
			return nil
		}

		return nil
	})

	return exists, mode, err
}

// CheckExclusionHoldingEntry checks if a ULID exists in the exclusion-holding bucket (O(1) lookup).
// Deprecated: Use CheckHoldingEntry instead.
func CheckExclusionHoldingEntry(db *DB, queueType string, nodeID string) (bool, error) {
	exists, mode, err := CheckHoldingEntry(db, queueType, nodeID)
	return exists && mode == "exclude", err
}

// ClearHoldingBucket removes all entries from the specified holding bucket.
func ClearHoldingBucket(db *DB, queueType string, mode string) error {
	return db.Update(func(tx *bolt.Tx) error {
		holdingBucket := GetHoldingBucket(tx, queueType, mode)
		if holdingBucket == nil {
			return nil // Bucket doesn't exist, nothing to clear
		}

		cursor := holdingBucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if err := holdingBucket.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
}

// ClearExclusionHoldingBucket removes all entries from the exclusion-holding bucket.
// Deprecated: Use ClearHoldingBucket with mode "exclude" instead.
func ClearExclusionHoldingBucket(db *DB, queueType string) error {
	return ClearHoldingBucket(db, queueType, "exclude")
}

// CountHoldingEntries returns the number of entries in the specified holding bucket.
func CountHoldingEntries(db *DB, queueType string, mode string) (int, error) {
	var count int

	err := db.View(func(tx *bolt.Tx) error {
		holdingBucket := GetHoldingBucket(tx, queueType, mode)
		if holdingBucket == nil {
			return nil // Bucket doesn't exist, count is 0
		}

		cursor := holdingBucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count++
		}

		return nil
	})

	return count, err
}

// CountExclusionHoldingEntries returns the number of entries in the exclusion-holding bucket.
// Deprecated: Use CountHoldingEntries with mode "exclude" instead.
func CountExclusionHoldingEntries(db *DB, queueType string) (int, error) {
	return CountHoldingEntries(db, queueType, "exclude")
}
