// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// EnsureExclusionHoldingBuckets ensures that the exclusion-holding buckets exist for both SRC and DST.
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
		}

		return nil
	})
}

// ExclusionEntry represents an entry in the exclusion-holding bucket.
type ExclusionEntry struct {
	PathHash string // Path hash (key)
	Depth    int    // Depth level (value)
}

// ScanExclusionHoldingBucketByLevel scans the exclusion-holding bucket and returns entries matching the specified level.
// Returns entries for the current level, and true if any entries were found with level > currentLevel.
// Scans entire bucket O(n) but only collects entries matching currentLevel up to limit.
func ScanExclusionHoldingBucketByLevel(db *DB, queueType string, currentLevel int, limit int) ([]ExclusionEntry, bool, error) {
	var entries []ExclusionEntry
	var hasHigherLevels bool

	err := db.View(func(tx *bolt.Tx) error {
		holdingBucket := GetExclusionHoldingBucket(tx, queueType)
		if holdingBucket == nil {
			return nil // Bucket doesn't exist, no entries
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
						PathHash: string(k),
						Depth:    depth,
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

		return nil
	})

	return entries, hasHigherLevels, err
}

// AddExclusionHoldingEntry adds a path hash to the exclusion-holding bucket with its depth level.
func AddExclusionHoldingEntry(db *DB, queueType string, pathHash string, depth int) error {
	return db.Update(func(tx *bolt.Tx) error {
		holdingBucket, err := GetOrCreateExclusionHoldingBucket(tx, queueType)
		if err != nil {
			return err
		}

		// Encode depth level as 8 bytes, big-endian
		depthBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(depthBytes, uint64(depth))

		return holdingBucket.Put([]byte(pathHash), depthBytes)
	})
}

// RemoveExclusionHoldingEntry removes a path hash from the exclusion-holding bucket.
func RemoveExclusionHoldingEntry(db *DB, queueType string, pathHash string) error {
	return db.Update(func(tx *bolt.Tx) error {
		holdingBucket := GetExclusionHoldingBucket(tx, queueType)
		if holdingBucket == nil {
			return nil // Bucket doesn't exist, nothing to remove
		}

		return holdingBucket.Delete([]byte(pathHash))
	})
}

// CheckExclusionHoldingEntry checks if a path hash exists in the exclusion-holding bucket (O(1) lookup).
func CheckExclusionHoldingEntry(db *DB, queueType string, pathHash string) (bool, error) {
	var exists bool

	err := db.View(func(tx *bolt.Tx) error {
		holdingBucket := GetExclusionHoldingBucket(tx, queueType)
		if holdingBucket == nil {
			return nil // Bucket doesn't exist, entry doesn't exist
		}

		exists = holdingBucket.Get([]byte(pathHash)) != nil
		return nil
	})

	return exists, err
}

// ClearExclusionHoldingBucket removes all entries from the exclusion-holding bucket.
func ClearExclusionHoldingBucket(db *DB, queueType string) error {
	return db.Update(func(tx *bolt.Tx) error {
		holdingBucket := GetExclusionHoldingBucket(tx, queueType)
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

// CountExclusionHoldingEntries returns the number of entries in the exclusion-holding bucket.
func CountExclusionHoldingEntries(db *DB, queueType string) (int, error) {
	var count int

	err := db.View(func(tx *bolt.Tx) error {
		holdingBucket := GetExclusionHoldingBucket(tx, queueType)
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
