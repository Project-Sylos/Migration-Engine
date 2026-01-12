// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/binary"
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"
)

const (
	// StatsBucketName is the name of the top-level stats bucket
	StatsBucketName = "STATS"
)

// bucketPathToString converts a bucket path array to a canonical string representation.
// Example: ["SRC", "levels", "00000001", "pending"] -> "SRC/levels/00000001/pending"
func bucketPathToString(bucketPath []string) string {
	return strings.Join(bucketPath, "/")
}

// getStatsBucket returns the stats bucket, creating it if it doesn't exist.
// Stats bucket is under Traversal-Data/STATS
func getStatsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	// Navigate through Traversal-Data -> STATS
	traversalBucket, err := tx.CreateBucketIfNotExists([]byte("Traversal-Data"))
	if err != nil {
		return nil, fmt.Errorf("failed to get Traversal-Data bucket: %w", err)
	}
	bucket, err := traversalBucket.CreateBucketIfNotExists([]byte(StatsBucketName))
	if err != nil {
		return nil, fmt.Errorf("failed to get stats bucket: %w", err)
	}
	return bucket, nil
}

// UpdateBucketStatsInTx updates the count for a bucket path by the given delta within an existing transaction.
// If the bucket path doesn't exist in stats, it's created with the delta value.
// Delta can be positive (increment) or negative (decrement).
// This is the internal function that can be called from within an existing transaction.
func UpdateBucketStatsInTx(tx *bolt.Tx, bucketPath []string, delta int64) error {
	if len(bucketPath) == 0 {
		return nil // Don't track empty paths
	}

	statsBucket, err := getStatsBucket(tx)
	if err != nil {
		return err
	}

	key := bucketPathToString(bucketPath)
	keyBytes := []byte(key)

	// Get current count (defaults to 0 if not exists)
	var currentCount int64
	existingValue := statsBucket.Get(keyBytes)
	if existingValue != nil {
		currentCount = int64(binary.BigEndian.Uint64(existingValue))
	}

	// Compute new count
	newCount := currentCount + delta

	// Update or delete stats entry
	if newCount < 0 {
		// Count should never go negative - this indicates a bug
		// For safety, set to 0 and log (but don't fail the transaction)
		newCount = 0
	}

	if newCount == 0 {
		// Remove stats entry if count is zero (cleanup)
		if err := statsBucket.Delete(keyBytes); err != nil {
			return fmt.Errorf("failed to delete stats entry: %w", err)
		}
	} else {
		// Store new count as 8-byte big-endian int64
		valueBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBytes, uint64(newCount))
		if err := statsBucket.Put(keyBytes, valueBytes); err != nil {
			return fmt.Errorf("failed to update stats entry: %w", err)
		}
	}

	return nil
}

// getBucketCount retrieves the count for a bucket path from the stats bucket.
// Returns 0 if the bucket path doesn't exist in stats.
func getBucketCount(tx *bolt.Tx, bucketPath []string) (int64, error) {
	if len(bucketPath) == 0 {
		return 0, nil
	}

	// Navigate through Traversal-Data -> STATS
	traversalBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalBucket == nil {
		// Traversal-Data bucket doesn't exist yet - return 0 (safe default)
		return 0, nil
	}
	statsBucket := traversalBucket.Bucket([]byte(StatsBucketName))
	if statsBucket == nil {
		// Stats bucket doesn't exist yet - return 0 (safe default)
		return 0, nil
	}

	key := bucketPathToString(bucketPath)
	keyBytes := []byte(key)

	value := statsBucket.Get(keyBytes)
	if value == nil {
		return 0, nil
	}

	if len(value) != 8 {
		return 0, fmt.Errorf("invalid stats value length: expected 8 bytes, got %d", len(value))
	}

	count := int64(binary.BigEndian.Uint64(value))
	return count, nil
}

// initializeStatsBucket creates the stats bucket and queue-stats sub-bucket if they don't exist.
// Called during database initialization.
func initializeStatsBucket(tx *bolt.Tx) error {
	statsBucket, err := getStatsBucket(tx)
	if err != nil {
		return err
	}

	// Create queue-stats sub-bucket for queue statistics
	if _, err := statsBucket.CreateBucketIfNotExists([]byte("queue-stats")); err != nil {
		return fmt.Errorf("failed to create queue-stats bucket: %w", err)
	}

	return nil
}

// EnsureStatsBucket ensures the stats bucket exists.
// This is a public API that can be called before operations that need stats.
func (db *DB) EnsureStatsBucket() error {
	return db.Update(func(tx *bolt.Tx) error {
		_, err := getStatsBucket(tx)
		if err != nil {
			return err
		}
		// Also ensure queue-stats bucket exists
		_, err = GetOrCreateQueueStatsBucket(tx)
		return err
	})
}

// UpdateBucketStats updates the count for a bucket path by the given delta.
// Delta can be positive (increment) or negative (decrement).
// This is a public wrapper that creates its own transaction.
// Thread-safe (uses Update transaction).
// For use within an existing transaction, use UpdateBucketStatsInTx instead.
func (db *DB) UpdateBucketStats(bucketPath []string, delta int64) error {
	return db.Update(func(tx *bolt.Tx) error {
		return UpdateBucketStatsInTx(tx, bucketPath, delta)
	})
}

// GetBucketCount retrieves the count for a bucket path from the stats bucket.
// This is the public API for reading stats. Returns 0 if stats don't exist.
// Thread-safe (uses View transaction).
func (db *DB) GetBucketCount(bucketPath []string) (int64, error) {
	var count int64
	err := db.View(func(tx *bolt.Tx) error {
		var err error
		count, err = getBucketCount(tx, bucketPath)
		return err
	})
	return count, err
}

// HasBucketItems is a convenience wrapper that checks if a bucket has any items.
// Returns true if count > 0, false otherwise.
func (db *DB) HasBucketItems(bucketPath []string) (bool, error) {
	count, err := db.GetBucketCount(bucketPath)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetQueueStats retrieves queue statistics from the queue-stats bucket.
// Returns the JSON-encoded stats for the specified queue key (e.g., "src-traversal", "dst-traversal").
// Returns nil if the stats don't exist.
func (db *DB) GetQueueStats(queueKey string) ([]byte, error) {
	var stats []byte
	err := db.View(func(tx *bolt.Tx) error {
		queueStatsBucket := GetQueueStatsBucket(tx)
		if queueStatsBucket == nil {
			return nil // Bucket doesn't exist, return nil stats
		}

		value := queueStatsBucket.Get([]byte(queueKey))
		if value != nil {
			stats = make([]byte, len(value))
			copy(stats, value)
		}
		return nil
	})
	return stats, err
}

// GetAllQueueStats retrieves all queue statistics from the queue-stats bucket.
// Returns a map of queue key -> JSON-encoded stats.
func (db *DB) GetAllQueueStats() (map[string][]byte, error) {
	allStats := make(map[string][]byte)
	err := db.View(func(tx *bolt.Tx) error {
		queueStatsBucket := GetQueueStatsBucket(tx)
		if queueStatsBucket == nil {
			return nil // Bucket doesn't exist, return empty map
		}

		cursor := queueStatsBucket.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			stats := make([]byte, len(value))
			copy(stats, value)
			allStats[string(key)] = stats
		}
		return nil
	})
	return allStats, err
}
