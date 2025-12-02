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
func getStatsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket, err := tx.CreateBucketIfNotExists([]byte(StatsBucketName))
	if err != nil {
		return nil, fmt.Errorf("failed to get stats bucket: %w", err)
	}
	return bucket, nil
}

// updateBucketStats updates the count for a bucket path by the given delta.
// If the bucket path doesn't exist in stats, it's created with the delta value.
// Delta can be positive (increment) or negative (decrement).
func updateBucketStats(tx *bolt.Tx, bucketPath []string, delta int64) error {
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

// setBucketStats sets the count for a bucket path to an absolute value.
// Used by SyncCounts to set accurate counts after scanning.
func setBucketStats(tx *bolt.Tx, bucketPath []string, count int64) error {
	if len(bucketPath) == 0 {
		return nil // Don't track empty paths
	}

	statsBucket, err := getStatsBucket(tx)
	if err != nil {
		return err
	}

	key := bucketPathToString(bucketPath)
	keyBytes := []byte(key)

	if count < 0 {
		count = 0 // Safety check
	}

	if count == 0 {
		// Remove stats entry if count is zero (cleanup)
		if err := statsBucket.Delete(keyBytes); err != nil {
			return fmt.Errorf("failed to delete stats entry: %w", err)
		}
	} else {
		// Store count as 8-byte big-endian int64
		valueBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBytes, uint64(count))
		if err := statsBucket.Put(keyBytes, valueBytes); err != nil {
			return fmt.Errorf("failed to set stats entry: %w", err)
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

	statsBucket := tx.Bucket([]byte(StatsBucketName))
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

// initializeStatsBucket creates the stats bucket if it doesn't exist.
// Called during database initialization.
func initializeStatsBucket(tx *bolt.Tx) error {
	_, err := getStatsBucket(tx)
	return err
}

// EnsureStatsBucket ensures the stats bucket exists.
// This is a public API that can be called before operations that need stats.
func (db *DB) EnsureStatsBucket() error {
	return db.Update(func(tx *bolt.Tx) error {
		_, err := getStatsBucket(tx)
		return err
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

// SyncCounts manually scans all buckets (except STATS itself) and updates the stats bucket
// with accurate counts. This is useful for recovery, initialization, or correcting drift.
// Performs O(n) scans of all leaf buckets to ensure stats accuracy.
func (db *DB) SyncCounts() error {
	return db.Update(func(tx *bolt.Tx) error {
		// Ensure stats bucket exists
		if _, err := getStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		// Sync nodes buckets
		for _, queueType := range []string{"SRC", "DST"} {
			nodesPath := GetNodesBucketPath(queueType)
			nodesBucket := GetNodesBucket(tx, queueType)
			if nodesBucket != nil {
				count := int64(0)
				cursor := nodesBucket.Cursor()
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					count++
				}
				if err := setBucketStats(tx, nodesPath, count); err != nil {
					return fmt.Errorf("failed to sync stats for %s/nodes: %w", queueType, err)
				}
			} else {
				// Bucket doesn't exist, set count to 0
				if err := setBucketStats(tx, nodesPath, 0); err != nil {
					return fmt.Errorf("failed to sync stats for %s/nodes: %w", queueType, err)
				}
			}
		}

		// Sync children buckets
		for _, queueType := range []string{"SRC", "DST"} {
			childrenPath := GetChildrenBucketPath(queueType)
			childrenBucket := GetChildrenBucket(tx, queueType)
			if childrenBucket != nil {
				count := int64(0)
				cursor := childrenBucket.Cursor()
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					count++
				}
				if err := setBucketStats(tx, childrenPath, count); err != nil {
					return fmt.Errorf("failed to sync stats for %s/children: %w", queueType, err)
				}
			} else {
				// Bucket doesn't exist, set count to 0
				if err := setBucketStats(tx, childrenPath, 0); err != nil {
					return fmt.Errorf("failed to sync stats for %s/children: %w", queueType, err)
				}
			}
		}

		// Sync status buckets for all levels
		for _, queueType := range []string{"SRC", "DST"} {
			levelsBucket := getBucket(tx, []string{queueType, SubBucketLevels})
			if levelsBucket == nil {
				continue // No levels yet
			}

			cursor := levelsBucket.Cursor()
			for levelKey, _ := cursor.First(); levelKey != nil; levelKey, _ = cursor.Next() {
				levelBucket := levelsBucket.Bucket(levelKey)
				if levelBucket == nil {
					continue
				}

				// Parse level number
				level, err := ParseLevel(string(levelKey))
				if err != nil {
					continue // Skip invalid level keys
				}

				// Sync all status buckets for this level
				statuses := []string{StatusPending, StatusSuccessful, StatusFailed}
				if queueType == "DST" {
					statuses = append(statuses, StatusNotOnSrc)
				}

				for _, status := range statuses {
					statusBucket := levelBucket.Bucket([]byte(status))
					statusPath := GetStatusBucketPath(queueType, level, status)
					if statusBucket != nil {
						count := int64(0)
						statusCursor := statusBucket.Cursor()
						for k, _ := statusCursor.First(); k != nil; k, _ = statusCursor.Next() {
							count++
						}
						if err := setBucketStats(tx, statusPath, count); err != nil {
							return fmt.Errorf("failed to sync stats for %s/levels/%d/%s: %w", queueType, level, status, err)
						}
					} else {
						// Bucket doesn't exist, set count to 0
						if err := setBucketStats(tx, statusPath, 0); err != nil {
							return fmt.Errorf("failed to sync stats for %s/levels/%d/%s: %w", queueType, level, status, err)
						}
					}
				}
			}
		}

		// Sync LOGS bucket
		logsPath := GetLogsBucketPath()
		logsBucket := GetLogsBucket(tx)
		if logsBucket != nil {
			count := int64(0)
			cursor := logsBucket.Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				count++
			}
			if err := setBucketStats(tx, logsPath, count); err != nil {
				return fmt.Errorf("failed to sync stats for LOGS: %w", err)
			}
		} else {
			// Bucket doesn't exist, set count to 0
			if err := setBucketStats(tx, logsPath, 0); err != nil {
				return fmt.Errorf("failed to sync stats for LOGS: %w", err)
			}
		}

		return nil
	})
}
