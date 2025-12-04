// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

// HashPath creates a fixed-length hash of a normalized path for use as keys.
// Uses SHA256 and returns first 32 hex characters (16 bytes).
// This ensures paths can be used as keys even if they contain special characters.
func HashPath(path string) string {
	h := sha256.Sum256([]byte(path))
	return hex.EncodeToString(h[:16]) // 32 hex characters
}

// Status constants for both traversal and copy phases
const (
	StatusPending    = "pending"
	StatusSuccessful = "successful"
	StatusFailed     = "failed"
	StatusNotOnSrc   = "not_on_src" // Only for dst nodes during traversal
)

// Legacy constants for compatibility during migration
const (
	TraversalStatusPending    = StatusPending
	TraversalStatusSuccessful = StatusSuccessful
	TraversalStatusFailed     = StatusFailed
	TraversalStatusNotOnSrc   = StatusNotOnSrc
	CopyStatusPending         = StatusPending
	CopyStatusSuccessful      = StatusSuccessful
	CopyStatusFailed          = StatusFailed
)

// Bucket path constants
const (
	BucketSrc  = "SRC"
	BucketDst  = "DST"
	BucketLogs = "LOGS"
)

// Sub-bucket names
const (
	SubBucketNodes    = "nodes"
	SubBucketChildren = "children"
	SubBucketLevels   = "levels"
)

// FormatLevel formats a level number as an 8-digit zero-padded string.
func FormatLevel(level int) string {
	return fmt.Sprintf("%08d", level)
}

// ParseLevel parses a level string back to an integer.
func ParseLevel(levelStr string) (int, error) {
	return strconv.Atoi(levelStr)
}

// GetNodesBucketPath returns the bucket path for the nodes bucket.
// Returns: ["SRC", "nodes"] or ["DST", "nodes"]
func GetNodesBucketPath(queueType string) []string {
	return []string{queueType, SubBucketNodes}
}

// GetChildrenBucketPath returns the bucket path for the children bucket.
// Returns: ["SRC", "children"] or ["DST", "children"]
func GetChildrenBucketPath(queueType string) []string {
	return []string{queueType, SubBucketChildren}
}

// GetLevelBucketPath returns the bucket path for a specific level.
// Returns: ["SRC", "levels", "00000001"] or ["DST", "levels", "00000001"]
func GetLevelBucketPath(queueType string, level int) []string {
	return []string{queueType, SubBucketLevels, FormatLevel(level)}
}

// GetStatusBucketPath returns the bucket path for a specific status at a level.
// Returns: ["SRC", "levels", "00000001", "pending"]
func GetStatusBucketPath(queueType string, level int, status string) []string {
	return []string{queueType, SubBucketLevels, FormatLevel(level), status}
}

// GetLogsBucketPath returns the bucket path for logs.
// Returns: ["LOGS"]
func GetLogsBucketPath() []string {
	return []string{BucketLogs}
}

// GetQueueStatsBucketPath returns the bucket path for queue statistics.
// Returns: ["STATS", "queue-stats"]
func GetQueueStatsBucketPath() []string {
	return []string{StatsBucketName, "queue-stats"}
}

// EnsureLevelBucket creates a level bucket and its status sub-buckets if they don't exist.
func EnsureLevelBucket(tx *bolt.Tx, queueType string, level int) error {
	// Navigate to the levels bucket
	topBucket := tx.Bucket([]byte(queueType))
	if topBucket == nil {
		return fmt.Errorf("top-level bucket %s not found", queueType)
	}

	levelsBucket := topBucket.Bucket([]byte(SubBucketLevels))
	if levelsBucket == nil {
		return fmt.Errorf("levels bucket not found in %s", queueType)
	}

	// Create the level bucket
	levelStr := FormatLevel(level)
	levelBucket, err := levelsBucket.CreateBucketIfNotExists([]byte(levelStr))
	if err != nil {
		return fmt.Errorf("failed to create level bucket %s: %w", levelStr, err)
	}

	// Create status sub-buckets
	statuses := []string{StatusPending, StatusSuccessful, StatusFailed}
	if queueType == BucketDst {
		statuses = append(statuses, StatusNotOnSrc)
	}

	for _, status := range statuses {
		if _, err := levelBucket.CreateBucketIfNotExists([]byte(status)); err != nil {
			return fmt.Errorf("failed to create status bucket %s: %w", status, err)
		}
	}

	return nil
}

// GetNodesBucket returns the nodes bucket for a queue type.
func GetNodesBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	return getBucket(tx, GetNodesBucketPath(queueType))
}

// GetChildrenBucket returns the children bucket for a queue type.
func GetChildrenBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	return getBucket(tx, GetChildrenBucketPath(queueType))
}

// GetStatusBucket returns the status bucket for a specific level and status.
func GetStatusBucket(tx *bolt.Tx, queueType string, level int, status string) *bolt.Bucket {
	return getBucket(tx, GetStatusBucketPath(queueType, level, status))
}

// GetOrCreateStatusBucket returns or creates the status bucket for a specific level and status.
func GetOrCreateStatusBucket(tx *bolt.Tx, queueType string, level int, status string) (*bolt.Bucket, error) {
	// Ensure the level bucket exists first
	if err := EnsureLevelBucket(tx, queueType, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetStatusBucketPath(queueType, level, status))
}

// GetLogsBucket returns the logs bucket.
func GetLogsBucket(tx *bolt.Tx) *bolt.Bucket {
	return tx.Bucket([]byte(BucketLogs))
}

// GetQueueStatsBucket returns the queue-stats bucket.
func GetQueueStatsBucket(tx *bolt.Tx) *bolt.Bucket {
	return getBucket(tx, GetQueueStatsBucketPath())
}

// GetOrCreateQueueStatsBucket returns or creates the queue-stats bucket.
func GetOrCreateQueueStatsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	return getOrCreateBucket(tx, GetQueueStatsBucketPath())
}
