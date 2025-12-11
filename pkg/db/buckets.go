// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

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
	TraversalDataBucket = "Traversal-Data" // Root bucket for all traversal-related data
	BucketSrc           = "SRC"
	BucketDst           = "DST"
	BucketLogs          = "LOGS" // Separate island, not under Traversal-Data
)

// Sub-bucket names
const (
	SubBucketNodes              = "nodes"
	SubBucketChildren           = "children"
	SubBucketLevels             = "levels"
	SubBucketStatusLookup       = "status-lookup"
	SubBucketExclusionHolding   = "exclusion-holding"
	SubBucketUnexclusionHolding = "unexclusion-holding"
	SubBucketJoinLookup         = "join-lookup"
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
// Returns: ["Traversal-Data", "SRC", "nodes"] or ["Traversal-Data", "DST", "nodes"]
func GetNodesBucketPath(queueType string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketNodes}
}

// GetChildrenBucketPath returns the bucket path for the children bucket.
// Returns: ["Traversal-Data", "SRC", "children"] or ["Traversal-Data", "DST", "children"]
func GetChildrenBucketPath(queueType string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketChildren}
}

// GetLevelBucketPath returns the bucket path for a specific level.
// Returns: ["Traversal-Data", "SRC", "levels", "00000001"] or ["Traversal-Data", "DST", "levels", "00000001"]
func GetLevelBucketPath(queueType string, level int) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level)}
}

// GetStatusBucketPath returns the bucket path for a specific status at a level.
// Returns: ["Traversal-Data", "SRC", "levels", "00000001", "pending"]
func GetStatusBucketPath(queueType string, level int, status string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level), status}
}

// GetStatusLookupBucketPath returns the bucket path for the status-lookup index at a level.
// Returns: ["Traversal-Data", "SRC", "levels", "00000001", "status-lookup"]
func GetStatusLookupBucketPath(queueType string, level int) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level), SubBucketStatusLookup}
}

// GetLogsBucketPath returns the bucket path for logs.
// Returns: ["LOGS"]
func GetLogsBucketPath() []string {
	return []string{BucketLogs}
}

// GetQueueStatsBucketPath returns the bucket path for queue statistics.
// Returns: ["Traversal-Data", "STATS", "queue-stats"]
func GetQueueStatsBucketPath() []string {
	return []string{TraversalDataBucket, StatsBucketName, "queue-stats"}
}

// GetExclusionHoldingBucketPath returns the bucket path for the exclusion-holding bucket.
// Returns: ["Traversal-Data", "SRC", "exclusion-holding"] or ["Traversal-Data", "DST", "exclusion-holding"]
func GetExclusionHoldingBucketPath(queueType string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketExclusionHolding}
}

// GetUnexclusionHoldingBucketPath returns the bucket path for the unexclusion-holding bucket.
// Returns: ["Traversal-Data", "SRC", "unexclusion-holding"] or ["Traversal-Data", "DST", "unexclusion-holding"]
func GetUnexclusionHoldingBucketPath(queueType string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketUnexclusionHolding}
}

// EnsureLevelBucket creates a level bucket and its status sub-buckets if they don't exist.
func EnsureLevelBucket(tx *bolt.Tx, queueType string, level int) error {
	// Navigate through Traversal-Data -> queueType -> levels
	traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
	if traversalBucket == nil {
		return fmt.Errorf("Traversal-Data bucket not found")
	}

	topBucket := traversalBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return fmt.Errorf("queue bucket %s not found in Traversal-Data", queueType)
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

	// Create status-lookup bucket (regular bucket, not nested bucket)
	// This stores ULID -> status string mappings
	lookupPath := GetStatusLookupBucketPath(queueType, level)
	if _, err := getOrCreateBucket(tx, lookupPath); err != nil {
		return fmt.Errorf("failed to create status-lookup bucket: %w", err)
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

// GetStatusLookupBucket returns the status-lookup bucket for a specific level.
// This bucket stores ULID -> status string mappings.
func GetStatusLookupBucket(tx *bolt.Tx, queueType string, level int) *bolt.Bucket {
	return getBucket(tx, GetStatusLookupBucketPath(queueType, level))
}

// GetOrCreateStatusLookupBucket returns or creates the status-lookup bucket for a specific level.
// This bucket stores ULID -> status string mappings.
func GetOrCreateStatusLookupBucket(tx *bolt.Tx, queueType string, level int) (*bolt.Bucket, error) {
	// Ensure the level bucket exists first
	if err := EnsureLevelBucket(tx, queueType, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetStatusLookupBucketPath(queueType, level))
}

// UpdateStatusLookup updates the status-lookup index for a node ULID at a given level.
// This should be called whenever a node's status changes.
// nodeID is the ULID of the node (as []byte).
func UpdateStatusLookup(tx *bolt.Tx, queueType string, level int, nodeID []byte, status string) error {
	lookupBucket, err := GetOrCreateStatusLookupBucket(tx, queueType, level)
	if err != nil {
		return fmt.Errorf("failed to get status-lookup bucket: %w", err)
	}
	return lookupBucket.Put(nodeID, []byte(status))
}

// GetExclusionHoldingBucket returns the exclusion-holding bucket for a queue type.
// This bucket stores path hash keys with their depth level as values.
// Key: path hash (string), Value: depth level (int, stored as bytes)
func GetExclusionHoldingBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	return getBucket(tx, GetExclusionHoldingBucketPath(queueType))
}

// GetOrCreateExclusionHoldingBucket returns or creates the exclusion-holding bucket for a queue type.
// This bucket stores path hash keys with their depth level as values.
// Key: path hash (string), Value: depth level (int, stored as bytes)
func GetOrCreateExclusionHoldingBucket(tx *bolt.Tx, queueType string) (*bolt.Bucket, error) {
	return getOrCreateBucket(tx, GetExclusionHoldingBucketPath(queueType))
}

// GetUnexclusionHoldingBucket returns the unexclusion-holding bucket for a queue type.
// This bucket stores path hash keys with their depth level as values.
// Key: path hash (string), Value: depth level (int, stored as bytes)
func GetUnexclusionHoldingBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	return getBucket(tx, GetUnexclusionHoldingBucketPath(queueType))
}

// GetOrCreateUnexclusionHoldingBucket returns or creates the unexclusion-holding bucket for a queue type.
// This bucket stores path hash keys with their depth level as values.
// Key: path hash (string), Value: depth level (int, stored as bytes)
func GetOrCreateUnexclusionHoldingBucket(tx *bolt.Tx, queueType string) (*bolt.Bucket, error) {
	return getOrCreateBucket(tx, GetUnexclusionHoldingBucketPath(queueType))
}

// GetHoldingBucket returns the appropriate holding bucket based on mode.
// mode should be "exclude" or "unexclude"
func GetHoldingBucket(tx *bolt.Tx, queueType string, mode string) *bolt.Bucket {
	switch mode {
	case "exclude":
		return GetExclusionHoldingBucket(tx, queueType)
	case "unexclude":
		return GetUnexclusionHoldingBucket(tx, queueType)
	default:
		return nil
	}
}

// GetOrCreateHoldingBucket returns or creates the appropriate holding bucket based on mode.
// mode should be "exclude" or "unexclude"
func GetOrCreateHoldingBucket(tx *bolt.Tx, queueType string, mode string) (*bolt.Bucket, error) {
	switch mode {
	case "exclude":
		return GetOrCreateExclusionHoldingBucket(tx, queueType)
	case "unexclude":
		return GetOrCreateUnexclusionHoldingBucket(tx, queueType)
	default:
		return nil, fmt.Errorf("invalid exclusion mode: %s", mode)
	}
}

// GetJoinLookupBucketPath returns the bucket path for the join-lookup bucket.
// Returns: ["Traversal-Data", "DST", "join-lookup"]
// This bucket maps DST node ULIDs to corresponding SRC node ULIDs (1:1 mapping).
func GetJoinLookupBucketPath() []string {
	return []string{TraversalDataBucket, BucketDst, SubBucketJoinLookup}
}

// GetJoinLookupBucket returns the join-lookup bucket for DST→SRC node mapping.
// Returns nil if the bucket doesn't exist.
func GetJoinLookupBucket(tx *bolt.Tx) *bolt.Bucket {
	return getBucket(tx, GetJoinLookupBucketPath())
}

// GetOrCreateJoinLookupBucket returns or creates the join-lookup bucket for DST→SRC node mapping.
func GetOrCreateJoinLookupBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	return getOrCreateBucket(tx, GetJoinLookupBucketPath())
}
