// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

// GetMaxKnownDepth scans the levels bucket and returns the highest level number found.
// Returns -1 if no levels exist or on error.
func (db *DB) GetMaxKnownDepth(queueType string) int {
	maxDepth := -1

	_ = db.View(func(tx *bolt.Tx) error {
		traversalBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalBucket == nil {
			return nil
		}

		topBucket := traversalBucket.Bucket([]byte(queueType))
		if topBucket == nil {
			return nil
		}

		levelsBucket := topBucket.Bucket([]byte(SubBucketLevels))
		if levelsBucket == nil {
			return nil
		}

		// Iterate through all level buckets
		cursor := levelsBucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			// Parse level number from bucket name (format: "00000000", "00000001", etc.)
			levelStr := string(k)
			levelNum, err := strconv.Atoi(levelStr)
			if err != nil {
				// Skip non-numeric bucket names
				continue
			}
			if levelNum > maxDepth {
				maxDepth = levelNum
			}
		}

		return nil
	})

	return maxDepth
}

// Status constants for traversal phase
const (
	StatusPending    = "pending"
	StatusSuccessful = "successful"
	StatusFailed     = "failed"
	StatusNotOnSrc   = "not_on_src" // Only for dst nodes during traversal
	StatusExcluded   = "excluded"   // Node is excluded from migration
)

// Copy status constants (SRC only)
const (
	CopyStatusPending    = "pending"
	CopyStatusInProgress = "in-progress" // Task is currently being processed
	CopyStatusSuccessful = "successful"
	CopyStatusSkipped    = "skipped" // Item already exists on dst, no copy needed
	CopyStatusFailed     = "failed"
)

// Node type constants for copy status bucket organization
const (
	NodeTypeFolder = "folder"
	NodeTypeFile   = "file"
)

// Legacy constants for compatibility during migration
const (
	TraversalStatusPending    = StatusPending
	TraversalStatusSuccessful = StatusSuccessful
	TraversalStatusFailed     = StatusFailed
	TraversalStatusNotOnSrc   = StatusNotOnSrc
)

// Bucket path constants
const (
	TraversalDataBucket = "Traversal-Data" // Root bucket for all traversal-related data
	BucketSrc           = "SRC"
	BucketDst           = "DST"
	BucketLogs          = "LOGS"   // Separate island, not under Traversal-Data
	BucketErrors        = "errors" // Task errors (traversal/copy failures); sub-buckets by phase
)

// Error phase sub-bucket names (under errors)
const (
	PhaseSrcTraversal = "src_traversal"
	PhaseSrcCopy      = "src_copy"
	PhaseDstTraversal = "dst_traversal"
	PhaseDstCopy      = "dst_copy"
)

// Sub-bucket names
const (
	SubBucketNodes              = "nodes"
	SubBucketChildren           = "children"
	SubBucketLevels             = "levels"
	SubBucketTraversal          = "traversal" // Traversal status sub-bucket under levels
	SubBucketCopy               = "copy"      // Copy status sub-bucket under levels (SRC only)
	SubBucketStatusLookup       = "status-lookup"
	SubBucketExclusionHolding   = "exclusion-holding"
	SubBucketUnexclusionHolding = "unexclusion-holding"
	SubBucketJoinLookup         = "join-lookup"
	SubBucketSrcToDst           = "src-to-dst"
	SubBucketDstToSrc           = "dst-to-src"
)

// FormatLevel formats a level number as an 8-digit zero-padded string.
func FormatLevel(level int) string {
	return fmt.Sprintf("%08d", level)
}

// ParseLevel parses a level string back to an integer.
func ParseLevel(levelStr string) (int, error) {
	return strconv.Atoi(levelStr)
}

// GetLevelShardPath returns the bucket path for a level shard (under which nodes, children, traversal, copy, join live).
// Returns: ["Traversal-Data", queueType, "levels", "00000000"]
func GetLevelShardPath(queueType string, level int) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level)}
}

// GetNodesBucketPath returns the bucket path for the nodes bucket at a given level (level-sharded).
// Returns: ["Traversal-Data", "SRC", "levels", "00000000", "nodes"]
func GetNodesBucketPath(queueType string, level int) []string {
	return append(GetLevelShardPath(queueType, level), SubBucketNodes)
}

// GetChildrenBucketPath returns the bucket path for the children bucket at a given level (level-sharded).
// Returns: ["Traversal-Data", "SRC", "levels", "00000000", "children"]
func GetChildrenBucketPath(queueType string, level int) []string {
	return append(GetLevelShardPath(queueType, level), SubBucketChildren)
}

// GetLevelBucketPath returns the bucket path for a specific level.
// Returns: ["Traversal-Data", "SRC", "levels", "00000001"] or ["Traversal-Data", "DST", "levels", "00000001"]
func GetLevelBucketPath(queueType string, level int) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level)}
}

// GetTraversalStatusBucketPath returns the bucket path for a specific traversal status at a level.
// Returns: ["Traversal-Data", "SRC", "levels", "00000001", "traversal", "pending"]
func GetTraversalStatusBucketPath(queueType string, level int, status string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level), SubBucketTraversal, status}
}

// GetCopyStatusBucketPath returns the bucket path for a specific copy status at a level (SRC only).
// Returns: ["Traversal-Data", "SRC", "levels", "00000001", "copy", "folder", "pending"]
func GetCopyStatusBucketPath(level int, nodeType string, status string) []string {
	return []string{TraversalDataBucket, BucketSrc, SubBucketLevels, FormatLevel(level), SubBucketCopy, nodeType, status}
}

// GetTraversalStatusLookupBucketPath returns the bucket path for the traversal status-lookup index at a level.
// Returns: ["Traversal-Data", "SRC", "levels", "00000001", "traversal", "status-lookup"]
func GetTraversalStatusLookupBucketPath(queueType string, level int) []string {
	return []string{TraversalDataBucket, queueType, SubBucketLevels, FormatLevel(level), SubBucketTraversal, SubBucketStatusLookup}
}

// GetCopyStatusLookupBucketPath returns the bucket path for the copy status-lookup index at a level (SRC only).
// Returns: ["Traversal-Data", "SRC", "levels", "00000001", "copy", "status-lookup"]
func GetCopyStatusLookupBucketPath(level int) []string {
	return []string{TraversalDataBucket, BucketSrc, SubBucketLevels, FormatLevel(level), SubBucketCopy, SubBucketStatusLookup}
}

// GetStatusBucketPath is deprecated - use GetTraversalStatusBucketPath instead.
// Kept for backward compatibility during migration.
func GetStatusBucketPath(queueType string, level int, status string) []string {
	return GetTraversalStatusBucketPath(queueType, level, status)
}

// GetStatusLookupBucketPath is deprecated - use GetTraversalStatusLookupBucketPath instead.
// Kept for backward compatibility during migration.
func GetStatusLookupBucketPath(queueType string, level int) []string {
	return GetTraversalStatusLookupBucketPath(queueType, level)
}

// GetLogsBucketPath returns the bucket path for logs.
// Returns: ["LOGS"]
func GetLogsBucketPath() []string {
	return []string{BucketLogs}
}

// GetErrorsBucketPath returns the bucket path for the errors phase sub-bucket.
// Phase must be one of PhaseSrcTraversal, PhaseSrcCopy, PhaseDstTraversal, PhaseDstCopy.
func GetErrorsBucketPath(phase string) []string {
	return []string{BucketErrors, phase}
}

// GetErrorsPhaseBucket returns the errors phase sub-bucket (read-only).
func GetErrorsPhaseBucket(tx *bolt.Tx, phase string) *bolt.Bucket {
	return getBucket(tx, GetErrorsBucketPath(phase))
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

// EnsureLevelBucket creates a level bucket and its traversal/copy status sub-buckets if they don't exist.
func EnsureLevelBucket(tx *bolt.Tx, queueType string, level int) error {
	// Navigate through Traversal-Data -> queueType -> levels
	rootBucket := tx.Bucket([]byte(TraversalDataBucket))
	if rootBucket == nil {
		return fmt.Errorf("Traversal-Data bucket not found")
	}

	topBucket := rootBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return fmt.Errorf("queue bucket %s not found in Traversal-Data", queueType)
	}

	levelsBucket := topBucket.Bucket([]byte(SubBucketLevels))
	if levelsBucket == nil {
		return fmt.Errorf("levels bucket not found in %s", queueType)
	}

	// Create the level bucket (level shard: nodes, children, traversal, copy, join live under here)
	levelStr := FormatLevel(level)
	levelBucket, err := levelsBucket.CreateBucketIfNotExists([]byte(levelStr))
	if err != nil {
		return fmt.Errorf("failed to create level bucket %s: %w", levelStr, err)
	}

	// Create nodes bucket for this level (key-value: node ID -> NodeState)
	if _, err := levelBucket.CreateBucketIfNotExists([]byte(SubBucketNodes)); err != nil {
		return fmt.Errorf("failed to create nodes bucket for level %s: %w", levelStr, err)
	}

	// Create children bucket for this level (key-value: parent ID -> child IDs)
	if _, err := levelBucket.CreateBucketIfNotExists([]byte(SubBucketChildren)); err != nil {
		return fmt.Errorf("failed to create children bucket for level %s: %w", levelStr, err)
	}

	// Create join-lookup bucket for this level (SRC: src-to-dst, DST: dst-to-src)
	if queueType == BucketSrc {
		if _, err := levelBucket.CreateBucketIfNotExists([]byte(SubBucketSrcToDst)); err != nil {
			return fmt.Errorf("failed to create src-to-dst bucket for level %s: %w", levelStr, err)
		}
	} else {
		if _, err := levelBucket.CreateBucketIfNotExists([]byte(SubBucketDstToSrc)); err != nil {
			return fmt.Errorf("failed to create dst-to-src bucket for level %s: %w", levelStr, err)
		}
	}

	// Create traversal sub-bucket
	traversalSubBucket, err := levelBucket.CreateBucketIfNotExists([]byte(SubBucketTraversal))
	if err != nil {
		return fmt.Errorf("failed to create traversal bucket: %w", err)
	}

	// Create traversal status sub-buckets
	traversalStatuses := []string{StatusPending, StatusSuccessful, StatusFailed, StatusExcluded}
	if queueType == BucketDst {
		traversalStatuses = append(traversalStatuses, StatusNotOnSrc)
	}

	for _, status := range traversalStatuses {
		if _, err := traversalSubBucket.CreateBucketIfNotExists([]byte(status)); err != nil {
			return fmt.Errorf("failed to create traversal status bucket %s: %w", status, err)
		}
	}

	// Create traversal status-lookup bucket (regular bucket, not nested bucket)
	// This stores ULID -> traversal status string mappings
	traversalLookupPath := GetTraversalStatusLookupBucketPath(queueType, level)
	if _, err := getOrCreateBucket(tx, traversalLookupPath); err != nil {
		return fmt.Errorf("failed to create traversal status-lookup bucket: %w", err)
	}

	// Create copy sub-bucket and status buckets (SRC only)
	if queueType == BucketSrc {
		copyBucket, err := levelBucket.CreateBucketIfNotExists([]byte(SubBucketCopy))
		if err != nil {
			return fmt.Errorf("failed to create copy bucket: %w", err)
		}

		// Create folder and file type sub-buckets under copy
		nodeTypes := []string{NodeTypeFolder, NodeTypeFile}
		copyStatuses := []string{CopyStatusPending, CopyStatusInProgress, CopyStatusSuccessful, CopyStatusSkipped, CopyStatusFailed}
		
		for _, nodeType := range nodeTypes {
			nodeTypeBucket, err := copyBucket.CreateBucketIfNotExists([]byte(nodeType))
			if err != nil {
				return fmt.Errorf("failed to create copy node type bucket %s: %w", nodeType, err)
			}

			// Create status buckets under each node type
			for _, status := range copyStatuses {
				if _, err := nodeTypeBucket.CreateBucketIfNotExists([]byte(status)); err != nil {
					return fmt.Errorf("failed to create copy status bucket %s/%s: %w", nodeType, status, err)
				}
			}
		}

		// Create copy status-lookup bucket (regular bucket, not nested bucket)
		// This stores ULID -> copy status string mappings (no node type needed in lookup)
		copyLookupPath := GetCopyStatusLookupBucketPath(level)
		if _, err := getOrCreateBucket(tx, copyLookupPath); err != nil {
			return fmt.Errorf("failed to create copy status-lookup bucket: %w", err)
		}
	}

	return nil
}

// GetNodesBucket returns the nodes bucket for a queue type at the given level (level-sharded).
func GetNodesBucket(tx *bolt.Tx, queueType string, level int) *bolt.Bucket {
	return getBucket(tx, GetNodesBucketPath(queueType, level))
}

// GetChildrenBucket returns the children bucket for a queue type at the given level (level-sharded).
func GetChildrenBucket(tx *bolt.Tx, queueType string, level int) *bolt.Bucket {
	return getBucket(tx, GetChildrenBucketPath(queueType, level))
}

// GetTraversalStatusBucket returns the traversal status bucket for a specific level and status.
func GetTraversalStatusBucket(tx *bolt.Tx, queueType string, level int, status string) *bolt.Bucket {
	return getBucket(tx, GetTraversalStatusBucketPath(queueType, level, status))
}

// GetOrCreateTraversalStatusBucket returns or creates the traversal status bucket for a specific level and status.
func GetOrCreateTraversalStatusBucket(tx *bolt.Tx, queueType string, level int, status string) (*bolt.Bucket, error) {
	// Ensure the level bucket exists first
	if err := EnsureLevelBucket(tx, queueType, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetTraversalStatusBucketPath(queueType, level, status))
}

// GetCopyStatusBucket returns the copy status bucket for a specific level, node type, and status (SRC only).
func GetCopyStatusBucket(tx *bolt.Tx, level int, nodeType string, status string) *bolt.Bucket {
	return getBucket(tx, GetCopyStatusBucketPath(level, nodeType, status))
}

// GetOrCreateCopyStatusBucket returns or creates the copy status bucket for a specific level, node type, and status (SRC only).
func GetOrCreateCopyStatusBucket(tx *bolt.Tx, level int, nodeType string, status string) (*bolt.Bucket, error) {
	// Ensure the level bucket exists first
	if err := EnsureLevelBucket(tx, BucketSrc, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetCopyStatusBucketPath(level, nodeType, status))
}

// GetStatusBucket is deprecated - use GetTraversalStatusBucket instead.
// Kept for backward compatibility during migration.
func GetStatusBucket(tx *bolt.Tx, queueType string, level int, status string) *bolt.Bucket {
	return GetTraversalStatusBucket(tx, queueType, level, status)
}

// GetOrCreateStatusBucket is deprecated - use GetOrCreateTraversalStatusBucket instead.
// Kept for backward compatibility during migration.
func GetOrCreateStatusBucket(tx *bolt.Tx, queueType string, level int, status string) (*bolt.Bucket, error) {
	return GetOrCreateTraversalStatusBucket(tx, queueType, level, status)
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

// GetTraversalStatusLookupBucket returns the traversal status-lookup bucket for a specific level.
// This bucket stores ULID -> traversal status string mappings.
func GetTraversalStatusLookupBucket(tx *bolt.Tx, queueType string, level int) *bolt.Bucket {
	return getBucket(tx, GetTraversalStatusLookupBucketPath(queueType, level))
}

// GetOrCreateTraversalStatusLookupBucket returns or creates the traversal status-lookup bucket for a specific level.
// This bucket stores ULID -> traversal status string mappings.
func GetOrCreateTraversalStatusLookupBucket(tx *bolt.Tx, queueType string, level int) (*bolt.Bucket, error) {
	// Ensure the level bucket exists first
	if err := EnsureLevelBucket(tx, queueType, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetTraversalStatusLookupBucketPath(queueType, level))
}

// GetCopyStatusLookupBucket returns the copy status-lookup bucket for a specific level (SRC only).
// This bucket stores ULID -> copy status string mappings.
func GetCopyStatusLookupBucket(tx *bolt.Tx, level int) *bolt.Bucket {
	return getBucket(tx, GetCopyStatusLookupBucketPath(level))
}

// GetOrCreateCopyStatusLookupBucket returns or creates the copy status-lookup bucket for a specific level (SRC only).
// This bucket stores ULID -> copy status string mappings.
func GetOrCreateCopyStatusLookupBucket(tx *bolt.Tx, level int) (*bolt.Bucket, error) {
	// Ensure the level bucket exists first
	if err := EnsureLevelBucket(tx, BucketSrc, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetCopyStatusLookupBucketPath(level))
}

// GetStatusLookupBucket is deprecated - use GetTraversalStatusLookupBucket instead.
// Kept for backward compatibility during migration.
func GetStatusLookupBucket(tx *bolt.Tx, queueType string, level int) *bolt.Bucket {
	return GetTraversalStatusLookupBucket(tx, queueType, level)
}

// GetOrCreateStatusLookupBucket is deprecated - use GetOrCreateTraversalStatusLookupBucket instead.
// Kept for backward compatibility during migration.
func GetOrCreateStatusLookupBucket(tx *bolt.Tx, queueType string, level int) (*bolt.Bucket, error) {
	return GetOrCreateTraversalStatusLookupBucket(tx, queueType, level)
}

// UpdateTraversalStatusLookup updates the traversal status-lookup index for a node ULID at a given level.
// This should be called whenever a node's traversal status changes.
// nodeID is the ULID of the node (as []byte).
func UpdateTraversalStatusLookup(tx *bolt.Tx, queueType string, level int, nodeID []byte, status string) error {
	lookupBucket, err := GetOrCreateTraversalStatusLookupBucket(tx, queueType, level)
	if err != nil {
		return fmt.Errorf("failed to get traversal status-lookup bucket: %w", err)
	}
	return lookupBucket.Put(nodeID, []byte(status))
}

// UpdateCopyStatusLookup updates the copy status-lookup index for a node ULID at a given level (SRC only).
// This should be called whenever a node's copy status changes.
// nodeID is the ULID of the node (as []byte).
func UpdateCopyStatusLookup(tx *bolt.Tx, level int, nodeID []byte, status string) error {
	lookupBucket, err := GetOrCreateCopyStatusLookupBucket(tx, level)
	if err != nil {
		return fmt.Errorf("failed to get copy status-lookup bucket: %w", err)
	}
	return lookupBucket.Put(nodeID, []byte(status))
}

// UpdateStatusLookup is deprecated - use UpdateTraversalStatusLookup instead.
// Kept for backward compatibility during migration.
func UpdateStatusLookup(tx *bolt.Tx, queueType string, level int, nodeID []byte, status string) error {
	return UpdateTraversalStatusLookup(tx, queueType, level, nodeID, status)
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

// GetJoinLookupBucketPath returns the bucket path for the DST join-lookup (dst-to-src) at the given level.
// Same as GetDstToSrcBucketPath(level). Kept for compatibility.
func GetJoinLookupBucketPath(level int) []string {
	return GetDstToSrcBucketPath(level)
}

// GetJoinLookupBucket returns the join-lookup bucket for DST→SRC node mapping at the given level.
func GetJoinLookupBucket(tx *bolt.Tx, level int) *bolt.Bucket {
	return GetDstToSrcBucket(tx, level)
}

// GetOrCreateJoinLookupBucket returns or creates the join-lookup bucket for DST→SRC node mapping at the given level.
func GetOrCreateJoinLookupBucket(tx *bolt.Tx, level int) (*bolt.Bucket, error) {
	return GetOrCreateDstToSrcBucket(tx, level)
}

// GetSrcToDstBucketPath returns the bucket path for the src-to-dst lookup bucket at the given level (level-sharded).
// Returns: ["Traversal-Data", "SRC", "levels", "00000000", "src-to-dst"]
func GetSrcToDstBucketPath(level int) []string {
	return append(GetLevelShardPath(BucketSrc, level), SubBucketSrcToDst)
}

// GetSrcToDstBucket returns the src-to-dst lookup bucket for SRC→DST node mapping at the given level.
func GetSrcToDstBucket(tx *bolt.Tx, level int) *bolt.Bucket {
	return getBucket(tx, GetSrcToDstBucketPath(level))
}

// GetOrCreateSrcToDstBucket returns or creates the src-to-dst lookup bucket for SRC→DST node mapping at the given level.
func GetOrCreateSrcToDstBucket(tx *bolt.Tx, level int) (*bolt.Bucket, error) {
	if err := EnsureLevelBucket(tx, BucketSrc, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetSrcToDstBucketPath(level))
}

// GetDstToSrcBucketPath returns the bucket path for the dst-to-src lookup bucket at the given level (level-sharded).
// Returns: ["Traversal-Data", "DST", "levels", "00000000", "dst-to-src"]
func GetDstToSrcBucketPath(level int) []string {
	return append(GetLevelShardPath(BucketDst, level), SubBucketDstToSrc)
}

// GetDstToSrcBucket returns the dst-to-src lookup bucket for DST→SRC node mapping at the given level.
func GetDstToSrcBucket(tx *bolt.Tx, level int) *bolt.Bucket {
	return getBucket(tx, GetDstToSrcBucketPath(level))
}

// GetOrCreateDstToSrcBucket returns or creates the dst-to-src lookup bucket for DST→SRC node mapping at the given level.
func GetOrCreateDstToSrcBucket(tx *bolt.Tx, level int) (*bolt.Bucket, error) {
	if err := EnsureLevelBucket(tx, BucketDst, level); err != nil {
		return nil, err
	}
	return getOrCreateBucket(tx, GetDstToSrcBucketPath(level))
}
