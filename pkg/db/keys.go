// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// HashPath creates a fixed-length hash of a normalized path for use in keys.
// Uses SHA256 and returns first 32 hex characters (16 bytes).
// This ensures paths can be used as keys even if they contain special characters.
func HashPath(path string) string {
	h := sha256.Sum256([]byte(path))
	return hex.EncodeToString(h[:16]) // 32 hex characters
}

// Traversal status constants
const (
	TraversalStatusPending    = "pending"
	TraversalStatusSuccessful = "successful"
	TraversalStatusFailed     = "failed"
	TraversalStatusNotOnSrc   = "not_on_src" // Only for dst nodes
)

// Copy status constants
const (
	CopyStatusPending    = "pending"
	CopyStatusSuccessful = "successful"
	CopyStatusFailed     = "failed"
)

// KeyNode generates a key for a node in BadgerDB.
// Format: {src|dst}:{level:08d}:{traversal_status}:{copy_status}:{path_hash}
// For dst nodes, copy_status is typically empty or "successful" (not used for copy operations).
// For src nodes, copy_status indicates whether copy is needed.
func KeyNode(queueType string, level int, traversalStatus, copyStatus, path string) []byte {
	pathHash := HashPath(path)
	return []byte(fmt.Sprintf("%s:%08d:%s:%s:%s", queueType, level, traversalStatus, copyStatus, pathHash))
}

// KeySrc generates a key for a source node.
// Format: src:{level:08d}:{traversal_status}:{copy_status}:{path_hash}
func KeySrc(level int, traversalStatus, copyStatus, path string) []byte {
	return KeyNode("src", level, traversalStatus, copyStatus, path)
}

// KeyDst generates a key for a destination node.
// Format: dst:{level:08d}:{traversal_status}:{copy_status}:{path_hash}
// Note: For dst nodes, copy_status is typically empty or "successful" (not actively used).
func KeyDst(level int, traversalStatus, copyStatus, path string) []byte {
	return KeyNode("dst", level, traversalStatus, copyStatus, path)
}

// PrefixForStatus generates a prefix for iterating all keys with a specific status at a level.
// Format: {queueType}:{level:08d}:{traversal_status}:
func PrefixForStatus(queueType string, level int, traversalStatus string) []byte {
	return []byte(fmt.Sprintf("%s:%08d:%s:", queueType, level, traversalStatus))
}

// PrefixForStatusAndCopy generates a prefix for iterating all keys with specific traversal and copy status.
// Format: {queueType}:{level:08d}:{traversal_status}:{copy_status}:
func PrefixForStatusAndCopy(queueType string, level int, traversalStatus, copyStatus string) []byte {
	return []byte(fmt.Sprintf("%s:%08d:%s:%s:", queueType, level, traversalStatus, copyStatus))
}

// PrefixForLevel generates a prefix for iterating all keys at a specific level.
// Format: {queueType}:{level:08d}:
func PrefixForLevel(queueType string, level int) []byte {
	return []byte(fmt.Sprintf("%s:%08d:", queueType, level))
}

// PrefixForAllLevels generates a prefix for iterating all keys regardless of level.
// Format: {queueType}:
func PrefixForAllLevels(queueType string) []byte {
	return []byte(queueType + ":")
}

