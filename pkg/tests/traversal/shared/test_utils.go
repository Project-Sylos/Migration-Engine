// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

// findNodeByPath is a test utility helper that finds a node by its path.
// This iterates through all nodes to find a match - only for test utilities.
func findNodeByPath(boltDB *db.DB, queueType string, path string) (*db.NodeState, error) {
	var found *db.NodeState
	err := boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		cursor := nodesBucket.Cursor()
		for nodeIDBytes, nodeData := cursor.First(); nodeIDBytes != nil; nodeIDBytes, nodeData = cursor.Next() {
			nodeState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue
			}
			if nodeState.Path == path {
				found = nodeState
				return nil
			}
		}
		return fmt.Errorf("node not found: %s", path)
	})
	return found, err
}

// SubtreeStats contains statistics about a subtree.
type SubtreeStats struct {
	TotalNodes   int
	TotalFolders int
	TotalFiles   int
	MaxDepth     int
}

// CountSubtree performs a DFS traversal to count all nodes in a subtree.
// Returns the total count of nodes (including the root node).
func CountSubtree(boltDB *db.DB, queueType string, rootPath string) (SubtreeStats, error) {
	stats := SubtreeStats{}
	visited := make(map[string]bool) // Track by ULID

	var dfs func(nodeID string, depth int) error
	dfs = func(nodeID string, depth int) error {
		if visited[nodeID] {
			return nil // Already counted
		}
		visited[nodeID] = true

		// Get node state by ULID
		nodeState, err := db.GetNodeState(boltDB, queueType, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node state for %s: %w", nodeID, err)
		}
		if nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		// Count this node
		stats.TotalNodes++
		if nodeState.Type == "folder" {
			stats.TotalFolders++
		} else {
			stats.TotalFiles++
		}
		if depth > stats.MaxDepth {
			stats.MaxDepth = depth
		}

		// Recurse into children (if folder)
		if nodeState.Type == "folder" {
			childIDs, err := db.GetChildrenIDsByParentID(boltDB, queueType, nodeState.ID)
			if err != nil {
				return fmt.Errorf("failed to get children for %s: %w", nodeID, err)
			}

			for _, childID := range childIDs {
				if err := dfs(childID, depth+1); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Find root node by path
	rootNode, err := findNodeByPath(boltDB, queueType, rootPath)
	if err != nil {
		return stats, fmt.Errorf("failed to find root node: %w", err)
	}

	if err := dfs(rootNode.ID, 0); err != nil {
		return stats, err
	}

	return stats, nil
}

// DeleteSubtree performs a DFS traversal to delete all nodes in a subtree from BoltDB.
// This removes nodes from the nodes bucket, status buckets, children bucket, and status-lookup index.
// Does NOT modify the Spectra database.
func DeleteSubtree(boltDB *db.DB, queueType string, rootPath string) error {
	visited := make(map[string]bool)
	var nodesToDelete []*db.NodeState

	// First pass: collect all nodes in subtree
	var dfsCollect func(nodeID string) error
	dfsCollect = func(nodeID string) error {
		if visited[nodeID] {
			return nil // Already collected
		}
		visited[nodeID] = true

		// Get node state by ULID
		nodeState, err := db.GetNodeState(boltDB, queueType, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node state for %s: %w", nodeID, err)
		}
		if nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		// Collect this node
		nodesToDelete = append(nodesToDelete, nodeState)

		// Recurse into children (if folder)
		if nodeState.Type == "folder" {
			childIDs, err := db.GetChildrenIDsByParentID(boltDB, queueType, nodeState.ID)
			if err != nil {
				return fmt.Errorf("failed to get children for %s: %w", nodeID, err)
			}

			for _, childID := range childIDs {
				if err := dfsCollect(childID); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Find root node by path
	rootNode, err := findNodeByPath(boltDB, queueType, rootPath)
	if err != nil {
		return fmt.Errorf("failed to find root node: %w", err)
	}

	// Collect all children of the root node (but NOT the root node itself)
	// The root node should remain so it can be retried by the retry sweep
	if rootNode.Type == "folder" {
		childIDs, err := db.GetChildrenIDsByParentID(boltDB, queueType, rootNode.ID)
		if err != nil {
			return fmt.Errorf("failed to get children for root node: %w", err)
		}

		for _, childID := range childIDs {
			if err := dfsCollect(childID); err != nil {
				return err
			}
		}
	}

	// Second pass: delete all collected nodes
	// Get all levels first (outside transaction)
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return fmt.Errorf("failed to get levels: %w", err)
	}

	return boltDB.Update(func(tx *bolt.Tx) error {
		// Track stats deltas for batch update
		statsDeltas := make(map[string]int64) // bucket path string -> delta

		for _, nodeState := range nodesToDelete {
			nodeIDBytes := []byte(nodeState.ID)
			var parentIDBytes []byte
			if nodeState.ParentID != "" {
				parentIDBytes = []byte(nodeState.ParentID)
			}

			// 1. Delete from nodes bucket
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket != nil && nodesBucket.Get(nodeIDBytes) != nil {
				nodesBucket.Delete(nodeIDBytes)
				// Decrement nodes bucket count
				nodesPath := db.GetNodesBucketPath(queueType)
				statsDeltas[strings.Join(nodesPath, "/")]--
			}

			// 2. Delete from status bucket (check all status buckets at all levels)
			// Check all statuses: pending, successful, failed, not_on_src, excluded
			statuses := []string{db.StatusPending, db.StatusSuccessful, db.StatusFailed, db.StatusNotOnSrc, db.StatusExcluded}
			for _, level := range levels {
				for _, status := range statuses {
					statusBucket := db.GetStatusBucket(tx, queueType, level, status)
					if statusBucket != nil && statusBucket.Get(nodeIDBytes) != nil {
						statusBucket.Delete(nodeIDBytes)
						// Decrement status bucket count
						statusPath := db.GetStatusBucketPath(queueType, level, status)
						statsDeltas[strings.Join(statusPath, "/")]--
					}
				}
			}

			// 3. Delete from status-lookup index
			for _, level := range levels {
				lookupBucket := db.GetStatusLookupBucket(tx, queueType, level)
				if lookupBucket != nil {
					lookupBucket.Delete(nodeIDBytes)
				}
			}

			// 4. Remove from parent's children list
			if nodeState.ParentID != "" {
				childrenBucket := db.GetChildrenBucket(tx, queueType)
				if childrenBucket != nil {
					childrenData := childrenBucket.Get(parentIDBytes)
					if childrenData != nil {
						var children []string
						if err := json.Unmarshal(childrenData, &children); err == nil {
							filtered := make([]string, 0, len(children))
							for _, c := range children {
								if c != nodeState.ID {
									filtered = append(filtered, c)
								}
							}

							if len(filtered) > 0 {
								updatedData, err := json.Marshal(filtered)
								if err == nil {
									childrenBucket.Put(parentIDBytes, updatedData)
								}
							} else {
								childrenBucket.Delete(parentIDBytes)
							}
						}
					}
				}
			}

			// 5. Delete node's own children list (if folder)
			if nodeState.Type == "folder" {
				childrenBucket := db.GetChildrenBucket(tx, queueType)
				if childrenBucket != nil {
					// Check if entry exists before deleting (for stats)
					if childrenBucket.Get(nodeIDBytes) != nil {
						childrenBucket.Delete(nodeIDBytes)
						// Decrement children bucket count
						childrenPath := db.GetChildrenBucketPath(queueType)
						statsDeltas[strings.Join(childrenPath, "/")]--
					}
				}
			}

			// 6. Delete from join-lookup tables
			switch queueType {
			case "SRC":
				// Delete SrcToDst mapping
				srcToDstBucket := db.GetSrcToDstBucket(tx)
				if srcToDstBucket != nil {
					srcToDstBucket.Delete(nodeIDBytes)
				}
			case "DST":
				// Delete DstToSrc mapping
				dstToSrcBucket := db.GetDstToSrcBucket(tx)
				if dstToSrcBucket != nil {
					dstToSrcBucket.Delete(nodeIDBytes)
				}
			}
		}

		// Update stats bucket for all deletions
		for pathStr, delta := range statsDeltas {
			path := strings.Split(pathStr, "/")
			if err := updateBucketStatsInTx(tx, path, delta); err != nil {
				return fmt.Errorf("failed to update stats for %s: %w", pathStr, err)
			}
		}

		return nil
	})
}

// updateBucketStatsInTx is a helper to update stats within a transaction.
// This manually updates the stats bucket since we can't call db.UpdateBucketStats from within an existing transaction.
func updateBucketStatsInTx(tx *bolt.Tx, bucketPath []string, delta int64) error {
	// Navigate to stats bucket using the same constants as the db package
	traversalBucket := tx.Bucket([]byte(db.TraversalDataBucket))
	if traversalBucket == nil {
		return nil // Stats bucket doesn't exist yet
	}
	statsBucket := traversalBucket.Bucket([]byte(db.StatsBucketName))
	if statsBucket == nil {
		return nil // Stats bucket doesn't exist yet
	}

	key := strings.Join(bucketPath, "/")
	keyBytes := []byte(key)

	// Get current count
	var currentCount int64
	existingValue := statsBucket.Get(keyBytes)
	if existingValue != nil {
		currentCount = int64(binary.BigEndian.Uint64(existingValue))
	}

	// Compute new count
	newCount := currentCount + delta
	if newCount < 0 {
		newCount = 0
	}

	if newCount == 0 {
		// Remove stats entry if count is zero
		return statsBucket.Delete(keyBytes)
	} else {
		// Store new count as 8-byte big-endian int64
		valueBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBytes, uint64(newCount))
		return statsBucket.Put(keyBytes, valueBytes)
	}
}

// MarkNodeAsPending marks a node as pending in the database.
func MarkNodeAsPending(boltDB *db.DB, queueType string, nodePath string) error {
	// Find node by path
	nodeState, err := findNodeByPath(boltDB, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}

	// Update status to pending using ULID
	_, err = db.UpdateNodeStatusByID(boltDB, queueType, nodeState.Depth, nodeState.TraversalStatus, db.StatusPending, nodeState.ID)
	return err
}

// MarkNodeAsFailed marks a node as failed in the database.
func MarkNodeAsFailed(boltDB *db.DB, queueType string, nodePath string) error {
	// Find node by path
	nodeState, err := findNodeByPath(boltDB, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}

	// Update status to failed using ULID
	_, err = db.UpdateNodeStatusByID(boltDB, queueType, nodeState.Depth, nodeState.TraversalStatus, db.StatusFailed, nodeState.ID)
	return err
}

// MarkNodeAsExcluded marks a node as explicitly excluded in the database.
func MarkNodeAsExcluded(boltDB *db.DB, queueType string, nodePath string) error {
	// Find node by path
	nodeState, err := findNodeByPath(boltDB, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}

	nodeIDBytes := []byte(nodeState.ID)

	return boltDB.Update(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		nodeData := nodesBucket.Get(nodeIDBytes)
		if nodeData == nil {
			return fmt.Errorf("node not found: %s", nodePath)
		}

		nodeState, err := db.DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		// Set explicit excluded flag
		nodeState.ExplicitExcluded = true

		// Serialize and save back
		updatedData, err := nodeState.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		return nodesBucket.Put(nodeIDBytes, updatedData)
	})
}

// MarkNodeAsUnexcluded marks a node as explicitly unexcluded in the database.
func MarkNodeAsUnexcluded(boltDB *db.DB, queueType string, nodePath string) error {
	// Find node by path
	nodeState, err := findNodeByPath(boltDB, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}

	nodeIDBytes := []byte(nodeState.ID)

	return boltDB.Update(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		nodeData := nodesBucket.Get(nodeIDBytes)
		if nodeData == nil {
			return fmt.Errorf("node not found: %s", nodePath)
		}

		nodeState, err := db.DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		// Clear explicit excluded flag
		nodeState.ExplicitExcluded = false

		// Serialize and save back
		updatedData, err := nodeState.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize node state: %w", err)
		}

		return nodesBucket.Put(nodeIDBytes, updatedData)
	})
}

// PickRandomExcludedTopLevelChild picks a random top-level child that is currently excluded.
// Only selects folders (not files) that are excluded (explicitly or inherited).
func PickRandomExcludedTopLevelChild(boltDB *db.DB, queueType string, rootPath string) (*db.NodeState, error) {
	children, err := GetTopLevelChildren(boltDB, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no top-level children found")
	}

	// Filter to only excluded folders
	excludedFolders := make([]*db.NodeState, 0, len(children))
	for _, child := range children {
		if child.Type == "folder" && (child.ExplicitExcluded || child.InheritedExcluded) {
			excludedFolders = append(excludedFolders, child)
		}
	}

	if len(excludedFolders) == 0 {
		return nil, fmt.Errorf("no excluded top-level folder children found")
	}

	// Pick random excluded folder
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	selected := excludedFolders[r.Intn(len(excludedFolders))]

	return selected, nil
}

// PickFirstExcludedTopLevelChild picks the first top-level child that is currently excluded.
// Only selects folders (not files) that are excluded (explicitly or inherited).
// This is useful for deterministic tests.
func PickFirstExcludedTopLevelChild(boltDB *db.DB, queueType string, rootPath string) (*db.NodeState, error) {
	children, err := GetTopLevelChildren(boltDB, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no top-level children found")
	}

	// Find first excluded folder
	for _, child := range children {
		if child.Type == "folder" && (child.ExplicitExcluded || child.InheritedExcluded) {
			return child, nil
		}
	}

	return nil, fmt.Errorf("no excluded top-level folder children found")
}

// GetTopLevelChildren returns all direct children of the root (depth 1).
// Uses multiple strategies:
// 1. Find root node by path, then get its children by ParentID
// 2. Fall back to finding all nodes at depth 1
func GetTopLevelChildren(boltDB *db.DB, queueType string, rootPath string) ([]*db.NodeState, error) {
	// Strategy 1: Find root node and get its children
	rootNode, err := findNodeByPath(boltDB, queueType, rootPath)
	if err == nil && rootNode != nil {
		childIDs, err := db.GetChildrenIDsByParentID(boltDB, queueType, rootNode.ID)
		if err == nil && len(childIDs) > 0 {
			// Convert child IDs to NodeStates
			var childStates []*db.NodeState
			for _, childID := range childIDs {
				childState, err := db.GetNodeState(boltDB, queueType, childID)
				if err == nil && childState != nil {
					childStates = append(childStates, childState)
				}
			}
			if len(childStates) > 0 {
				return childStates, nil
			}
		}
	}

	// Strategy 2: Fall back to finding all nodes at depth 1
	// This is more reliable if children bucket structure is inconsistent
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return nil, fmt.Errorf("failed to get levels: %w", err)
	}

	hasLevel1 := false
	for _, level := range levels {
		if level == 1 {
			hasLevel1 = true
			break
		}
	}

	if !hasLevel1 {
		return []*db.NodeState{}, nil // No level 1 nodes
	}

	var depth1Nodes []*db.NodeState
	err = boltDB.View(func(tx *bolt.Tx) error {
		// Get all nodes at level 1 from all status buckets
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		// Iterate through all status buckets at level 1
		statuses := []string{db.StatusPending, db.StatusSuccessful, db.StatusFailed, db.StatusNotOnSrc}
		seenIDs := make(map[string]bool)

		for _, status := range statuses {
			statusBucket := db.GetStatusBucket(tx, queueType, 1, status)
			if statusBucket == nil {
				continue
			}

			cursor := statusBucket.Cursor()
			for nodeIDBytes, _ := cursor.First(); nodeIDBytes != nil; nodeIDBytes, _ = cursor.Next() {
				nodeIDStr := string(nodeIDBytes)
				if seenIDs[nodeIDStr] {
					continue // Already processed
				}
				seenIDs[nodeIDStr] = true

				// Get node data
				nodeData := nodesBucket.Get(nodeIDBytes)
				if nodeData == nil {
					continue
				}

				nodeState, err := db.DeserializeNodeState(nodeData)
				if err != nil {
					continue
				}

				// Verify it's actually depth 1 and parent is root (ParentID empty or matches root)
				if nodeState.Depth == 1 && (nodeState.ParentPath == "/" || nodeState.ParentPath == "") {
					depth1Nodes = append(depth1Nodes, nodeState)
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find depth 1 nodes: %w", err)
	}

	return depth1Nodes, nil
}

// PickRandomTopLevelChild picks a random top-level child from the root.
// Only selects folders (not files).
func PickRandomTopLevelChild(boltDB *db.DB, queueType string, rootPath string) (*db.NodeState, error) {
	children, err := GetTopLevelChildren(boltDB, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no top-level children found")
	}

	// Filter to only folders
	folders := make([]*db.NodeState, 0, len(children))
	for _, child := range children {
		if child.Type == "folder" {
			folders = append(folders, child)
		}
	}

	if len(folders) == 0 {
		return nil, fmt.Errorf("no top-level folder children found (only files)")
	}

	// Pick random folder
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	selected := folders[r.Intn(len(folders))]

	return selected, nil
}

// CountPendingNodes counts all pending nodes across all levels for a queue type.
func CountPendingNodes(boltDB *db.DB, queueType string) (int, error) {
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return 0, err
	}

	totalPending := 0
	for _, level := range levels {
		count, err := boltDB.CountStatusBucket(queueType, level, db.StatusPending)
		if err != nil {
			continue
		}
		totalPending += count
	}

	return totalPending, nil
}

// CountExcludedNodes counts all excluded nodes (both explicit_excluded = true OR inherited_excluded = true).
// A node is considered excluded if it is either explicitly excluded by the user or inherited exclusion from a parent.
func CountExcludedNodes(boltDB *db.DB, queueType string) (int, error) {
	count := 0
	err := boltDB.IterateNodeStates(queueType, db.IteratorOptions{}, func(pathHash []byte, state *db.NodeState) error {
		// Count if explicitly excluded OR inherited excluded
		if state.ExplicitExcluded || state.InheritedExcluded {
			count++
		}
		return nil
	})
	return count, err
}

// CountExcludedInSubtree counts excluded nodes (explicit_excluded = true OR inherited_excluded = true) within a subtree.
func CountExcludedInSubtree(boltDB *db.DB, queueType string, rootPath string) (int, error) {
	count := 0
	visited := make(map[string]bool) // Track by ULID

	var dfs func(nodeID string) error
	dfs = func(nodeID string) error {
		if visited[nodeID] {
			return nil
		}
		visited[nodeID] = true

		// Get node state by ULID
		nodeState, err := db.GetNodeState(boltDB, queueType, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get node state for %s: %w", nodeID, err)
		}
		if nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		// Count if excluded (explicitly OR inherited)
		if nodeState.ExplicitExcluded || nodeState.InheritedExcluded {
			count++
		}

		// Recurse into children (if folder)
		if nodeState.Type == "folder" {
			childIDs, err := db.GetChildrenIDsByParentID(boltDB, queueType, nodeState.ID)
			if err != nil {
				return fmt.Errorf("failed to get children for %s: %w", nodeID, err)
			}

			for _, childID := range childIDs {
				if err := dfs(childID); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Find root node by path
	rootNode, err := findNodeByPath(boltDB, queueType, rootPath)
	if err != nil {
		return 0, fmt.Errorf("failed to find root node: %w", err)
	}

	if err := dfs(rootNode.ID); err != nil {
		return 0, err
	}

	return count, nil
}
