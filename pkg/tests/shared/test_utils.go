// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

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
	visited := make(map[string]bool)

	var dfs func(path string, depth int) error
	dfs = func(path string, depth int) error {
		pathHash := db.HashPath(path)
		if visited[pathHash] {
			return nil // Already counted
		}
		visited[pathHash] = true

		// Get node state
		var nodeState *db.NodeState
		err := boltDB.View(func(tx *bolt.Tx) error {
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found")
			}

			nodeData := nodesBucket.Get([]byte(pathHash))
			if nodeData == nil {
				return fmt.Errorf("node not found: %s", path)
			}

			var err error
			nodeState, err = db.DeserializeNodeState(nodeData)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to get node state for %s: %w", path, err)
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
			children, err := db.GetChildrenStates(boltDB, queueType, path)
			if err != nil {
				return fmt.Errorf("failed to get children for %s: %w", path, err)
			}

			for _, child := range children {
				if err := dfs(child.Path, depth+1); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := dfs(rootPath, 0); err != nil {
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
	var dfsCollect func(path string) error
	dfsCollect = func(path string) error {
		pathHash := db.HashPath(path)
		if visited[pathHash] {
			return nil // Already collected
		}
		visited[pathHash] = true

		// Get node state
		var nodeState *db.NodeState
		err := boltDB.View(func(tx *bolt.Tx) error {
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found")
			}

			nodeData := nodesBucket.Get([]byte(pathHash))
			if nodeData == nil {
				return fmt.Errorf("node not found: %s", path)
			}

			var err2 error
			nodeState, err2 = db.DeserializeNodeState(nodeData)
			return err2
		})
		if err != nil {
			return fmt.Errorf("failed to get node state for %s: %w", path, err)
		}

		// Collect this node
		nodesToDelete = append(nodesToDelete, nodeState)

		// Recurse into children (if folder)
		if nodeState.Type == "folder" {
			children, err := db.GetChildrenStates(boltDB, queueType, path)
			if err != nil {
				return fmt.Errorf("failed to get children for %s: %w", path, err)
			}

			for _, child := range children {
				if err := dfsCollect(child.Path); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := dfsCollect(rootPath); err != nil {
		return err
	}

	// Second pass: delete all collected nodes
	// Get all levels first (outside transaction)
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return fmt.Errorf("failed to get levels: %w", err)
	}

	return boltDB.Update(func(tx *bolt.Tx) error {
		for _, nodeState := range nodesToDelete {
			pathHash := []byte(db.HashPath(nodeState.Path))
			parentHash := []byte(db.HashPath(nodeState.ParentPath))

			// 1. Delete from nodes bucket
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket != nil {
				nodesBucket.Delete(pathHash)
			}

			// 2. Delete from status bucket (check all status buckets at all levels)
			// We'll check common statuses: pending, successful, failed, not_on_src
			statuses := []string{db.StatusPending, db.StatusSuccessful, db.StatusFailed, db.StatusNotOnSrc}
			for _, level := range levels {
				for _, status := range statuses {
					statusBucket := db.GetStatusBucket(tx, queueType, level, status)
					if statusBucket != nil && statusBucket.Get(pathHash) != nil {
						statusBucket.Delete(pathHash)
					}
				}
			}

			// 3. Delete from status-lookup index
			for _, level := range levels {
				lookupBucket := db.GetStatusLookupBucket(tx, queueType, level)
				if lookupBucket != nil {
					lookupBucket.Delete(pathHash)
				}
			}

			// 4. Remove from parent's children list
			if nodeState.ParentPath != "" {
				childrenBucket := db.GetChildrenBucket(tx, queueType)
				if childrenBucket != nil {
					childrenData := childrenBucket.Get(parentHash)
					if childrenData != nil {
						var children []string
						if err := json.Unmarshal(childrenData, &children); err == nil {
							childHash := db.HashPath(nodeState.Path)
							filtered := make([]string, 0, len(children))
							for _, c := range children {
								if c != childHash {
									filtered = append(filtered, c)
								}
							}

							if len(filtered) > 0 {
								updatedData, err := json.Marshal(filtered)
								if err == nil {
									childrenBucket.Put(parentHash, updatedData)
								}
							} else {
								childrenBucket.Delete(parentHash)
							}
						}
					}
				}
			}
		}

		return nil
	})
}

// MarkNodeAsFailed marks a node as failed in the database.
func MarkNodeAsFailed(boltDB *db.DB, queueType string, nodePath string) error {
	pathHash := db.HashPath(nodePath)

	// First, get the node to find its level and current status
	var nodeState *db.NodeState
	var currentLevel int
	var currentStatus string

	err := boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		nodeData := nodesBucket.Get([]byte(pathHash))
		if nodeData == nil {
			return fmt.Errorf("node not found: %s", nodePath)
		}

		var err error
		nodeState, err = db.DeserializeNodeState(nodeData)
		if err != nil {
			return err
		}

		currentLevel = nodeState.Depth
		currentStatus = nodeState.TraversalStatus
		return nil
	})
	if err != nil {
		return err
	}

	// Update status to failed
	_, err = db.UpdateNodeStatus(boltDB, queueType, currentLevel, currentStatus, db.StatusFailed, nodePath)
	return err
}

// MarkNodeAsExcluded marks a node as explicitly excluded in the database.
func MarkNodeAsExcluded(boltDB *db.DB, queueType string, nodePath string) error {
	pathHash := []byte(db.HashPath(nodePath))

	return boltDB.Update(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		nodeData := nodesBucket.Get([]byte(pathHash))
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

		return nodesBucket.Put(pathHash, updatedData)
	})
}

// MarkNodeAsUnexcluded marks a node as explicitly unexcluded in the database.
func MarkNodeAsUnexcluded(boltDB *db.DB, queueType string, nodePath string) error {
	pathHash := []byte(db.HashPath(nodePath))

	return boltDB.Update(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		nodeData := nodesBucket.Get([]byte(pathHash))
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

		return nodesBucket.Put(pathHash, updatedData)
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
// 1. Try children bucket with "/" (normalized root path)
// 2. Try children bucket with "" (root's ParentPath)
// 3. Fall back to finding all nodes at depth 1
func GetTopLevelChildren(boltDB *db.DB, queueType string, rootPath string) ([]*db.NodeState, error) {
	// Strategy 1: Try with "/" (normalized root path)
	children, err := db.GetChildrenStates(boltDB, queueType, "/")
	if err == nil && len(children) > 0 {
		return children, nil
	}

	// Strategy 2: Try with empty string (root's actual ParentPath)
	children, err = db.GetChildrenStates(boltDB, queueType, "")
	if err == nil && len(children) > 0 {
		return children, nil
	}

	// Strategy 3: Fall back to finding all nodes at depth 1
	// This is more reliable if children bucket structure is inconsistent
	// First check if level 1 exists (outside transaction)
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
		seenHashes := make(map[string]bool)

		for _, status := range statuses {
			statusBucket := db.GetStatusBucket(tx, queueType, 1, status)
			if statusBucket == nil {
				continue
			}

			cursor := statusBucket.Cursor()
			for pathHash, _ := cursor.First(); pathHash != nil; pathHash, _ = cursor.Next() {
				hashStr := string(pathHash)
				if seenHashes[hashStr] {
					continue // Already processed
				}
				seenHashes[hashStr] = true

				// Get node data
				nodeData := nodesBucket.Get(pathHash)
				if nodeData == nil {
					continue
				}

				nodeState, err := db.DeserializeNodeState(nodeData)
				if err != nil {
					continue
				}

				// Verify it's actually depth 1 and parent is root
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
	visited := make(map[string]bool)

	var dfs func(path string) error
	dfs = func(path string) error {
		pathHash := db.HashPath(path)
		if visited[pathHash] {
			return nil
		}
		visited[pathHash] = true

		// Get node state
		var nodeState *db.NodeState
		err := boltDB.View(func(tx *bolt.Tx) error {
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found")
			}

			nodeData := nodesBucket.Get([]byte(pathHash))
			if nodeData == nil {
				return fmt.Errorf("node not found: %s", path)
			}

			var err error
			nodeState, err = db.DeserializeNodeState(nodeData)
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to get node state for %s: %w", path, err)
		}

		// Count if excluded (explicitly OR inherited)
		if nodeState.ExplicitExcluded || nodeState.InheritedExcluded {
			count++
		}

		// Recurse into children (if folder)
		if nodeState.Type == "folder" {
			children, err := db.GetChildrenStates(boltDB, queueType, path)
			if err != nil {
				return fmt.Errorf("failed to get children for %s: %w", path, err)
			}

			for _, child := range children {
				if err := dfs(child.Path); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := dfs(rootPath); err != nil {
		return 0, err
	}

	return count, nil
}
