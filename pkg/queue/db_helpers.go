// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// LoadRootFolders returns root folder rows (depth_level=0) with traversal_status='Pending' from BoltDB.
func LoadRootFolders(boltDB *db.DB, queueType string) ([]types.Folder, error) {
	if boltDB == nil {
		return nil, fmt.Errorf("boltDB cannot be nil")
	}

	// Iterate all pending nodes at level 0
	var folders []types.Folder

	err := boltDB.IterateStatusBucket(queueType, 0, db.StatusPending, db.IteratorOptions{}, func(pathHash []byte) error {
		// Get the node state from nodes bucket
		state, err := db.GetNodeState(boltDB, queueType, pathHash)
		if err != nil || state == nil {
			return nil // Skip if not found
		}

		// Filter for folders only
		if state.Type == types.NodeTypeFolder {
			folder := types.Folder{
				Id:           state.ID,
				ParentId:     state.ParentID,
				ParentPath:   types.NormalizeParentPath(state.ParentPath),
				DisplayName:  state.Name,
				LocationPath: types.NormalizeLocationPath(state.Path),
				LastUpdated:  state.MTime,
				DepthLevel:   state.Depth,
				Type:         state.Type,
			}
			folders = append(folders, folder)
		}
		return nil
	})

	return folders, err
}

// LoadPendingFolders returns all folder rows with traversal_status='Pending' from BoltDB.
func LoadPendingFolders(boltDB *db.DB, queueType string) ([]types.Folder, error) {
	if boltDB == nil {
		return nil, fmt.Errorf("boltDB cannot be nil")
	}

	var folders []types.Folder

	// Get all levels
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return nil, err
	}

	// Iterate each level's pending bucket
	for _, level := range levels {
		err := boltDB.IterateStatusBucket(queueType, level, db.StatusPending, db.IteratorOptions{}, func(pathHash []byte) error {
			// Get the node state from nodes bucket
			state, err := db.GetNodeState(boltDB, queueType, pathHash)
			if err != nil || state == nil {
				return nil // Skip if not found
			}

			// Filter for folders only
			if state.Type == types.NodeTypeFolder {
				folder := types.Folder{
					Id:           state.ID,
					ParentId:     state.ParentID,
					ParentPath:   types.NormalizeParentPath(state.ParentPath),
					DisplayName:  state.Name,
					LocationPath: types.NormalizeLocationPath(state.Path),
					LastUpdated:  state.MTime,
					DepthLevel:   state.Depth,
					Type:         state.Type,
				}
				folders = append(folders, folder)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return folders, err
}

// LoadExpectedChildren returns the expected folders and files for a destination folder path based on src nodes in BoltDB.
// Uses the children index for O(k) lookup where k = number of children.
// dstLevel is the level of the DST task; SRC children will be at dstLevel+1.
func LoadExpectedChildren(boltDB *db.DB, parentPath string, dstLevel int) ([]types.Folder, []types.File, error) {
	if boltDB == nil {
		return nil, nil, fmt.Errorf("boltDB cannot be nil")
	}

	normalizedParent := types.NormalizeLocationPath(parentPath)

	// Use the children index for efficient O(k) lookup
	children, err := db.GetChildrenStates(boltDB, "SRC", normalizedParent)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch children from index: %w", err)
	}

	var folders []types.Folder
	var files []types.File

	for _, state := range children {
		switch state.Type {
		case types.NodeTypeFolder:
			folders = append(folders, types.Folder{
				Id:           state.ID,
				ParentId:     state.ParentID,
				ParentPath:   types.NormalizeParentPath(state.ParentPath),
				DisplayName:  state.Name,
				LocationPath: types.NormalizeLocationPath(state.Path),
				LastUpdated:  state.MTime,
				DepthLevel:   state.Depth,
				Type:         state.Type,
			})
		case types.NodeTypeFile:
			files = append(files, types.File{
				Id:           state.ID,
				ParentId:     state.ParentID,
				ParentPath:   types.NormalizeParentPath(state.ParentPath),
				DisplayName:  state.Name,
				LocationPath: types.NormalizeLocationPath(state.Path),
				LastUpdated:  state.MTime,
				DepthLevel:   state.Depth,
				Size:         state.Size,
				Type:         state.Type,
			})
		}
	}

	return folders, files, nil
}

// BatchLoadExpectedChildren loads expected children for multiple parent paths in a single DB transaction.
// Returns a map of normalized parent path -> (folders, files).
// This is more efficient than calling LoadExpectedChildren() multiple times as it batches all DB operations.
func BatchLoadExpectedChildren(boltDB *db.DB, parentPaths []string) (map[string][]types.Folder, map[string][]types.File, error) {
	if boltDB == nil {
		return nil, nil, fmt.Errorf("boltDB cannot be nil")
	}

	if len(parentPaths) == 0 {
		return make(map[string][]types.Folder), make(map[string][]types.File), nil
	}

	// Normalize all parent paths and build lookup maps
	normalizedPaths := make([]string, 0, len(parentPaths))
	normalizedToOriginal := make(map[string]string)
	for _, path := range parentPaths {
		normalized := types.NormalizeLocationPath(path)
		normalizedPaths = append(normalizedPaths, normalized)
		normalizedToOriginal[normalized] = path
	}

	// Initialize result maps (using original paths as keys for caller convenience)
	resultFolders := make(map[string][]types.Folder)
	resultFiles := make(map[string][]types.File)

	// Single transaction to load all children
	err := boltDB.View(func(tx *bolt.Tx) error {
		childrenBucket := db.GetChildrenBucket(tx, "SRC")
		if childrenBucket == nil {
			return fmt.Errorf("children bucket not found for SRC")
		}

		nodesBucket := db.GetNodesBucket(tx, "SRC")
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for SRC")
		}

		// Map to collect all unique child hashes and their parent associations
		// childHash -> []normalizedParentPaths (a child might be referenced by multiple parents in edge cases)
		childHashToParents := make(map[string][]string)

		// Step 1: Get children hashes for all parents
		for _, normalizedParent := range normalizedPaths {
			parentHash := []byte(db.HashPath(normalizedParent))
			childrenData := childrenBucket.Get(parentHash)
			if childrenData == nil {
				// No children for this parent - initialize empty slices
				originalPath := normalizedToOriginal[normalizedParent]
				resultFolders[originalPath] = []types.Folder{}
				resultFiles[originalPath] = []types.File{}
				continue
			}

			var childHashes []string
			if err := json.Unmarshal(childrenData, &childHashes); err != nil {
				return fmt.Errorf("failed to unmarshal children list for %s: %w", normalizedParent, err)
			}

			// Associate each child hash with this parent
			for _, childHash := range childHashes {
				childHashToParents[childHash] = append(childHashToParents[childHash], normalizedParent)
			}
		}

		// Step 2: Fetch all unique child NodeStates in one pass
		childStates := make(map[string]*db.NodeState)
		for childHash := range childHashToParents {
			nodeData := nodesBucket.Get([]byte(childHash))
			if nodeData == nil {
				continue // Child may have been deleted
			}

			ns, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				// Log but continue - don't fail entire batch for one bad node
				continue
			}
			childStates[childHash] = ns
		}

		// Step 3: Group children by parent and convert to Folder/File types
		for _, normalizedParent := range normalizedPaths {
			originalPath := normalizedToOriginal[normalizedParent]
			var folders []types.Folder
			var files []types.File

			parentHash := []byte(db.HashPath(normalizedParent))
			childrenData := childrenBucket.Get(parentHash)
			if childrenData != nil {
				var childHashes []string
				if err := json.Unmarshal(childrenData, &childHashes); err == nil {
					for _, childHash := range childHashes {
						state, exists := childStates[childHash]
						if !exists {
							continue // Child was deleted or deserialization failed
						}

						switch state.Type {
						case types.NodeTypeFolder:
							folders = append(folders, types.Folder{
								Id:           state.ID,
								ParentId:     state.ParentID,
								ParentPath:   types.NormalizeParentPath(state.ParentPath),
								DisplayName:  state.Name,
								LocationPath: types.NormalizeLocationPath(state.Path),
								LastUpdated:  state.MTime,
								DepthLevel:   state.Depth,
								Type:         state.Type,
							})
						case types.NodeTypeFile:
							files = append(files, types.File{
								Id:           state.ID,
								ParentId:     state.ParentID,
								ParentPath:   types.NormalizeParentPath(state.ParentPath),
								DisplayName:  state.Name,
								LocationPath: types.NormalizeLocationPath(state.Path),
								LastUpdated:  state.MTime,
								DepthLevel:   state.Depth,
								Size:         state.Size,
								Type:         state.Type,
							})
						}
					}
				}
			}

			resultFolders[originalPath] = folders
			resultFiles[originalPath] = files
		}

		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to batch load expected children: %w", err)
	}

	return resultFolders, resultFiles, nil
}
