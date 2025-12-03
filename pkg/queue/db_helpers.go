// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

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
