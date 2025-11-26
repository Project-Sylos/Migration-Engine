// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
)

// LoadRootFolders returns root folder rows (depth_level=0) with traversal_status='Pending' from BadgerDB.
func LoadRootFolders(badgerDB *db.DB, queueType string) ([]fsservices.Folder, error) {
	if badgerDB == nil {
		return nil, fmt.Errorf("badgerDB cannot be nil")
	}

	// Iterate all pending nodes at level 0
	prefix := db.PrefixForStatus(queueType, 0, db.TraversalStatusPending)

	var folders []fsservices.Folder
	err := badgerDB.IterateNodeStates(db.IteratorOptions{
		Prefix: prefix,
	}, func(_ []byte, state *db.NodeState) error {
		// Filter for folders only
		if state.Type == fsservices.NodeTypeFolder {
			folder := fsservices.Folder{
				Id:           state.ID,
				ParentId:     state.ParentID,
				ParentPath:   fsservices.NormalizeParentPath(state.ParentPath),
				DisplayName:  state.Name,
				LocationPath: fsservices.NormalizeLocationPath(state.Path),
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

// LoadPendingFolders returns all folder rows with traversal_status='Pending' from BadgerDB.
func LoadPendingFolders(badgerDB *db.DB, queueType string) ([]fsservices.Folder, error) {
	if badgerDB == nil {
		return nil, fmt.Errorf("badgerDB cannot be nil")
	}

	// Iterate all levels and filter by pending status
	prefix := db.PrefixForAllLevels(queueType)
	var folders []fsservices.Folder

	err := badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: prefix,
	}, func(key []byte) error {
		// Parse key: {queueType}:{level}:{traversal_status}:{copy_status}:{path_hash}
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		// Check if status is pending
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			// Fetch the node state
			value, err := badgerDB.Get(key)
			if err != nil {
				return nil // Skip if not found
			}
			state, err := db.DeserializeNodeState(value)
			if err != nil {
				return nil // Skip if deserialization fails
			}
			if state.Type == fsservices.NodeTypeFolder {
				folder := fsservices.Folder{
					Id:           state.ID,
					ParentId:     state.ParentID,
					ParentPath:   fsservices.NormalizeParentPath(state.ParentPath),
					DisplayName:  state.Name,
					LocationPath: fsservices.NormalizeLocationPath(state.Path),
					LastUpdated:  state.MTime,
					DepthLevel:   state.Depth,
					Type:         state.Type,
				}
				folders = append(folders, folder)
			}
		}
		return nil
	})

	return folders, err
}

// splitKeyParts splits a BadgerDB key by colons.
func splitKeyParts(key string) []string {
	var parts []string
	lastIdx := 0
	for i, r := range key {
		if r == ':' {
			parts = append(parts, key[lastIdx:i])
			lastIdx = i + 1
		}
	}
	if lastIdx < len(key) {
		parts = append(parts, key[lastIdx:])
	}
	return parts
}

// LoadExpectedChildren returns the expected folders and files for a destination folder path based on src nodes in BadgerDB.
func LoadExpectedChildren(badgerDB *db.DB, parentPath string) ([]fsservices.Folder, []fsservices.File, error) {
	if badgerDB == nil {
		return nil, nil, fmt.Errorf("badgerDB cannot be nil")
	}

	normalizedParent := fsservices.NormalizeLocationPath(parentPath)

	// Iterate all src nodes and filter by parent_path
	prefix := db.PrefixForAllLevels("src")
	var folders []fsservices.Folder
	var files []fsservices.File

	err := badgerDB.IterateNodeStates(db.IteratorOptions{
		Prefix: prefix,
	}, func(_ []byte, state *db.NodeState) error {
		if state.ParentPath == normalizedParent {
			last := state.MTime
			switch state.Type {
			case fsservices.NodeTypeFolder:
				folders = append(folders, fsservices.Folder{
					Id:           state.ID,
					ParentId:     state.ParentID,
					ParentPath:   fsservices.NormalizeParentPath(state.ParentPath),
					DisplayName:  state.Name,
					LocationPath: fsservices.NormalizeLocationPath(state.Path),
					LastUpdated:  last,
					DepthLevel:   state.Depth,
					Type:         state.Type,
				})
			case fsservices.NodeTypeFile:
				file := fsservices.File{
					Id:           state.ID,
					ParentId:     state.ParentID,
					ParentPath:   fsservices.NormalizeParentPath(state.ParentPath),
					DisplayName:  state.Name,
					LocationPath: fsservices.NormalizeLocationPath(state.Path),
					LastUpdated:  last,
					DepthLevel:   state.Depth,
					Size:         state.Size,
					Type:         state.Type,
				}
				files = append(files, file)
			}
		}
		return nil
	})

	return folders, files, err
}
