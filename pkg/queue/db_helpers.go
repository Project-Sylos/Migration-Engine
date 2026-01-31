// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// LoadRootFolders returns root folder rows (depth_level=0) with traversal_status='Pending' from BoltDB.
func LoadRootFolders(boltDB *db.DB, queueType string) ([]types.Folder, error) {
	if boltDB == nil {
		return nil, fmt.Errorf("boltDB cannot be nil")
	}

	// Iterate all pending nodes at level 0
	var folders []types.Folder

	err := boltDB.IterateStatusBucket(queueType, 0, db.StatusPending, db.IteratorOptions{}, func(nodeIDBytes []byte) error {
		// Get the node state from nodes bucket (convert ULID bytes to string)
		state, err := db.GetNodeState(boltDB, queueType, string(nodeIDBytes))
		if err != nil || state == nil {
			return nil // Skip if not found
		}

		// Filter for folders only
		if state.Type == types.NodeTypeFolder {
			folder := types.Folder{
				ServiceID:    state.ServiceID,
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
		err := boltDB.IterateStatusBucket(queueType, level, db.StatusPending, db.IteratorOptions{}, func(nodeIDBytes []byte) error {
			// Get the node state from nodes bucket (convert ULID bytes to string)
			state, err := db.GetNodeState(boltDB, queueType, string(nodeIDBytes))
			if err != nil || state == nil {
				return nil // Skip if not found
			}

			// Filter for folders only
			if state.Type == types.NodeTypeFolder {
				folder := types.Folder{
					ServiceID:    state.ServiceID,
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
	children, err := db.GetChildrenStatesByParentID(boltDB, "SRC", normalizedParent)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch children from index: %w", err)
	}

	var folders []types.Folder
	var files []types.File

	for _, state := range children {
		switch state.Type {
		case types.NodeTypeFolder:
			folders = append(folders, types.Folder{
				ServiceID:    state.ServiceID,
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
				ServiceID:    state.ServiceID,
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

// BatchLoadExpectedChildrenByDSTIDs loads expected children for DST parent nodes using SrcID from DST NodeState.
// Takes DST parent ULIDs, gets SrcID from each DST NodeState, then loads SRC children and maps them back to DST parents.
// Returns maps keyed by DST ULID -> (folders, files), a map of SRC node IDs keyed by Type+Name for matching, and srcIDToMeta (Depth/CopyStatus per SRC ID) for copy-status updates without per-child DB lookups.
func BatchLoadExpectedChildrenByDSTIDs(boltDB *db.DB, dstParentIDs []string, dstIDToPath map[string]string) (map[string][]types.Folder, map[string][]types.File, map[string]map[string]string, map[string]SrcNodeMeta, error) {
	if boltDB == nil {
		return nil, nil, nil, nil, fmt.Errorf("boltDB cannot be nil")
	}

	if len(dstParentIDs) == 0 {
		return make(map[string][]types.Folder), make(map[string][]types.File), make(map[string]map[string]string), make(map[string]SrcNodeMeta), nil
	}

	// Initialize result maps (keyed by DST ULID)
	resultFolders := make(map[string][]types.Folder)
	resultFiles := make(map[string][]types.File)
	// Map: DST ULID -> (Type+Name -> SRC node ID)
	srcIDMap := make(map[string]map[string]string)
	var srcIDToMeta map[string]SrcNodeMeta

	// Single transaction to load all children
	err := boltDB.View(func(tx *bolt.Tx) error {
		dstNodesBucket := db.GetNodesBucket(tx, "DST")
		if dstNodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for DST")
		}

		srcChildrenBucket := db.GetChildrenBucket(tx, "SRC")
		if srcChildrenBucket == nil {
			return fmt.Errorf("children bucket not found for SRC")
		}

		srcNodesBucket := db.GetNodesBucket(tx, "SRC")
		if srcNodesBucket == nil {
			return fmt.Errorf("nodes bucket not found for SRC")
		}

		// Step 1: Get SrcID from lookup table for each DST parent
		// Map: DST ULID -> SRC parent ULID
		dstToSrcParent := make(map[string]string)
		// Map: SRC parent ULID -> []DST ULIDs (multiple DST parents might map to same SRC parent)
		srcParentToDSTs := make(map[string][]string)

		// Get dst-to-src lookup bucket
		dstToSrcBucket := db.GetDstToSrcBucket(tx)
		if dstToSrcBucket == nil {
			// Lookup bucket doesn't exist - initialize empty slices for all DST parents
			for _, dstID := range dstParentIDs {
				resultFolders[dstID] = []types.Folder{}
				resultFiles[dstID] = []types.File{}
				srcIDMap[dstID] = make(map[string]string)
			}
			fmt.Println("Lookup bucket doesn't exist - initializing empty slices for all DST parents")
			return nil // No mappings available
		}

		for _, dstID := range dstParentIDs {
			dstIDBytes := []byte(dstID)
			dstNodeData := dstNodesBucket.Get(dstIDBytes)
			if dstNodeData == nil {
				// DST node not found - initialize empty slices
				resultFolders[dstID] = []types.Folder{}
				resultFiles[dstID] = []types.File{}
				srcIDMap[dstID] = make(map[string]string)
				continue
			}

			_, err := db.DeserializeNodeState(dstNodeData)
			if err != nil {
				// Deserialization failed - initialize empty slices
				resultFolders[dstID] = []types.Folder{}
				resultFiles[dstID] = []types.File{}
				srcIDMap[dstID] = make(map[string]string)
				continue
			}

			// Get SrcID from lookup table directly within this transaction
			srcParentIDBytes := dstToSrcBucket.Get(dstIDBytes)
			if srcParentIDBytes == nil {
				// No SrcID mapping found - initialize empty slices
				resultFolders[dstID] = []types.Folder{}
				resultFiles[dstID] = []types.File{}
				srcIDMap[dstID] = make(map[string]string)
				continue
			}

			srcParentID := string(srcParentIDBytes)
			dstToSrcParent[dstID] = srcParentID
			srcParentToDSTs[srcParentID] = append(srcParentToDSTs[srcParentID], dstID)
			srcIDMap[dstID] = make(map[string]string)
		}

		// Step 2: Get all SRC child ULIDs for all SRC parents
		// Map: SRC child ULID -> []DST parent ULIDs (a child might be shared by multiple DST parents)
		srcChildIDToDSTParents := make(map[string][]string)
		allSrcChildIDs := make(map[string]bool) // Set of all unique SRC child ULIDs

		for srcParentID, dstIDs := range srcParentToDSTs {
			childrenData := srcChildrenBucket.Get([]byte(srcParentID))
			if childrenData == nil {
				// No children for this SRC parent - initialize empty slices for all corresponding DST parents
				for _, dstID := range dstIDs {
					if _, exists := resultFolders[dstID]; !exists {
						resultFolders[dstID] = []types.Folder{}
						resultFiles[dstID] = []types.File{}
					}
				}
				continue
			}

			var childIDs []string
			if err := json.Unmarshal(childrenData, &childIDs); err != nil {
				return fmt.Errorf("failed to unmarshal children list for SRC parent %s: %w", srcParentID, err)
			}

			// Associate each SRC child with all corresponding DST parents
			for _, childID := range childIDs {
				allSrcChildIDs[childID] = true
				srcChildIDToDSTParents[childID] = append(srcChildIDToDSTParents[childID], dstIDs...)
			}
		}

		// Step 3: Fetch all unique SRC child NodeStates in one pass
		childStates := make(map[string]*db.NodeState)
		for childID := range allSrcChildIDs {
			nodeData := srcNodesBucket.Get([]byte(childID))
			if nodeData == nil {
				continue // Child may have been deleted
			}

			ns, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				// Log but continue - don't fail entire batch for one bad node
				continue
			}
			childStates[childID] = ns
		}

		// Step 4: Group children by DST parent and convert to Folder/File types
		for childID, dstParentIDs := range srcChildIDToDSTParents {
			state, exists := childStates[childID]
			if !exists {
				continue // Child was deleted or deserialization failed
			}

			// Create key for matching: Type+Name
			matchKey := state.Type + ":" + state.Name

			// Convert NodeState to Folder or File
			var folder *types.Folder
			var file *types.File

			switch state.Type {
			case types.NodeTypeFolder:
				folder = &types.Folder{
					ServiceID:    state.ServiceID,
					ParentId:     state.ParentServiceID, // Use ParentServiceID for FS interactions
					ParentPath:   types.NormalizeParentPath(state.ParentPath),
					DisplayName:  state.Name,
					LocationPath: types.NormalizeLocationPath(state.Path),
					LastUpdated:  state.MTime,
					DepthLevel:   state.Depth,
					Type:         state.Type,
				}
			case types.NodeTypeFile:
				file = &types.File{
					ServiceID:    state.ServiceID,
					ParentId:     state.ParentServiceID, // Use ParentServiceID for FS interactions
					ParentPath:   types.NormalizeParentPath(state.ParentPath),
					DisplayName:  state.Name,
					LocationPath: types.NormalizeLocationPath(state.Path),
					LastUpdated:  state.MTime,
					DepthLevel:   state.Depth,
					Size:         state.Size,
					Type:         state.Type,
				}
			}

			// Add this child to all corresponding DST parents and track SRC node ID
			for _, dstID := range dstParentIDs {
				if folder != nil {
					resultFolders[dstID] = append(resultFolders[dstID], *folder)
					srcIDMap[dstID][matchKey] = childID
				}
				if file != nil {
					resultFiles[dstID] = append(resultFiles[dstID], *file)
					srcIDMap[dstID][matchKey] = childID
				}
			}
		}

		// Ensure all DST parents have entries (even if empty)
		for _, dstID := range dstParentIDs {
			if _, exists := resultFolders[dstID]; !exists {
				resultFolders[dstID] = []types.Folder{}
			}
			if _, exists := resultFiles[dstID]; !exists {
				resultFiles[dstID] = []types.File{}
			}
			if _, exists := srcIDMap[dstID]; !exists {
				srcIDMap[dstID] = make(map[string]string)
			}
		}

		// Build SRC ID -> (Depth, CopyStatus) for copy-status updates at completion without per-child GetNodeState
		srcIDToMeta = make(map[string]SrcNodeMeta)
		for childID, ns := range childStates {
			meta := SrcNodeMeta{Depth: ns.Depth, CopyStatus: ns.CopyStatus}
			if meta.CopyStatus == "" {
				meta.CopyStatus = db.CopyStatusPending
			}
			srcIDToMeta[childID] = meta
		}

		return nil
	})

	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to batch load expected children: %w", err)
	}

	return resultFolders, resultFiles, srcIDMap, srcIDToMeta, nil
}

// BatchLoadRetryDstCleanup loads DST counterpart and children meta for SRC folder tasks in retry mode.
// Returns map[srcID]*RetryDstCleanup so completion can queue DST status update and child deletions without per-child DB lookups.
func BatchLoadRetryDstCleanup(boltDB *db.DB, srcIDs []string) (map[string]*RetryDstCleanup, error) {
	out := make(map[string]*RetryDstCleanup)
	if boltDB == nil || len(srcIDs) == 0 {
		return out, nil
	}
	srcToDst, err := db.BatchGetDstIDsFromSrcIDs(boltDB, srcIDs)
	if err != nil {
		return nil, fmt.Errorf("batch get DST IDs from SRC IDs: %w", err)
	}
	if len(srcToDst) == 0 {
		return out, nil
	}
	dstIDs := make([]string, 0, len(srcToDst))
	for _, dstID := range srcToDst {
		dstIDs = append(dstIDs, dstID)
	}
	dstMeta, err := db.BatchGetNodeMeta(boltDB, "DST", dstIDs)
	if err != nil {
		return nil, fmt.Errorf("batch get DST node meta: %w", err)
	}
	parentToChildren, err := db.BatchGetChildrenIDsByParentIDs(boltDB, "DST", dstIDs)
	if err != nil {
		return nil, fmt.Errorf("batch get DST children by parent: %w", err)
	}
	var allChildIDs []string
	for _, childIDs := range parentToChildren {
		allChildIDs = append(allChildIDs, childIDs...)
	}
	childMeta := make(map[string]db.NodeMeta)
	if len(allChildIDs) > 0 {
		childMeta, err = db.BatchGetNodeMeta(boltDB, "DST", allChildIDs)
		if err != nil {
			return nil, fmt.Errorf("batch get DST child meta: %w", err)
		}
	}
	for srcID, dstID := range srcToDst {
		meta, ok := dstMeta[dstID]
		if !ok {
			continue
		}
		oldStatus := meta.TraversalStatus
		if oldStatus == "" {
			oldStatus = db.StatusSuccessful
		}
		children := parentToChildren[dstID]
		cleanupChildren := make([]RetryDstChild, 0, len(children))
		for _, childID := range children {
			cm, ok := childMeta[childID]
			if !ok {
				continue
			}
			childStatus := cm.TraversalStatus
			if childStatus == "" {
				childStatus = db.StatusSuccessful
			}
			cleanupChildren = append(cleanupChildren, RetryDstChild{
				ID:              childID,
				Depth:           cm.Depth,
				TraversalStatus: childStatus,
			})
		}
		out[srcID] = &RetryDstCleanup{
			DstID:        dstID,
			DstDepth:     meta.Depth,
			DstOldStatus: oldStatus,
			Children:     cleanupChildren,
		}
	}
	return out, nil
}
