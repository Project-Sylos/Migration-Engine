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

	// Use the children index for efficient O(k) lookup (parent at dstLevel, children list in that level shard)
	children, err := db.GetChildrenStatesByParentID(boltDB, "SRC", dstLevel, normalizedParent)
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

	// Single transaction; level-scoped buckets (plan: nodes/children/join under levels/<level>/)
	err := boltDB.View(func(tx *bolt.Tx) error {
		levels, _ := db.GetAllLevelsFromTx(tx, "DST")
		if levels == nil {
			levels = []int{}
		}

		// Step 1: For each DST parent find level and SrcID (dst-to-src is per level)
		dstToSrcParent := make(map[string]string)
		srcParentToDSTs := make(map[string][]string)
		srcParentLevel := make(map[string]int) // SRC parent ULID -> level (for children bucket)

		for _, level := range levels {
			dstNodesBucket := db.GetNodesBucket(tx, "DST", level)
			dstToSrcBucket := db.GetDstToSrcBucket(tx, level)
			if dstNodesBucket == nil || dstToSrcBucket == nil {
				continue
			}
			for _, dstID := range dstParentIDs {
				if _, done := dstToSrcParent[dstID]; done {
					continue
				}
				dstIDBytes := []byte(dstID)
				if dstNodesBucket.Get(dstIDBytes) == nil {
					continue
				}
				srcParentIDBytes := dstToSrcBucket.Get(dstIDBytes)
				if srcParentIDBytes == nil {
					resultFolders[dstID] = []types.Folder{}
					resultFiles[dstID] = []types.File{}
					srcIDMap[dstID] = make(map[string]string)
					dstToSrcParent[dstID] = ""
					continue
				}
				srcParentID := string(srcParentIDBytes)
				dstToSrcParent[dstID] = srcParentID
				srcParentToDSTs[srcParentID] = append(srcParentToDSTs[srcParentID], dstID)
				srcParentLevel[srcParentID] = level
				srcIDMap[dstID] = make(map[string]string)
			}
		}
		for _, dstID := range dstParentIDs {
			if _, exists := resultFolders[dstID]; !exists {
				resultFolders[dstID] = []types.Folder{}
				resultFiles[dstID] = []types.File{}
				srcIDMap[dstID] = make(map[string]string)
			}
		}

		// Step 2: Get SRC child ULIDs (children bucket per level)
		srcChildIDToDSTParents := make(map[string][]string)
		allSrcChildIDs := make(map[string]bool)
		childIDToLevel := make(map[string]int)

		for srcParentID, dstIDs := range srcParentToDSTs {
			level := srcParentLevel[srcParentID]
			srcChildrenBucket := db.GetChildrenBucket(tx, "SRC", level)
			if srcChildrenBucket == nil {
				continue
			}
			childrenData := srcChildrenBucket.Get([]byte(srcParentID))
			if childrenData == nil {
				continue
			}
			var childIDs []string
			if err := json.Unmarshal(childrenData, &childIDs); err != nil {
				return fmt.Errorf("failed to unmarshal children list for SRC parent %s: %w", srcParentID, err)
			}
			childLevel := level + 1
			for _, childID := range childIDs {
				allSrcChildIDs[childID] = true
				childIDToLevel[childID] = childLevel
				srcChildIDToDSTParents[childID] = append(srcChildIDToDSTParents[childID], dstIDs...)
			}
		}

		// Step 3: Fetch SRC child NodeStates (nodes bucket per level)
		childStates := make(map[string]*db.NodeState)
		for childID := range allSrcChildIDs {
			childLevel := childIDToLevel[childID]
			srcNodesBucket := db.GetNodesBucket(tx, "SRC", childLevel)
			if srcNodesBucket == nil {
				continue
			}
			nodeData := srcNodesBucket.Get([]byte(childID))
			if nodeData == nil {
				continue
			}
			ns, err := db.DeserializeNodeState(nodeData)
			if err != nil {
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
	// Join lookup is per level; get SRC level per srcID then call BatchGetDstIDsFromSrcIDs per level
	srcMeta, err := db.BatchGetNodeMeta(boltDB, "SRC", srcIDs)
	if err != nil {
		return nil, fmt.Errorf("batch get SRC node meta: %w", err)
	}
	levelToSrcIDs := make(map[int][]string)
	for _, id := range srcIDs {
		meta, ok := srcMeta[id]
		if !ok {
			continue
		}
		levelToSrcIDs[meta.Depth] = append(levelToSrcIDs[meta.Depth], id)
	}
	srcToDst := make(map[string]string)
	for level, ids := range levelToSrcIDs {
		m, err := db.BatchGetDstIDsFromSrcIDs(boltDB, level, ids)
		if err != nil {
			return nil, fmt.Errorf("batch get DST IDs from SRC IDs: %w", err)
		}
		for k, v := range m {
			srcToDst[k] = v
		}
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
	// Children bucket is per level; group DST parents by level then call BatchGetChildrenIDsByParentIDs per level
	levelToDstIDs := make(map[int][]string)
	for _, id := range dstIDs {
		meta, ok := dstMeta[id]
		if !ok {
			continue
		}
		levelToDstIDs[meta.Depth] = append(levelToDstIDs[meta.Depth], id)
	}
	parentToChildren := make(map[string][]string)
	for level, ids := range levelToDstIDs {
		m, err := db.BatchGetChildrenIDsByParentIDs(boltDB, "DST", level, ids)
		if err != nil {
			return nil, fmt.Errorf("batch get DST children by parent: %w", err)
		}
		for k, v := range m {
			parentToChildren[k] = v
		}
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
