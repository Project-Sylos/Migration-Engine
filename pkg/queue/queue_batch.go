// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// BuildExpectedMapsFromDstWithChildren builds expectedFoldersMap, expectedFilesMap, srcIDMap, and srcIDToMeta from a DST batch and its SRC children (e.g. from ListDstBatchWithSrcChildren). Keyed by DST node ID.
func BuildExpectedMapsFromDstWithChildren(dstBatch []db.FetchResult, childrenByDstID map[string][]*db.NodeState) (
	expectedFoldersMap map[string][]types.Folder,
	expectedFilesMap map[string][]types.File,
	srcIDMap map[string]map[string]string,
	srcIDToMeta map[string]SrcNodeMeta,
) {
	expectedFoldersMap = make(map[string][]types.Folder)
	expectedFilesMap = make(map[string][]types.File)
	srcIDMap = make(map[string]map[string]string)
	srcIDToMeta = make(map[string]SrcNodeMeta)
	for _, fr := range dstBatch {
		dstID := fr.Key
		nodes := childrenByDstID[dstID]
		if len(nodes) == 0 {
			continue
		}
		var folders []types.Folder
		var files []types.File
		idMap := make(map[string]string)
		for _, n := range nodes {
			displayName := n.Name
			if displayName == "" && n.Path != "" {
				displayName = n.Path
			}
			matchKey := n.Type + ":" + displayName
			idMap[matchKey] = n.ID
			srcIDToMeta[n.ID] = SrcNodeMeta{Depth: n.Depth, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus}
			if n.Type == types.NodeTypeFolder {
				folders = append(folders, types.Folder{
					ServiceID:    n.ServiceID,
					ParentId:     n.ParentServiceID,
					ParentPath:   n.ParentPath,
					DisplayName:  displayName,
					LocationPath: n.Path,
					LastUpdated:  n.MTime,
					DepthLevel:   n.Depth,
					Type:         n.Type,
				})
			} else {
				files = append(files, types.File{
					ServiceID:    n.ServiceID,
					ParentId:     n.ParentServiceID,
					ParentPath:   n.ParentPath,
					DisplayName:  displayName,
					LocationPath: n.Path,
					LastUpdated:  n.MTime,
					Size:         n.Size,
					DepthLevel:   n.Depth,
					Type:         n.Type,
				})
			}
		}
		expectedFoldersMap[dstID] = folders
		expectedFilesMap[dstID] = files
		srcIDMap[dstID] = idMap
	}
	return expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta
}

// BatchLoadExpectedChildrenByDSTIDs loads SRC children for the given DST folder IDs by joining on parent_path = dst.path.
// Prefer ListDstBatchWithSrcChildren + BuildExpectedMapsFromDstWithChildren for keyset-based DST pull (no IN query).
// Returns expectedFoldersMap, expectedFilesMap (keyed by DST task ID), srcIDMap (Type+DisplayName -> SRC node ID per DST ID), and srcIDToMeta (SRC ID -> meta).
func BatchLoadExpectedChildrenByDSTIDs(database *db.DB, dstParentIDs []string, dstIDToPath map[string]string) (
	expectedFoldersMap map[string][]types.Folder,
	expectedFilesMap map[string][]types.File,
	srcIDMap map[string]map[string]string,
	srcIDToMeta map[string]SrcNodeMeta,
	err error,
) {
	expectedFoldersMap = make(map[string][]types.Folder)
	expectedFilesMap = make(map[string][]types.File)
	srcIDMap = make(map[string]map[string]string)
	srcIDToMeta = make(map[string]SrcNodeMeta)
	if len(dstIDToPath) == 0 {
		return expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta, nil
	}
	paths := make([]string, 0, len(dstIDToPath))
	pathToDstID := make(map[string]string)
	for _, id := range dstParentIDs {
		path, ok := dstIDToPath[id]
		if !ok {
			continue
		}
		paths = append(paths, path)
		pathToDstID[path] = id
	}
	if len(paths) == 0 {
		return expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta, nil
	}
	byPath, err := db.GetSrcChildrenGroupedByParentPath(database, paths)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for path, nodes := range byPath {
		dstID := pathToDstID[path]
		if dstID == "" {
			continue
		}
		var folders []types.Folder
		var files []types.File
		idMap := make(map[string]string)
		for _, n := range nodes {
			displayName := n.Name
			if displayName == "" && n.Path != "" {
				displayName = n.Path
			}
			matchKey := n.Type + ":" + displayName
			idMap[matchKey] = n.ID
			srcIDToMeta[n.ID] = SrcNodeMeta{Depth: n.Depth, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus}
			if n.Type == types.NodeTypeFolder {
				folders = append(folders, types.Folder{
					ServiceID:    n.ServiceID,
					ParentId:     n.ParentServiceID,
					ParentPath:   n.ParentPath,
					DisplayName:  displayName,
					LocationPath: n.Path,
					LastUpdated:  n.MTime,
					DepthLevel:   n.Depth,
					Type:         n.Type,
				})
			} else {
				files = append(files, types.File{
					ServiceID:    n.ServiceID,
					ParentId:     n.ParentServiceID,
					ParentPath:   n.ParentPath,
					DisplayName:  displayName,
					LocationPath: n.Path,
					LastUpdated:  n.MTime,
					Size:         n.Size,
					DepthLevel:   n.Depth,
					Type:         n.Type,
				})
			}
		}
		expectedFoldersMap[dstID] = folders
		expectedFilesMap[dstID] = files
		srcIDMap[dstID] = idMap
	}
	return expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta, nil
}

// BatchLoadRetryDstCleanup loads DST counterpart and children meta for each SRC folder ID (for retry mode DST cleanup).
func BatchLoadRetryDstCleanup(database *db.DB, srcFolderIDs []string) (map[string]*RetryDstCleanup, error) {
	out := make(map[string]*RetryDstCleanup)
	if len(srcFolderIDs) == 0 {
		return out, nil
	}
	srcToDst, err := db.BatchGetDstIDsFromSrcIDs(database, srcFolderIDs)
	if err != nil {
		return nil, err
	}
	dstIDs := make([]string, 0, len(srcToDst))
	for _, dstID := range srcToDst {
		dstIDs = append(dstIDs, dstID)
	}
	dstMeta, err := db.BatchGetNodeMeta(database, "DST", dstIDs)
	if err != nil {
		return nil, err
	}
	parentToChildren, err := db.BatchGetChildrenIDsByParentIDs(database, "DST", dstIDs)
	if err != nil {
		return nil, err
	}
	allChildIDs := make([]string, 0)
	for _, ids := range parentToChildren {
		allChildIDs = append(allChildIDs, ids...)
	}
	var childMeta map[string]db.NodeMeta
	if len(allChildIDs) > 0 {
		childMeta, err = db.BatchGetNodeMeta(database, "DST", allChildIDs)
		if err != nil {
			return nil, err
		}
	}
	if childMeta == nil {
		childMeta = make(map[string]db.NodeMeta)
	}
	for _, srcID := range srcFolderIDs {
		dstID := srcToDst[srcID]
		if dstID == "" {
			continue
		}
		meta, ok := dstMeta[dstID]
		if !ok {
			continue
		}
		childIDs := parentToChildren[dstID]
		children := make([]RetryDstChild, 0, len(childIDs))
		for _, cid := range childIDs {
			cm, ok := childMeta[cid]
			if !ok {
				continue
			}
			children = append(children, RetryDstChild{
				ID:              cm.ID,
				Depth:           cm.Depth,
				TraversalStatus: cm.TraversalStatus,
			})
		}
		oldStatus := meta.TraversalStatus
		if oldStatus == "" {
			oldStatus = db.StatusSuccessful
		}
		out[srcID] = &RetryDstCleanup{
			DstID:       dstID,
			DstDepth:    meta.Depth,
			DstOldStatus: oldStatus,
			Children:    children,
		}
	}
	return out, nil
}
