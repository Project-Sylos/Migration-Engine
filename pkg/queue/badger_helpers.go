// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
)

// taskToNodeState converts a TaskBase to a NodeState for BadgerDB storage.
func taskToNodeState(task *TaskBase) *db.NodeState {
	var id, parentID, name, path, parentPath, nodeType string
	var size int64
	var mtime string

	if task.IsFolder() {
		folder := task.Folder
		id = folder.Id
		parentID = folder.ParentId
		name = folder.DisplayName
		path = fsservices.NormalizeLocationPath(folder.LocationPath)
		parentPath = fsservices.NormalizeParentPath(folder.ParentPath)
		nodeType = fsservices.NodeTypeFolder
		size = 0
		mtime = folder.LastUpdated
	} else if task.IsFile() {
		file := task.File
		id = file.Id
		parentID = file.ParentId
		name = file.DisplayName
		path = fsservices.NormalizeLocationPath(file.LocationPath)
		parentPath = fsservices.NormalizeParentPath(file.ParentPath)
		nodeType = fsservices.NodeTypeFile
		size = file.Size
		mtime = file.LastUpdated
	} else {
		// Invalid task
		return nil
	}

	return &db.NodeState{
		ID:         id,
		ParentID:   parentID,
		ParentPath: parentPath,
		Name:       name,
		Path:       path,
		Type:       nodeType,
		Size:       size,
		MTime:      mtime,
		Depth:      task.Round,
		CopyNeeded: false, // Will be set during traversal comparison
	}
}

// nodeStateToTask converts a NodeState back to a TaskBase.
// Note: This reconstructs the task but doesn't restore DiscoveredChildren or ExpectedFolders/Files.
// Those need to be populated separately if needed.
func nodeStateToTask(state *db.NodeState, taskType string) *TaskBase {
	task := &TaskBase{
		Type:  taskType,
		Round: state.Depth,
	}

	switch state.Type {
	case fsservices.NodeTypeFolder:
		task.Folder = fsservices.Folder{
			Id:           state.ID,
			ParentId:     state.ParentID,
			ParentPath:   state.ParentPath,
			DisplayName:  state.Name,
			LocationPath: state.Path,
			LastUpdated:  state.MTime,
			DepthLevel:   state.Depth,
			Type:         state.Type,
		}
	case fsservices.NodeTypeFile:
		task.File = fsservices.File{
			Id:           state.ID,
			ParentId:     state.ParentID,
			ParentPath:   state.ParentPath,
			DisplayName:  state.Name,
			LocationPath: state.Path,
			LastUpdated:  state.MTime,
			DepthLevel:   state.Depth,
			Size:         state.Size,
			Type:         state.Type,
		}
	}

	return task
}

// getQueueType returns "src" or "dst" based on queue name.
func getQueueType(queueName string) string {
	if queueName == "dst" {
		return "dst"
	}
	return "src"
}

// getChildrenIndexPrefix returns the BadgerDB index prefix for children lookup.
func getChildrenIndexPrefix(queueName string) string {
	switch queueName {
	case "src":
		return db.IndexPrefixSrcChildren
	case "dst":
		return db.IndexPrefixDstChildren
	default:
		return ""
	}
}
