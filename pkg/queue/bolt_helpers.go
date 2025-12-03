// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// taskToNodeState converts a TaskBase to a NodeState for BoltDB storage.
func taskToNodeState(task *TaskBase) *db.NodeState {
	var id, parentID, name, path, parentPath, nodeType string
	var size int64
	var mtime string

	if task.IsFolder() {
		folder := task.Folder
		id = folder.Id
		parentID = folder.ParentId
		name = folder.DisplayName
		path = types.NormalizeLocationPath(folder.LocationPath)
		parentPath = types.NormalizeParentPath(folder.ParentPath)
		nodeType = types.NodeTypeFolder
		size = 0
		mtime = folder.LastUpdated
	} else if task.IsFile() {
		file := task.File
		id = file.Id
		parentID = file.ParentId
		name = file.DisplayName
		path = types.NormalizeLocationPath(file.LocationPath)
		parentPath = types.NormalizeParentPath(file.ParentPath)
		nodeType = types.NodeTypeFile
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
	case types.NodeTypeFolder:
		task.Folder = types.Folder{
			Id:           state.ID,
			ParentId:     state.ParentID,
			ParentPath:   state.ParentPath,
			DisplayName:  state.Name,
			LocationPath: state.Path,
			LastUpdated:  state.MTime,
			DepthLevel:   state.Depth,
			Type:         state.Type,
		}
	case types.NodeTypeFile:
		task.File = types.File{
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

// getQueueType returns "SRC" or "DST" based on queue name (matches Bolt bucket names).
func getQueueType(queueName string) string {
	if queueName == "dst" {
		return "DST"
	}
	return "SRC"
}
