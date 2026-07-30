// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"path"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// copyTaskLogicalPath returns the SRC root-relative path captured at copy task pull time.
func CopyTaskLogicalPath(task *queue.TaskBase) string {
	if task == nil {
		return "/"
	}
	if p := strings.TrimSpace(task.SrcLogicalPath); p != "" {
		return db.NormalizeRootRelativePath(p)
	}
	return db.NormalizeRootRelativePath(task.LocationPath())
}

// copyTaskLogicalParentPath returns the SRC parent path for copy DST node inserts.
func CopyTaskLogicalParentPath(task *queue.TaskBase) string {
	if task == nil {
		return "/"
	}
	if p := strings.TrimSpace(task.SrcLogicalParentPath); p != "" {
		return db.NormalizeRootRelativePath(p)
	}
	if task.IsFolder() {
		return db.NormalizeRootRelativePath(task.Folder.ParentPath)
	}
	return db.NormalizeRootRelativePath(task.File.ParentPath)
}

// copyTaskCreateMetadata returns adapter metadata for destination creates (logical tree paths).
func copyTaskCreateMetadata(task *queue.TaskBase) map[string]string {
	if task == nil {
		return nil
	}
	return map[string]string{
		"location_path": CopyTaskLogicalPath(task),
		"parent_path":   CopyTaskLogicalParentPath(task),
	}
}

// applyCopyDstFolderFromAdapter keeps SRC logical paths on the task while adopting dst service ids.
func applyCopyDstFolderFromAdapter(task *queue.TaskBase, adapter types.Folder) {
	if task == nil {
		return
	}
	logicalPath := CopyTaskLogicalPath(task)
	logicalParent := CopyTaskLogicalParentPath(task)
	displayName := task.Folder.DisplayName
	if displayName == "" {
		displayName = adapter.DisplayName
	}
	if displayName == "" {
		displayName = path.Base(logicalPath)
	}
	mtime := adapter.LastUpdated
	if mtime == "" {
		mtime = task.Folder.LastUpdated
	}
	task.Folder = types.Folder{
		ServiceID:    adapter.ServiceID,
		ParentId:     adapter.ParentId,
		ParentPath:   logicalParent,
		DisplayName:  displayName,
		LocationPath: logicalPath,
		LastUpdated:  mtime,
		DepthLevel:   task.Round,
		Type:         types.NodeTypeFolder,
	}
}

// applyCopyDstFileFromAdapter keeps SRC logical paths on the task while adopting dst service ids.
func applyCopyDstFileFromAdapter(task *queue.TaskBase, adapter types.File) {
	if task == nil {
		return
	}
	logicalPath := CopyTaskLogicalPath(task)
	logicalParent := CopyTaskLogicalParentPath(task)
	displayName := task.File.DisplayName
	if displayName == "" {
		displayName = adapter.DisplayName
	}
	if displayName == "" {
		displayName = path.Base(logicalPath)
	}
	mtime := adapter.LastUpdated
	if mtime == "" {
		mtime = task.File.LastUpdated
	}
	size := adapter.Size
	if size == 0 {
		size = task.File.Size
	}
	task.File = types.File{
		ServiceID:    adapter.ServiceID,
		ParentId:     adapter.ParentId,
		ParentPath:   logicalParent,
		DisplayName:  displayName,
		LocationPath: logicalPath,
		LastUpdated:  mtime,
		Size:         size,
		DepthLevel:   task.Round,
		Type:         types.NodeTypeFile,
	}
}
