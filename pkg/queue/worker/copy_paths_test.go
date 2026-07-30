// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"testing"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestCopyTaskLogicalPathsFromSrcFields(t *testing.T) {
	task := &queue.TaskBase{
		Round:                2,
		Type:                 queue.TaskTypeCopyFolder,
		SrcLogicalPath:       "/Interview Prep/Beyond Feeback",
		SrcLogicalParentPath: "/Interview Prep",
		Folder: types.Folder{
			LocationPath: "/Beyond Feeback",
			ParentPath:   "/",
			DisplayName:  "Beyond Feeback",
		},
	}
	if got := CopyTaskLogicalPath(task); got != "/Interview Prep/Beyond Feeback" {
		t.Fatalf("logical path = %q", got)
	}
	if got := CopyTaskLogicalParentPath(task); got != "/Interview Prep" {
		t.Fatalf("logical parent = %q", got)
	}
}

func TestApplyCopyDstFolderFromAdapterPreservesLogicalPaths(t *testing.T) {
	task := &queue.TaskBase{
		Round:                2,
		Type:                 queue.TaskTypeCopyFolder,
		SrcLogicalPath:       "/Italian Notes & Resources",
		SrcLogicalParentPath: "/",
		Folder: types.Folder{
			DisplayName: "Italian Notes & Resources",
		},
	}
	applyCopyDstFolderFromAdapter(task, types.Folder{
		ServiceID:    "dst-svc-1",
		ParentId:     "dst-parent",
		LocationPath: "/Italian Notes & Resources",
		ParentPath:   "/",
		DisplayName:  "Italian Notes & Resources",
	})
	if task.Folder.ServiceID != "dst-svc-1" {
		t.Fatalf("service id = %q", task.Folder.ServiceID)
	}
	if task.Folder.LocationPath != "/Italian Notes & Resources" {
		t.Fatalf("location = %q", task.Folder.LocationPath)
	}
	if task.Folder.ParentPath != "/" {
		t.Fatalf("parent = %q", task.Folder.ParentPath)
	}
}

