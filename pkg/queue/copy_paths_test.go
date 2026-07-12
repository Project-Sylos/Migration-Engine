// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestCopyTaskLogicalPathsFromSrcFields(t *testing.T) {
	task := &TaskBase{
		Round:                2,
		Type:                 TaskTypeCopyFolder,
		SrcLogicalPath:       "/Interview Prep/Beyond Feeback",
		SrcLogicalParentPath: "/Interview Prep",
		Folder: types.Folder{
			LocationPath: "/Beyond Feeback",
			ParentPath:   "/",
			DisplayName:  "Beyond Feeback",
		},
	}
	if got := copyTaskLogicalPath(task); got != "/Interview Prep/Beyond Feeback" {
		t.Fatalf("logical path = %q", got)
	}
	if got := copyTaskLogicalParentPath(task); got != "/Interview Prep" {
		t.Fatalf("logical parent = %q", got)
	}
}

func TestApplyCopyDstFolderFromAdapterPreservesLogicalPaths(t *testing.T) {
	task := &TaskBase{
		Round:                2,
		Type:                 TaskTypeCopyFolder,
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

func TestNodeStateToCopyTaskCapturesLogicalPaths(t *testing.T) {
	state := &db.NodeState{
		ID:              "src-1",
		Path:            "/Interview Prep/Beyond Feeback",
		ParentPath:      "/Interview Prep",
		Name:            "Beyond Feeback",
		Type:            types.NodeTypeFolder,
		Depth:           2,
		TraversalStatus: db.StatusSuccessful,
		CopyStatus:      db.CopyStatusPending,
	}
	task := nodeStateToCopyTask(state, TaskTypeCopyFolder, 1)
	if task.SrcLogicalPath != "/Interview Prep/Beyond Feeback" {
		t.Fatalf("SrcLogicalPath = %q", task.SrcLogicalPath)
	}
	if task.SrcLogicalParentPath != "/Interview Prep" {
		t.Fatalf("SrcLogicalParentPath = %q", task.SrcLogicalParentPath)
	}
}
