// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"testing"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestCopyTaskCreateBasenamePrefersDisplayName(t *testing.T) {
	idPath := "/aaaaaaaa-bbbb-5ccc-dddd-eeeeeeeeeeee/ffffffff-1111-5222-8333-444444444444"
	task := &queue.TaskBase{
		Type: queue.TaskTypeCopyFolder,
		Folder: types.Folder{
			DisplayName:  "Reports",
			LocationPath: idPath,
		},
	}
	if got := copyTaskCreateBasename(task); got != "Reports" {
		t.Fatalf("got %q want Reports (must not use id_path leaf)", got)
	}

	task.ResolvedDstName = "Reports_clean"
	if got := copyTaskCreateBasename(task); got != "Reports_clean" {
		t.Fatalf("resolved rename: got %q", got)
	}

	fileTask := &queue.TaskBase{
		Type: queue.TaskTypeCopyFile,
		File: types.File{
			DisplayName:  "summary.txt",
			LocationPath: idPath + "/bbbbbbbb-cccc-5ddd-eeee-ffffffffffff",
		},
	}
	if got := copyTaskCreateBasename(fileTask); got != "summary.txt" {
		t.Fatalf("file: got %q", got)
	}
}

func TestCopyTaskCreateBasenameRejectsIDPathFallback(t *testing.T) {
	leaf := "ffffffff-1111-5222-8333-444444444444"
	task := &queue.TaskBase{
		Type: queue.TaskTypeCopyFolder,
		Folder: types.Folder{
			DisplayName:  "",
			LocationPath: "/aaaaaaaa-bbbb-5ccc-dddd-eeeeeeeeeeee/" + leaf,
		},
	}
	if got := copyTaskCreateBasename(task); got != "" {
		t.Fatalf("empty DisplayName must not fall back to id_path UUID, got %q", got)
	}
}

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

func TestCopyFileMatchesDestination(t *testing.T) {
	srcTime := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	src := types.File{Size: 1024, LastUpdated: srcTime.Format(time.RFC3339Nano)}

	tests := []struct {
		name string
		dst  types.File
		want bool
	}{
		{
			name: "same size newer destination",
			dst:  types.File{Size: 1024, LastUpdated: srcTime.Add(time.Minute).Format(time.RFC3339Nano)},
			want: true,
		},
		{
			name: "same size same timestamp",
			dst:  types.File{Size: 1024, LastUpdated: srcTime.Format(time.RFC3339Nano)},
			want: true,
		},
		{
			name: "partial destination newer timestamp",
			dst:  types.File{Size: 512, LastUpdated: srcTime.Add(time.Minute).Format(time.RFC3339Nano)},
		},
		{
			name: "empty destination newer timestamp",
			dst:  types.File{Size: 0, LastUpdated: srcTime.Add(time.Minute).Format(time.RFC3339Nano)},
		},
		{
			name: "same size older destination",
			dst:  types.File{Size: 1024, LastUpdated: srcTime.Add(-time.Minute).Format(time.RFC3339Nano)},
		},
		{
			name: "same size unparseable timestamp",
			dst:  types.File{Size: 1024, LastUpdated: "unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyFileMatchesDestination(src, tt.dst); got != tt.want {
				t.Fatalf("copyFileMatchesDestination() = %v, want %v", got, tt.want)
			}
		})
	}
}
