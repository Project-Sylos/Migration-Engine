// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestChildResultToNodeStateLeavesDeleteUnsetUntilCopyComplete(t *testing.T) {
	tests := []struct {
		name      string
		queueType string
		child     ChildResult
		depth     int
		parentPath string
		wantCopy  string
	}{
		{
			name:      "src file",
			queueType: "SRC",
			child: ChildResult{
				File:   types.File{DisplayName: "file.txt"},
				Status: db.StatusSuccessful,
				IsFile: true,
			},
			depth:      1,
			parentPath: "/",
			wantCopy:   db.CopyStatusPending,
		},
		{
			name:      "src folder",
			queueType: "SRC",
			child: ChildResult{
				Folder: types.Folder{DisplayName: "folder"},
				Status: db.StatusPending,
				IsFile: false,
			},
			depth:      1,
			parentPath: "/",
			wantCopy:   db.CopyStatusPending,
		},
		{
			name:      "dst only file",
			queueType: "DST",
			child: ChildResult{
				File:   types.File{DisplayName: "dst-only.txt"},
				Status: db.StatusNotOnSrc,
				IsFile: true,
			},
			depth:      1,
			parentPath: "/",
		},
		{
			name:      "src deeper child",
			queueType: "SRC",
			child: ChildResult{
				File:   types.File{DisplayName: "nested.txt"},
				Status: db.StatusSuccessful,
				IsFile: true,
			},
			depth:      2,
			parentPath: "/folder",
			wantCopy:   db.CopyStatusPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := ChildResultToNodeState(tt.child, tt.parentPath, tt.depth, tt.queueType, "parent")
			if state == nil {
				t.Fatal("expected node state")
			}
			if state.CopyStatus != tt.wantCopy {
				t.Fatalf("copy status = %q, want %q", state.CopyStatus, tt.wantCopy)
			}
			if state.DeleteStatus != "" {
				t.Fatalf("delete status = %q, want unset until copy-complete", state.DeleteStatus)
			}
		})
	}
}
