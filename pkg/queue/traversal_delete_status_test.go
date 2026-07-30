// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestChildResultToNodeStateInitializesDeleteStatusForSrcOnly(t *testing.T) {
	tests := []struct {
		name       string
		queueType  string
		child      ChildResult
		wantCopy   string
		wantDelete string
	}{
		{
			name:      "src file",
			queueType: "SRC",
			child: ChildResult{
				File:   types.File{DisplayName: "file.txt"},
				Status: db.StatusSuccessful,
				IsFile: true,
			},
			wantCopy:   db.CopyStatusPending,
			wantDelete: db.DeleteStatusPending,
		},
		{
			name:      "src folder",
			queueType: "SRC",
			child: ChildResult{
				Folder: types.Folder{DisplayName: "folder"},
				Status: db.StatusPending,
				IsFile: false,
			},
			wantCopy:   db.CopyStatusPending,
			wantDelete: db.DeleteStatusPending,
		},
		{
			name:      "dst only file",
			queueType: "DST",
			child: ChildResult{
				File:   types.File{DisplayName: "dst-only.txt"},
				Status: db.StatusNotOnSrc,
				IsFile: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := ChildResultToNodeState(tt.child, "/", 1, tt.queueType, "parent")
			if state == nil {
				t.Fatal("expected node state")
			}
			if state.CopyStatus != tt.wantCopy {
				t.Fatalf("copy status = %q, want %q", state.CopyStatus, tt.wantCopy)
			}
			if state.DeleteStatus != tt.wantDelete {
				t.Fatalf("delete status = %q, want %q", state.DeleteStatus, tt.wantDelete)
			}
		})
	}
}
