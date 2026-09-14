// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestChildResultToNodeState_usesIDPath(t *testing.T) {
	parentID := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	child := ChildResult{
		IsFile: true,
		File: types.File{
			DisplayName: "report*.txt",
			ServiceID:   "svc-1",
			ParentId:    "parent-svc",
		},
	}
	st := ChildResultToNodeState(child, "/", 1, "SRC", parentID)
	if st == nil {
		t.Fatal("nil state")
	}
	wantPath := db.JoinIDPath("/", st.ID)
	if st.Path != wantPath {
		t.Fatalf("path=%q want id_path %q", st.Path, wantPath)
	}
	if st.Name != "report*.txt" {
		t.Fatalf("name=%q", st.Name)
	}
	if st.Path == "/report*.txt" {
		t.Fatal("path must not be name-based")
	}
}
