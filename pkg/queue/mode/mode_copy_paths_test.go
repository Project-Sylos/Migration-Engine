// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

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
	task := nodeStateToCopyTask(state, queue.TaskTypeCopyFolder, 1)
	if task.SrcLogicalPath != "/Interview Prep/Beyond Feeback" {
		t.Fatalf("SrcLogicalPath = %q", task.SrcLogicalPath)
	}
	if task.SrcLogicalParentPath != "/Interview Prep" {
		t.Fatalf("SrcLogicalParentPath = %q", task.SrcLogicalParentPath)
	}
}
