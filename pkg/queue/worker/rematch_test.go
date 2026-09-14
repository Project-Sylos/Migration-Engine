// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestRematchDSTChildrenFiles_mapsCleanedName(t *testing.T) {
	srcID := "src-bad"
	payload := db.GPLStatePayload{
		Valid: false,
		Part:  db.GPLScopeState{Valid: false, ProposedClean: "clean.txt"},
	}
	raw, _ := json.Marshal(payload)
	task := &queue.TaskBase{
		ExpectedSrcNodeMeta: map[string]queue.SrcNodeMeta{
			srcID: {GPLState: string(raw)},
		},
	}
	expected := []types.File{{
		Type: types.NodeTypeFile, DisplayName: "bad*.txt", LocationPath: "/bad*.txt",
		LastUpdated: "2020-01-01T00:00:00Z", Size: 10,
	}}
	actualMap := map[string]types.File{
		"file:clean.txt": {
			Type: types.NodeTypeFile, DisplayName: "clean.txt", LocationPath: "/clean.txt",
			LastUpdated: "2020-01-01T00:00:00Z", Size: 10,
		},
	}
	expectedMap := map[string]types.File{
		"file:bad*.txt": expected[0],
	}
	srcIDMap := map[string]string{"file:bad*.txt": srcID}

	rematchDSTChildrenFiles(task, expected, actualMap, expectedMap, srcIDMap)
	if len(task.DiscoveredChildren) != 1 {
		t.Fatalf("want 1 rematch, got %d", len(task.DiscoveredChildren))
	}
	c := task.DiscoveredChildren[0]
	if c.SrcID != srcID || !c.IsFile || c.File.DisplayName != "clean.txt" {
		t.Fatalf("unexpected child %#v", c)
	}
	if c.SrcCopyStatus != db.CopyStatusAlreadyExisted {
		t.Fatalf("copy status=%q", c.SrcCopyStatus)
	}
}
