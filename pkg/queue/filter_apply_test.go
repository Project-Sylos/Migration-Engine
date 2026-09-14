// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestStampDisplayPathDoesNotMutateStatuses(t *testing.T) {
	state := &db.NodeState{
		ID:              "n1",
		Name:            "junk.tmp",
		Type:            db.NodeTypeFile,
		Depth:           1,
		CopyStatus:      db.CopyStatusPending,
		TraversalStatus: db.StatusPending,
	}
	StampDisplayPath("/", 0, state)
	if state.DisplayPath != "/junk.tmp" {
		t.Fatalf("display_path=%q", state.DisplayPath)
	}
	if state.CopyStatus != db.CopyStatusPending || state.TraversalStatus != db.StatusPending || state.Excluded {
		t.Fatalf("stamp mutated state: %+v", state)
	}
}
