// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestGetCopyStatusCountsAlreadyExistedSeparateFromSuccessful(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-already-existed.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 0, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusAlreadyExisted},
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusAlreadyExisted},
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful},
	})

	counts, err := GetCopyStatusCountsFromEvents(database)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Successful != 1 {
		t.Fatalf("Successful=%d want 1 (actual copy only)", counts.Successful)
	}
	if counts.AlreadyExisted != 2 {
		t.Fatalf("AlreadyExisted=%d want 2 (root + match)", counts.AlreadyExisted)
	}
	if counts.Pending != 1 {
		t.Fatalf("Pending=%d want 1", counts.Pending)
	}
	if counts.Complete() != 3 {
		t.Fatalf("Complete=%d want 3", counts.Complete())
	}
}
