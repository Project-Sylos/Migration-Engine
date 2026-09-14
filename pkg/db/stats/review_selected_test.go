// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestOverlayReviewSelected_copyEligibleVsPending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/overlay-copy.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{Type: db.NodeTypeFile, Depth: 1, Size: 250, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful},
		{Type: db.NodeTypeFile, Depth: 1, Size: 50, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed},
		{Type: db.NodeTypeFile, Depth: 1, Size: 999, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit},
	})

	pending, err := OverlayReviewSelected(database, ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: SelectedPending})
	if err != nil {
		t.Fatal(err)
	}
	if pending.Folders != 1 || pending.Files != 1 || pending.SelectedBytes != 100 {
		t.Fatalf("pending overlay: %+v want folders=1 files=1 bytes=100", pending)
	}

	eligible, err := OverlayReviewSelected(database, ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: SelectedEligible})
	if err != nil {
		t.Fatal(err)
	}
	if eligible.Folders != 1 || eligible.Files != 3 || eligible.SelectedBytes != 400 {
		t.Fatalf("eligible overlay: %+v want folders=1 files=3 bytes=400", eligible)
	}
}

func TestOverlayReviewSelected_deletePending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/overlay-delete.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 250, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted},
		{Type: db.NodeTypeFile, Depth: 1, Size: 50, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped},
	})

	pending, err := OverlayReviewSelected(database, ReviewSelectedSpec{Kind: db.StatsKindDelete, Population: SelectedPending})
	if err != nil {
		t.Fatal(err)
	}
	if pending.Folders != 1 || pending.Files != 1 || pending.SelectedBytes != 100 {
		t.Fatalf("delete pending overlay: %+v want folders=1 files=1 bytes=100", pending)
	}
}
