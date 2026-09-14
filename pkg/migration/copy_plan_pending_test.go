// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestGetPathReviewStatsForView_copyPlanWhileAwaitingTraversalReview(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-plan-view.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	// Selected/Pending on copy-plan come from depth-stat pending population.
	if err := database.ApplyDepthStatsDeltas([]db.DepthStatsDelta{
		{Table: "SRC", Depth: 1, Key: db.StatsKeyTyped(db.StatsKindCopy, db.CopyStatusPending, db.NodeTypeFolder), Delta: 2},
		{Table: "SRC", Depth: 1, Key: db.StatsKeyTyped(db.StatsKindCopy, db.CopyStatusPending, db.NodeTypeFile), Delta: 1},
		{Table: "SRC", Depth: 1, Key: db.StatsKeyCopyFileBytes(db.CopyStatusPending), Delta: 40},
	}); err != nil {
		t.Fatal(err)
	}
	// Denormalized copy/pending can lag at 0 after discovery; that must not drop the field.
	if err := database.WriteReviewStatsSnapshot(db.ReviewStatsSnapshot{CopyPending: 0}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.phase = PhaseTraversalReview // engine status while copy-plan UI is open

	trav := m.GetPathReviewStatsForView("")
	if trav.PendingCount != nil {
		t.Fatalf("discover must omit pendingCount, got %d", *trav.PendingCount)
	}

	plan := m.GetPathReviewStatsForView("copy-plan")
	if plan.PendingCount == nil {
		t.Fatal("copy-plan must return pendingCount even while awaiting-traversal-review")
	}
	if *plan.PendingCount != 3 {
		t.Fatalf("copy-plan pendingCount=%d want 3 (overlay folders+files)", *plan.PendingCount)
	}
	if plan.FoldersCount != 2 || plan.FilesCount != 1 {
		t.Fatalf("copy-plan folders/files=%d/%d want 2/1", plan.FoldersCount, plan.FilesCount)
	}
}
