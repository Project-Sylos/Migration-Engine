// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestGetCopyProgressCountsFromStats(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-progress.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.WriteReviewStatsSnapshot(db.ReviewStatsSnapshot{
		CopyPending:    25,
		CopySuccessful: 70,
		CopyFailed:     5,
	}); err != nil {
		t.Fatal(err)
	}

	counts, err := GetPhaseProgressCountsFromStats(database, db.StatsKindCopy)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 25 || counts.Successful != 70 || counts.Failed != 5 {
		t.Fatalf("counts=%+v", counts)
	}
	if got := db.DeterministicProgressPercent(counts.Pending, counts.Successful, counts.Failed, false); got != 75 {
		t.Fatalf("progress=%v want 75", got)
	}
}

func TestGetDeleteProgressCountsEligibleOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/delete-progress.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted},
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusFailed},
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped},
		{Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, DeleteStatus: db.DeleteStatusPendingExplicit},
	})

	counts, err := GetDeleteProgressCounts(database)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 1 || counts.Successful != 1 || counts.Failed != 1 {
		t.Fatalf("counts=%+v want pending=1 successful=1 failed=1 (skipped/not-copied excluded)", counts)
	}
	if got := db.DeterministicProgressPercent(counts.Pending, counts.Successful, counts.Failed, false); got != (100.0*2)/3 {
		t.Fatalf("progress=%v want %v", got, (100.0*2)/3)
	}

	deleteCounts, err := GetEligibleDeleteStatusCounts(database)
	if err != nil {
		t.Fatal(err)
	}
	if deleteCounts.Pending != 1 || deleteCounts.Deleted != 1 ||
		deleteCounts.Failed != 1 || deleteCounts.Skipped != 1 {
		t.Fatalf("eligible delete counts=%+v want one in each tracked status", deleteCounts)
	}
}
