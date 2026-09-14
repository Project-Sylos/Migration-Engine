package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestRebuildReviewStatsRepairsNegativeDeletePending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/rebuild-delete.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 10,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 20,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusFailed,
	})
	// Corrupt review counters, then rebuild from depth/node state.
	if err := database.WriteReviewStatsSnapshot(db.ReviewStatsSnapshot{DeletePending: -709, DeleteDeleted: 1, DeleteFailed: 1}); err != nil {
		t.Fatal(err)
	}

	before, err := GetReviewStatsSnapshot(database)
	if err != nil {
		t.Fatal(err)
	}
	if before.DeletePending != -709 {
		t.Fatalf("precondition delete/pending=%d want -709", before.DeletePending)
	}

	after, err := RebuildReviewStats(database)
	if err != nil {
		t.Fatal(err)
	}
	if after.DeletePending != 0 {
		t.Fatalf("rebuild delete/pending=%d want 0", after.DeletePending)
	}
	if after.DeleteDeleted != 1 || after.DeleteFailed != 1 {
		t.Fatalf("rebuild deleted/failed=%d/%d want 1/1", after.DeleteDeleted, after.DeleteFailed)
	}

	snap, err := GetReviewStatsSnapshot(database)
	if err != nil {
		t.Fatal(err)
	}
	if snap.DeletePending != 0 {
		t.Fatalf("persisted delete/pending=%d want 0", snap.DeletePending)
	}
}
