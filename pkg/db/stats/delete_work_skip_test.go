package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestSnapshotDeleteWorkExcludesSkipped(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-skip.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 200, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 400, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped},
	})

	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Folders != 1 || totals.Files != 2 || totals.Bytes != 300 {
		t.Fatalf("delete_work totals=%+v want folders=1 files=2 bytes=300 (skipped excluded)", totals)
	}
}

func TestSnapshotDeleteWorkAfterPriorInflatedSeal(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-shrink.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 400, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
	})
	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Files != 2 || totals.Bytes != 500 {
		t.Fatalf("pre-skip totals=%+v", totals)
	}

	// Move one file from pending_explicit to skipped in depth stats.
	if err := database.ApplyDepthStatsDeltas([]db.DepthStatsDelta{
		{Table: "SRC", Depth: 1, Key: db.StatsKeyTyped(db.StatsKindDelete, db.DeleteStatusPendingExplicit, db.NodeTypeFile), Delta: -1},
		{Table: "SRC", Depth: 1, Key: db.StatsKeyTyped(db.StatsKindDelete, db.DeleteStatusSkipped, db.NodeTypeFile), Delta: 1},
		{Table: "SRC", Depth: 1, Key: db.StatsKeyDeleteFileBytes(db.DeleteStatusPendingExplicit), Delta: -400},
		{Table: "SRC", Depth: 1, Key: db.StatsKeyDeleteFileBytes(db.DeleteStatusSkipped), Delta: 400},
	}); err != nil {
		t.Fatal(err)
	}

	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals2, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals2.Files != 1 || totals2.Bytes != 100 {
		t.Fatalf("post-skip reseal totals=%+v want files=1 bytes=100", totals2)
	}
}

func TestSnapshotDeleteWorkIgnoresNonCopyCompletePending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-noncomplete.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	// Only copy-successful pending delete counts; excluded copy with delete pending does not.
	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 900, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit, DeleteStatus: db.DeleteStatusPendingExplicit},
	})
	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Files != 1 || totals.Bytes != 100 {
		t.Fatalf("totals=%+v want only copy-complete pending (1 file, 100 bytes)", totals)
	}
}

func TestSnapshotDeleteWorkIncludesPendingInherited(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-inherited.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 2, Size: 50, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingInherited},
		{Type: db.NodeTypeFile, Depth: 2, Size: 75, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingInherited},
	})
	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Folders != 1 || totals.Files != 2 || totals.Bytes != 125 {
		t.Fatalf("delete_work totals=%+v want folders=1 files=2 bytes=125 (inherited included)", totals)
	}
}
