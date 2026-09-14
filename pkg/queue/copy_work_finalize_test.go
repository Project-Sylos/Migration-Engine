package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

func TestFinalizeCopyWorkOnStopDecoupled(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/stop-flush.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	insertStopFlushNode(t, database, "/a.txt", 1, 10, db.CopyStatusPending)
	insertStopFlushNode(t, database, "/b.txt", 2, 20, db.CopyStatusPending)
	insertStopFlushNode(t, database, "/c.txt", 1, 5, db.CopyStatusAlreadyExisted)

	coord := NewQueueCoordinator()
	coord.UpdateRound("src", 1) // SRC finished round 0 → depth 1 discovered
	coord.UpdateRound("dst", 1) // DST finished round 0 → depth 1 AE corrections

	if err := FinalizeCopyWorkOnStop(database, coord); err != nil {
		t.Fatal(err)
	}
	totals, err := stats.ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	// SRC sealed depth 0..1 discoveries: a(10)+c(5) at depth 1; depth 0 empty.
	// DST corrected depth 0..1 AE: c(-5).
	// Depth 2 not sealed yet (src/dst only through round 1).
	if totals.Files != 1 || totals.Bytes != 10 {
		t.Fatalf("stop flush depth<=1 net=%+v want files=1 bytes=10", totals)
	}

	coord.MarkCompleted("src")
	coord.MarkCompleted("dst")
	if err := FinalizeCopyWorkOnStop(database, coord); err != nil {
		t.Fatal(err)
	}
	totals2, err := stats.ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	// Now depth 2 pending file included; no AE there.
	if totals2.Files != 2 || totals2.Bytes != 30 {
		t.Fatalf("after both done totals=%+v want files=2 bytes=30", totals2)
	}
}

func insertStopFlushNode(t *testing.T, database *db.DB, path string, depth int, size int64, copyStatus string) {
	t.Helper()
	id := "src-file-" + path
	n := &db.NodeState{
		ID:   id,
		Path: path, ParentPath: "/", Name: path,
		Type: db.NodeTypeFile, Depth: depth, Size: size,
		TraversalStatus: db.StatusSuccessful, CopyStatus: copyStatus,
	}
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: depth, Status: db.StatusSuccessful, State: n,
	}}); err != nil {
		t.Fatal(err)
	}
}
