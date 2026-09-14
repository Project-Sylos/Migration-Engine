// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func countOps(t *testing.T, database *db.DB, op string) int {
	t.Helper()
	recs, err := database.Ops().ListDBOps(op, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return len(recs)
}

func TestRecordOpFlushesToOpsStore(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/ops-flush.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	database.RecordOp(db.OpSealFlush, "", 12, 5*time.Millisecond, nil)
	database.FlushRecordedOps()
	if got := countOps(t, database, db.OpSealFlush); got != 1 {
		t.Fatalf("seal_flush rows=%d want 1", got)
	}
}

func TestRecordOpTruncatesSQL(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/ops-trunc.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	long := strings.Repeat("x", 2048+50)
	database.RecordOp(db.OpReviewQuery, long, 3, time.Millisecond, nil)
	database.FlushRecordedOps()
	recs, err := database.Ops().ListDBOps(db.OpReviewQuery, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("rows=%d want 1", len(recs))
	}
	if len(recs[0].SQL) != 2048 {
		t.Fatalf("sql_text len=%d want 2048", len(recs[0].SQL))
	}
}

func TestRecordOpRingDropsOldest(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/ring.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// Suppress flush so the in-memory ring retains samples and drops oldest past cap.
	database.AbortTraversalPhase()
	for i := 0; i < 4097; i++ {
		database.RecordOp(db.OpRunWrite, "", int64(i), time.Millisecond, nil)
	}
	if err := database.BeginTraversalPhase(context.Background()); err != nil {
		t.Fatal(err)
	}
	database.FlushRecordedOps()
	if got := countOps(t, database, db.OpRunWrite); got != 4096 {
		t.Fatalf("ring cap rows=%d want 4096", got)
	}
}

func TestHardAbortedSkipsOpsFlush(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/abort.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.AbortTraversalPhase()
	database.RecordOp(db.OpCheckpoint, "CHECKPOINT", 0, time.Millisecond, nil)
	database.FlushRecordedOps()
	if got := countOps(t, database, db.OpCheckpoint); got != 0 {
		t.Fatalf("flushed %d checkpoint rows after abort", got)
	}
}

func TestCompactionSQLPrefixParseable(t *testing.T) {
	got := db.CompactionSQLPrefix("SRC", 7)
	if !strings.HasPrefix(got, "side=SRC depth=7\n") {
		t.Fatalf("prefix=%q", got)
	}
}

func TestRecordOpDoesNotRequireDuck(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/no-duck.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.RecordOp(db.OpFrontierPull, "src", 4, 2*time.Millisecond, nil)
	database.FlushRecordedOps()
	if got := countOps(t, database, db.OpRunWrite); got != 0 {
		t.Fatalf("unexpected run_write=%d", got)
	}
	if got := countOps(t, database, db.OpFrontierPull); got != 1 {
		t.Fatalf("frontier_pull rows=%d want 1", got)
	}
}

func TestSealFlushRecordsOp(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/seal-op.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	id := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt")
	node := &db.NodeState{
		ID: id, Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	if err := database.SealLevel("SRC", 1, []*db.NodeState{node}, 0, 1, 0, 1, -1, -1, -1); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	database.FlushRecordedOps()
	recs, err := database.Ops().ListDBOps(db.OpBadgerSync, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("expected badger_sync op after seal flush")
	}
	if recs[0].Rows <= 0 || recs[0].DurationNs <= 0 {
		t.Fatalf("badger_sync rows=%d duration_ns=%d", recs[0].Rows, recs[0].DurationNs)
	}
}

func TestFrontierPullRecordsOp(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/pull-op.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	q := queue.NewQueue("src-traversal", 3, 1, nil, nil)
	q.SetDatabase(database)
	q.RecordDBPull(5, 3*time.Millisecond)
	database.FlushRecordedOps()
	recs, err := database.Ops().ListDBOps(db.OpFrontierPull, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("frontier_pull rows=%d want 1", len(recs))
	}
	if recs[0].SQL != "src-traversal" || recs[0].Rows != 5 || recs[0].DurationNs <= 0 {
		t.Fatalf("frontier_pull sql=%q rows=%d duration_ns=%d", recs[0].SQL, recs[0].Rows, recs[0].DurationNs)
	}
}
