// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"bytes"
	"context"
	"os"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestApplyTraversalResumeRestoresRoundAndCursor(t *testing.T) {
	src := queue.NewQueue("src", 3, 1, nil, nil)
	dst := queue.NewQueue("dst", 3, 1, nil, nil)
	coord := queue.NewQueueCoordinator()
	applyTraversalResume(nil, &RuntimeSuspendV1{
		Version:         1,
		Kind:            "traversal",
		LastRoundSrc:    7,
		LastRoundDst:    6,
		SrcKeysetCursor: "src-mid",
		DstKeysetCursor: "dst-mid",
	}, src, dst, coord)
	if src.GetMode() != queue.QueueModeTraversal || dst.GetMode() != queue.QueueModeTraversal {
		t.Fatalf("mode src=%s dst=%s want traversal", src.GetMode(), dst.GetMode())
	}
	if src.GetRound() != 7 || dst.GetRound() != 6 {
		t.Fatalf("round src=%d dst=%d want 7/6", src.GetRound(), dst.GetRound())
	}
	if src.GetKeysetCursor() != "src-mid" || dst.GetKeysetCursor() != "dst-mid" {
		t.Fatalf("cursor src=%q dst=%q", src.GetKeysetCursor(), dst.GetKeysetCursor())
	}
	if coord.GetRound("src") != 7 || coord.GetRound("dst") != 6 {
		t.Fatalf("coordinator src=%d dst=%d", coord.GetRound("src"), coord.GetRound("dst"))
	}
}

func TestApplyTraversalResumeEmptyCursorStartsAtFirstPending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/resume-empty-cursor.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedTraversalFolder(t, database, "SRC", "folder-a", "/a", 7, db.StatusSuccessful)
	seedTraversalFolder(t, database, "SRC", "folder-b", "/b", 7, db.StatusPending)

	src := queue.NewQueue("src", 3, 1, nil, nil)
	dst := queue.NewQueue("dst", 3, 1, nil, nil)
	applyTraversalResume(database, &RuntimeSuspendV1{
		Version:      1,
		Kind:         "traversal",
		LastRoundSrc: 7,
		LastRoundDst: 7,
	}, src, dst, nil)
	if src.GetMode() != queue.QueueModeTraversal {
		t.Fatalf("mode=%s want traversal (not retry)", src.GetMode())
	}
	if src.GetRound() != 7 {
		t.Fatalf("round=%d want 7", src.GetRound())
	}
	if src.GetKeysetCursor() != "folder-a" {
		t.Fatalf("reconstructed cursor=%q want predecessor folder-a", src.GetKeysetCursor())
	}
	batch, err := pull.ListNodesPendingAtDepthKeyset(database, "SRC", 7, src.GetKeysetCursor(), 1, db.NodeTypeFolder)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0].Key != "folder-b" {
		t.Fatalf("next pending=%v want folder-b", keysOf(batch))
	}
}

func TestApplyTraversalResumeIgnoresStaleRoundZero(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/resume-stale-zero.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedTraversalFolder(t, database, "SRC", "root", "/r", 0, db.StatusPending)
	seedTraversalFolder(t, database, "SRC", "d7-pend", "/d7", 7, db.StatusPending)

	src := queue.NewQueue("src", 3, 1, nil, nil)
	dst := queue.NewQueue("dst", 3, 1, nil, nil)
	applyTraversalResume(database, &RuntimeSuspendV1{
		Version:      1,
		Kind:         "traversal",
		LastRoundSrc: 0,
		LastRoundDst: 0,
	}, src, dst, nil)
	if src.GetRound() != 7 {
		t.Fatalf("round=%d want 7 (skip stale depth-0 pending when deeper work exists)", src.GetRound())
	}
}

func TestApplyTraversalResumeFallbackMinPendingDepth(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/resume-min-pending.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedTraversalFolder(t, database, "SRC", "d3-pend", "/d3", 3, db.StatusPending)

	src := queue.NewQueue("src", 3, 1, nil, nil)
	dst := queue.NewQueue("dst", 3, 1, nil, nil)
	applyTraversalResume(database, &RuntimeSuspendV1{Version: 1, Kind: "traversal"}, src, dst, nil)
	if src.GetMode() != queue.QueueModeTraversal {
		t.Fatalf("mode=%s want traversal", src.GetMode())
	}
	if src.GetRound() != 3 {
		t.Fatalf("round=%d want min pending 3", src.GetRound())
	}
}

func TestRunRetrySweepSourceStartsAtRoundZero(t *testing.T) {
	src, err := os.ReadFile("sweeps.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(src, []byte("srcQueue.SetMode(queue.QueueModeRetry)")) {
		t.Fatal("RunRetrySweep must stay QueueModeRetry")
	}
	if !bytes.Contains(src, []byte("srcQueue.SetRound(0)")) || !bytes.Contains(src, []byte("dstQueue.SetRound(0)")) {
		t.Fatal("RunRetrySweep must start at round 0")
	}
}

func seedTraversalFolder(t *testing.T, database *db.DB, side, id, path string, depth int, traversal string) {
	t.Helper()
	node := &db.NodeState{
		ID: id, Path: path, ParentPath: "/", Name: path[1:],
		Type: db.NodeTypeFolder, Depth: depth, TraversalStatus: traversal,
	}
	queue := "SRC"
	if side == "DST" {
		queue = "DST"
	}
	if err := database.AppendDiscoveredNodes([]db.InsertOperation{{
		QueueType: queue, Level: depth, Status: traversal, State: node,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func keysOf(batch []db.FetchResult) []string {
	out := make([]string, len(batch))
	for i, r := range batch {
		out[i] = r.Key
	}
	return out
}
