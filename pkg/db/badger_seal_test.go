// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"path/filepath"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestBadgerSealSoftCapFlushes(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-cap.db"),
		OpsDir: filepath.Join(dir, "seal-cap.ops"),
		SealBuffer: &SealBufferOptions{
			RowThreshold: 10,
			HardCap:      20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	seal.rowThreshold = 10
	seal.hardCap = 20

	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("id-%d", i)
		ops := []InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{ID: id, Depth: 0, Type: NodeTypeFile, TraversalStatus: StatusSuccessful},
		}}
		if err := seal.AddDiscoveryNodes(ops); err != nil {
			t.Fatal(err)
		}
	}
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	n, ok, err := database.Ops().GetNode(opsdb.SideSRC, "id-0")
	if err != nil || !ok {
		t.Fatalf("expected node in ops store, ok=%v err=%v", ok, err)
	}
	_ = n
}

func TestBadgerSealDiscoveryIncrReviewStats(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-stats.db"),
		OpsDir: filepath.Join(dir, "seal-stats.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	file := &NodeState{
		ID: "file-1", Depth: 1, Type: NodeTypeFile, Size: 42,
		CopyStatus: CopyStatusPending, TraversalStatus: StatusSuccessful,
	}
	folder := &NodeState{
		ID: "folder-1", Depth: 1, Type: NodeTypeFolder,
		CopyStatus: CopyStatusPending, TraversalStatus: StatusSuccessful,
	}
	dst := &NodeState{
		ID: "dst-file", Depth: 1, Type: NodeTypeFile, Size: 9,
		TraversalStatus: StatusSuccessful,
	}
	if err := seal.AddDiscoveryNodes([]InsertOperation{
		{QueueType: "SRC", Level: 1, Status: StatusSuccessful, State: file},
		{QueueType: "SRC", Level: 1, Status: StatusSuccessful, State: folder},
		{QueueType: "DST", Level: 1, Status: StatusSuccessful, State: dst},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}

	assertStat := func(key string, want int64) {
		t.Helper()
		got, err := database.Ops().GetStat(key)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s=%d want %d", key, got, want)
		}
	}
	assertStat(ReviewKeyFiles, 1)
	assertStat(ReviewKeyFolders, 1)
	assertStat(ReviewKeyCopyPending, 2)
	assertStat(ReviewKeySizeSrc, 42)
	assertStat(ReviewKeySizeSelected, 42)
	assertStat(ReviewKeySizeDst, 9)

	seal.AddDiscoveryStatusEvent("SRC", StatusEvent{
		ID:                  file.ID,
		Depth:               1,
		NodeType:            NodeTypeFile,
		Size:                42,
		TraversalStatus:     StatusSuccessful,
		PrevTraversalStatus: StatusSuccessful,
		CopyStatus:          CopyStatusAlreadyExisted,
		PrevCopyStatus:      CopyStatusPending,
	}, false)
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	assertStat(ReviewKeyCopyPending, 1)
	assertStat(ReviewKeyCopySuccessful, 1)
	assertStat(ReviewKeyFiles, 0)
	assertStat(ReviewKeyFolders, 1)
	assertStat(ReviewKeySizeSelected, 0)
	assertStat(ReviewKeySizeSrc, 42)
}

func TestBadgerSealRediscoveryDoesNotDoubleCount(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-rediscover.db"),
		OpsDir: filepath.Join(dir, "seal-rediscover.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	child := &NodeState{
		ID: "child-1", Depth: 2, Type: NodeTypeFolder,
		CopyStatus: CopyStatusPending, TraversalStatus: StatusSuccessful,
	}
	if err := seal.AddDiscoveryNodes([]InsertOperation{
		{QueueType: "SRC", Level: 2, Status: StatusSuccessful, State: child},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := database.Ops().GetStat(ReviewKeyCopyPending)
	if err != nil || got != 1 {
		t.Fatalf("copy/pending=%d err=%v want 1", got, err)
	}

	// Retry re-lists the same child: must not inflate copy/pending or overwrite status.
	again := &NodeState{
		ID: "child-1", Depth: 2, Type: NodeTypeFolder,
		CopyStatus: CopyStatusPending, TraversalStatus: StatusPending,
	}
	if err := seal.AddDiscoveryNodes([]InsertOperation{
		{QueueType: "SRC", Level: 2, Status: StatusPending, State: again},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err = database.Ops().GetStat(ReviewKeyCopyPending)
	if err != nil || got != 1 {
		t.Fatalf("after rediscovery copy/pending=%d err=%v want 1", got, err)
	}
	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, "child-1")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if st.TraversalStatus != StatusSuccessful {
		t.Fatalf("sealed traversal=%q want successful (rediscovery must not overwrite)", st.TraversalStatus)
	}
}

func TestBadgerSealSameBatchDuplicateDiscoveryDoesNotDoubleCount(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-dup.db"),
		OpsDir: filepath.Join(dir, "seal-dup.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	child := &NodeState{
		ID: "child-dup", Depth: 2, Type: NodeTypeFile, Size: 7,
		CopyStatus: CopyStatusPending, TraversalStatus: StatusSuccessful,
	}
	// Same id twice in one flush (duplicate list rows / concurrent parents).
	if err := seal.AddDiscoveryNodes([]InsertOperation{
		{QueueType: "SRC", Level: 2, Status: StatusSuccessful, State: child},
		{QueueType: "SRC", Level: 2, Status: StatusSuccessful, State: child},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := database.Ops().GetStat(ReviewKeyCopyPending)
	if err != nil || got != 1 {
		t.Fatalf("copy/pending=%d err=%v want 1", got, err)
	}
	files, err := database.Ops().GetStat(ReviewKeyFiles)
	if err != nil || files != 1 {
		t.Fatalf("files=%d err=%v want 1", files, err)
	}
}

func TestBadgerSealTraversalFailKeepsCopyPendingCounter(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-trav-fail.db"),
		OpsDir: filepath.Join(dir, "seal-trav-fail.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	id := "folder-fail"
	if err := database.AppendDiscoveredNodes([]InsertOperation{{
		QueueType: "SRC", Level: 1, Status: StatusPending,
		State: &NodeState{
			ID: id, Depth: 1, Type: NodeTypeFolder,
			TraversalStatus: StatusPending, CopyStatus: CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !database.AppendStatusEvent("SRC", StatusEvent{
		ID: id, TraversalStatus: StatusFailed, PrevTraversalStatus: StatusPending,
		EventTime: 1, Depth: 1, NodeType: NodeTypeFolder,
	}, false) {
		t.Fatal("AppendStatusEvent rejected")
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	pending, err := database.Ops().GetStat(ReviewKeyCopyPending)
	if err != nil || pending != 1 {
		t.Fatalf("copy/pending=%d err=%v want 1 (fail must not park copy)", pending, err)
	}
	failed, err := database.Ops().GetStat(ReviewKeyTraversalFailed)
	if err != nil || failed != 1 {
		t.Fatalf("traversal/failed=%d err=%v want 1", failed, err)
	}
	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, id)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if st.CopyStatus != CopyStatusPending || st.TraversalStatus != StatusFailed {
		t.Fatalf("status copy=%q trav=%q want pending/failed", st.CopyStatus, st.TraversalStatus)
	}
}

func TestBadgerSealCopyFailClearsPending(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-copy-fail.db"),
		OpsDir: filepath.Join(dir, "seal-copy-fail.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	id := "file-fail"
	if err := database.AppendDiscoveredNodes([]InsertOperation{{
		QueueType: "SRC", Level: 1, Status: StatusSuccessful,
		State: &NodeState{
			ID: id, Depth: 1, Type: NodeTypeFile, Size: 10,
			TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, err := database.Ops().GetStat(ReviewKeyCopyPending)
	if err != nil || got != 1 {
		t.Fatalf("copy/pending=%d err=%v want 1", got, err)
	}

	// Copy events must omit TraversalStatus so merge keeps discovery state and
	// review deltas only move the copy counters.
	if !database.AppendStatusEvent("SRC", StatusEvent{
		ID: id, CopyStatus: CopyStatusFailed, PrevCopyStatus: CopyStatusPending,
		EventTime: 1, Depth: 1, NodeType: NodeTypeFile, Size: 10,
	}, false) {
		t.Fatal("AppendStatusEvent rejected")
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	pending, err := database.Ops().GetStat(ReviewKeyCopyPending)
	if err != nil || pending != 0 {
		t.Fatalf("after fail copy/pending=%d err=%v want 0", pending, err)
	}
	failed, err := database.Ops().GetStat(ReviewKeyCopyFailed)
	if err != nil || failed != 1 {
		t.Fatalf("after fail copy/failed=%d err=%v want 1", failed, err)
	}
	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, id)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if st.CopyStatus != CopyStatusFailed || st.TraversalStatus != StatusSuccessful {
		t.Fatalf("status copy=%q trav=%q want failed/successful", st.CopyStatus, st.TraversalStatus)
	}
}

func TestBadgerSealDiscoveryPendingCoupled(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "seal-pend.db"),
		OpsDir: filepath.Join(dir, "seal-pend.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	child := &NodeState{
		ID: "folder-pending", Depth: 3, Type: NodeTypeFolder,
		TraversalStatus: StatusPending, CopyStatus: CopyStatusPending,
	}
	if err := database.AppendDiscoveredNodes([]InsertOperation{
		{QueueType: "SRC", Level: 3, Status: StatusPending, State: child},
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	ops := database.Ops()
	st, ok, err := ops.GetStatus(opsdb.SideSRC, child.ID)
	if err != nil || !ok || st.TraversalStatus != StatusPending {
		t.Fatalf("status %+v ok=%v err=%v", st, ok, err)
	}
	ids, err := ops.ListSchedAtDepth(opsdb.SideSRC, opsdb.PhaseTrav, 3, NodeTypeFolder, "", 10)
	if err != nil || len(ids) != 1 || ids[0] != child.ID {
		t.Fatalf("trav pend %+v err=%v", ids, err)
	}
	copyIDs, err := ops.ListSchedAtDepth(opsdb.SideSRC, opsdb.PhaseCopy, 3, NodeTypeFolder, "", 10)
	if err != nil || len(copyIDs) != 1 || copyIDs[0] != child.ID {
		t.Fatalf("copy pend %+v err=%v", copyIDs, err)
	}
	n, err := ops.GetSchedCountAtDepth(opsdb.SideSRC, opsdb.PhaseTrav, 3, NodeTypeFolder)
	if err != nil || n != 1 {
		t.Fatalf("schedcnt trav=%d err=%v", n, err)
	}
}

func TestAlreadyExistedDropsCopyFrontier(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/ae-copy-pend.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	child := &NodeState{
		ID: "folder-ae", Depth: 5, Type: NodeTypeFolder,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	if err := database.AppendDiscoveredNodes([]InsertOperation{
		{QueueType: "SRC", Level: 5, Status: StatusSuccessful, State: child},
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	database.AppendStatusEvent("SRC", StatusEvent{
		ID:             child.ID,
		CopyStatus:     CopyStatusAlreadyExisted,
		PrevCopyStatus: CopyStatusPending,
		EventTime:      1,
		Depth:          5,
		NodeType:       NodeTypeFolder,
	}, false)
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	ops := database.Ops()
	st, ok, err := ops.GetStatus(opsdb.SideSRC, child.ID)
	if err != nil || !ok || st.CopyStatus != CopyStatusAlreadyExisted {
		t.Fatalf("copy status %+v ok=%v err=%v", st, ok, err)
	}
	copyIDs, err := ops.ListSchedAtDepth(opsdb.SideSRC, opsdb.PhaseCopy, 5, NodeTypeFolder, "", 10)
	if err != nil || len(copyIDs) != 0 {
		t.Fatalf("copy pend after already_existed %+v err=%v", copyIDs, err)
	}
	n, err := ops.GetSchedCountAtDepth(opsdb.SideSRC, opsdb.PhaseCopy, 5, NodeTypeFolder)
	if err != nil || n != 0 {
		t.Fatalf("copy schedcnt=%d want 0 err=%v", n, err)
	}
}

func TestAlreadyExistedDropsCopyFrontierWhenPrevStale(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/ae-copy-stale.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	child := &NodeState{
		ID: "folder-stale", Depth: 5, Type: NodeTypeFolder,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	if err := database.AppendDiscoveredNodes([]InsertOperation{
		{QueueType: "SRC", Level: 5, Status: StatusSuccessful, State: child},
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	database.AppendStatusEvent("SRC", StatusEvent{
		ID:             child.ID,
		CopyStatus:     CopyStatusAlreadyExisted,
		PrevCopyStatus: CopyStatusAlreadyExisted,
		EventTime:      1,
		Depth:          5,
		NodeType:       NodeTypeFolder,
	}, false)
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	ops := database.Ops()
	copyIDs, err := ops.ListSchedAtDepth(opsdb.SideSRC, opsdb.PhaseCopy, 5, NodeTypeFolder, "", 10)
	if err != nil || len(copyIDs) != 0 {
		t.Fatalf("copy pend after stale prev %+v err=%v", copyIDs, err)
	}
}

func TestSealWritesNodesAndIDMapToOps(t *testing.T) {
	database := TestOpen(t, "catalog-idmap")
	if err := database.BeginTraversalPhase(t.Context()); err != nil {
		t.Fatal(err)
	}
	src := &NodeState{
		ID: "src-a", Path: "/a", Name: "a", Type: NodeTypeFolder, Depth: 1,
		TraversalStatus: StatusSuccessful,
	}
	dst := &NodeState{
		ID: "dst-a", Path: "/a", Name: "a", Type: NodeTypeFolder, Depth: 1,
		TraversalStatus: StatusPending,
	}
	if err := database.AppendDiscoveredNodes([]InsertOperation{
		{QueueType: "SRC", Level: 1, Status: StatusSuccessful, State: src},
		{QueueType: "DST", Level: 1, Status: StatusPending, State: dst},
	}); err != nil {
		t.Fatal(err)
	}
	database.AppendIDMapEvent(IDMapEvent{
		SrcInternalID: src.ID, DstInternalID: dst.ID,
		Status: IDMapStatusActive, Depth: 1,
	})
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	ops := database.Ops()
	n, ok, err := ops.GetNode(opsdb.SideSRC, src.ID)
	if err != nil || !ok || n.Path != "/a" {
		t.Fatalf("src node %+v ok=%v err=%v", n, ok, err)
	}
	m, ok, err := ops.GetMapBySrc(src.ID)
	if err != nil || !ok || m.DstID != dst.ID {
		t.Fatalf("id map %+v ok=%v err=%v", m, ok, err)
	}
	id, err := ops.GetNodeIDByPath(opsdb.SideSRC, "/a")
	if err != nil || id != src.ID {
		t.Fatalf("path index id=%q err=%v", id, err)
	}
}
