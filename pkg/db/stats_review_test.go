// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestReviewStatsSnapshot_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Path: filepath.Join(dir, "stats.duckdb")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	ctx := context.Background()
	snap := ReviewStatsSnapshot{
		TraversalPending: 10,
		TraversalFailed:  2,
		CopyPending:      5,
		CopyFailed:       1,
		Excluded:         3,
		Folders:          100,
		Files:            200,
		SizeSrc:          1000,
		SizeDst:          800,
	}
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.WriteReviewStatsSnapshot(snap)
		})
	})
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	got, err := d.GetReviewStatsSnapshot()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if got.TraversalPending != snap.TraversalPending || got.TraversalFailed != snap.TraversalFailed ||
		got.CopyPending != snap.CopyPending || got.CopyFailed != snap.CopyFailed ||
		got.Excluded != snap.Excluded || got.Folders != snap.Folders || got.Files != snap.Files ||
		got.SizeSrc != snap.SizeSrc || got.SizeDst != snap.SizeDst {
		t.Errorf("get snapshot: got %+v, want %+v", got, snap)
	}
}

func TestApplyReviewStatsDeltas(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Path: filepath.Join(dir, "stats.duckdb")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	ctx := context.Background()
	// Write initial snapshot
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.WriteReviewStatsSnapshot(ReviewStatsSnapshot{
				TraversalPending: 5,
				Excluded:         1,
			})
		})
	})
	if err != nil {
		t.Fatalf("write initial: %v", err)
	}

	// Apply deltas
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.ApplyReviewStatsDeltas([]ReviewStatsDelta{
				{Key: ReviewKeyTraversalPending, Delta: 2},
				{Key: ReviewKeyExcluded, Delta: -1},
			})
		})
	})
	if err != nil {
		t.Fatalf("apply deltas: %v", err)
	}

	got, err := d.GetReviewStatsSnapshot()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if got.TraversalPending != 7 {
		t.Errorf("TraversalPending want 7 got %d", got.TraversalPending)
	}
	if got.Excluded != 0 {
		t.Errorf("Excluded want 0 got %d", got.Excluded)
	}
}

func TestInsertRootNode_LiveReviewStatsStayBalanced(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Path: filepath.Join(dir, "root-live.duckdb")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	srcRoot := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusPending,
		CopyStatus:      CopyStatusPending,
	}
	dstRoot := &NodeState{
		ID:              DeterministicNodeID("DST", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusPending,
	}
	if err := InsertRootNode(d, "SRC", srcRoot); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	if err := InsertRootNode(d, "DST", dstRoot); err != nil {
		t.Fatalf("insert dst root: %v", err)
	}

	snap, err := d.GetPathReviewStatsFromDB()
	if err != nil {
		t.Fatalf("get snapshot after insert: %v", err)
	}
	if snap.TraversalPending != 2 {
		t.Fatalf("TraversalPending after insert want 2 got %d", snap.TraversalPending)
	}
	if snap.CopyPending != 1 {
		t.Fatalf("CopyPending after insert want 1 got %d", snap.CopyPending)
	}

	// Simulate completion: insert later status events so both roots are successful (arg_max(event_time) picks latest).
	completionTime := time.Now().UnixNano() + 1
	err = d.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.InsertStatusEvent("SRC", &StatusEvent{ID: srcRoot.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, EventTime: completionTime, Depth: 0}); err != nil {
				return err
			}
			return w.InsertStatusEvent("DST", &StatusEvent{ID: dstRoot.ID, TraversalStatus: StatusSuccessful, EventTime: completionTime, Depth: 0})
		})
	})
	if err != nil {
		t.Fatalf("insert completion events: %v", err)
	}

	snap, err = d.GetPathReviewStatsFromDB()
	if err != nil {
		t.Fatalf("get snapshot after completion: %v", err)
	}
	if snap.TraversalPending != 0 {
		t.Errorf("TraversalPending after completion want 0 got %d", snap.TraversalPending)
	}
	if snap.CopyPending != 0 {
		t.Errorf("CopyPending after completion want 0 got %d", snap.CopyPending)
	}
}

func TestResyncReviewStats(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "resync.duckdb")
	d, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	ctx := context.Background()
	eventTime := int64(1000000)
	// Seed one SRC root (successful) and one DST root (pending)
	srcRoot := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusSuccessful,
		CopyStatus:      CopyStatusPending,
	}
	dstRoot := &NodeState{
		ID:              DeterministicNodeID("DST", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusPending,
	}
	if err := InsertRootNode(d, "SRC", srcRoot); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	if err := InsertRootNode(d, "DST", dstRoot); err != nil {
		t.Fatalf("insert dst root: %v", err)
	}
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.InsertStatusEvent("SRC", &StatusEvent{ID: srcRoot.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 0}); err != nil {
				return err
			}
			return w.InsertStatusEvent("DST", &StatusEvent{ID: dstRoot.ID, TraversalStatus: StatusPending, EventTime: eventTime, Depth: 0})
		})
	})
	if err != nil {
		t.Fatalf("insert events: %v", err)
	}

	err = d.ResyncReviewStats()
	if err != nil {
		t.Fatalf("resync: %v", err)
	}

	snap, err := d.GetReviewStatsSnapshot()
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	// SRC successful, DST pending -> traversal pending 1, traversal failed 0. Merged view has one row per path (one root "/" = 1 folder).
	if snap.TraversalPending != 1 {
		t.Errorf("TraversalPending want 1 got %d", snap.TraversalPending)
	}
	if snap.TraversalFailed != 0 {
		t.Errorf("TraversalFailed want 0 got %d", snap.TraversalFailed)
	}
	if snap.Folders != 1 {
		t.Errorf("Folders want 1 got %d", snap.Folders)
	}
	if snap.CopyPending != 1 {
		t.Errorf("CopyPending want 1 (SRC root) got %d", snap.CopyPending)
	}
}

func TestGetPathReviewStatsFromDB_ExcludedAndMergedCounts(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Path: filepath.Join(dir, "snapshot.duckdb")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	srcRoot := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusPending,
		CopyStatus:      CopyStatusPending,
	}
	dstRoot := &NodeState{
		ID:              DeterministicNodeID("DST", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusPending,
	}
	if err := InsertRootNode(d, "SRC", srcRoot); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	if err := InsertRootNode(d, "DST", dstRoot); err != nil {
		t.Fatalf("insert dst root: %v", err)
	}

	snap, err := d.GetPathReviewStatsFromDB()
	if err != nil {
		t.Fatalf("GetPathReviewStatsFromDB: %v", err)
	}
	if snap.TraversalPending != 2 || snap.Excluded != 0 {
		t.Errorf("after roots: TraversalPending want 2 got %d, Excluded want 0 got %d", snap.TraversalPending, snap.Excluded)
	}
	if snap.Folders != 1 || snap.Files != 0 {
		t.Errorf("Folders want 1 Files want 0 got %d %d", snap.Folders, snap.Files)
	}

	// Exclude SRC root; snapshot should reflect one excluded path (merged view).
	err = d.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SetNodeExcluded("SRC", srcRoot.ID, true)
		})
	})
	if err != nil {
		t.Fatalf("SetNodeExcluded: %v", err)
	}
	snap, err = d.GetPathReviewStatsFromDB()
	if err != nil {
		t.Fatalf("GetPathReviewStatsFromDB after exclude: %v", err)
	}
	if snap.Excluded != 1 {
		t.Errorf("Excluded want 1 (one path excluded) got %d", snap.Excluded)
	}
	if snap.TraversalPending != 1 {
		t.Errorf("TraversalPending want 1 (DST only) got %d", snap.TraversalPending)
	}
}
