// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMergedReviewQueryLayer(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "review_test.duckdb")
	d, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		d.Close()
		_ = os.Remove(dbPath)
	}()

	ctx := context.Background()
	eventTime := time.Now().UnixNano()

	// Seed SRC and DST roots and one child each (path /a)
	srcRoot := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusSuccessful,
		CopyStatus:      CopyStatusSuccessful,
	}
	dstRoot := &NodeState{
		ID:              DeterministicNodeID("DST", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusSuccessful,
	}
	if err := InsertRootNode(d, "SRC", srcRoot); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	if err := InsertRootNode(d, "DST", dstRoot); err != nil {
		t.Fatalf("insert dst root: %v", err)
	}

	srcChild := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/a"),
		Path:            "/a",
		ParentPath:      "/",
		Type:            NodeTypeFolder,
		Depth:           1,
		TraversalStatus: StatusSuccessful,
		CopyStatus:      CopyStatusPending,
	}
	dstChild := &NodeState{
		ID:              DeterministicNodeID("DST", NodeTypeFolder, "/a"),
		Path:            "/a",
		ParentPath:      "/",
		Type:            NodeTypeFolder,
		Depth:           1,
		TraversalStatus: StatusSuccessful,
	}
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppenderInsert(tableSrcNodes, []*NodeState{srcChild}); err != nil {
				return err
			}
			if err := w.AppenderInsert(tableDstNodes, []*NodeState{dstChild}); err != nil {
				return err
			}
			ev := &StatusEvent{ID: srcChild.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1}
			if err := w.InsertStatusEvent("SRC", ev); err != nil {
				return err
			}
			evDst := &StatusEvent{ID: dstChild.ID, TraversalStatus: StatusSuccessful, EventTime: eventTime, Depth: 1}
			return w.InsertStatusEvent("DST", evDst)
		})
	})
	if err != nil {
		t.Fatalf("insert children: %v", err)
	}

	// List children of root: expect 1 row (/a)
	rows, total, err := ListMergedReviewDiffs(d, ReviewFilter{ParentPath: "/"}, "path ASC", 10, 0)
	if err != nil {
		t.Fatalf("ListMergedReviewDiffs: %v", err)
	}
	if total != 1 {
		t.Errorf("total want 1 got %d", total)
	}
	if len(rows) != 1 {
		t.Fatalf("rows want 1 got %d", len(rows))
	}
	if rows[0].Path != "/a" || rows[0].SrcNodeID == "" || rows[0].DstNodeID == "" {
		t.Errorf("row: path=%q src=%q dst=%q", rows[0].Path, rows[0].SrcNodeID, rows[0].DstNodeID)
	}

	// Count same filter
	n, err := CountMergedReviewRows(d, ReviewFilter{ParentPath: "/"})
	if err != nil {
		t.Fatalf("CountMergedReviewRows: %v", err)
	}
	if n != 1 {
		t.Errorf("CountMergedReviewRows want 1 got %d", n)
	}

	// Stats for same filter
	stats, err := GetMergedReviewStats(d, ReviewFilter{ParentPath: "/"})
	if err != nil {
		t.Fatalf("GetMergedReviewStats: %v", err)
	}
	if stats.Total != 1 || stats.Folders != 1 {
		t.Errorf("stats: total=%d folders=%d", stats.Total, stats.Folders)
	}

	// Global "search" (no parent path): exclude root, so we get /a
	rows2, total2, err := ListMergedReviewDiffs(d, ReviewFilter{ExcludeRoot: true}, "path ASC", 10, 0)
	if err != nil {
		t.Fatalf("ListMergedReviewDiffs global: %v", err)
	}
	if total2 != 1 {
		t.Errorf("global total want 1 got %d", total2)
	}
	if len(rows2) != 1 || rows2[0].Path != "/a" {
		t.Errorf("global row: got %d rows path=%q", len(rows2), rows2[0].Path)
	}

	// Query filter: path/name contains "a"
	rows3, total3, err := ListMergedReviewDiffs(d, ReviewFilter{Query: "a", ExcludeRoot: true}, "path ASC", 10, 0)
	if err != nil {
		t.Fatalf("ListMergedReviewDiffs query: %v", err)
	}
	if total3 != 1 || len(rows3) != 1 {
		t.Errorf("query total=%d len=%d", total3, len(rows3))
	}

	// FoldersOnly filter
	statsFolders, err := GetMergedReviewStats(d, ReviewFilter{ParentPath: "/", FoldersOnly: true})
	if err != nil {
		t.Fatalf("GetMergedReviewStats folders: %v", err)
	}
	if statsFolders.Total != 1 || statsFolders.Folders != 1 {
		t.Errorf("folders stats: total=%d folders=%d", statsFolders.Total, statsFolders.Folders)
	}
}

func TestUnexcludeRestoresSuccessfulAndCopyPending(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "unexclude_test.duckdb")
	d, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	srcRoot := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusSuccessful,
		CopyStatus:      CopyStatusSuccessful,
	}
	if err := InsertRootNode(d, "SRC", srcRoot); err != nil {
		t.Fatalf("insert src root: %v", err)
	}

	// Exclude the node
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SetNodeExcluded("SRC", srcRoot.ID, true)
		})
	})
	if err != nil {
		t.Fatalf("set excluded true: %v", err)
	}
	node, err := GetNodeByID(d, "SRC", srcRoot.ID)
	if err != nil || node == nil {
		t.Fatalf("get node: %v", err)
	}
	if !node.Excluded {
		t.Error("expected node excluded after SetNodeExcluded(true)")
	}

	// Unexclude: should restore traversal=successful, copy=pending
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SetNodeExcluded("SRC", srcRoot.ID, false)
		})
	})
	if err != nil {
		t.Fatalf("set excluded false: %v", err)
	}
	node, err = GetNodeByID(d, "SRC", srcRoot.ID)
	if err != nil || node == nil {
		t.Fatalf("get node after unexclude: %v", err)
	}
	if node.Excluded {
		t.Error("expected node not excluded after SetNodeExcluded(false)")
	}
	if node.TraversalStatus != StatusSuccessful {
		t.Errorf("traversal_status want %q got %q", StatusSuccessful, node.TraversalStatus)
	}
	if node.CopyStatus != CopyStatusPending {
		t.Errorf("copy_status want %q got %q", CopyStatusPending, node.CopyStatus)
	}
}

func TestInsertUnexcludeEventsForSubtree(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "unexclude_subtree_test.duckdb")
	d, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer d.Close()

	eventTime := time.Now().UnixNano()
	srcRoot := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            NodeTypeFolder,
		Depth:           0,
		TraversalStatus: StatusExcluded,
		CopyStatus:      CopyStatusPending,
	}
	if err := InsertRootNode(d, "SRC", srcRoot); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	srcChild := &NodeState{
		ID:              DeterministicNodeID("SRC", NodeTypeFile, "/f"),
		Path:            "/f",
		ParentPath:      "/",
		Type:            NodeTypeFile,
		Depth:           1,
		TraversalStatus: StatusExclusionInherited,
		CopyStatus:      CopyStatusPending,
	}
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppenderInsert(tableSrcNodes, []*NodeState{srcChild}); err != nil {
				return err
			}
			ev := &StatusEvent{ID: srcChild.ID, TraversalStatus: StatusExclusionInherited, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1}
			return w.InsertStatusEvent("SRC", ev)
		})
	})
	if err != nil {
		t.Fatalf("insert child: %v", err)
	}

	// Propagate unexclude to subtree
	err = d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.InsertUnexcludeEventsForSubtree("SRC", "/")
		})
	})
	if err != nil {
		t.Fatalf("InsertUnexcludeEventsForSubtree: %v", err)
	}

	root, _ := GetNodeByID(d, "SRC", srcRoot.ID)
	child, _ := GetNodeByID(d, "SRC", srcChild.ID)
	if root != nil && root.TraversalStatus != StatusSuccessful {
		t.Errorf("root traversal_status want successful got %q", root.TraversalStatus)
	}
	if root != nil && root.CopyStatus != CopyStatusPending {
		t.Errorf("root copy_status want pending got %q", root.CopyStatus)
	}
	if child != nil && child.TraversalStatus != StatusSuccessful {
		t.Errorf("child traversal_status want successful got %q", child.TraversalStatus)
	}
	if child != nil && child.CopyStatus != CopyStatusPending {
		t.Errorf("child copy_status want pending got %q", child.CopyStatus)
	}
}
