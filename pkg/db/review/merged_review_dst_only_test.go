// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestCopyStatusIndexTransitionRemovesOldKey(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-idx.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ops := database.Ops()
	id := "dst-peer-1"
	if err := ops.PutStatus(opsdb.SideDST, id, opsdb.StatusRecord{CopyStatus: db.CopyStatusDestinationOnly}); err != nil {
		t.Fatal(err)
	}
	ids, err := ops.ListIDsByCopyStatus(opsdb.SideDST, db.CopyStatusDestinationOnly, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("destination_only idx=%v", ids)
	}
	if err := ops.PutStatus(opsdb.SideDST, id, opsdb.StatusRecord{CopyStatus: db.CopyStatusNeedsOverwrite}); err != nil {
		t.Fatal(err)
	}
	ids, err = ops.ListIDsByCopyStatus(opsdb.SideDST, db.CopyStatusDestinationOnly, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("old idx key remains: %v", ids)
	}
	ids, err = ops.ListIDsByCopyStatus(opsdb.SideDST, db.CopyStatusNeedsOverwrite, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("needs_overwrite idx=%v", ids)
	}
}

func TestListMergedReviewDiffsPageNotOnSrcIncludesModernDSTOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/dst-only-not-on-src.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcNamed := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/cascade*"),
		Path: "/cascade*", ParentPath: "/", Name: "cascade*",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	// Modern destination-only: destination_only copy + successful trav, no id_map peer.
	dstOnly := &db.NodeState{
		ID:   db.DeterministicNodeID("DST", db.NodeTypeFolder, "/cascade"),
		Path: "/cascade", ParentPath: "/", Name: "cascade",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful,
		CopyStatus: db.CopyStatusDestinationOnly,
	}
	seedReviewTree(t, database, []*db.NodeState{srcNamed}, []*db.NodeState{dstOnly}, nil)

	page, hasMore, err := ListMergedReviewDiffsPageCtx(context.Background(), database, ReviewFilter{
		TraversalStatus:  db.StatusNotOnSrc,
		StatusSearchType: "traversal",
		ExcludeRoot:      true,
	}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore {
		t.Fatal("unexpected hasMore")
	}
	if len(page) != 1 {
		t.Fatalf("not_on_src rows=%d want 1 (%+v)", len(page), page)
	}
	if page[0].Path != "/cascade" || page[0].SrcNodeID != "" || page[0].DstNodeID == "" {
		t.Fatalf("want unpaired /cascade, got %+v", page[0])
	}
}

func TestListMergedReviewDiffsExcludeDestinationOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/dst-only.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcBoth := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/both.txt"),
		Path: "/both.txt", ParentPath: "/", Name: "both.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
	}
	srcOnly := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/src-only.txt"),
		Path: "/src-only.txt", ParentPath: "/", Name: "src-only.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstBoth := &db.NodeState{
		ID:   db.DeterministicNodeID("DST", db.NodeTypeFile, "/both.txt"),
		Path: "/both.txt", ParentPath: "/", Name: "both.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	dstOnly := &db.NodeState{
		ID:   db.DeterministicNodeID("DST", db.NodeTypeFile, "/dst-only.txt"),
		Path: "/dst-only.txt", ParentPath: "/", Name: "dst-only.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
		CopyStatus: db.CopyStatusDestinationOnly,
	}
	seedReviewTree(t, database, []*db.NodeState{srcBoth, srcOnly}, []*db.NodeState{dstBoth, dstOnly}, []db.IDMapEvent{{
		SrcInternalID: srcBoth.ID, DstInternalID: dstBoth.ID, Status: db.IDMapStatusActive,
	}})

	all, totalAll, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/"}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if totalAll != 3 {
		t.Fatalf("include all: total=%d want 3 (got rows=%d)", totalAll, len(all))
	}

	filtered, totalFiltered, err := ListMergedReviewDiffs(database, ReviewFilter{
		ParentPath:             "/",
		ExcludeDestinationOnly: true,
	}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if totalFiltered != 2 {
		t.Fatalf("exclude dst-only: total=%d want 2", totalFiltered)
	}
	if len(filtered) != 2 {
		t.Fatalf("exclude dst-only: rows=%d want 2", len(filtered))
	}
	for _, row := range filtered {
		if row.SrcNodeID == "" {
			t.Fatalf("unexpected destination-only row in filtered results: %+v", row)
		}
		if row.Path == "/dst-only.txt" {
			t.Fatalf("destination-only path still present: %s", row.Path)
		}
	}

	stats, err := GetMergedReviewStatsCtx(context.Background(), database, ReviewFilter{
		ParentPath:             "/",
		ExcludeDestinationOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 2 {
		t.Fatalf("stats.Total=%d want 2", stats.Total)
	}
}
