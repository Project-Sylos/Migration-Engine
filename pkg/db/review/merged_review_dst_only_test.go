// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

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
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusNotOnSrc,
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

	stats, err := GetMergedReviewStats(database, ReviewFilter{
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
