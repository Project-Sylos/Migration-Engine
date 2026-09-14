// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// dst_nodes.path mirrors the SRC display path even when the destination was created under a
// GPL-cleaned basename, so the name column is the only record of the real destination name.
func TestMergedReviewNameComesFromStoredColumn(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/display-name.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	src := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/Reports*Q1.txt"),
		Path: "/Reports*Q1.txt", ParentPath: "/", Name: "Reports*Q1.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstOnly := &db.NodeState{
		ID:   db.DeterministicNodeID("DST", db.NodeTypeFile, "/Leftover*.txt"),
		Path: "/Leftover*.txt", ParentPath: "/", Name: "LeftoverClean.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	seedReviewTree(t, database, []*db.NodeState{src}, []*db.NodeState{dstOnly}, nil)

	names := func(f ReviewFilter) []string {
		t.Helper()
		rows, _, err := ListMergedReviewDiffs(database, f, "path ASC", 100, 0)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			out = append(out, r.Name)
		}
		return out
	}

	all := names(ReviewFilter{ParentPath: "/"})
	if len(all) != 2 {
		t.Fatalf("want 2 rows, got %v", all)
	}

	got := names(ReviewFilter{ParentPath: "/", Query: "leftoverclean", QueryField: "name"})
	if len(got) != 1 || got[0] != "LeftoverClean.txt" {
		t.Fatalf("name search on stored dst name got %v", got)
	}
	gotPath := names(ReviewFilter{ParentPath: "/", PathSegments: []string{"leftoverclean"}})
	if len(gotPath) != 1 || gotPath[0] != "LeftoverClean.txt" {
		t.Fatalf("path segment search on stored dst name got %v", gotPath)
	}

	if got := names(ReviewFilter{ParentPath: "/", Query: "reports*q1", QueryField: "name"}); len(got) != 1 || got[0] != "Reports*Q1.txt" {
		t.Fatalf("name search on src name got %v", got)
	}
}
