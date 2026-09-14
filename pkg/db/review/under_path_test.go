// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestUnderPathScopesSubtree(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/under-path.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	reports := db.MintNodeID("SRC", root, db.NodeTypeFolder, "reports")
	q1 := db.MintNodeID("SRC", reports, db.NodeTypeFolder, "q1")
	nested := db.MintNodeID("SRC", q1, db.NodeTypeFile, "summary.txt")
	other := db.MintNodeID("SRC", root, db.NodeTypeFolder, "other")
	sibling := db.MintNodeID("SRC", other, db.NodeTypeFile, "summary.txt")

	reportsPath := db.JoinIDPath("/", reports)
	q1Path := db.JoinIDPath(reportsPath, q1)
	nestedPath := db.JoinIDPath(q1Path, nested)
	otherPath := db.JoinIDPath("/", other)
	siblingPath := db.JoinIDPath(otherPath, sibling)

	nodes := []*db.NodeState{
		{
			ID: root, Path: "/", Name: "/", Type: db.NodeTypeFolder, Depth: 0,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: reports, ParentID: root, Path: reportsPath, ParentPath: "/", Name: "reports",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: q1, ParentID: reports, Path: q1Path, ParentPath: reportsPath, Name: "q1",
			Type: db.NodeTypeFolder, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: nested, ParentID: q1, Path: nestedPath, ParentPath: q1Path, Name: "summary.txt",
			Type: db.NodeTypeFile, Depth: 3, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: other, ParentID: root, Path: otherPath, ParentPath: "/", Name: "other",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: sibling, ParentID: other, Path: siblingPath, ParentPath: otherPath, Name: "summary.txt",
			Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
	}
	seedReviewTree(t, database, nodes, nil, nil)

	if !ReviewFilterHasSearchPredicate(ReviewFilter{UnderPath: reportsPath}) {
		t.Fatal("UnderPath alone must count as search predicate")
	}
	if ReviewFilterHasSearchPredicate(ReviewFilter{UnderPath: "/"}) {
		t.Fatal("root UnderPath must not count")
	}
	if StatusDrivenSearch(ReviewFilter{CopyStatus: db.CopyStatusPending, UnderPath: reportsPath}) {
		t.Fatal("UnderPath must disable status-driven fast path")
	}

	page, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{UnderPath: reportsPath}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := pathsOf(page)
	if len(got) != 3 {
		t.Fatalf("under reports got %v want folder + q1 + nested file", got)
	}
	for _, p := range got {
		if p == siblingPath || p == otherPath {
			t.Fatalf("sibling leaked into underPath results: %v", got)
		}
	}

	named, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		UnderPath: reportsPath, Query: "summary", QueryField: "name",
	}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(named) != 1 || named[0].Path != nestedPath {
		t.Fatalf("name under scope got %+v want %s", pathsOf(named), nestedPath)
	}
}
