// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func seedPathSegmentTree(t *testing.T) (*db.DB, map[string]string) {
	t.Helper()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path-segments.db"})
	if err != nil {
		t.Fatal(err)
	}

	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	reports := db.MintNodeID("SRC", root, db.NodeTypeFolder, "Reports")
	q1 := db.MintNodeID("SRC", reports, db.NodeTypeFolder, "Q1")
	file := db.MintNodeID("SRC", q1, db.NodeTypeFile, "summary.txt")
	other := db.MintNodeID("SRC", root, db.NodeTypeFolder, "Other")
	revQ1 := db.MintNodeID("SRC", other, db.NodeTypeFolder, "Q1")
	revReports := db.MintNodeID("SRC", revQ1, db.NodeTypeFolder, "Reports")
	revFile := db.MintNodeID("SRC", revReports, db.NodeTypeFile, "summary.txt")

	reportsPath := db.JoinIDPath("/", reports)
	q1Path := db.JoinIDPath(reportsPath, q1)
	filePath := db.JoinIDPath(q1Path, file)
	otherPath := db.JoinIDPath("/", other)
	revQ1Path := db.JoinIDPath(otherPath, revQ1)
	revReportsPath := db.JoinIDPath(revQ1Path, revReports)
	revFilePath := db.JoinIDPath(revReportsPath, revFile)

	nodes := []*db.NodeState{
		{ID: root, Path: "/", Type: db.NodeTypeFolder, Depth: 0, Name: "/", TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: reports, ParentID: root, Path: reportsPath, ParentPath: "/", Name: "Reports", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: q1, ParentID: reports, Path: q1Path, ParentPath: reportsPath, Name: "Q1", Type: db.NodeTypeFolder, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: file, ParentID: q1, Path: filePath, ParentPath: q1Path, Name: "summary.txt", Type: db.NodeTypeFile, Depth: 3, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: other, ParentID: root, Path: otherPath, ParentPath: "/", Name: "Other", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: revQ1, ParentID: other, Path: revQ1Path, ParentPath: otherPath, Name: "Q1", Type: db.NodeTypeFolder, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: revReports, ParentID: revQ1, Path: revReportsPath, ParentPath: revQ1Path, Name: "Reports", Type: db.NodeTypeFolder, Depth: 3, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: revFile, ParentID: revReports, Path: revFilePath, ParentPath: revReportsPath, Name: "summary.txt", Type: db.NodeTypeFile, Depth: 4, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
	}
	seedReviewTree(t, database, nodes, nil, nil)

	ids := map[string]string{
		"file":    file,
		"revFile": revFile,
		"q1":      q1,
	}
	return database, ids
}

func TestPathSegmentsOrderedContains(t *testing.T) {
	database, ids := seedPathSegmentTree(t)
	defer database.Close()

	page, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		PathSegments: []string{"Reports", "Q1"},
		ExcludeRoot:  true,
	}, "path ASC", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range page {
		got[r.SrcNodeID] = true
	}
	if !got[ids["file"]] {
		t.Fatalf("want file under Reports/.../Q1, got ids %v", got)
	}
	if !got[ids["q1"]] {
		t.Fatalf("want Q1 folder under Reports, got ids %v", got)
	}
	if got[ids["revFile"]] {
		t.Fatalf("reverse order Q1/.../Reports must not match Reports then Q1; got %+v", page)
	}
}

func TestPathSegmentsSingleNameContains(t *testing.T) {
	database, ids := seedPathSegmentTree(t)
	defer database.Close()

	page, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		PathSegments: []string{"summ"},
		ExcludeRoot:  true,
	}, "path ASC", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 {
		t.Fatalf("want both summary.txt files, got %d", len(page))
	}
	got := map[string]bool{}
	for _, r := range page {
		got[r.SrcNodeID] = true
	}
	if !got[ids["file"]] || !got[ids["revFile"]] {
		t.Fatalf("want both summary files, got %v", got)
	}
}

func TestPathSegmentsNameModeUnchanged(t *testing.T) {
	database, ids := seedPathSegmentTree(t)
	defer database.Close()

	page, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		Query: "summary.txt", QueryField: "name", ExcludeRoot: true,
	}, "path ASC", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 {
		t.Fatalf("name search want 2, got %d", len(page))
	}
	_ = ids
}
