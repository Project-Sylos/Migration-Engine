// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestRebuildCurrentMaterializes(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/current-materialize.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idA := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt")
	seedReviewTree(t, database, []*db.NodeState{{
		ID: idA, Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}}, nil, nil)

	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, idA)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing ops status")
	}
	if st.CopyStatus != db.CopyStatusPending {
		t.Fatalf("copy=%q want %s", st.CopyStatus, db.CopyStatusPending)
	}
}

func TestExcludeSubtreeVisibleInMergedReviewAfterRefresh(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/exclude-refresh.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	childID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/a.txt")
	seedReviewTree(t, database, []*db.NodeState{
		{
			ID: rootID, Path: "/folder", ParentPath: "/", Name: "folder", Type: db.NodeTypeFolder, Depth: 1,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit,
		},
		{
			ID: childID, Path: "/folder/a.txt", ParentPath: "/folder", ParentID: rootID, Name: "a.txt", Type: db.NodeTypeFile, Depth: 2,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedInherited,
		},
	}, nil, nil)

	rows, total, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/"}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total < 1 {
		t.Fatalf("expected folder row, total=%d", total)
	}
	found := false
	for _, r := range rows {
		if r.Path == "/folder" {
			found = true
			if !r.Excluded && r.CopyStatus != db.CopyStatusExcludedExplicit {
				t.Fatalf("folder not excluded after exclude+refresh: copy=%q excluded=%v", r.CopyStatus, r.Excluded)
			}
		}
	}
	if !found {
		t.Fatalf("folder missing from diffs: %+v", rows)
	}

	childRows, _, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/folder"}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range childRows {
		if r.Path == "/folder/a.txt" && !r.Excluded && r.CopyStatus != db.CopyStatusExcludedInherited {
			t.Fatalf("child not excluded after exclude+refresh: copy=%q", r.CopyStatus)
		}
	}
}

func TestMergedReviewReadsSrcCurrent(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/review-current.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	id := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt")
	seedReviewTree(t, database, []*db.NodeState{{
		ID: id, Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
	}}, nil, nil)

	rows, total, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/"}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("total=%d rows=%d", total, len(rows))
	}
	if rows[0].CopyStatus != db.CopyStatusSuccessful || rows[0].SrcTraversalStatus != db.StatusSuccessful {
		t.Fatalf("row=%+v", rows[0])
	}
}
