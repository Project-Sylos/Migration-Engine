// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

func TestReviewFilterHasSearchPredicate(t *testing.T) {
	if ReviewFilterHasSearchPredicate(ReviewFilter{}) {
		t.Fatal("empty filter must not count as search predicate")
	}
	if ReviewFilterHasSearchPredicate(ReviewFilter{ParentPath: "/", ExcludeRoot: true}) {
		t.Fatal("ParentPath/ExcludeRoot alone must not count as search predicate")
	}
	if ReviewFilterHasSearchPredicate(ReviewFilter{StatusSearchType: "traversal"}) {
		t.Fatal("StatusSearchType alone must not count")
	}
	cases := []ReviewFilter{
		{Query: "foo"},
		{UnderPath: "/reports"},
		{FoldersOnly: true},
		{TypeFilter: "file"},
		{TraversalStatus: db.StatusFailed},
		{CopyStatus: db.CopyStatusPending},
		{DeleteStatus: db.DeleteStatusPendingExplicit},
		{PathIssueFilter: "issues"},
		{PathIssueCategory: "InvalidChar"},
		{DepthOperator: "=", DepthValue: intPtr(1)},
		{SizeOperator: ">", SizeValue: int64Ptr(100)},
		{ExcludeDestinationOnly: true},
		{CompiledFilter: &filter.CompiledRuleset{}},
	}
	for i, f := range cases {
		if !ReviewFilterHasSearchPredicate(f) {
			t.Fatalf("case %d: expected search predicate %+v", i, f)
		}
	}
}

func TestListMergedReviewDiffsPageHasMoreNoCount(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/merged-page-hasmore.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	nodes := []*db.NodeState{
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending,
		},
	}
	seedReviewTree(t, database, nodes, nil, nil)

	f := ReviewFilter{TraversalStatus: db.StatusFailed, StatusSearchType: "traversal", ExcludeRoot: true}
	page, hasMore, err := ListMergedReviewDiffsPage(database, f, "id ASC", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMore {
		t.Fatal("hasMore=false want true when limit+1 exists")
	}
	if len(page) != 2 {
		t.Fatalf("page len=%d want 2 (extra row trimmed)", len(page))
	}
	got := pathsOf(page)
	want := map[string]bool{"/a.txt": true, "/b.txt": true, "/c.txt": true}
	for _, p := range got {
		if !want[p] {
			t.Fatalf("unexpected path %q in page %v", p, got)
		}
	}

	last, hasMoreLast, err := ListMergedReviewDiffsPage(database, f, "id ASC", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if hasMoreLast {
		t.Fatal("hasMore=true want false on final page")
	}
	if len(last) != 1 {
		t.Fatalf("final page len=%d want 1", len(last))
	}
	if !want[last[0].Path] {
		t.Fatalf("final page path=%q", last[0].Path)
	}

	counted, total, err := ListMergedReviewDiffs(database, f, "path ASC", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(counted) != 2 {
		t.Fatalf("counted total=%d rows=%d want 3/2", total, len(counted))
	}
}

func intPtr(n int) *int       { return &n }
func int64Ptr(n int64) *int64 { return &n }
