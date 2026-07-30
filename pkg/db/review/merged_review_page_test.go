// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"testing"
	"time"
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
		{FoldersOnly: true},
		{TypeFilter: "file"},
		{TraversalStatus: db.StatusFailed},
		{CopyStatus: db.CopyStatusPending},
		{DeleteStatus: db.DeleteStatusPending},
		{PathIssueFilter: "issues"},
		{PathIssueCategory: "InvalidChar"},
		{DepthOperator: "=", DepthValue: intPtr(1)},
		{SizeOperator: ">", SizeValue: int64Ptr(100)},
		{ExcludeDestinationOnly: true},
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

	eventTime := time.Now().UnixNano()
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
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, db.StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					EventTime: eventTime, Depth: 1,
				})
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{TraversalStatus: db.StatusFailed, StatusSearchType: "traversal"}
	page, hasMore, err := ListMergedReviewDiffsPage(database, f, "path ASC", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMore {
		t.Fatal("hasMore=false want true when limit+1 exists")
	}
	if len(page) != 2 {
		t.Fatalf("page len=%d want 2 (extra row trimmed)", len(page))
	}
	if page[0].Path != "/a.txt" || page[1].Path != "/b.txt" {
		t.Fatalf("unexpected page paths: %+v", page)
	}

	last, hasMoreLast, err := ListMergedReviewDiffsPage(database, f, "path ASC", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if hasMoreLast {
		t.Fatal("hasMore=true want false on final page")
	}
	if len(last) != 1 || last[0].Path != "/c.txt" {
		t.Fatalf("final page=%+v want [/c.txt]", last)
	}

	// Counted path still available and exact.
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
