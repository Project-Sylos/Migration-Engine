// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"strconv"
	"testing"
	"time"
)

func TestListMergedReviewDiffsPageZipperIncludesDSTOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-dst-only.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	src := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/shared.txt"), Path: "/shared.txt", ParentPath: "/", Name: "shared.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstMapped := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/shared.txt"), Path: "/shared.txt", ParentPath: "/", Name: "shared.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	dstOnly := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/orphan.txt"), Path: "/orphan.txt", ParentPath: "/", Name: "orphan.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{src}); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, []*db.NodeState{dstMapped, dstOnly}); err != nil {
				return err
			}
			if err := w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: src.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
			}); err != nil {
				return err
			}
			if err := w.BatchInsertDstStatusEvents([]db.StatusEvent{
				{ID: dstMapped.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1},
				{ID: dstOnly.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertIDMapEvents([]db.IDMapEvent{{
				SrcInternalID: src.ID,
				DstInternalID: dstMapped.ID,
				EventTime:     eventTime,
				Status:        db.IDMapStatusActive,
			}})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	page, hasMore, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		Query: "txt", QueryField: "name", ExcludeRoot: true,
	}, "path ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore {
		t.Fatal("unexpected hasMore")
	}
	if len(page) != 2 {
		t.Fatalf("want 2 rows (paired + dst-only), got %d: %+v", len(page), pathsOf(page))
	}
	if page[0].Path != "/orphan.txt" || page[0].SrcNodeID != "" || page[0].DstNodeID != dstOnly.ID {
		t.Fatalf("first row want dst-only orphan, got %+v", page[0])
	}
	if page[1].Path != "/shared.txt" || page[1].SrcNodeID != src.ID || page[1].DstNodeID != dstMapped.ID {
		t.Fatalf("second row want paired shared, got %+v", page[1])
	}
}

func TestListMergedReviewDiffsPageZipperHasMoreAndOffset(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-page.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	srcNodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt", Type: db.NodeTypeFile, Depth: 1},
	}
	dstOnly := []*db.NodeState{
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt", Type: db.NodeTypeFile, Depth: 1},
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/d.txt"), Path: "/d.txt", ParentPath: "/", Name: "d.txt", Type: db.NodeTypeFile, Depth: 1},
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, srcNodes); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, dstOnly); err != nil {
				return err
			}
			var srcEv []db.StatusEvent
			for _, n := range srcNodes {
				srcEv = append(srcEv, db.StatusEvent{ID: n.ID, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1})
			}
			if err := w.BatchInsertSrcStatusEvents(srcEv); err != nil {
				return err
			}
			var dstEv []db.StatusEvent
			for _, n := range dstOnly {
				dstEv = append(dstEv, db.StatusEvent{ID: n.ID, TraversalStatus: db.StatusFailed, EventTime: eventTime, Depth: 1})
			}
			return w.BatchInsertDstStatusEvents(dstEv)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{TraversalStatus: db.StatusFailed, StatusSearchType: "traversal", ExcludeRoot: true}
	page, hasMore, err := ListMergedReviewDiffsPage(database, f, "path ASC", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMore {
		t.Fatal("hasMore=false want true")
	}
	if got := pathsOf(page); len(got) != 2 || got[0] != "/a.txt" || got[1] != "/b.txt" {
		t.Fatalf("page1=%v want [/a.txt /b.txt]", got)
	}

	page2, hasMore2, err := ListMergedReviewDiffsPage(database, f, "path ASC", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore2 {
		t.Fatal("hasMore on final page")
	}
	if got := pathsOf(page2); len(got) != 2 || got[0] != "/c.txt" || got[1] != "/d.txt" {
		t.Fatalf("page2=%v want [/c.txt /d.txt]", got)
	}
}

func TestListMergedReviewDiffsPageSkipsDSTOnlyForCopyFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-copy.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	src := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/pending.txt"), Path: "/pending.txt", ParentPath: "/", Name: "pending.txt",
		Type: db.NodeTypeFile, Depth: 1,
	}
	dstOnly := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/ghost.txt"), Path: "/ghost.txt", ParentPath: "/", Name: "ghost.txt",
		Type: db.NodeTypeFile, Depth: 1,
	}
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{src}); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, []*db.NodeState{dstOnly}); err != nil {
				return err
			}
			if err := w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: src.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertDstStatusEvents([]db.StatusEvent{
				{ID: dstOnly.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	page, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		CopyStatus: db.CopyStatusPending, StatusSearchType: "copy", ExcludeRoot: true,
	}, "path ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Path != "/pending.txt" {
		t.Fatalf("copy filter should be SRC-only, got %+v", pathsOf(page))
	}
}

func TestReviewSearchIncludeDSTOnly(t *testing.T) {
	if !reviewSearchIncludeDSTOnly(ReviewFilter{Query: "x"}) {
		t.Fatal("default query should include DST-only")
	}
	if reviewSearchIncludeDSTOnly(ReviewFilter{ExcludeDestinationOnly: true, Query: "x"}) {
		t.Fatal("ExcludeDestinationOnly should skip DST-only")
	}
	if reviewSearchIncludeDSTOnly(ReviewFilter{PathIssueFilter: "issues"}) {
		t.Fatal("path issues are SRC-native")
	}
	if reviewSearchIncludeDSTOnly(ReviewFilter{CopyStatus: db.CopyStatusPending}) {
		t.Fatal("copy filter is SRC-native")
	}
	if !reviewSearchIncludeDSTOnly(ReviewFilter{TraversalStatus: db.StatusFailed, StatusSearchType: "traversal"}) {
		t.Fatal("traversal filter should still scan DST-only")
	}
}

// sanitizeSort emits "<col> <dir>, path ASC" for every non-path column, so the primary
// direction has to survive the trailing secondary key.
func TestParseReviewOrderBy(t *testing.T) {
	cases := []struct {
		orderBy string
		col     string
		desc    bool
	}{
		{"path ASC", "path", false},
		{"path DESC", "path", true},
		{"name DESC, path ASC", "name", true},
		{"name ASC, path ASC", "name", false},
		{"size DESC, path ASC", "size", true},
		{"src_traversal_status ASC, path ASC", "src_traversal_status", false},
		{"copy_status DESC, path ASC", "copy_status", true},
		{"", "path", false},
		{"bogus DESC, path ASC", "path", true},
	}
	for _, tc := range cases {
		col, desc := parseReviewOrderBy(tc.orderBy)
		if col != tc.col || desc != tc.desc {
			t.Fatalf("%q -> (%q,%v) want (%q,%v)", tc.orderBy, col, desc, tc.col, tc.desc)
		}
	}
}

// The zipper must interleave sides by the requested column, not concatenate them.
func TestListMergedReviewDiffsPageSortsAcrossSides(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-sort.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	srcNodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/s_b.txt"), Path: "/s_b.txt", ParentPath: "/", Name: "b.txt", Type: db.NodeTypeFile, Depth: 1, Size: 20},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/s_d.txt"), Path: "/s_d.txt", ParentPath: "/", Name: "d.txt", Type: db.NodeTypeFile, Depth: 1, Size: 10},
	}
	dstNodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/d_a.txt"), Path: "/d_a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, Size: 20},
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/d_c.txt"), Path: "/d_c.txt", ParentPath: "/", Name: "c.txt", Type: db.NodeTypeFile, Depth: 1, Size: 10},
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, srcNodes); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, dstNodes); err != nil {
				return err
			}
			var srcEv []db.StatusEvent
			for _, n := range srcNodes {
				srcEv = append(srcEv, db.StatusEvent{ID: n.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1})
			}
			if err := w.BatchInsertSrcStatusEvents(srcEv); err != nil {
				return err
			}
			var dstEv []db.StatusEvent
			for _, n := range dstNodes {
				dstEv = append(dstEv, db.StatusEvent{ID: n.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1})
			}
			return w.BatchInsertDstStatusEvents(dstEv)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{Query: "txt", QueryField: "name", ExcludeRoot: true}
	cases := []struct {
		orderBy string
		want    []string
	}{
		{"name ASC, path ASC", []string{"/d_a.txt", "/s_b.txt", "/d_c.txt", "/s_d.txt"}},
		{"name DESC, path ASC", []string{"/s_d.txt", "/d_c.txt", "/s_b.txt", "/d_a.txt"}},
		// Ties on size fall back to path ASC, which spans both sides.
		{"size ASC, path ASC", []string{"/d_c.txt", "/s_d.txt", "/d_a.txt", "/s_b.txt"}},
		{"size DESC, path ASC", []string{"/d_a.txt", "/s_b.txt", "/d_c.txt", "/s_d.txt"}},
		// Every row ties on type, so the whole page is ordered by the tie-break.
		{"type ASC, path ASC", []string{"/d_a.txt", "/d_c.txt", "/s_b.txt", "/s_d.txt"}},
		{"path DESC", []string{"/s_d.txt", "/s_b.txt", "/d_c.txt", "/d_a.txt"}},
	}
	for _, tc := range cases {
		page, _, err := ListMergedReviewDiffsPage(database, f, tc.orderBy, 10, 0)
		if err != nil {
			t.Fatalf("%s: %v", tc.orderBy, err)
		}
		got := pathsOf(page)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %v want %v", tc.orderBy, got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: got %v want %v", tc.orderBy, got, tc.want)
			}
		}
	}
}

// Paging a non-path sort must stay consistent with the single-page order.
func TestListMergedReviewDiffsPageSortedPagingIsConsistent(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-sort-page.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	var srcNodes, dstNodes []*db.NodeState
	for i := 0; i < 6; i++ {
		sp := "/s" + strconv.Itoa(i) + ".txt"
		dp := "/d" + strconv.Itoa(i) + ".txt"
		srcNodes = append(srcNodes, &db.NodeState{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, sp), Path: sp, ParentPath: "/",
			Name: "s" + strconv.Itoa(i) + ".txt", Type: db.NodeTypeFile, Depth: 1, Size: int64(i * 2),
		})
		dstNodes = append(dstNodes, &db.NodeState{
			ID: db.DeterministicNodeID("DST", db.NodeTypeFile, dp), Path: dp, ParentPath: "/",
			Name: "d" + strconv.Itoa(i) + ".txt", Type: db.NodeTypeFile, Depth: 1, Size: int64(i*2 + 1),
		})
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, srcNodes); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, dstNodes); err != nil {
				return err
			}
			var srcEv []db.StatusEvent
			for _, n := range srcNodes {
				srcEv = append(srcEv, db.StatusEvent{ID: n.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1})
			}
			if err := w.BatchInsertSrcStatusEvents(srcEv); err != nil {
				return err
			}
			var dstEv []db.StatusEvent
			for _, n := range dstNodes {
				dstEv = append(dstEv, db.StatusEvent{ID: n.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1})
			}
			return w.BatchInsertDstStatusEvents(dstEv)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{Query: "txt", QueryField: "name", ExcludeRoot: true}
	const orderBy = "size DESC, path ASC"

	full, _, err := ListMergedReviewDiffsPage(database, f, orderBy, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != 12 {
		t.Fatalf("full page len=%d want 12", len(full))
	}
	for i := 1; i < len(full); i++ {
		if full[i-1].Size < full[i].Size {
			t.Fatalf("not descending by size: %v", full)
		}
	}

	var paged []MergedReviewRow
	for offset := 0; ; offset += 5 {
		page, hasMore, err := ListMergedReviewDiffsPage(database, f, orderBy, 5, offset)
		if err != nil {
			t.Fatal(err)
		}
		paged = append(paged, page...)
		if !hasMore {
			break
		}
	}
	if len(paged) != len(full) {
		t.Fatalf("paged len=%d want %d", len(paged), len(full))
	}
	for i := range full {
		if paged[i].Path != full[i].Path {
			t.Fatalf("paged order diverges at %d: %v vs %v", i, pathsOf(paged), pathsOf(full))
		}
	}
}

func pathsOf(rows []MergedReviewRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Path
	}
	return out
}
