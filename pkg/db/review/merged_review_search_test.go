// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"fmt"
	"strconv"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestListMergedReviewDiffsPageZipperIncludesDSTOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-dst-only.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

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
	seedReviewTree(t, database, []*db.NodeState{src}, []*db.NodeState{dstMapped, dstOnly}, []db.IDMapEvent{{
		SrcInternalID: src.ID, DstInternalID: dstMapped.ID, Status: db.IDMapStatusActive,
	}})

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

	srcNodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt", Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending},
	}
	dstOnly := []*db.NodeState{
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt", Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed},
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/d.txt"), Path: "/d.txt", ParentPath: "/", Name: "d.txt", Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed},
	}
	seedReviewTree(t, database, srcNodes, dstOnly, nil)

	f := ReviewFilter{TraversalStatus: db.StatusFailed, StatusSearchType: "traversal", ExcludeRoot: true}
	page, hasMore, err := ListMergedReviewDiffsPage(database, f, "id ASC", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore {
		t.Fatal("hasMore=true want false (only two SRC failed rows)")
	}
	if len(page) != 2 {
		t.Fatalf("page len=%d want 2", len(page))
	}
	got := map[string]bool{}
	for _, p := range pathsOf(page) {
		got[p] = true
	}
	if !got["/a.txt"] || !got["/c.txt"] {
		t.Fatalf("page=%v want SRC [/a.txt /c.txt]; DST-only rows are not in status overlay", got)
	}
}

func TestListMergedReviewDiffsPageSkipsDSTOnlyForCopyFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/zipper-copy.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	src := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/pending.txt"), Path: "/pending.txt", ParentPath: "/", Name: "pending.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstOnly := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/ghost.txt"), Path: "/ghost.txt", ParentPath: "/", Name: "ghost.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	seedReviewTree(t, database, []*db.NodeState{src}, []*db.NodeState{dstOnly}, nil)

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

	srcNodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/s_b.txt"), Path: "/s_b.txt", ParentPath: "/", Name: "b.txt", Type: db.NodeTypeFile, Depth: 1, Size: 20, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/s_d.txt"), Path: "/s_d.txt", ParentPath: "/", Name: "d.txt", Type: db.NodeTypeFile, Depth: 1, Size: 10, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
	}
	dstNodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/d_a.txt"), Path: "/d_a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, Size: 20, TraversalStatus: db.StatusSuccessful},
		{ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/d_c.txt"), Path: "/d_c.txt", ParentPath: "/", Name: "c.txt", Type: db.NodeTypeFile, Depth: 1, Size: 10, TraversalStatus: db.StatusSuccessful},
	}
	seedReviewTree(t, database, srcNodes, dstNodes, nil)

	f := ReviewFilter{Query: "txt", QueryField: "name", ExcludeRoot: true}
	cases := []struct {
		orderBy string
		want    []string
	}{
		{"name ASC, path ASC", []string{"/d_a.txt", "/s_b.txt", "/d_c.txt", "/s_d.txt"}},
		{"name DESC, path ASC", []string{"/s_d.txt", "/d_c.txt", "/s_b.txt", "/d_a.txt"}},
		{"size ASC, path ASC", []string{"/d_c.txt", "/s_d.txt", "/d_a.txt", "/s_b.txt"}},
		{"size DESC, path ASC", []string{"/d_a.txt", "/s_b.txt", "/d_c.txt", "/s_d.txt"}},
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

	var srcNodes, dstNodes []*db.NodeState
	for i := 0; i < 6; i++ {
		sp := "/s" + strconv.Itoa(i) + ".txt"
		dp := "/d" + strconv.Itoa(i) + ".txt"
		srcNodes = append(srcNodes, &db.NodeState{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, sp), Path: sp, ParentPath: "/",
			Name: "s" + strconv.Itoa(i) + ".txt", Type: db.NodeTypeFile, Depth: 1, Size: int64(i * 2),
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		})
		dstNodes = append(dstNodes, &db.NodeState{
			ID: db.DeterministicNodeID("DST", db.NodeTypeFile, dp), Path: dp, ParentPath: "/",
			Name: "d" + strconv.Itoa(i) + ".txt", Type: db.NodeTypeFile, Depth: 1, Size: int64(i*2 + 1),
			TraversalStatus: db.StatusSuccessful,
		})
	}
	seedReviewTree(t, database, srcNodes, dstNodes, nil)

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

func TestListMergedReviewSearchRecordsSQL(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-ops-core.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	src := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/shared.txt"), Path: "/shared.txt", ParentPath: "/", Name: "shared.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	seedReviewTree(t, database, []*db.NodeState{src}, nil, nil)

	page, hasMore, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		Query: "txt", QueryField: "name", ExcludeRoot: true,
	}, "path ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore {
		t.Fatal("unexpected hasMore")
	}
	if len(page) != 1 || page[0].Path != "/shared.txt" || page[0].SrcNodeID != src.ID {
		t.Fatalf("ops search page=%+v", page)
	}
}

func TestListMergedReviewDiffsPageCopyStatusAndSizeFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-size.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	twoGB := int64(2 * 1024 * 1024 * 1024)
	largePending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/large-pending.bin"), Path: "/large-pending.bin", ParentPath: "/", Name: "large-pending.bin",
		Type: db.NodeTypeFile, Depth: 1, Size: twoGB + 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	smallPending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/small-pending.bin"), Path: "/small-pending.bin", ParentPath: "/", Name: "small-pending.bin",
		Type: db.NodeTypeFile, Depth: 1, Size: 1024, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	largeDone := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/large-done.bin"), Path: "/large-done.bin", ParentPath: "/", Name: "large-done.bin",
		Type: db.NodeTypeFile, Depth: 1, Size: twoGB + 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
	}
	seedReviewTree(t, database, []*db.NodeState{largePending, smallPending, largeDone}, nil, nil)

	f := ReviewFilter{
		CopyStatus:       db.CopyStatusPending,
		StatusSearchType: "copy",
		SizeOperator:     "gt",
		SizeValue:        &twoGB,
	}
	if StatusDrivenSearch(f) {
		t.Fatal("status+size should use planner path, not status overlay")
	}

	page, _, err := ListMergedReviewDiffsPage(database, f, "path ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].Path != "/large-pending.bin" {
		t.Fatalf("want only large pending file, got %+v", pathsOf(page))
	}

	stats, err := GetMergedReviewStats(database, f)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 1 || stats.Files != 1 {
		t.Fatalf("stats=%+v want total=1 files=1", stats)
	}
}

func pathsOf(rows []MergedReviewRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Path
	}
	return out
}

func TestListMergedReviewDiffsPagePathSegmentDotCursorBeyondScanCap(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path-seg-dotcursor.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	cursor := db.MintNodeID("SRC", root, db.NodeTypeFolder, ".cursor")
	cursorPath := db.JoinIDPath("/", cursor)
	cfg := db.MintNodeID("SRC", cursor, db.NodeTypeFile, "config.json")
	cfgPath := db.JoinIDPath(cursorPath, cfg)

	nodes := []*db.NodeState{
		{ID: root, Path: "/", Type: db.NodeTypeFolder, Depth: 0, Name: "/", TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: cursor, ParentID: root, Path: cursorPath, ParentPath: "/", Name: ".cursor", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{ID: cfg, ParentID: cursor, Path: cfgPath, ParentPath: cursorPath, Name: "config.json", Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
	}
	for i := 0; i < 6000; i++ {
		id := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/"+fmt.Sprintf("aaa-filler-%05d", i))
		p := db.JoinIDPath("/", id)
		nodes = append(nodes, &db.NodeState{
			ID: id, ParentID: root, Path: p, ParentPath: "/", Name: fmt.Sprintf("aaa-filler-%05d", i),
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		})
	}
	seedReviewTree(t, database, nodes, nil, nil)

	page, _, err := ListMergedReviewDiffsPage(database, ReviewFilter{
		PathSegments: []string{".cursor"},
		ExcludeRoot:  true,
	}, "path ASC", 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range page {
		got[r.SrcNodeID] = true
	}
	if !got[cursor] || !got[cfg] {
		t.Fatalf("want .cursor folder and child, got ids %v paths %v", got, pathsOf(page))
	}
}
