// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"fmt"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestStatusDrivenSearchPagesByIDHydrate(t *testing.T) {
	database := db.TestOpen(t, "status-hydrate")

	nodes := []*db.NodeState{
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/z.txt"), Path: "/z.txt", ParentPath: "/", Name: "z.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/m.txt"), Path: "/m.txt", ParentPath: "/", Name: "m.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
		},
	}
	ops := make([]db.InsertOperation, 0, len(nodes))
	for _, n := range nodes {
		ops = append(ops, db.InsertOperation{QueueType: "SRC", Level: 1, Status: n.TraversalStatus, State: n})
	}
	if err := database.AppendDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{CopyStatus: db.CopyStatusPending, StatusSearchType: "copy"}
	if !StatusDrivenSearch(f) {
		t.Fatal("expected StatusDrivenSearch")
	}

	page, hasMore, err := ListMergedReviewDiffsPage(database, f, "id ASC", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMore {
		t.Fatal("hasMore=false want true")
	}
	if len(page) != 1 {
		t.Fatalf("page len=%d want 1", len(page))
	}
	if page[0].Path == "" || page[0].SrcNodeID == "" {
		t.Fatalf("hydrate failed: %+v", page[0])
	}
	if page[0].CopyStatus != db.CopyStatusPending {
		t.Fatalf("copy_status=%q", page[0].CopyStatus)
	}

	page2, hasMore2, err := ListMergedReviewDiffsPage(database, f, "id ASC", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore2 {
		t.Fatal("hasMore=true want false on last page")
	}
	if len(page2) != 1 {
		t.Fatalf("page2 len=%d want 1", len(page2))
	}
	if page2[0].SrcNodeID == page[0].SrcNodeID {
		t.Fatal("pages should advance by id")
	}

	n, err := CountSrcCurrentMatchingStatus(database, f)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("count=%d want 2", n)
	}
	stats, err := GetMergedReviewStats(database, f)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 2 {
		t.Fatalf("stats.Total=%d want 2", stats.Total)
	}
}

func TestStatusDrivenSearchUsesOverlayNotWindow(t *testing.T) {
	database := db.TestOpen(t, "status-overlay")

	var ops []db.InsertOperation
	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("ok-%02d.txt", i)
		path := "/" + name
		n := &db.NodeState{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, path), Path: path, ParentPath: "/", Name: name,
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
		}
		ops = append(ops, db.InsertOperation{QueueType: "SRC", Level: 1, Status: n.TraversalStatus, State: n})
	}
	pending := []*db.NodeState{
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder_pending_a"), Path: "/folder_pending_a", ParentPath: "/", Name: "folder_pending_a",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder_pending_b"), Path: "/folder_pending_b", ParentPath: "/", Name: "folder_pending_b",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder_pending_c"), Path: "/folder_pending_c", ParentPath: "/", Name: "folder_pending_c",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusPending,
		},
	}
	for _, n := range pending {
		ops = append(ops, db.InsertOperation{QueueType: "SRC", Level: 1, Status: n.TraversalStatus, State: n})
	}
	if err := database.AppendDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{TraversalStatus: db.StatusPending, StatusSearchType: "traversal"}
	if !StatusDrivenSearch(f) {
		t.Fatal("expected StatusDrivenSearch for traversal overlay")
	}
	page, hasMore, err := ListMergedReviewDiffsPage(database, f, "id ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if hasMore {
		t.Fatal("hasMore=true want false")
	}
	if len(page) != 3 {
		t.Fatalf("pending rows=%d want 3", len(page))
	}
	stats, err := GetMergedReviewStats(database, f)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 3 || stats.Folders != 3 {
		t.Fatalf("stats total=%d folders=%d want 3,3", stats.Total, stats.Folders)
	}
}

func TestStatusDrivenSearchRejectsPathPredicates(t *testing.T) {
	if !StatusDrivenSearch(ReviewFilter{CopyStatus: db.CopyStatusPending, ExcludeRoot: true}) {
		t.Fatal("ExcludeRoot alone must not disable status overlay")
	}
	if StatusDrivenSearch(ReviewFilter{CopyStatus: db.CopyStatusPending, Query: "x"}) {
		t.Fatal("query must disable status drive")
	}
	if StatusDrivenSearch(ReviewFilter{CopyStatus: db.CopyStatusPending, ParentPath: "/a"}) {
		t.Fatal("parent must disable status drive")
	}
	if !StatusDrivenSearch(ReviewFilter{TraversalStatus: db.StatusPending, StatusSearchType: "traversal"}) {
		t.Fatal("traversal-only should use overlay search")
	}
	size := int64(2 * 1024 * 1024 * 1024)
	if StatusDrivenSearch(ReviewFilter{CopyStatus: db.CopyStatusPending, SizeOperator: "gt", SizeValue: &size}) {
		t.Fatal("size filter should disable status overlay fast path")
	}
}

func TestStatusDrivenSearchCopyPendingMatchesEmptyCopyStatus(t *testing.T) {
	database := db.TestOpen(t, "copy-empty-pending")

	node := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/x.txt"), Path: "/x.txt", ParentPath: "/", Name: "x.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: "",
	}
	if err := database.AppendDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: 1, Status: db.StatusSuccessful, State: node,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{CopyStatus: db.CopyStatusPending, StatusSearchType: "copy", ExcludeRoot: true}
	page, _, err := ListMergedReviewDiffsPage(database, f, "id ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 {
		t.Fatalf("want 1 row for empty copy_status pending search, got %d", len(page))
	}
}

func TestStatusDrivenSearchDeletePendingMatchesInherited(t *testing.T) {
	database := db.TestOpen(t, "delete-pending-search")

	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	folder := db.MintNodeID("SRC", root, db.NodeTypeFolder, "folder")
	folderPath := db.JoinIDPath("/", folder)
	file := db.MintNodeID("SRC", folder, db.NodeTypeFile, "a.txt")
	filePath := db.JoinIDPath(folderPath, file)
	nodes := []*db.NodeState{
		{ID: root, Path: "/", Type: db.NodeTypeFolder, Depth: 0, Name: "/", TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful},
		{ID: folder, ParentID: root, Path: folderPath, ParentPath: "/", Name: "folder", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{ID: file, ParentID: folder, Path: filePath, ParentPath: folderPath, Name: "a.txt", Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingInherited},
	}
	ops := make([]db.InsertOperation, 0, len(nodes))
	for _, n := range nodes {
		ops = append(ops, db.InsertOperation{QueueType: "SRC", Level: n.Depth, Status: n.TraversalStatus, State: n})
	}
	if err := database.AppendDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{DeleteStatus: "pending", StatusSearchType: "delete", ExcludeRoot: true}
	page, _, err := ListMergedReviewDiffsPage(database, f, "id ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 {
		t.Fatalf("delete pending search want 2 rows, got %d", len(page))
	}
}

func TestStatusDrivenSearchCopyExcludedMatchesTraversalExcludedFolder(t *testing.T) {
	database := db.TestOpen(t, "trav-excluded-search")

	folder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/excluded-folder"), Path: "/excluded-folder", ParentPath: "/", Name: "excluded-folder",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusExcluded, CopyStatus: "",
	}
	if err := database.AppendDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: 1, Status: db.StatusExcluded, State: folder,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	f := ReviewFilter{CopyStatus: db.CopyStatusExcluded, StatusSearchType: "copy", ExcludeRoot: true}
	page, _, err := ListMergedReviewDiffsPage(database, f, "id ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].SrcNodeID != folder.ID {
		t.Fatalf("excluded search want traversal-excluded folder, got %+v", page)
	}
}

func TestTraversalPendingFilterExcludesFailed(t *testing.T) {
	t.Parallel()
	f := ReviewFilter{StatusSearchType: "both", CopyStatus: db.CopyStatusPending, TraversalStatus: "not_failed"}
	if statusRecordMatchesSRC(opsdb.StatusRecord{
		TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending,
	}, f) {
		t.Fatal("failed+copy-pending must not match traversal pending filter")
	}
	if !statusRecordMatchesSRC(opsdb.StatusRecord{
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}, f) {
		t.Fatal("successful+copy-pending must match traversal pending filter")
	}
}
