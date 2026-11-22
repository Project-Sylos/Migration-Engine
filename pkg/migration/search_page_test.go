// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func seedFailedSearchFixture(t *testing.T, database *db.DB, n int) {
	t.Helper()
	srcRoot := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	ops := make([]db.InsertOperation, 0, n)
	for i := 0; i < n; i++ {
		name := string(rune('a'+i)) + ".txt"
		path := "/" + name
		node := &db.NodeState{
			ID: db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, name), Path: path, ParentPath: "/", Name: name,
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending,
		}
		ops = append(ops, db.InsertOperation{QueueType: "SRC", Level: 1, Status: db.StatusFailed, State: node})
	}
	if err := database.SeedDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
}

func TestSearchPathReviewItemsAllowsEmptyFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-empty.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := newMigrationStore(database, nil)

	page, err := store.searchPathReviewItems(context.Background(), SearchRequest{Limit: 10})
	if err != nil {
		t.Fatalf("empty search: %v", err)
	}
	if page.Items == nil {
		t.Fatal("items nil")
	}

	page, err = store.searchPathReviewItems(context.Background(), SearchRequest{Path: "", Limit: 10})
	if err != nil {
		t.Fatalf("global empty search: %v", err)
	}
	if page.Items == nil {
		t.Fatal("items nil")
	}
}

func TestSearchPathReviewItemsHasMoreWithoutTotal(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-hasmore.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	seedFailedSearchFixture(t, database, 3)
	store := newMigrationStore(database, nil)

	req := SearchRequest{
		Limit:            2,
		Offset:           0,
		StatusSearchType: "traversal",
		TraversalStatus:  db.StatusFailed,
		SortBy:           "path",
		SortDirection:    "asc",
	}
	page, err := store.searchPathReviewItems(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != nil {
		t.Fatalf("Total=%v want nil (no count on search hot path)", *page.Total)
	}
	if !page.HasMore {
		t.Fatal("HasMore=false want true")
	}
	if len(page.Items) != 2 {
		t.Fatalf("items=%d want 2", len(page.Items))
	}

	req.Offset = 2
	last, err := store.searchPathReviewItems(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if last.Total != nil {
		t.Fatalf("last Total=%v want nil", *last.Total)
	}
	if last.HasMore {
		t.Fatal("HasMore=true want false on final page")
	}
	if len(last.Items) != 1 {
		t.Fatalf("last items=%d want 1", len(last.Items))
	}

	stats, err := store.getSearchStats(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 3 {
		t.Fatalf("GetSearchStats.Total=%d want 3", stats.Total)
	}
}
