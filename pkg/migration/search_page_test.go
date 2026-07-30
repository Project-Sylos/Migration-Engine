// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func seedFailedSearchFixture(t *testing.T, database *db.DB, n int) {
	t.Helper()
	eventTime := time.Now().UnixNano()
	srcRoot := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	nodes := make([]*db.NodeState, 0, n)
	for i := 0; i < n; i++ {
		name := string(rune('a'+i)) + ".txt"
		path := "/" + name
		nodes = append(nodes, &db.NodeState{
			ID: db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, name), Path: path, ParentPath: "/", Name: name,
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusFailed, CopyStatus: db.CopyStatusPending,
		})
	}
	err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, node := range nodes {
				events = append(events, db.StatusEvent{
					ID: node.ID, TraversalStatus: node.TraversalStatus, CopyStatus: node.CopyStatus,
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
}

func TestSearchPathReviewItemsRejectsEmptyFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-empty.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := newMigrationStore(database, nil)

	_, err = store.searchPathReviewItems(SearchRequest{Limit: 10})
	if !errors.Is(err, ErrSearchRequiresFilter) {
		t.Fatalf("err=%v want ErrSearchRequiresFilter", err)
	}

	// Path/ExcludeRoot alone is still zero-filter.
	_, err = store.searchPathReviewItems(SearchRequest{Path: "", Limit: 10})
	if !errors.Is(err, ErrSearchRequiresFilter) {
		t.Fatalf("global empty err=%v want ErrSearchRequiresFilter", err)
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
	page, err := store.searchPathReviewItems(req)
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
	last, err := store.searchPathReviewItems(req)
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

	// Exact count remains available via GetSearchStats.
	stats, err := store.getSearchStats(req)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 3 {
		t.Fatalf("GetSearchStats.Total=%d want 3", stats.Total)
	}
}
