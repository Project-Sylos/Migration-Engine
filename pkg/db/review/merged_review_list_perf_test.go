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

func TestListMergedReviewDiffsSingleQueryPreservesTotalPastOffset(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/merged-page.db"})
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
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
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

	page, total, err := ListMergedReviewDiffs(database, ReviewFilter{
		TraversalStatus:  db.StatusFailed,
		StatusSearchType: "traversal",
	}, "path ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(page) != 2 {
		t.Fatalf("failed filter: total=%d rows=%d want 2/2", total, len(page))
	}

	// Offset past the end must still report the filtered total (single-query counted CTE).
	empty, totalPast, err := ListMergedReviewDiffs(database, ReviewFilter{
		TraversalStatus:  db.StatusFailed,
		StatusSearchType: "traversal",
	}, "path ASC", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("past-offset rows=%d want 0", len(empty))
	}
	if totalPast != 2 {
		t.Fatalf("past-offset total=%d want 2", totalPast)
	}
}

func TestSrcCurrentStatusSinglePassCopyFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/src-cur.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	id := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/x.txt")
	t0 := time.Now().UnixNano()
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{{
				ID: id, Path: "/x.txt", ParentPath: "/", Name: "x.txt",
				Type: db.NodeTypeFile, Depth: 1,
			}}); err != nil {
				return err
			}
			// Later copy-only event must not clear traversal; copy comes from non-empty rows only.
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: id, TraversalStatus: db.StatusSuccessful, EventTime: t0, Depth: 1},
				{ID: id, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0 + 1, Depth: 1},
				{ID: id, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, EventTime: t0 + 2, Depth: 1},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	rows, total, err := ListMergedReviewDiffs(database, ReviewFilter{}, "path ASC", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(rows) != 1 {
		t.Fatalf("rows=%d total=%d want 1", len(rows), total)
	}
	if rows[0].SrcTraversalStatus != db.StatusSuccessful {
		t.Fatalf("traversal=%q want %q", rows[0].SrcTraversalStatus, db.StatusSuccessful)
	}
	if rows[0].CopyStatus != db.CopyStatusSuccessful {
		t.Fatalf("copy=%q want %q", rows[0].CopyStatus, db.CopyStatusSuccessful)
	}
}
