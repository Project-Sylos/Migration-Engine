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

// dst_nodes.path mirrors the SRC display path even when the destination was created under a
// GPL-cleaned basename, so the name column is the only record of the real destination name.
func TestMergedReviewNameComesFromStoredColumn(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/display-name.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	src := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/Reports*Q1.txt"),
		Path: "/Reports*Q1.txt", ParentPath: "/", Name: "Reports*Q1.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstOnly := &db.NodeState{
		ID:   db.DeterministicNodeID("DST", db.NodeTypeFile, "/Leftover*.txt"),
		Path: "/Leftover*.txt", ParentPath: "/", Name: "LeftoverClean.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
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

	names := func(f ReviewFilter) []string {
		t.Helper()
		rows, _, err := ListMergedReviewDiffs(database, f, "path ASC", 100, 0)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			out = append(out, r.Name)
		}
		return out
	}

	all := names(ReviewFilter{ParentPath: "/"})
	if len(all) != 2 {
		t.Fatalf("want 2 rows, got %v", all)
	}

	// The destination-only row reports its stored name, not the basename of its SRC-shaped path.
	got := names(ReviewFilter{ParentPath: "/", Query: "leftoverclean", QueryField: "name"})
	if len(got) != 1 || got[0] != "LeftoverClean.txt" {
		t.Fatalf("name search on stored dst name got %v", got)
	}
	if got := names(ReviewFilter{ParentPath: "/", Query: "leftoverclean", QueryField: "path"}); len(got) != 0 {
		t.Fatalf("path search should not match the cleaned name, got %v", got)
	}

	if got := names(ReviewFilter{ParentPath: "/", Query: "reports*q1", QueryField: "name"}); len(got) != 1 || got[0] != "Reports*Q1.txt" {
		t.Fatalf("name search on src name got %v", got)
	}
}
