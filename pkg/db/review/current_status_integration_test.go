// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"context"
	"testing"

	"time"
)

func TestRebuildSrcCurrentByDepthMatchesArgMax(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/current-depth.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idA := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt")
	idB := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt")
	t0 := time.Now().UnixNano()
	t1 := t0 + 1

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: idA, Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1},
				{ID: idB, Path: "/b.txt", ParentPath: "/", Name: "b.txt", Type: db.NodeTypeFile, Depth: 2},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: idA, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 1},
				{ID: idA, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t1, Depth: 1},
				{ID: idB, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, EventTime: t0, Depth: 2},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := database.RebuildCurrentAtDepth("SRC", 1); err != nil {
		t.Fatal(err)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var trav, copy string
	var depth int
	err = conn.QueryRowContext(context.Background(),
		`SELECT traversal_status, copy_status, depth FROM src_current WHERE id = $1`, idA).Scan(&trav, &copy, &depth)
	if err != nil {
		t.Fatal(err)
	}
	if trav != db.StatusSuccessful || copy != db.CopyStatusPending || depth != 1 {
		t.Fatalf("idA current=%s/%s depth=%d", trav, copy, depth)
	}
	var n int
	if err := conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM src_current WHERE id = $1`, idB).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("depth-1 rebuild should not include depth-2 idB, got count=%d", n)
	}

	if err := database.RebuildCurrentByIDs("SRC", []string{idB}); err != nil {
		t.Fatal(err)
	}
	err = conn.QueryRowContext(context.Background(),
		`SELECT traversal_status, copy_status FROM src_current WHERE id = $1`, idB).Scan(&trav, &copy)
	if err != nil {
		t.Fatal(err)
	}
	if trav != db.StatusSuccessful || copy != db.CopyStatusSuccessful {
		t.Fatalf("idB current=%s/%s", trav, copy)
	}
}

func TestExcludeSubtreeVisibleInMergedReviewWithoutRebuild(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/dualwrite.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	childID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/a.txt")
	eventTime := time.Now().UnixNano()

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: rootID, Path: "/folder", ParentPath: "/", Name: "folder", Type: db.NodeTypeFolder, Depth: 1},
				{ID: childID, Path: "/folder/a.txt", ParentPath: "/folder", Name: "a.txt", Type: db.NodeTypeFile, Depth: 2},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: rootID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: childID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 2},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			_, err := subtree.InsertExclusionEventsForSubtree(w, "SRC", "/folder")
			return err
		})
	})
	if err != nil {
		t.Fatal(err)
	}

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
				t.Fatalf("folder not excluded after dual-write: copy=%q excluded=%v", r.CopyStatus, r.Excluded)
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
			t.Fatalf("child not excluded after dual-write: copy=%q", r.CopyStatus)
		}
	}
}
