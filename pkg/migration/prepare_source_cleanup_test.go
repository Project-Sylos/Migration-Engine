// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

func TestPrepareSourceCleanupEmptyDoesNotResetSkipped(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prepare-cleanup.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	keepID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/keep.txt")
	skipID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/skip.txt")
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{
				{
					ID: keepID, Path: "/keep.txt", ParentPath: "/", Name: "keep.txt",
					Type: db.NodeTypeFile, Depth: 1, Size: 100,
					TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
					DeleteStatus: db.DeleteStatusPending,
				},
				{
					ID: skipID, Path: "/skip.txt", ParentPath: "/", Name: "skip.txt",
					Type: db.NodeTypeFile, Depth: 1, Size: 200,
					TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
					DeleteStatus: db.DeleteStatusSkipped,
				},
			}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			evs := make([]db.StatusEvent, 0, len(nodes))
			now := time.Now().UnixNano()
			for i, n := range nodes {
				evs = append(evs, db.StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					DeleteStatus: n.DeleteStatus, EventTime: now + int64(i), Depth: 1,
				})
			}
			if err := w.BatchInsertSrcStatusEvents(evs); err != nil {
				return err
			}
			return w.RefreshCurrentByPathPrefix("SRC", "/")
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database, store: newMigrationStore(database, nil), phase: PhaseCopyReview}
	res, err := m.PrepareSourceCleanup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedCount != 0 {
		t.Fatalf("empty prepare affected=%d want 0 (must not reset skipped)", res.AffectedCount)
	}

	skipNode, err := pull.GetNodeByID(database, "SRC", skipID)
	if err != nil || skipNode == nil {
		t.Fatalf("get skip node: %v %#v", err, skipNode)
	}
	if skipNode.DeleteStatus != db.DeleteStatusSkipped {
		t.Fatalf("skip node delete_status=%q want skipped", skipNode.DeleteStatus)
	}
	keepNode, err := pull.GetNodeByID(database, "SRC", keepID)
	if err != nil || keepNode == nil {
		t.Fatalf("get keep node: %v %#v", err, keepNode)
	}
	if keepNode.DeleteStatus != db.DeleteStatusPending {
		t.Fatalf("keep node delete_status=%q want pending", keepNode.DeleteStatus)
	}

	if err := stats.SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals, err := stats.ReadSealedWorkTotals(database,
		db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Files != 1 || totals.Bytes != 100 {
		t.Fatalf("delete_work after prepare+skip totals=%+v want files=1 bytes=100", totals)
	}
}

func TestPrepareSourceCleanupEmptyInitsUnsetOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prepare-init.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	unsetID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/unset.txt")
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			n := &db.NodeState{
				ID: unsetID, Path: "/unset.txt", ParentPath: "/", Name: "unset.txt",
				Type: db.NodeTypeFile, Depth: 1, Size: 50,
				TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
				// DeleteStatus intentionally empty
			}
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{n}); err != nil {
				return err
			}
			if err := w.BatchInsertSrcStatusEvents([]db.StatusEvent{{
				ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
				EventTime: time.Now().UnixNano(), Depth: 1,
			}}); err != nil {
				return err
			}
			return w.RefreshCurrentByPathPrefix("SRC", "/")
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database, store: newMigrationStore(database, nil), phase: PhaseCopyReview}
	res, err := m.PrepareSourceCleanup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedCount != 1 {
		t.Fatalf("init unset affected=%d want 1", res.AffectedCount)
	}
	node, err := pull.GetNodeByID(database, "SRC", unsetID)
	if err != nil || node == nil {
		t.Fatalf("get node: %v %#v", err, node)
	}
	if node.DeleteStatus != db.DeleteStatusPending {
		t.Fatalf("delete_status=%q want pending", node.DeleteStatus)
	}
}
