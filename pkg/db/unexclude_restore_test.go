// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestUnexcludeRestoresPriorCopyStatus(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/unexclude-restore.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := DeterministicNodeID("SRC", NodeTypeFolder, "/")
	folderID := DeterministicNodeID("SRC", NodeTypeFolder, "/folder")
	failedID := DeterministicNodeID("SRC", NodeTypeFile, "/folder/failed.txt")
	okID := DeterministicNodeID("SRC", NodeTypeFile, "/folder/ok.txt")
	pendingID := DeterministicNodeID("SRC", NodeTypeFile, "/folder/pending.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			nodes := []*NodeState{
				{ID: rootID, Path: "/", Type: NodeTypeFolder, Depth: 0},
				{ID: folderID, Path: "/folder", ParentPath: "/", ParentID: rootID, Name: "folder", Type: NodeTypeFolder, Depth: 1},
				{ID: failedID, Path: "/folder/failed.txt", ParentPath: "/folder", ParentID: folderID, Name: "failed.txt", Type: NodeTypeFile, Depth: 2},
				{ID: okID, Path: "/folder/ok.txt", ParentPath: "/folder", ParentID: folderID, Name: "ok.txt", Type: NodeTypeFile, Depth: 2},
				{ID: pendingID, Path: "/folder/pending.txt", ParentPath: "/folder", ParentID: folderID, Name: "pending.txt", Type: NodeTypeFile, Depth: 2},
			}
			if err := w.AppenderInsert(tableSrcNodes, nodes); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]StatusEvent{
				{ID: rootID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: t0, Depth: 0},
				{ID: folderID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: t0, Depth: 1},
				{ID: failedID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusFailed, EventTime: t0, Depth: 2},
				{ID: okID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, EventTime: t0, Depth: 2},
				{ID: pendingID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.InsertExclusionEventsForSubtree("SRC", "/folder")
		})
	}); err != nil {
		t.Fatal(err)
	}

	prior, err := func() (CopyStatusBucketsSubtreeNotExcluded, error) {
		var out CopyStatusBucketsSubtreeNotExcluded
		err := database.RunWrite(context.Background(), func(s *WriteSession) error {
			return s.WithTx(func(w *Writer) error {
				var err2 error
				out, err2 = w.CountCopyStatusBucketsSubtreeExcludedPrior("/folder")
				return err2
			})
		})
		return out, err
	}()
	if err != nil {
		t.Fatal(err)
	}
	if prior.Pending != 2 || prior.Failed != 1 || prior.Successful != 1 {
		t.Fatalf("prior buckets: pending=%d failed=%d successful=%d want pending=2 failed=1 successful=1",
			prior.Pending, prior.Failed, prior.Successful)
	}

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.InsertUnexcludeEventsForSubtree("SRC", "/folder")
		})
	}); err != nil {
		t.Fatal(err)
	}

	assertCopy := func(id, want string) {
		t.Helper()
		n, err := GetNodeByID(database, "SRC", id)
		if err != nil {
			t.Fatal(err)
		}
		if n == nil {
			t.Fatalf("node %s missing", id)
		}
		if n.CopyStatus != want {
			t.Fatalf("node %s copy_status=%q want %q", id, n.CopyStatus, want)
		}
		if n.Excluded {
			t.Fatalf("node %s still excluded", id)
		}
	}
	assertCopy(folderID, CopyStatusPending)
	assertCopy(failedID, CopyStatusFailed)
	assertCopy(okID, CopyStatusSuccessful)
	assertCopy(pendingID, CopyStatusPending)
}

func TestSetNodeExcludedUnexcludeRestoresPriorCopyStatus(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/unexclude-single.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := DeterministicNodeID("SRC", NodeTypeFolder, "/")
	fileID := DeterministicNodeID("SRC", NodeTypeFile, "/failed.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppenderInsert(tableSrcNodes, []*NodeState{
				{ID: rootID, Path: "/", Type: NodeTypeFolder, Depth: 0},
				{ID: fileID, Path: "/failed.txt", ParentPath: "/", ParentID: rootID, Name: "failed.txt", Type: NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]StatusEvent{
				{ID: rootID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: t0, Depth: 0},
				{ID: fileID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusFailed, EventTime: t0, Depth: 1},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SetNodeExcluded("SRC", fileID, true)
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SetNodeExcluded("SRC", fileID, false)
		})
	}); err != nil {
		t.Fatal(err)
	}

	n, err := GetNodeByID(database, "SRC", fileID)
	if err != nil {
		t.Fatal(err)
	}
	if n == nil || n.CopyStatus != CopyStatusFailed || n.Excluded {
		t.Fatalf("after unexclude got copy=%q excluded=%v want failed", n.CopyStatus, n.Excluded)
	}
}
