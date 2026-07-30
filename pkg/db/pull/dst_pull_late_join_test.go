// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDstPullScanWindow(t *testing.T) {
	cases := []struct {
		limit int
		want  int
	}{
		{1, db.DstPullScanWindowMin},
		{500, 1000},
		{501, 1002},
		{3000, db.DstPullScanWindowMax},
		{10000, db.DstPullScanWindowMax},
	}
	for _, tc := range cases {
		if got := db.DstPullScanWindow(tc.limit); got != tc.want {
			t.Errorf("dstPullScanWindow(%d)=%d want %d", tc.limit, got, tc.want)
		}
	}
}

func TestListDstBatchWithSrcChildren_lateJoinAndCursor(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/dst-pull.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	srcRoot := &db.NodeState{
		ID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "", Type: db.NodeTypeFolder, Depth: 0,
	}
	srcChild := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a/f.txt"), Path: "/a/f.txt", ParentPath: "/a",
		ParentID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/a"), Name: "f.txt",
		Type: db.NodeTypeFile, Depth: 2, Size: 9,
	}
	srcFolder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/a"), Path: "/a", ParentPath: "/",
		ParentID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Name: "a", Type: db.NodeTypeFolder, Depth: 1,
	}

	var dstNodes []*db.NodeState
	var dstEvents []db.StatusEvent
	// Mix successful + pending so gather must filter; pending ids sort after some successful ones.
	for i := 0; i < 8; i++ {
		path := fmt.Sprintf("/done-%d", i)
		id := db.DeterministicNodeID("DST", db.NodeTypeFolder, path)
		dstNodes = append(dstNodes, &db.NodeState{
			ID: id, Path: path, ParentPath: "/", Name: fmt.Sprintf("done-%d", i),
			Type: db.NodeTypeFolder, Depth: 1,
		})
		dstEvents = append(dstEvents, db.StatusEvent{ID: id, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1})
	}
	pendingIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("/pend-%d", i)
		id := db.DeterministicNodeID("DST", db.NodeTypeFolder, path)
		pendingIDs = append(pendingIDs, id)
		dstNodes = append(dstNodes, &db.NodeState{
			ID: id, Path: path, ParentPath: "/", Name: fmt.Sprintf("pend-%d", i),
			Type: db.NodeTypeFolder, Depth: 1,
		})
		dstEvents = append(dstEvents, db.StatusEvent{ID: id, TraversalStatus: db.StatusPending, EventTime: eventTime, Depth: 1})
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{srcRoot, srcFolder, srcChild}); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, dstNodes); err != nil {
				return err
			}
			if err := w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: srcRoot.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 0},
				{ID: srcFolder.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1},
				{ID: srcChild.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 2},
			}); err != nil {
				return err
			}
			if err := w.BatchInsertDstStatusEvents(dstEvents); err != nil {
				return err
			}
			var maps []db.IDMapEvent
			for _, dstID := range pendingIDs {
				maps = append(maps, db.IDMapEvent{
					SrcInternalID: srcFolder.ID, DstInternalID: dstID,
					EventTime: eventTime, Status: db.IDMapStatusActive,
				})
			}
			return w.BatchInsertIDMapEvents(maps)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	batch, children, lastScanned, err := ListDstBatchWithSrcChildren(database, 1, "", 2, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch len=%d want 2", len(batch))
	}
	for _, fr := range batch {
		if fr.State.TraversalStatus != db.StatusPending {
			t.Fatalf("got non-pending %s status=%s", fr.Key, fr.State.TraversalStatus)
		}
		ch := children[fr.Key]
		if len(ch) != 1 || ch[0].Name != "f.txt" {
			t.Fatalf("children for %s: %+v", fr.Key, ch)
		}
		if ch[0].CopyStatus != db.CopyStatusPending {
			t.Fatalf("child copy status=%q want pending", ch[0].CopyStatus)
		}
	}
	if lastScanned != batch[len(batch)-1].Key {
		t.Fatalf("lastScanned=%q want last pending %q (mid-window fill)", lastScanned, batch[len(batch)-1].Key)
	}

	// Second page: remaining pending.
	batch2, _, last2, err := ListDstBatchWithSrcChildren(database, 1, lastScanned, 10, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch2) != 1 {
		t.Fatalf("second page len=%d want 1", len(batch2))
	}
	if batch2[0].State.TraversalStatus != db.StatusPending {
		t.Fatalf("second page status=%s", batch2[0].State.TraversalStatus)
	}
	if last2 == "" {
		t.Fatal("expected lastScanned on exhausted pull")
	}
}

func TestListDstBatchWithSrcChildren_skipsFilesInWindow(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/dst-pull-folders.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	srcRoot := &db.NodeState{
		ID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "", Type: db.NodeTypeFolder, Depth: 0,
	}
	// Interleave file IDs between folder IDs in sort order via path-derived deterministic IDs.
	fileA := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/",
		Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, Size: 1,
	}
	folderPend := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFolder, "/b"), Path: "/b", ParentPath: "/",
		Name: "b", Type: db.NodeTypeFolder, Depth: 1,
	}
	fileC := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/",
		Name: "c.txt", Type: db.NodeTypeFile, Depth: 1, Size: 1,
	}
	srcFolder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/b"), Path: "/b", ParentPath: "/",
		ParentID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Name: "b", Type: db.NodeTypeFolder, Depth: 1,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{srcRoot, srcFolder}); err != nil {
				return err
			}
			if err := w.AppenderInsert(db.TableDstNodes, []*db.NodeState{fileA, folderPend, fileC}); err != nil {
				return err
			}
			if err := w.BatchInsertDstStatusEvents([]db.StatusEvent{
				{ID: fileA.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1},
				{ID: folderPend.ID, TraversalStatus: db.StatusPending, EventTime: eventTime, Depth: 1},
				{ID: fileC.ID, TraversalStatus: db.StatusSuccessful, EventTime: eventTime, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertIDMapEvents([]db.IDMapEvent{{
				SrcInternalID: srcFolder.ID, DstInternalID: folderPend.ID,
				EventTime: eventTime, Status: db.IDMapStatusActive,
			}})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	batch, _, lastScanned, err := ListDstBatchWithSrcChildren(database, 1, "", 10, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("batch len=%d want 1 (folder only; files must not appear)", len(batch))
	}
	if batch[0].Key != folderPend.ID {
		t.Fatalf("got %s want folder %s", batch[0].Key, folderPend.ID)
	}
	if lastScanned != folderPend.ID {
		t.Fatalf("lastScanned=%q want folder id", lastScanned)
	}
}
