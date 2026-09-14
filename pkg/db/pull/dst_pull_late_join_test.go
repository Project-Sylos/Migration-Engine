// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"fmt"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
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
	database := db.TestOpen(t, "dst-pull")

	srcRoot := &db.NodeState{
		ID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "", Type: db.NodeTypeFolder, Depth: 0,
		TraversalStatus: db.StatusSuccessful,
	}
	srcFolder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/a"), Path: "/a", ParentPath: "/",
		ParentID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Name: "a", Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit,
	}
	srcChild := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a/f.txt"), Path: "/a/f.txt", ParentPath: "/a",
		ParentID: srcFolder.ID, Name: "f.txt",
		Type: db.NodeTypeFile, Depth: 2, Size: 9,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}

	ops := []db.InsertOperation{
		{QueueType: "SRC", Level: 0, Status: db.StatusSuccessful, State: srcRoot},
		{QueueType: "SRC", Level: 1, Status: db.StatusSuccessful, State: srcFolder},
		{QueueType: "SRC", Level: 2, Status: db.StatusSuccessful, State: srcChild},
	}
	for i := 0; i < 8; i++ {
		path := fmt.Sprintf("/done-%d", i)
		id := db.DeterministicNodeID("DST", db.NodeTypeFolder, path)
		ops = append(ops, db.InsertOperation{
			QueueType: "DST", Level: 1, Status: db.StatusSuccessful,
			State: &db.NodeState{
				ID: id, Path: path, ParentPath: "/", Name: fmt.Sprintf("done-%d", i),
				Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful,
			},
		})
	}
	pendingIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("/pend-%d", i)
		id := db.DeterministicNodeID("DST", db.NodeTypeFolder, path)
		pendingIDs = append(pendingIDs, id)
		ops = append(ops, db.InsertOperation{
			QueueType: "DST", Level: 1, Status: db.StatusPending,
			State: &db.NodeState{
				ID: id, Path: path, ParentPath: "/", Name: fmt.Sprintf("pend-%d", i),
				Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending,
			},
		})
	}
	if err := database.AppendDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
	for _, dstID := range pendingIDs {
		database.AppendIDMapEvent(db.IDMapEvent{
			SrcInternalID: srcFolder.ID, DstInternalID: dstID,
			Status: db.IDMapStatusActive, Depth: 1,
		})
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	batch, children, srcParentDelete, lastScanned, err := ListDstBatchWithSrcChildren(database, 1, "", 2, db.StatusPending)
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
		if got := srcParentDelete[fr.Key]; got != db.DeleteStatusPendingExplicit {
			t.Fatalf("src parent delete for %s = %q want %q", fr.Key, got, db.DeleteStatusPendingExplicit)
		}
	}
	if lastScanned != batch[len(batch)-1].Key {
		t.Fatalf("lastScanned=%q want last pending %q", lastScanned, batch[len(batch)-1].Key)
	}

	batch2, _, _, last2, err := ListDstBatchWithSrcChildren(database, 1, lastScanned, 10, db.StatusPending)
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
	database := db.TestOpen(t, "dst-pull-folders")

	srcRoot := &db.NodeState{
		ID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "", Type: db.NodeTypeFolder, Depth: 0,
		TraversalStatus: db.StatusSuccessful,
	}
	fileA := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/",
		Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, Size: 1, TraversalStatus: db.StatusSuccessful,
	}
	folderPend := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFolder, "/b"), Path: "/b", ParentPath: "/",
		Name: "b", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending,
	}
	fileC := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/",
		Name: "c.txt", Type: db.NodeTypeFile, Depth: 1, Size: 1, TraversalStatus: db.StatusSuccessful,
	}
	srcFolder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/b"), Path: "/b", ParentPath: "/",
		ParentID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Name: "b", Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful,
	}

	if err := database.AppendDiscoveredNodes([]db.InsertOperation{
		{QueueType: "SRC", Level: 0, Status: db.StatusSuccessful, State: srcRoot},
		{QueueType: "SRC", Level: 1, Status: db.StatusSuccessful, State: srcFolder},
		{QueueType: "DST", Level: 1, Status: db.StatusSuccessful, State: fileA},
		{QueueType: "DST", Level: 1, Status: db.StatusPending, State: folderPend},
		{QueueType: "DST", Level: 1, Status: db.StatusSuccessful, State: fileC},
	}); err != nil {
		t.Fatal(err)
	}
	database.AppendIDMapEvent(db.IDMapEvent{
		SrcInternalID: srcFolder.ID, DstInternalID: folderPend.ID,
		Status: db.IDMapStatusActive, Depth: 1,
	})
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}

	batch, _, _, lastScanned, err := ListDstBatchWithSrcChildren(database, 1, "", 10, db.StatusPending)
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
