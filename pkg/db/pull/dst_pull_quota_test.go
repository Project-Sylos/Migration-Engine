// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"fmt"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestDstPullChildQuota(t *testing.T) {
	if got := DstPullChildQuota(1000, 10); got != 10000 {
		t.Fatalf("got %d want 10000", got)
	}
	if got := DstPullChildQuota(0, 10); got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func seedDstPullQuotaDB(t *testing.T, folderCount, childrenPerFolder int) *db.DB {
	t.Helper()
	database := db.TestOpen(t, "dst-pull-quota")

	srcRoot := &db.NodeState{
		ID: db.MintNodeID("SRC", "", db.NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "", Type: db.NodeTypeFolder, Depth: 0,
		TraversalStatus: db.StatusSuccessful,
	}
	ops := []db.InsertOperation{
		{QueueType: "SRC", Level: 0, Status: db.StatusSuccessful, State: srcRoot},
	}
	for i := 0; i < folderCount; i++ {
		srcPath := fmt.Sprintf("/src-%d", i)
		dstPath := fmt.Sprintf("/dst-%d", i)
		srcID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, srcPath)
		dstID := db.DeterministicNodeID("DST", db.NodeTypeFolder, dstPath)
		ops = append(ops, db.InsertOperation{
			QueueType: "SRC", Level: 1, Status: db.StatusSuccessful,
			State: &db.NodeState{
				ID: srcID, Path: srcPath, ParentPath: "/", ParentID: srcRoot.ID, Name: fmt.Sprintf("src-%d", i),
				Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful,
			},
		})
		ops = append(ops, db.InsertOperation{
			QueueType: "DST", Level: 1, Status: db.StatusPending,
			State: &db.NodeState{
				ID: dstID, Path: dstPath, ParentPath: "/", Name: fmt.Sprintf("dst-%d", i),
				Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending,
			},
		})
		for j := 0; j < childrenPerFolder; j++ {
			name := fmt.Sprintf("f-%d.txt", j)
			childPath := srcPath + "/" + name
			ops = append(ops, db.InsertOperation{
				QueueType: "SRC", Level: 2, Status: db.StatusSuccessful,
				State: &db.NodeState{
					ID: db.MintNodeID("SRC", srcID, db.NodeTypeFile, name), Path: childPath, ParentPath: srcPath,
					ParentID: srcID, Name: name, Type: db.NodeTypeFile, Depth: 2, Size: 1,
					TraversalStatus: db.StatusSuccessful,
				},
			})
		}
		database.AppendIDMapEvent(db.IDMapEvent{
			SrcInternalID: srcID, DstInternalID: dstID, Status: db.IDMapStatusActive, Depth: 1,
		})
	}
	if err := database.AppendDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	return database
}

func TestListDstBatchWithSrcChildrenQuota_stopsAtTaskQuota(t *testing.T) {
	database := seedDstPullQuotaDB(t, 5, 1)
	batch, _, _, last, partial, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 2, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch len=%d want 2", len(batch))
	}
	if partial {
		t.Fatal("expected partial=false when more folders remain")
	}
	if last != batch[len(batch)-1].Key {
		t.Fatalf("last=%q want %q", last, batch[len(batch)-1].Key)
	}
}

func TestListDstBatchWithSrcChildrenQuota_stopsAtChildQuota(t *testing.T) {
	database := seedDstPullQuotaDB(t, 5, 5)
	batch, children, _, last, partial, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 100, MaxChildren: 8}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) < 1 {
		t.Fatal("expected at least one folder")
	}
	childCount := 0
	for _, ch := range children {
		childCount += len(ch)
	}
	if childCount > 8 {
		t.Fatalf("childCount=%d want <=8", childCount)
	}
	if len(batch) >= 5 {
		t.Fatalf("batch len=%d want fewer than all folders due to child quota", len(batch))
	}
	if partial {
		t.Fatal("expected partial=false when child quota binds with folders remaining")
	}
	if last == "" {
		t.Fatal("expected last enqueued id")
	}
}

func TestListDstBatchWithSrcChildrenQuota_cursorDoesNotSkipDeferredFolder(t *testing.T) {
	database := seedDstPullQuotaDB(t, 4, 10)
	first, _, _, last1, _, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 1, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first len=%d want 1", len(first))
	}
	second, _, _, last2, partial, err := ListDstBatchWithSrcChildrenQuota(database, 1, last1, DstPullQuota{MaxTasks: 1, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 {
		t.Fatalf("second len=%d want 1", len(second))
	}
	if second[0].Key == first[0].Key {
		t.Fatal("second pull should advance to next folder, not repeat")
	}
	if partial {
		t.Fatal("expected partial=false with folders remaining")
	}
	if last2 == last1 {
		t.Fatal("cursor should advance after second pull")
	}
}

func TestListDstBatchWithSrcChildrenQuota_includesHugeFirstFolder(t *testing.T) {
	database := seedDstPullQuotaDB(t, 2, 50)
	batch, children, _, _, partial, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 10, MaxChildren: 5}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("batch len=%d want 1 (always include first folder)", len(batch))
	}
	if len(children[batch[0].Key]) != 50 {
		t.Fatalf("children=%d want 50", len(children[batch[0].Key]))
	}
	if partial {
		t.Fatal("expected partial=false after oversized first folder when another folder remains")
	}
}

func TestListDstBatchWithSrcChildrenQuota_exhaustsSingleFolderRound(t *testing.T) {
	database := seedDstPullQuotaDB(t, 1, 1)
	batch, _, _, _, partial, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 10, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("batch len=%d want 1", len(batch))
	}
	if !partial {
		t.Fatal("expected partial=true when depth frontier is exhausted")
	}
}

func TestListDstBatchWithSrcChildrenQuota_emptyDepthExhausted(t *testing.T) {
	database := seedDstPullQuotaDB(t, 0, 0)
	_, _, _, _, partial, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 10, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if !partial {
		t.Fatal("expected partial=true when depth has no pending folders")
	}
}

func TestListDstBatchWithSrcChildrenQuota_exhaustsAfterCursor(t *testing.T) {
	database := seedDstPullQuotaDB(t, 2, 1)
	first, _, _, last1, partial1, err := ListDstBatchWithSrcChildrenQuota(database, 1, "", DstPullQuota{MaxTasks: 1, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first len=%d want 1", len(first))
	}
	if partial1 {
		t.Fatal("expected partial=false after first of two folders")
	}
	second, _, _, _, partial2, err := ListDstBatchWithSrcChildrenQuota(database, 1, last1, DstPullQuota{MaxTasks: 10, MaxChildren: 100}, db.StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 {
		t.Fatalf("second len=%d want 1", len(second))
	}
	if !partial2 {
		t.Fatal("expected partial=true after last folder at depth")
	}
}
