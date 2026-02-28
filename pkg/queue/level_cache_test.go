// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestLevelCache_PutGetSnapshot(t *testing.T) {
	lc := NewLevelCache()
	n1 := &db.NodeState{ID: "a", Path: "/a", Depth: 1, TraversalStatus: db.StatusPending}
	n2 := &db.NodeState{ID: "b", Path: "/b", Depth: 1, TraversalStatus: db.StatusPending}
	lc.Put("a", n1)
	lc.Put("b", n2)
	if lc.Count() != 2 {
		t.Errorf("Count = %d, want 2", lc.Count())
	}
	got := lc.Get("a")
	if got == nil || got.ID != "a" {
		t.Errorf("Get(a) = %v", got)
	}
	snap := lc.Snapshot()
	if len(snap) != 2 {
		t.Errorf("Snapshot len = %d, want 2", len(snap))
	}
	lc.UpdateStatus("a", db.StatusSuccessful, "")
	if lc.GetRef("a").TraversalStatus != db.StatusSuccessful {
		t.Errorf("UpdateStatus did not update")
	}
}

func TestLevelCache_ListPendingCopy(t *testing.T) {
	lc := NewLevelCache()
	lc.Put("id1", &db.NodeState{ID: "id1", CopyStatus: db.CopyStatusPending, Type: "folder"})
	lc.Put("id2", &db.NodeState{ID: "id2", CopyStatus: db.CopyStatusSuccessful, Type: "folder"})
	lc.Put("id3", &db.NodeState{ID: "id3", CopyStatus: db.CopyStatusPending, Type: "file"})
	out := lc.ListPendingCopy("", 10, "folder")
	if len(out) != 1 {
		t.Errorf("ListPendingCopy(folder) len = %d, want 1", len(out))
	}
	outAll := lc.ListPendingCopy("", 10, "")
	if len(outAll) != 2 {
		t.Errorf("ListPendingCopy(all) len = %d, want 2", len(outAll))
	}
}

func TestNodeCache_DropPromote(t *testing.T) {
	nc := NewNodeCache()
	nc.EnsureLevel(1).Put("n1", &db.NodeState{ID: "n1", Depth: 1})
	nc.EnsureLevel(2).Put("n2", &db.NodeState{ID: "n2", Depth: 2})
	if nc.GetLevel(1) == nil || nc.GetLevel(2) == nil {
		t.Fatal("levels not created")
	}
	nc.DropLevel(1)
	if nc.GetLevel(1) != nil {
		t.Error("DropLevel(1) did not remove level")
	}
	if nc.GetLevel(2) == nil {
		t.Error("level 2 should still exist")
	}
	nc.PromoteLevel(2, 1)
	// After promote, fromDepth (2) is replaced with a fresh empty level; toDepth (1) holds the former level 2 content.
	if nc.GetLevel(2) == nil || nc.GetLevel(2).Count() != 0 {
		t.Error("PromoteLevel leaves fromDepth as fresh empty level")
	}
	l1 := nc.GetLevel(1)
	if l1 == nil {
		t.Fatal("level 1 after promote is nil")
	}
	if l1.GetRef("n2") == nil {
		t.Error("promoted level should contain n2")
	}
}

func TestNodeCache_RecordCopyTransition(t *testing.T) {
	nc := NewNodeCache()
	nc.EnsureLevel(1)
	nc.RecordCopyTransition(1, db.CopyStatusPending, db.CopyStatusInProgress)
	stats := nc.GetLevelStats(1)
	if stats == nil {
		t.Fatal("stats nil")
	}
	if stats.CopyPending != -1 {
		t.Errorf("CopyPending after pending->in_progress = %d, want -1", stats.CopyPending)
	}
	nc.RecordCopyTransition(1, db.CopyStatusInProgress, db.CopyStatusSuccessful)
	if stats.CopySuccessful != 1 {
		t.Errorf("CopySuccessful = %d, want 1", stats.CopySuccessful)
	}
}

func TestNodeCache_LevelDepths(t *testing.T) {
	nc := NewNodeCache()
	if len(nc.LevelDepths()) != 0 {
		t.Errorf("empty LevelDepths = %v", nc.LevelDepths())
	}
	nc.EnsureLevel(3)
	nc.EnsureLevel(1)
	nc.EnsureLevel(2)
	deps := nc.LevelDepths()
	if len(deps) != 3 || deps[0] != 1 || deps[1] != 2 || deps[2] != 3 {
		t.Errorf("LevelDepths = %v", deps)
	}
}

func TestLevelCache_ListChildrenByParentPath_GetByPath(t *testing.T) {
	lc := NewLevelCache()
	// Root-level nodes (parentPath ""); not indexed in ByParentPath per plan
	lc.Put("id1", &db.NodeState{ID: "id1", Path: "/a", ParentPath: "", Type: "folder"})
	lc.Put("id2", &db.NodeState{ID: "id2", Path: "/b", ParentPath: "", Type: "folder"})
	// Children of /a
	lc.Put("id3", &db.NodeState{ID: "id3", Path: "/a/x", ParentPath: "/a", Type: "file"})
	lc.Put("id4", &db.NodeState{ID: "id4", Path: "/a/y", ParentPath: "/a", Type: "file"})
	// Children of /b
	lc.Put("id5", &db.NodeState{ID: "id5", Path: "/b/z", ParentPath: "/b", Type: "file"})

	childrenRoot := lc.ListChildrenByParentPath("")
	if childrenRoot != nil {
		t.Errorf("ListChildrenByParentPath(\"\") = %v, want nil (root not indexed)", childrenRoot)
	}
	childrenA := lc.ListChildrenByParentPath("/a")
	if len(childrenA) != 2 {
		t.Errorf("ListChildrenByParentPath(\"/a\") len = %d, want 2", len(childrenA))
	}
	childrenB := lc.ListChildrenByParentPath("/b")
	if len(childrenB) != 1 {
		t.Errorf("ListChildrenByParentPath(\"/b\") len = %d, want 1", len(childrenB))
	}
	childrenMissing := lc.ListChildrenByParentPath("/none")
	if childrenMissing != nil {
		t.Errorf("ListChildrenByParentPath(\"/none\") = %v, want nil", childrenMissing)
	}

	if got := lc.GetByPath("/a"); got == nil || got.ID != "id1" {
		t.Errorf("GetByPath(\"/a\") = %v, want node id1", got)
	}
	if got := lc.GetByPath("/a/x"); got == nil || got.ID != "id3" {
		t.Errorf("GetByPath(\"/a/x\") = %v, want node id3", got)
	}
	if lc.GetByPath("/missing") != nil {
		t.Error("GetByPath(\"/missing\") should be nil")
	}

	// Overwrite node: indexes must stay consistent (same path/parentPath)
	lc.Put("id3", &db.NodeState{ID: "id3", Path: "/a/x", ParentPath: "/a", Type: "file", CopyStatus: "done"})
	childrenA2 := lc.ListChildrenByParentPath("/a")
	if len(childrenA2) != 2 {
		t.Errorf("after overwrite ListChildrenByParentPath(\"/a\") len = %d, want 2", len(childrenA2))
	}
	if got := lc.GetByPath("/a/x"); got == nil || got.CopyStatus != "done" {
		t.Errorf("GetByPath(\"/a/x\") after overwrite = %v", got)
	}

	lc.Clear()
	if lc.ListChildrenByParentPath("/a") != nil {
		t.Error("ListChildrenByParentPath after Clear should return nil")
	}
	if lc.GetByPath("/a") != nil {
		t.Error("GetByPath after Clear should return nil")
	}
}

func TestLevelCache_ListPending_statusSets(t *testing.T) {
	lc := NewLevelCache()
	lc.Put("a", &db.NodeState{ID: "a", Path: "/a", TraversalStatus: db.StatusPending})
	lc.Put("b", &db.NodeState{ID: "b", Path: "/b", TraversalStatus: db.StatusPending})
	lc.Put("c", &db.NodeState{ID: "c", Path: "/c", TraversalStatus: db.StatusPending})

	pending := lc.ListPending("", 10)
	if len(pending) != 3 {
		t.Errorf("ListPending len = %d, want 3", len(pending))
	}
	lc.UpdateStatus("b", db.StatusSuccessful, "")
	pending2 := lc.ListPending("", 10)
	if len(pending2) != 2 {
		t.Errorf("after UpdateStatus ListPending len = %d, want 2", len(pending2))
	}
	for _, n := range pending2 {
		if n.ID == "b" {
			t.Error("ListPending should not return b after successful")
		}
	}
	// Keyset: afterID "b" should return only c
	pending3 := lc.ListPending("b", 10)
	if len(pending3) != 1 || pending3[0].ID != "c" {
		t.Errorf("ListPending(afterID b) = %v, want [c]", pending3)
	}
}

func TestLevelCache_ListPendingCopy_statusSets(t *testing.T) {
	lc := NewLevelCache()
	lc.Put("f1", &db.NodeState{ID: "f1", Path: "/f1", Type: db.NodeTypeFolder, CopyStatus: db.CopyStatusPending})
	lc.Put("f2", &db.NodeState{ID: "f2", Path: "/f2", Type: db.NodeTypeFolder, CopyStatus: db.CopyStatusPending})
	lc.Put("x1", &db.NodeState{ID: "x1", Path: "/x1", Type: db.NodeTypeFile, CopyStatus: db.CopyStatusPending})

	folderPending := lc.ListPendingCopy("", 10, db.NodeTypeFolder)
	if len(folderPending) != 2 {
		t.Errorf("ListPendingCopy(folder) len = %d, want 2", len(folderPending))
	}
	lc.UpdateStatus("f1", "", db.CopyStatusSuccessful)
	folderPending2 := lc.ListPendingCopy("", 10, db.NodeTypeFolder)
	if len(folderPending2) != 1 || folderPending2[0].ID != "f2" {
		t.Errorf("after copy success ListPendingCopy(folder) = %v, want [f2]", folderPending2)
	}
}
