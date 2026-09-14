// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// Review mutations must return API deltas scaled by the write-scan Affected count,
// not ±1 for the clicked root.
func TestExcludeSubtreeReviewDeltasScaleWithWriteScan(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/excl-deltas.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir")
	fileA := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/a.txt")
	fileB := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/b.txt")
	ops := []db.InsertOperation{
		{QueueType: "SRC", Level: 1, Status: db.StatusSuccessful, State: &db.NodeState{
			ID: folderID, Path: "/dir", ParentPath: "/", Name: "dir",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		}},
		{QueueType: "SRC", Level: 2, Status: db.StatusSuccessful, State: &db.NodeState{
			ID: fileA, Path: "/dir/a.txt", ParentPath: "/dir", Name: "a.txt", ParentID: folderID,
			Type: db.NodeTypeFile, Size: 10, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		}},
		{QueueType: "SRC", Level: 2, Status: db.StatusSuccessful, State: &db.NodeState{
			ID: fileB, Path: "/dir/b.txt", ParentPath: "/dir", Name: "b.txt", ParentID: folderID,
			Type: db.NodeTypeFile, Size: 20, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		}},
	}
	if err := database.SeedDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}

	store := newMigrationStore(database, nil)
	affected, deltas, err := store.setNodeExcludedWithPropagation(folderID, true)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 3 {
		t.Fatalf("affected=%d want 3", affected)
	}
	if deltas[DeltaCopyPending] != -3 || deltas[DeltaExcluded] != 3 {
		t.Fatalf("exclude status deltas = %#v want pending -3 excluded +3", deltas)
	}
	if deltas[DeltaFolders] != -1 || deltas[DeltaFiles] != -2 {
		t.Fatalf("exclude type deltas = %#v want folders -1 files -2", deltas)
	}
	if deltas[DeltaSizeSelected] != -30 {
		t.Fatalf("exclude sizeSelected=%d want -30", deltas[DeltaSizeSelected])
	}
}

func TestCopyRetrySubtreeReviewDeltasScaleWithWriteScan(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-retry-deltas.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir")
	fileA := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/a.txt")
	fileB := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/b.txt")
	ops := []db.InsertOperation{
		{QueueType: "SRC", Level: 1, Status: db.StatusSuccessful, State: &db.NodeState{
			ID: folderID, Path: "/dir", ParentPath: "/", Name: "dir",
			Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed,
		}},
		{QueueType: "SRC", Level: 2, Status: db.StatusSuccessful, State: &db.NodeState{
			ID: fileA, Path: "/dir/a.txt", ParentPath: "/dir", Name: "a.txt", ParentID: folderID,
			Type: db.NodeTypeFile, Size: 10, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed,
		}},
		{QueueType: "SRC", Level: 2, Status: db.StatusSuccessful, State: &db.NodeState{
			ID: fileB, Path: "/dir/b.txt", ParentPath: "/dir", Name: "b.txt", ParentID: folderID,
			Type: db.NodeTypeFile, Size: 20, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed,
		}},
	}
	if err := database.SeedDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}

	store := newMigrationStore(database, nil)
	affected, deltas, err := store.setNodeCopyStatus(folderID, db.CopyStatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 3 {
		t.Fatalf("affected=%d want 3", affected)
	}
	if deltas[DeltaCopyFailed] != -3 || deltas[DeltaCopyPending] != 3 {
		t.Fatalf("copy retry status deltas = %#v want failed -3 pending +3", deltas)
	}
	if deltas[DeltaCopyPendingRetry] != 3 {
		t.Fatalf("copyPendingRetry=%d want +3", deltas[DeltaCopyPendingRetry])
	}
	if deltas[DeltaFolders] != 1 || deltas[DeltaFiles] != 2 || deltas[DeltaSizeSelected] != 30 {
		t.Fatalf("copy retry selected deltas = %#v", deltas)
	}
}
