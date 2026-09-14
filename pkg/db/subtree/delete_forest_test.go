// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package subtree

import (
	"path/filepath"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestNormalizeDeleteForestOps(t *testing.T) {
	dir := t.TempDir()
	database, err := db.Open(db.Options{
		Path:   filepath.Join(dir, "delete-forest.db"),
		OpsDir: filepath.Join(dir, "delete-forest.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Fatal("expected ops store")
	}

	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir")
	fileID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/a.txt")
	siblingID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt")

	writes := []opsdb.SealNodeWrite{
		{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{ID: folderID, Path: "/dir", Name: "dir", Type: opsdb.NodeTypeFolder},
			Status: opsdb.StatusRecord{
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusSuccessful,
				DeleteStatus:    db.DeleteStatusPendingExplicit,
			},
			Depth:      1,
			InsertOnly: true,
		},
		{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{ID: fileID, Path: "/dir/a.txt", Name: "a.txt", Type: opsdb.NodeTypeFile, ParentID: folderID, Size: 10},
			Status: opsdb.StatusRecord{
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusSuccessful,
				DeleteStatus:    db.DeleteStatusPendingExplicit,
			},
			Depth:      2,
			InsertOnly: true,
		},
		{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{ID: siblingID, Path: "/b.txt", Name: "b.txt", Type: opsdb.NodeTypeFile, Size: 5},
			Status: opsdb.StatusRecord{
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusSuccessful,
				DeleteStatus:    db.DeleteStatusPendingExplicit,
			},
			Depth:      1,
			InsertOnly: true,
		},
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if err := database.NormalizeDeleteForestOps(); err != nil {
		t.Fatal(err)
	}

	stMap, err := database.Ops().BatchGetStatus(opsdb.SideSRC, []string{folderID, fileID, siblingID})
	if err != nil {
		t.Fatal(err)
	}
	if stMap[folderID].DeleteStatus != db.DeleteStatusPendingExplicit {
		t.Fatalf("folder=%q want pending_explicit", stMap[folderID].DeleteStatus)
	}
	if stMap[fileID].DeleteStatus != db.DeleteStatusPendingInherited {
		t.Fatalf("file=%q want pending_inherited", stMap[fileID].DeleteStatus)
	}
	if stMap[siblingID].DeleteStatus != db.DeleteStatusPendingExplicit {
		t.Fatalf("sibling=%q want pending_explicit", stMap[siblingID].DeleteStatus)
	}
}
