// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"strings"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestAcceptPathChange_pendingKeepsSRCName(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/accept-pending.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "local", "windows", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}

	childID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "Reports")
	childPath := db.JoinIDPath("/", childID)
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID:              childID,
			ServiceID:       "src-reports",
			ParentID:        srcRootID,
			ParentPath:      "/",
			Path:            childPath,
			Name:            "Reports",
			DisplayPath:     filter.DisplayPathForChild("/", 0, "Reports"),
			Type:            types.NodeTypeFolder,
			Depth:           1,
			TraversalStatus: db.StatusSuccessful,
			CopyStatus:      db.CopyStatusPending,
			Status:          db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "local", "windows")

	if err := m.AcceptPathChange(childID, "ReportsClean", false); err != nil {
		t.Fatalf("AcceptPathChange: %v", err)
	}
	node, err := pull.GetNodeByID(database, "SRC", childID)
	if err != nil || node == nil {
		t.Fatalf("reload: %v", err)
	}
	if node.Name != "Reports" {
		t.Fatalf("src name=%q want Reports (source must stay unchanged)", node.Name)
	}
	if node.DisplayPath != "/Reports" {
		t.Fatalf("displayPath=%q want /Reports", node.DisplayPath)
	}
	gpl, ok, err := database.Ops().GetGPL(childID)
	if err != nil || !ok {
		t.Fatalf("gpl: ok=%v err=%v", ok, err)
	}
	if gpl.ProposedName != "ReportsClean" {
		t.Fatalf("proposed=%q want ReportsClean", gpl.ProposedName)
	}
	if gpl.Status != db.GPLIssueStatusAccepted {
		t.Fatalf("status=%q want accepted", gpl.Status)
	}
	if gpl.DstAction != "" {
		t.Fatalf("pending rename must not set dst_action, got %q", gpl.DstAction)
	}
}

func TestAcceptPathChange_rejectsSuccessful(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/accept-ok.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "local", "windows", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	childID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFile, "done.txt")
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID:              childID,
			ServiceID:       "src-done",
			ParentID:        srcRootID,
			ParentPath:      "/",
			Path:            db.JoinIDPath("/", childID),
			Name:            "done.txt",
			DisplayPath:     "/done.txt",
			Type:            types.NodeTypeFile,
			Depth:           1,
			TraversalStatus: db.StatusSuccessful,
			CopyStatus:      db.CopyStatusSuccessful,
			Status:          db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "local", "windows")
	err = m.AcceptPathChange(childID, "renamed.txt", false)
	if err == nil || !strings.Contains(err.Error(), "cannot rename") {
		t.Fatalf("want cannot rename error, got %v", err)
	}
}

func TestAcceptPathChange_alreadyExistedSetsDstAction(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/accept-existed.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "local", "windows", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	dstRootID, _, ok := pull.GetRootNode(database, "DST")
	if !ok {
		t.Fatal("missing DST root")
	}

	srcID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "Shared")
	dstID := db.MintNodeID("DST", dstRootID, types.NodeTypeFolder, "Shared")
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{
		{
			QueueType: "SRC",
			Level:     1,
			Status:    db.StatusSuccessful,
			State: &db.NodeState{
				ID:              srcID,
				ServiceID:       "src-shared",
				ParentID:        srcRootID,
				ParentPath:      "/",
				Path:            db.JoinIDPath("/", srcID),
				Name:            "Shared",
				DisplayPath:     "/Shared",
				Type:            types.NodeTypeFolder,
				Depth:           1,
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusAlreadyExisted,
				Status:          db.StatusSuccessful,
			},
		},
		{
			QueueType: "DST",
			Level:     1,
			Status:    db.StatusSuccessful,
			State: &db.NodeState{
				ID:              dstID,
				ServiceID:       "dst-shared",
				ParentID:        dstRootID,
				ParentPath:      "/",
				Path:            db.JoinIDPath("/", dstID),
				Name:            "Shared",
				DisplayPath:     "/Shared",
				Type:            types.NodeTypeFolder,
				Depth:           1,
				TraversalStatus: db.StatusSuccessful,
				Status:          db.StatusSuccessful,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.SeedIDMapEvents([]db.IDMapEvent{{
		SrcInternalID: srcID,
		DstInternalID: dstID,
		Status:        db.IDMapStatusActive,
		Depth:         1,
	}}); err != nil {
		t.Fatal(err)
	}
	// Root pairing so DST sibling lookup can resolve the parent map.
	if err := database.SeedIDMapEvents([]db.IDMapEvent{{
		SrcInternalID: srcRootID,
		DstInternalID: dstRootID,
		Status:        db.IDMapStatusActive,
		Depth:         0,
	}}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "local", "windows")
	if err := m.AcceptPathChange(srcID, "SharedRenamed", false); err != nil {
		t.Fatalf("AcceptPathChange: %v", err)
	}
	gpl, ok, err := database.Ops().GetGPL(srcID)
	if err != nil || !ok {
		t.Fatalf("gpl: ok=%v err=%v", ok, err)
	}
	if gpl.DstAction != db.DstActionRename {
		t.Fatalf("dst_action=%q want %q", gpl.DstAction, db.DstActionRename)
	}
	node, err := pull.GetNodeByID(database, "SRC", srcID)
	if err != nil || node == nil {
		t.Fatalf("reload: %v", err)
	}
	if node.Name != "Shared" {
		t.Fatalf("src name=%q want Shared (source must stay unchanged)", node.Name)
	}
	if gpl.ProposedName != "SharedRenamed" {
		t.Fatalf("proposed=%q want SharedRenamed", gpl.ProposedName)
	}
}

func TestResetPathRemap_undoesAcceptedRename(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/reset-rename.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "local", "windows", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	childID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "Reports")
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID: childID, ServiceID: "src-reports", ParentID: srcRootID, ParentPath: "/",
			Path: db.JoinIDPath("/", childID), Name: "Reports",
			DisplayPath: filter.DisplayPathForChild("/", 0, "Reports"),
			Type:        types.NodeTypeFolder, Depth: 1,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, Status: db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "local", "windows")
	if err := m.AcceptPathChange(childID, "ReportsClean", false); err != nil {
		t.Fatalf("AcceptPathChange: %v", err)
	}
	if err := m.ResetPathRemap(childID); err != nil {
		t.Fatalf("ResetPathRemap: %v", err)
	}
	if _, ok, _ := database.Ops().GetGPL(childID); ok {
		// Sweep may recreate a soft pending issue; accepted rename must be gone.
		rec, _, _ := database.Ops().GetGPL(childID)
		if rec.Status == db.GPLIssueStatusAccepted {
			t.Fatalf("still accepted after reset: %+v", rec)
		}
	}
	node, err := pull.GetNodeByID(database, "SRC", childID)
	if err != nil || node == nil {
		t.Fatalf("reload: %v", err)
	}
	if node.Name != "Reports" {
		t.Fatalf("src name changed to %q", node.Name)
	}
}
