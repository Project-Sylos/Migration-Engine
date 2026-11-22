// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestApplyNodeRename_updatesDisplayPathAndTrigams(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/rename-dp.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ops := database.Ops()

	root := opsdb.NodeRecord{
		ID: "root", Path: "/", DisplayPath: "/", Name: "root",
		Type: opsdb.NodeTypeFolder, Depth: 0, ServiceID: "/data",
	}
	folder := opsdb.NodeRecord{
		ID: "folder", ParentID: "root", Path: "/id-folder", DisplayPath: "/Reports", Name: "Reports",
		Type: opsdb.NodeTypeFolder, Depth: 1, ServiceID: "/data/Reports", ParentServiceID: "/data",
	}
	child := opsdb.NodeRecord{
		ID: "child", ParentID: "folder", Path: "/id-folder/id-child",
		DisplayPath: "/Reports/Q1", Name: "Q1", Type: opsdb.NodeTypeFolder, Depth: 2,
		ServiceID: "/data/Reports/Q1", ParentServiceID: "/data/Reports",
	}
	file := opsdb.NodeRecord{
		ID: "file", ParentID: "child", Path: "/id-folder/id-child/id-file",
		DisplayPath: "/Reports/Q1/a.txt", Name: "a.txt", Type: opsdb.NodeTypeFile, Depth: 3,
		ServiceID: "/data/Reports/Q1/a.txt", ParentServiceID: "/data/Reports/Q1",
	}
	for _, n := range []opsdb.NodeRecord{root, folder, child, file} {
		if err := ops.PutNode(opsdb.SideSRC, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := ops.AppendKidTicket(opsdb.SideSRC, "root", 0, opsdb.KidRecord{
		ID: "folder", Name: "Reports", Type: opsdb.NodeTypeFolder, Path: folder.Path,
		ServiceID: folder.ServiceID,
	}); err != nil {
		t.Fatal(err)
	}

	if err := database.ApplyNodeRenameWithHooks(opsdb.SideSRC, "folder", "Docs", "/data/Reports", "/data/Docs", NodeRenameHooks{}); err != nil {
		t.Fatal(err)
	}

	gotFolder, ok, err := ops.GetNode(opsdb.SideSRC, "folder")
	if err != nil || !ok {
		t.Fatalf("folder ok=%v err=%v", ok, err)
	}
	if gotFolder.Name != "Docs" || gotFolder.DisplayPath != "/Docs" {
		t.Fatalf("folder name=%q display=%q", gotFolder.Name, gotFolder.DisplayPath)
	}
	if gotFolder.ServiceID != "/data/Docs" {
		t.Fatalf("folder service_id=%q", gotFolder.ServiceID)
	}

	gotChild, ok, err := ops.GetNode(opsdb.SideSRC, "child")
	if err != nil || !ok {
		t.Fatalf("child ok=%v err=%v", ok, err)
	}
	if gotChild.DisplayPath != "/Docs/Q1" {
		t.Fatalf("child display=%q", gotChild.DisplayPath)
	}
	if gotChild.ServiceID != "/data/Docs/Q1" || gotChild.ParentServiceID != "/data/Docs" {
		t.Fatalf("child service=%q parent=%q", gotChild.ServiceID, gotChild.ParentServiceID)
	}

	gotFile, ok, err := ops.GetNode(opsdb.SideSRC, "file")
	if err != nil || !ok {
		t.Fatalf("file ok=%v err=%v", ok, err)
	}
	if gotFile.DisplayPath != "/Docs/Q1/a.txt" {
		t.Fatalf("file display=%q", gotFile.DisplayPath)
	}
	if gotFile.ServiceID != "/data/Docs/Q1/a.txt" {
		t.Fatalf("file service=%q", gotFile.ServiceID)
	}

	packs, err := ops.BatchGetKids(opsdb.SideSRC, []string{"root"})
	if err != nil {
		t.Fatal(err)
	}
	kids := packs["root"]
	if len(kids) != 1 || kids[0].ID != "folder" || kids[0].Name != "Docs" {
		t.Fatalf("root kids pack=%+v want folder renamed Docs", kids)
	}
	if kids[0].ServiceID != "/data/Docs" {
		t.Fatalf("kids pack service_id=%q", kids[0].ServiceID)
	}

	oldIDs, err := ops.ScanTrigramIntersect(opsdb.SideSRC, "Reports", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range oldIDs {
		if id == "folder" || id == "child" || id == "file" {
			t.Fatalf("stale trigram hit for Reports still lists %s", id)
		}
	}
	newIDs, err := ops.ScanTrigramIntersect(opsdb.SideSRC, "Docs", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range newIDs {
		if id == "folder" || id == "child" || id == "file" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected Docs trigram hits, got %v", newIDs)
	}
}

func TestPutGPL_replacesStatusIndex(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/gplst.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ops := database.Ops()

	if err := ops.PutGPL(opsdb.GPLRecord{SrcID: "n1", Status: "pending", ProposedName: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := ops.PutGPL(opsdb.GPLRecord{SrcID: "n1", Status: "accepted", ProposedName: "b"}); err != nil {
		t.Fatal(err)
	}
	pending, err := ops.ListGPLByStatus("pending", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range pending {
		if id == "n1" {
			t.Fatal("pending gplst still lists accepted issue")
		}
	}
	accepted, err := ops.ListGPLByStatus("accepted", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 || accepted[0] != "n1" {
		t.Fatalf("accepted=%v", accepted)
	}
}
