// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"strings"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func seedCollisionFixture(t *testing.T) (database *db.DB, srcRootID, srcID, dstID string) {
	t.Helper()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/collision.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "onedrive", "onedrive", false, nil); err != nil {
		t.Fatal(err)
	}
	var ok bool
	srcRootID, _, ok = pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	dstRootID, _, ok := pull.GetRootNode(database, "DST")
	if !ok {
		t.Fatal("missing DST root")
	}

	srcID = db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "Extra space ")
	srcPath := db.JoinIDPath("/", srcID)
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID:              srcID,
			ServiceID:       "src-extra",
			ParentID:        srcRootID,
			ParentPath:      "/",
			Path:            srcPath,
			Name:            "Extra space ",
			DisplayPath:     filter.DisplayPathForChild("/", 0, "Extra space "),
			Type:            types.NodeTypeFolder,
			Depth:           1,
			TraversalStatus: db.StatusSuccessful,
			CopyStatus:      db.CopyStatusPending,
			Status:          db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	dstID = db.MintNodeID("DST", dstRootID, types.NodeTypeFolder, "Extra Space")
	dstPath := db.JoinIDPath("/", dstID)
	if err := database.Ops().PutNode(opsdb.SideDST, opsdb.NodeRecord{
		ID: dstID, ServiceID: "dst-extra", ParentID: dstRootID, ParentPath: "/",
		Path: dstPath, Name: "Extra Space", Type: opsdb.NodeTypeFolder, Depth: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().PutStatus(opsdb.SideDST, dstID, opsdb.StatusRecord{
		TraversalStatus: db.StatusSuccessful,
	}); err != nil {
		t.Fatal(err)
	}

	issuesJSON := `[{"category":"SiblingCollision","part":"Extra Space","detail":"Extra Space","message":"collides with sibling \"Extra Space\""}]`
	if err := database.PutGPLIssue(srcID, db.GPLIssueStatusManualReview, "", issuesJSON, "", 1); err != nil {
		t.Fatal(err)
	}
	return database, srcRootID, srcID, dstID
}

func TestRematchPendingGPLCollisions(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/rematch.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "onedrive", "onedrive", false, nil); err != nil {
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

	srcID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "cascade*")
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID: srcID, ServiceID: "src-cascade", ParentID: srcRootID, ParentPath: "/",
			Path: db.JoinIDPath("/", srcID), Name: "cascade*", Type: types.NodeTypeFolder, Depth: 1,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, Status: db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	dstID := db.MintNodeID("DST", dstRootID, types.NodeTypeFolder, "cascade")
	if err := database.Ops().PutNode(opsdb.SideDST, opsdb.NodeRecord{
		ID: dstID, ServiceID: "dst-cascade", ParentID: dstRootID, ParentPath: "/",
		Path: db.JoinIDPath("/", dstID), Name: "cascade", Type: opsdb.NodeTypeFolder, Depth: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().PutStatus(opsdb.SideDST, dstID, opsdb.StatusRecord{
		TraversalStatus: db.StatusSuccessful,
	}); err != nil {
		t.Fatal(err)
	}
	// Soft pending proposal only (no SiblingCollision) — what DST compare can miss.
	softIssues := `[{"category":"InvalidChar","part":"*","detail":"*","message":"Removed invalid characters."}]`
	if err := database.PutGPLIssue(srcID, db.GPLIssueStatusPending, "cascade", softIssues, "", 1); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "onedrive", "onedrive")
	n, err := m.RematchPendingGPLCollisions()
	if err != nil {
		t.Fatalf("RematchPendingGPLCollisions: %v", err)
	}
	if n != 1 {
		t.Fatalf("upgraded=%d want 1", n)
	}
	rec, ok, err := database.Ops().GetGPL(srcID)
	if err != nil || !ok {
		t.Fatalf("gpl: ok=%v err=%v", ok, err)
	}
	if rec.Status != db.GPLIssueStatusManualReview {
		t.Fatalf("status=%q want manual_review", rec.Status)
	}
	if rec.ProposedName != "cascade" {
		t.Fatalf("proposed=%q want cascade", rec.ProposedName)
	}
	if PathCollisionSibling(rec.IssuesJSON) != "cascade" {
		t.Fatalf("issues=%s want SiblingCollision cascade", rec.IssuesJSON)
	}
}

func TestAcceptUniquePathName_AvoidsEqualFoldClash(t *testing.T) {
	database, _, srcID, _ := seedCollisionFixture(t)

	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "onedrive", "onedrive")
	preview, err := m.PreviewUniquePathName(srcID)
	if err != nil {
		t.Fatalf("PreviewUniquePathName: %v", err)
	}
	if strings.EqualFold(preview, "Extra Space") {
		t.Fatalf("preview still clashes with DST: %q", preview)
	}
	name, err := m.AcceptUniquePathName(srcID, "")
	if err != nil {
		t.Fatalf("AcceptUniquePathName: %v", err)
	}
	if name != preview {
		t.Fatalf("accepted=%q preview=%q", name, preview)
	}
	if strings.EqualFold(name, "Extra Space") {
		t.Fatalf("unique name still clashes with DST: %q", name)
	}
	if !strings.Contains(name, "(") {
		t.Fatalf("expected numbered unique suffix, got %q", name)
	}
}

func TestSetGPLSubtreeIgnored_RefusesManualReview(t *testing.T) {
	database, _, srcID, _ := seedCollisionFixture(t)
	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "onedrive", "onedrive")
	err := m.SetGPLSubtreeIgnored(srcID, true)
	if err == nil {
		t.Fatal("expected ignore of manual_review to fail")
	}
}

func TestPreviewPathResolutionCascade_FileOverwriteWinner(t *testing.T) {
	database, srcRootID, srcFolderID, dstFolderID := seedCollisionFixture(t)
	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "onedrive", "onedrive")

	older := "2020-01-01T00:00:00Z"
	newer := "2024-06-01T00:00:00Z"
	srcFile := db.MintNodeID("SRC", srcFolderID, types.NodeTypeFile, "report.txt")
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     2,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID: srcFile, ServiceID: "src-report", ParentID: srcFolderID,
			ParentPath: db.JoinIDPath("/", srcFolderID), Path: db.JoinIDPath(db.JoinIDPath("/", srcFolderID), srcFile),
			Name: "report.txt", Type: types.NodeTypeFile, Depth: 2, Size: 10, MTime: newer,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, Status: db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	_ = srcRootID
	dstFile := db.MintNodeID("DST", dstFolderID, types.NodeTypeFile, "report.txt")
	if err := database.Ops().PutNode(opsdb.SideDST, opsdb.NodeRecord{
		ID: dstFile, ServiceID: "dst-report", ParentID: dstFolderID,
		ParentPath: db.JoinIDPath("/", dstFolderID), Path: db.JoinIDPath(db.JoinIDPath("/", dstFolderID), dstFile),
		Name: "report.txt", Type: opsdb.NodeTypeFile, Depth: 2, Size: 10, MTime: older,
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().PutStatus(opsdb.SideDST, dstFile, opsdb.StatusRecord{
		TraversalStatus: db.StatusNotOnSrc,
	}); err != nil {
		t.Fatal(err)
	}

	prev, err := m.PreviewPathResolutionCascade(srcFolderID, PathCascadeActionRemap, "Extra Space")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if prev.FileOverwriteCount != 1 {
		t.Fatalf("overwrite count=%d want 1", prev.FileOverwriteCount)
	}
	if prev.FileOverwrites[0].DefaultWinner != "src" {
		t.Fatalf("winner=%q want src (newer SRC mtime)", prev.FileOverwrites[0].DefaultWinner)
	}
}

func TestPreviewPathResolutionCascade_CaseFoldNestedOverwrite(t *testing.T) {
	database, _, srcFolderID, dstFolderID := seedCollisionFixture(t)
	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "onedrive", "onedrive")

	srcChild := db.MintNodeID("SRC", srcFolderID, types.NodeTypeFolder, "Docs")
	srcChildPath := db.JoinIDPath(db.JoinIDPath("/", srcFolderID), srcChild)
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     2,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID: srcChild, ServiceID: "src-nested", ParentID: srcFolderID,
			ParentPath: db.JoinIDPath("/", srcFolderID), Path: srcChildPath,
			Name: "Docs", Type: types.NodeTypeFolder, Depth: 2,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, Status: db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	dstChild := db.MintNodeID("DST", dstFolderID, types.NodeTypeFolder, "docs")
	if err := database.Ops().PutNode(opsdb.SideDST, opsdb.NodeRecord{
		ID: dstChild, ServiceID: "dst-nested", ParentID: dstFolderID,
		ParentPath: db.JoinIDPath("/", dstFolderID), Path: db.JoinIDPath(db.JoinIDPath("/", dstFolderID), dstChild),
		Name: "docs", Type: opsdb.NodeTypeFolder, Depth: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().PutStatus(opsdb.SideDST, dstChild, opsdb.StatusRecord{
		TraversalStatus: db.StatusNotOnSrc,
	}); err != nil {
		t.Fatal(err)
	}

	srcFile := db.MintNodeID("SRC", srcChild, types.NodeTypeFile, "Notes.txt")
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     3,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID: srcFile, ServiceID: "src-notes", ParentID: srcChild,
			ParentPath: srcChildPath, Path: db.JoinIDPath(srcChildPath, srcFile),
			Name: "Notes.txt", Type: types.NodeTypeFile, Depth: 3, Size: 3, MTime: "2024-01-01T00:00:00Z",
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, Status: db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	dstFile := db.MintNodeID("DST", dstChild, types.NodeTypeFile, "notes.txt")
	if err := database.Ops().PutNode(opsdb.SideDST, opsdb.NodeRecord{
		ID: dstFile, ServiceID: "dst-notes", ParentID: dstChild,
		ParentPath: db.JoinIDPath(db.JoinIDPath("/", dstFolderID), dstChild),
		Path:       db.JoinIDPath(db.JoinIDPath(db.JoinIDPath("/", dstFolderID), dstChild), dstFile),
		Name:       "notes.txt", Type: opsdb.NodeTypeFile, Depth: 3, Size: 3, MTime: "2020-01-01T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().PutStatus(opsdb.SideDST, dstFile, opsdb.StatusRecord{
		TraversalStatus: db.StatusNotOnSrc,
	}); err != nil {
		t.Fatal(err)
	}

	prev, err := m.PreviewPathResolutionCascade(srcFolderID, PathCascadeActionRemap, "Extra Space")
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if prev.FolderCollisionCount != 0 {
		t.Fatalf("folder collisions=%d want 0 (case-fold folders recurse as peers)", prev.FolderCollisionCount)
	}
	if prev.FileOverwriteCount != 1 {
		t.Fatalf("file overwrites=%d want 1 (nested under Docs/docs)", prev.FileOverwriteCount)
	}
}

func TestPreviewPathResolutionCascade_RejectsEmptyAction(t *testing.T) {
	database, _, srcFolderID, _ := seedCollisionFixture(t)
	m := &Migration{DB: database}
	m.SetPathCheckProviders("local", "onedrive", "onedrive")
	if _, err := m.PreviewPathResolutionCascade(srcFolderID, "", ""); err == nil {
		t.Fatal("expected empty cascade action to be rejected")
	}
	if _, err := m.PreviewPathResolutionCascade(srcFolderID, "merge", ""); err == nil {
		t.Fatal("expected merge cascade action to be rejected")
	}
}
