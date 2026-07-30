// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestSeedRootTasksWithPreparation_srcPrepared(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prep-src.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	src := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	prep := RootPreparation{
		SourcePrepared: true,
		SourceChildren: []RootChildSeed{
			{ServiceID: "a", Name: "keep", Type: "folder"},
			{ServiceID: "b", Name: "skip", Type: "folder", Excluded: true},
			{ServiceID: "c", Name: "file.txt", Type: "file", Size: 10},
		},
	}

	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep); err != nil {
		t.Fatal(err)
	}

	srcRootID, srcRoot, ok := pull.GetRootNode(database, "SRC")
	if !ok || srcRoot == nil {
		t.Fatal("missing SRC root")
	}
	if srcRoot.TraversalStatus != db.StatusSuccessful {
		t.Fatalf("SRC root traversal=%s want successful", srcRoot.TraversalStatus)
	}
	_, dstRoot, ok := pull.GetRootNode(database, "DST")
	if !ok || dstRoot == nil {
		t.Fatal("missing DST root")
	}
	if dstRoot.TraversalStatus != db.StatusPending {
		t.Fatalf("DST root traversal=%s want pending", dstRoot.TraversalStatus)
	}

	keepID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "keep")
	keep, err := pull.GetNodeByID(database, "SRC", keepID)
	if err != nil || keep == nil {
		t.Fatalf("keep folder: %v", err)
	}
	if keep.TraversalStatus != db.StatusPending {
		t.Fatalf("keep traversal=%s want pending", keep.TraversalStatus)
	}

	skipID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "skip")
	skip, err := pull.GetNodeByID(database, "SRC", skipID)
	if err != nil || skip == nil {
		t.Fatalf("skip folder: %v", err)
	}
	if skip.TraversalStatus != db.StatusExcluded {
		t.Fatalf("excluded folder traversal=%s want excluded", skip.TraversalStatus)
	}
	if skip.CopyStatus != db.CopyStatusExcludedExplicit {
		t.Fatalf("excluded copy=%s", skip.CopyStatus)
	}

	fileID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFile, "file.txt")
	file, err := pull.GetNodeByID(database, "SRC", fileID)
	if err != nil || file == nil {
		t.Fatalf("file: %v", err)
	}
	if file.TraversalStatus != db.StatusSuccessful || file.CopyStatus != db.CopyStatusPending {
		t.Fatalf("file trav=%s copy=%s", file.TraversalStatus, file.CopyStatus)
	}

	if prep.SourceStartRound() != 1 || prep.DestStartRound() != 0 {
		t.Fatalf("rounds src=%d dst=%d", prep.SourceStartRound(), prep.DestStartRound())
	}

	// Skip folder never enters copy_work. SRC root is already_existed (not a copy task),
	// so AE nets it out; sealed work is keep folder + file only.
	totals, err := stats.ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Folders != 1 || totals.Files != 1 || totals.Bytes != 10 {
		t.Fatalf("copy_work totals=%+v want folders=1 files=1 bytes=10", totals)
	}
	// Re-seal is a no-op catch-up (same absolute, already credited via root_prep reasons).
	if _, err := stats.SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover); err != nil {
		t.Fatal(err)
	}
	totals2, err := stats.ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals2 != totals {
		t.Fatalf("catch-up changed totals: %+v vs %+v", totals2, totals)
	}
}

func TestSeedRootTasksWithPreparation_excludedFileStaysSuccessful(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prep-excl-file.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	src := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	prep := RootPreparation{
		SourcePrepared: true,
		SourceChildren: []RootChildSeed{
			{ServiceID: "keep", Name: "keep", Type: "folder"},
			{ServiceID: "skip.txt", Name: "skip.txt", Type: "file", Size: 3, Excluded: true},
		},
	}
	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	fileID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFile, "skip.txt")
	file, err := pull.GetNodeByID(database, "SRC", fileID)
	if err != nil || file == nil {
		t.Fatalf("excluded file: %v", err)
	}
	if file.TraversalStatus != db.StatusSuccessful {
		t.Fatalf("excluded file trav=%s want successful", file.TraversalStatus)
	}
	if file.CopyStatus != db.CopyStatusExcludedExplicit {
		t.Fatalf("excluded file copy=%s", file.CopyStatus)
	}
}

func TestSeedRootTasksWithPreparation_bothPreparedDstOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prep-both.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	src := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	prep := RootPreparation{
		SourcePrepared: true,
		DestPrepared:   true,
		SourceChildren: []RootChildSeed{
			{ServiceID: "a", Name: "shared", Type: "folder"},
		},
		DestChildren: []RootChildSeed{
			{ServiceID: "d1", Name: "shared", Type: "folder"},
			{ServiceID: "d2", Name: "only-dst", Type: "folder", DstOnly: true},
		},
	}

	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep); err != nil {
		t.Fatal(err)
	}

	dstRootID, dstRoot, ok := pull.GetRootNode(database, "DST")
	if !ok || dstRoot == nil {
		t.Fatal("missing DST root")
	}
	if dstRoot.TraversalStatus != db.StatusSuccessful {
		t.Fatalf("DST root=%s", dstRoot.TraversalStatus)
	}

	onlyID := db.MintNodeID("DST", dstRootID, types.NodeTypeFolder, "only-dst")
	only, err := pull.GetNodeByID(database, "DST", onlyID)
	if err != nil || only == nil {
		t.Fatalf("dst-only: %v", err)
	}
	if only.TraversalStatus != db.StatusNotOnSrc {
		t.Fatalf("dst-only trav=%s want not_on_src", only.TraversalStatus)
	}

	sharedID := db.MintNodeID("DST", dstRootID, types.NodeTypeFolder, "shared")
	shared, err := pull.GetNodeByID(database, "DST", sharedID)
	if err != nil || shared == nil {
		t.Fatalf("shared: %v", err)
	}
	if shared.TraversalStatus != db.StatusPending {
		t.Fatalf("shared trav=%s", shared.TraversalStatus)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	srcShared := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "shared")
	mapped, err := pull.GetDstIDFromSrcID(database, srcShared)
	if err != nil {
		t.Fatalf("id_map lookup: %v", err)
	}
	if mapped != sharedID {
		t.Fatalf("id_map dst=%s want %s", mapped, sharedID)
	}
}

func TestSeedRootTasks_unprepared(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/unprep.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	src := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasks(src, dst, database); err != nil {
		t.Fatal(err)
	}
	srcRootID, srcRoot, ok := pull.GetRootNode(database, "SRC")
	if !ok || srcRoot == nil {
		t.Fatal("missing SRC root")
	}
	_ = srcRootID
	if srcRoot.TraversalStatus != db.StatusPending {
		t.Fatalf("unprepared SRC root=%s", srcRoot.TraversalStatus)
	}
}
