// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
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

	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep, "", "", "", false, nil); err != nil {
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
	if err != nil {
		t.Fatalf("skip folder lookup: %v", err)
	}
	if skip != nil {
		t.Fatalf("excluded folder should be omitted (silent), got trav=%s", skip.TraversalStatus)
	}
	if got := mustIncludeOnly(t, database, srcRootID); len(got) != 2 {
		t.Fatalf("root include_only=%v want keep+file service ids", got)
	} else {
		if _, ok := got["a"]; !ok {
			t.Fatalf("include_only missing keep id a: %v", got)
		}
		if _, ok := got["c"]; !ok {
			t.Fatalf("include_only missing file id c: %v", got)
		}
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

	pending, err := database.Ops().GetStat(db.ReviewKeyCopyPending)
	if err != nil || pending != 2 {
		t.Fatalf("copy/pending=%d err=%v want 2 (keep folder + file, counted once)", pending, err)
	}
}

func TestSeedRootTasksWithPreparation_excludedFileOmitted(t *testing.T) {
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
	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep, "", "", "", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	fileID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFile, "skip.txt")
	file, err := pull.GetNodeByID(database, "SRC", fileID)
	if err != nil {
		t.Fatalf("excluded file lookup: %v", err)
	}
	if file != nil {
		t.Fatalf("excluded file should be omitted, got trav=%s", file.TraversalStatus)
	}
	allow := mustIncludeOnly(t, database, srcRootID)
	if _, ok := allow["keep"]; !ok || len(allow) != 1 {
		t.Fatalf("include_only=%v want only keep", allow)
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

	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep, "", "", "", false, nil); err != nil {
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

	pending, err := database.Ops().GetStat(db.ReviewKeyCopyPending)
	if err != nil || pending != 0 {
		t.Fatalf("copy/pending=%d err=%v want 0 (shared folder already_existed)", pending, err)
	}
}

func TestSeedRootTasksWithPreparation_bothPreparedCopyPendingOnce(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prep-both-pending.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	src := types.Folder{ServiceID: "src-root", DisplayName: "A", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "B", LocationPath: "/", Type: types.NodeTypeFolder}
	prep := RootPreparation{
		SourcePrepared: true,
		DestPrepared:   true,
		SourceChildren: []RootChildSeed{
			{ServiceID: "dir_empty", Name: "dir_empty", Type: "folder"},
			{ServiceID: "dir_shared", Name: "dir_shared", Type: "folder"},
			{ServiceID: "dir_mid", Name: "dir_mid", Type: "folder"},
			{ServiceID: "src_only_dir", Name: "src_only_dir", Type: "folder"},
			{ServiceID: "shared_alpha.txt", Name: "shared_alpha.txt", Type: "file", Size: 13},
			{ServiceID: "src_only_1.txt", Name: "src_only_1.txt", Type: "file", Size: 11},
		},
		DestChildren: []RootChildSeed{
			{ServiceID: "dir_empty", Name: "dir_empty", Type: "folder"},
			{ServiceID: "dir_shared", Name: "dir_shared", Type: "folder"},
			{ServiceID: "dir_mid", Name: "dir_mid", Type: "folder"},
			{ServiceID: "dst_only_dir", Name: "dst_only_dir", Type: "folder", DstOnly: true},
			{ServiceID: "shared_alpha.txt", Name: "shared_alpha.txt", Type: "file", Size: 13},
			{ServiceID: "dst_only_1.txt", Name: "dst_only_1.txt", Type: "file", Size: 11, DstOnly: true},
		},
	}

	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep, "", "", "", false, nil); err != nil {
		t.Fatal(err)
	}

	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	onlyDir := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "src_only_dir")
	onlyFile := db.MintNodeID("SRC", srcRootID, types.NodeTypeFile, "src_only_1.txt")
	sharedDir := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "dir_shared")
	onlyDirNode, err := pull.GetNodeByID(database, "SRC", onlyDir)
	if err != nil || onlyDirNode == nil {
		t.Fatalf("src_only_dir: %v", err)
	}
	if onlyDirNode.CopyStatus != db.CopyStatusPending {
		t.Fatalf("src_only_dir copy=%s want pending", onlyDirNode.CopyStatus)
	}
	onlyFileNode, err := pull.GetNodeByID(database, "SRC", onlyFile)
	if err != nil || onlyFileNode == nil {
		t.Fatalf("src_only_1.txt: %v", err)
	}
	if onlyFileNode.CopyStatus != db.CopyStatusPending {
		t.Fatalf("src_only_1.txt copy=%s want pending", onlyFileNode.CopyStatus)
	}
	sharedNode, err := pull.GetNodeByID(database, "SRC", sharedDir)
	if err != nil || sharedNode == nil {
		t.Fatalf("dir_shared: %v", err)
	}
	if sharedNode.CopyStatus != db.CopyStatusAlreadyExisted {
		t.Fatalf("dir_shared copy=%s want already_existed", sharedNode.CopyStatus)
	}

	pending, err := database.Ops().GetStat(db.ReviewKeyCopyPending)
	if err != nil || pending != 2 {
		t.Fatalf("copy/pending=%d err=%v want 2 (src_only_dir + src_only_1.txt, counted once)", pending, err)
	}
}

func TestSeedRootTasks_unprepared(t *testing.T) {
	database := db.TestOpen(t, "unprep")

	src := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(src, dst, database, RootPreparation{}, "", "", "", false, nil); err != nil {
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

func TestSeedRootTasksWithPreparation_preservesTrailingSpaceName(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/prep-space.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	src := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dst := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	prep := RootPreparation{
		SourcePrepared: true,
		SourceChildren: []RootChildSeed{
			{ServiceID: "space", Name: "Extra Space ", Type: "folder"},
		},
	}
	if _, err := SeedRootTasksWithPreparation(src, dst, database, prep, "local", "local", "windows", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	id := db.MintNodeID("SRC", srcRootID, types.NodeTypeFolder, "Extra Space ")
	node, err := pull.GetNodeByID(database, "SRC", id)
	if err != nil || node == nil {
		t.Fatalf("node: %v", err)
	}
	if node.Name != "Extra Space " {
		t.Fatalf("name=%q want trailing space preserved", node.Name)
	}
}

func TestSeedRootTasksWithPreparation_appliesWindowsGPL(t *testing.T) {
	t.Skip("GPL gated off on Badger branch (db.GPLDisabled)")
}

func TestComputeSourceStartRound(t *testing.T) {
	if got := ComputeSourceStartRound(nil); got != 1 {
		t.Fatalf("empty=%d", got)
	}
	kids := []RootChildSeed{
		{Name: "a", Type: "folder", ServiceID: "a"},
		{
			Name: "b", Type: "folder", ServiceID: "b",
			Children: []RootChildSeed{
				{Name: "c", Type: "folder", ServiceID: "c"},
			},
		},
	}
	if got := ComputeSourceStartRound(kids); got != 1 {
		t.Fatalf("got %d want 1 (pending leaf a at depth 1)", got)
	}
	deepOnly := []RootChildSeed{
		{
			Name: "b", Type: "folder", ServiceID: "b",
			Children: []RootChildSeed{
				{Name: "c", Type: "folder", ServiceID: "c"},
			},
		},
	}
	if got := ComputeSourceStartRound(deepOnly); got != 2 {
		t.Fatalf("got %d want 2", got)
	}
}

func mustIncludeOnly(t *testing.T, database *db.DB, nodeID string) map[string]struct{} {
	t.Helper()
	n, ok, err := database.Ops().GetNode(opsdb.SideSRC, nodeID)
	if err != nil || !ok {
		t.Fatalf("include_only for %s: ok=%v err=%v", nodeID, ok, err)
	}
	return queue.ParseIncludeOnlyJSON(n.IncludeOnly)
}
