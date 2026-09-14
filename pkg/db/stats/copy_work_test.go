package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func insertSrcCopyNode(t *testing.T, database *db.DB, n *db.NodeState) {
	t.Helper()
	if n.TraversalStatus == "" {
		n.TraversalStatus = db.StatusPending
	}
	if n.CopyStatus == "" {
		n.CopyStatus = db.CopyStatusPending
	}
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: n.Depth, Status: n.TraversalStatus, State: n,
	}}); err != nil {
		t.Fatal(err)
	}
}

// markSrcCopyStatusAtDepth adjusts depth counters when a node's copy status changes in tests.
func markSrcCopyStatusAtDepth(t *testing.T, database *db.DB, n *db.NodeState, from, to string) {
	t.Helper()
	nt := db.NormalizeQueueNodeType(n.Type)
	deltas := []db.DepthStatsDelta{
		{Table: "SRC", Depth: n.Depth, Key: db.StatsKeyTyped(db.StatsKindCopy, from, nt), Delta: -1},
		{Table: "SRC", Depth: n.Depth, Key: db.StatsKeyTyped(db.StatsKindCopy, to, nt), Delta: 1},
	}
	if nt == db.NodeTypeFile && n.Size > 0 {
		deltas = append(deltas,
			db.DepthStatsDelta{Table: "SRC", Depth: n.Depth, Key: db.StatsKeyCopyFileBytes(from), Delta: -n.Size},
			db.DepthStatsDelta{Table: "SRC", Depth: n.Depth, Key: db.StatsKeyCopyFileBytes(to), Delta: n.Size},
		)
	}
	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		st = opsdb.StatusRecord{TraversalStatus: n.TraversalStatus}
	}
	st.CopyStatus = to
	if err := database.Ops().PutStatus(opsdb.SideSRC, n.ID, st); err != nil {
		t.Fatal(err)
	}
	if err := database.ApplyDepthStatsDeltas(deltas); err != nil {
		t.Fatal(err)
	}
}

func countCopyWorkEligibleUnderPath(t *testing.T, database *db.DB, rootPath string) db.DepthWorkAbsolute {
	t.Helper()
	ids, err := database.Ops().ListSubtreeIDs(opsdb.SideSRC, rootPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	nodes, statuses, err := database.Ops().BatchGetNodeStatus(opsdb.SideSRC, ids)
	if err != nil {
		t.Fatal(err)
	}
	var out db.DepthWorkAbsolute
	for id, n := range nodes {
		st := statuses[id]
		if st.TraversalStatus == db.StatusExcluded || st.TraversalStatus == db.StatusExclusionInherited {
			continue
		}
		if st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited {
			continue
		}
		if st.CopyStatus == db.CopyStatusAlreadyExisted {
			continue
		}
		switch db.NormalizeQueueNodeType(n.Type) {
		case db.NodeTypeFolder:
			out.Folders++
		case db.NodeTypeFile:
			out.Files++
			out.Bytes += n.Size
		}
	}
	return out
}

func TestSrcDiscoverOmitsExcluded(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/src-discover-excl.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/keep"), Path: "/keep", ParentPath: "/", Name: "keep",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/skip"), Path: "/skip", ParentPath: "/", Name: "skip",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusExcluded, CopyStatus: db.CopyStatusExcludedExplicit,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 10,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/skip.txt"), Path: "/skip.txt", ParentPath: "/", Name: "skip.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 99,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit,
	})

	got, err := copyWorkAbsoluteFromSrcStats(database, 1, copyDiscoveredStatuses)
	if err != nil {
		t.Fatal(err)
	}
	if got.Folders != 1 || got.Files != 1 || got.Bytes != 10 {
		t.Fatalf("discovered=%+v want folders=1 files=1 bytes=10", got)
	}

	delta, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Folders != 1 || delta.Files != 1 || delta.Bytes != 10 {
		t.Fatalf("src delta=%+v want folders=1 files=1 bytes=10", delta)
	}
}

func TestSrcDiscoverThenDstAECorrection(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/src-dst-seal.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 250,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	})

	srcDelta, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover)
	if err != nil {
		t.Fatal(err)
	}
	if srcDelta.Folders != 1 || srcDelta.Files != 2 || srcDelta.Bytes != 350 {
		t.Fatalf("src delta=%+v want folders=1 files=2 bytes=350", srcDelta)
	}

	dstDelta, err := SealDstCopyWorkAlreadyExistedCorrection(database, 1, db.CopyWorkReasonDstAECorrection)
	if err != nil {
		t.Fatal(err)
	}
	if dstDelta.Folders != 0 || dstDelta.Files != -1 || dstDelta.Bytes != -250 {
		t.Fatalf("dst delta=%+v want files=-1 bytes=-250", dstDelta)
	}

	totals, err := ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Folders != 1 || totals.Files != 1 || totals.Bytes != 100 {
		t.Fatalf("net totals=%+v want folders=1 files=1 bytes=100", totals)
	}

	if _, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover); err != nil {
		t.Fatal(err)
	}
	if _, err := SealDstCopyWorkAlreadyExistedCorrection(database, 1, db.CopyWorkReasonDstAECorrection); err != nil {
		t.Fatal(err)
	}
	totals2, err := ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals2 != totals {
		t.Fatalf("re-seal changed totals: %+v vs %+v", totals2, totals)
	}
}

func TestSrcDiscoverRetryGrowsThenDstCorrects(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/retry-seal.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	a := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 10,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	insertSrcCopyNode(t, database, a)
	if _, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover); err != nil {
		t.Fatal(err)
	}

	b := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 40,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	insertSrcCopyNode(t, database, b)
	delta, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Files != 1 || delta.Bytes != 40 {
		t.Fatalf("retry src delta=%+v want files=1 bytes=40", delta)
	}

	markSrcCopyStatusAtDepth(t, database, a, db.CopyStatusPending, db.CopyStatusAlreadyExisted)
	markSrcCopyStatusAtDepth(t, database, b, db.CopyStatusPending, db.CopyStatusAlreadyExisted)

	corr, err := SealDstCopyWorkAlreadyExistedCorrection(database, 1, db.CopyWorkReasonDstAECorrection)
	if err != nil {
		t.Fatal(err)
	}
	if corr.Files != -2 || corr.Bytes != -50 {
		t.Fatalf("dst corr=%+v want files=-2 bytes=-50", corr)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Files != 0 || totals.Bytes != 0 {
		t.Fatalf("after full AE correction totals=%+v want 0", totals)
	}

	corr2, err := SealDstCopyWorkAlreadyExistedCorrection(database, 1, db.CopyWorkReasonDstAECorrection)
	if err != nil {
		t.Fatal(err)
	}
	if corr2.Files != 0 || corr2.Bytes != 0 {
		t.Fatalf("second dst corr=%+v want zero", corr2)
	}
}

func TestCountCopyWorkEligibleSubtreeNotExcluded(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-work-count.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/a.txt"), Path: "/dir/a.txt", ParentPath: "/dir", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 2, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/dir/b.txt"), Path: "/dir/b.txt", ParentPath: "/dir", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 2, Size: 250,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	})

	got := countCopyWorkEligibleUnderPath(t, database, "/dir")
	if got.Folders != 1 || got.Files != 1 || got.Bytes != 100 {
		t.Fatalf("got=%+v want folders=1 files=1 bytes=100", got)
	}
}

func TestAdjustCopyWorkForReviewOnExclude(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-work-excl.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 250,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})

	if _, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover); err != nil {
		t.Fatal(err)
	}
	if _, err := SealDstCopyWorkAlreadyExistedCorrection(database, 1, db.CopyWorkReasonDstAECorrection); err != nil {
		t.Fatal(err)
	}
	pre, err := ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if pre.Folders != 1 || pre.Files != 2 || pre.Bytes != 350 {
		t.Fatalf("pre-exclusion totals=%+v want folders=1 files=2 bytes=350", pre)
	}

	if err := AdjustCopyWorkForReview(database, db.DepthWorkAbsolute{Files: -1, Bytes: -250}, db.CopyWorkReasonReviewExclude); err != nil {
		t.Fatal(err)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Folders != 1 || totals.Files != 1 || totals.Bytes != 100 {
		t.Fatalf("post-exclude totals=%+v want folders=1 files=1 bytes=100", totals)
	}

	if err := AdjustCopyWorkForReview(database, db.DepthWorkAbsolute{Files: 1, Bytes: 250}, db.CopyWorkReasonReviewUnexclude); err != nil {
		t.Fatal(err)
	}
	restored, err := ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Folders != pre.Folders || restored.Files != pre.Files || restored.Bytes != pre.Bytes {
		t.Fatalf("post-unexclude totals=%+v want folders/files/bytes=%d/%d/%d", restored, pre.Folders, pre.Files, pre.Bytes)
	}
}

func TestSnapshotDeleteWorkAtPhaseStart(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/delete-work.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedSrcDepthStats(t, database, []*db.NodeState{
		{
			Type: db.NodeTypeFile, Depth: 1, Size: 100,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
			DeleteStatus: db.DeleteStatusPendingExplicit,
		},
	})

	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals.Files != 1 || totals.Bytes != 100 {
		t.Fatalf("delete totals=%+v", totals)
	}
	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals2, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals2 != totals {
		t.Fatalf("second snapshot changed totals: %+v vs %+v", totals2, totals)
	}
}
