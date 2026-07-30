package stats

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"testing"
	"time"
)

func insertSrcCopyNode(t *testing.T, database *db.DB, n *db.NodeState) {
	t.Helper()
	err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{n}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{{
				ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
				EventTime: time.Now().UnixNano(), Depth: n.Depth,
			}})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
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

	got, err := GetCopyDiscoveredAtDepth(database, 1)
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

	// 1 pending folder + 1 pending file + 1 already_existed file at depth 1.
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
	// Net: 1 folder + 1 file + 100 bytes (already_existed subtracted).
	if totals.Folders != 1 || totals.Files != 1 || totals.Bytes != 100 {
		t.Fatalf("net totals=%+v want folders=1 files=1 bytes=100", totals)
	}

	// Idempotent re-seal.
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

	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 10,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	if _, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover); err != nil {
		t.Fatal(err)
	}

	// Retry traversal discovers more at same depth.
	insertSrcCopyNode(t, database, &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 40,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	})
	delta, err := SealSrcCopyWorkDiscovered(database, 1, db.CopyWorkReasonSrcDiscover)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Files != 1 || delta.Bytes != 40 {
		t.Fatalf("retry src delta=%+v want files=1 bytes=40", delta)
	}

	// DST marks both already_existed on a later pass.
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			t0 := time.Now().UnixNano()
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted, EventTime: t0, Depth: 1},
				{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted, EventTime: t0, Depth: 1},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

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

	// Second DST seal must not double-subtract.
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

	var got db.DepthWorkAbsolute
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			got, err2 = subtree.CountCopyWorkEligibleSubtreeNotExcluded(w, "/dir")
			return err2
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	// Folder + pending file; already_existed file omitted.
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

	// Discovery: 1 folder + 2 files (350 bytes).
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

	// Path review excludes the larger file — sealed copy_work must shrink immediately.
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

	// Unexclude restores the denominator.
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

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			n := &db.NodeState{
				ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
				Type: db.NodeTypeFile, Depth: 1, Size: 100,
				TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
			}
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{n}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{{
				ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
				DeleteStatus: n.DeleteStatus, EventTime: time.Now().UnixNano(), Depth: 1,
			}})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

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
