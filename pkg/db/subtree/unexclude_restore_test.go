package subtree_test

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
)

func TestUnexcludeRestoresOnlyPendingOrigin(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/unexclude-restore.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/")
	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	failedID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/failed.txt")
	okID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/ok.txt")
	pendingID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/pending.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{
				{ID: rootID, Path: "/", Type: db.NodeTypeFolder, Depth: 0},
				{ID: folderID, Path: "/folder", ParentPath: "/", ParentID: rootID, Name: "folder", Type: db.NodeTypeFolder, Depth: 1},
				{ID: failedID, Path: "/folder/failed.txt", ParentPath: "/folder", ParentID: folderID, Name: "failed.txt", Type: db.NodeTypeFile, Depth: 2, Size: 10},
				{ID: okID, Path: "/folder/ok.txt", ParentPath: "/folder", ParentID: folderID, Name: "ok.txt", Type: db.NodeTypeFile, Depth: 2, Size: 20},
				{ID: pendingID, Path: "/folder/pending.txt", ParentPath: "/folder", ParentID: folderID, Name: "pending.txt", Type: db.NodeTypeFile, Depth: 2, Size: 30},
			}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: rootID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 0},
				{ID: folderID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 1},
				{ID: failedID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed, EventTime: t0, Depth: 2},
				{ID: okID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, EventTime: t0, Depth: 2},
				{ID: pendingID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	var excl subtree.SubtreeCopyMutationResult
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			excl, err2 = subtree.InsertExclusionEventsForSubtree(w, "SRC", "/folder")
			return err2
		})
	}); err != nil {
		t.Fatal(err)
	}
	// folder + pending only; failed/successful untouched
	if excl.Affected != 2 || excl.Folders != 1 || excl.Files != 1 || excl.PendingBytes != 30 {
		t.Fatalf("exclude mut=%+v want affected=2 folders=1 files=1 bytes=30", excl)
	}

	assertCopy := func(id, want string, wantExcluded bool) {
		t.Helper()
		n, err := pull.GetNodeByID(database, "SRC", id)
		if err != nil {
			t.Fatal(err)
		}
		if n == nil {
			t.Fatalf("node %s missing", id)
		}
		if n.CopyStatus != want {
			t.Fatalf("node %s copy_status=%q want %q", id, n.CopyStatus, want)
		}
		if n.Excluded != wantExcluded {
			t.Fatalf("node %s excluded=%v want %v", id, n.Excluded, wantExcluded)
		}
	}
	assertCopy(folderID, db.CopyStatusExcludedExplicit, true)
	assertCopy(pendingID, db.CopyStatusExcludedInherited, true)
	assertCopy(failedID, db.CopyStatusFailed, false)
	assertCopy(okID, db.CopyStatusSuccessful, false)

	var unex subtree.SubtreeCopyMutationResult
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			unex, err2 = subtree.InsertUnexcludeEventsForSubtree(w, "SRC", "/folder")
			return err2
		})
	}); err != nil {
		t.Fatal(err)
	}
	if unex.Affected != 2 || unex.PendingBytes != 30 {
		t.Fatalf("unexclude mut=%+v want affected=2 bytes=30", unex)
	}
	assertCopy(folderID, db.CopyStatusPending, false)
	assertCopy(pendingID, db.CopyStatusPending, false)
	assertCopy(failedID, db.CopyStatusFailed, false)
	assertCopy(okID, db.CopyStatusSuccessful, false)
}

func TestSetNodeExcludedFailedIsNoOp(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/unexclude-single.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/")
	fileID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/failed.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: rootID, Path: "/", Type: db.NodeTypeFolder, Depth: 0},
				{ID: fileID, Path: "/failed.txt", ParentPath: "/", ParentID: rootID, Name: "failed.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: rootID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 0},
				{ID: fileID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed, EventTime: t0, Depth: 1},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeExcluded("SRC", fileID, true)
		})
	}); err != nil {
		t.Fatal(err)
	}
	n, err := pull.GetNodeByID(database, "SRC", fileID)
	if err != nil {
		t.Fatal(err)
	}
	if n == nil || n.CopyStatus != db.CopyStatusFailed || n.Excluded {
		t.Fatalf("exclude failed node: copy=%q excluded=%v want failed untouched", n.CopyStatus, n.Excluded)
	}
}

func TestExcludeLeavesAlreadyExistedSibling(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/exclude-ae.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/")
	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	aeID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/ae.txt")
	pendingID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/pending.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: rootID, Path: "/", Type: db.NodeTypeFolder, Depth: 0},
				{ID: folderID, Path: "/folder", ParentPath: "/", ParentID: rootID, Name: "folder", Type: db.NodeTypeFolder, Depth: 1},
				{ID: aeID, Path: "/folder/ae.txt", ParentPath: "/folder", ParentID: folderID, Name: "ae.txt", Type: db.NodeTypeFile, Depth: 2, Size: 100},
				{ID: pendingID, Path: "/folder/pending.txt", ParentPath: "/folder", ParentID: folderID, Name: "pending.txt", Type: db.NodeTypeFile, Depth: 2, Size: 50},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: rootID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 0},
				{ID: folderID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted, EventTime: t0, Depth: 1},
				{ID: aeID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted, EventTime: t0, Depth: 2},
				{ID: pendingID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	var mut subtree.SubtreeCopyMutationResult
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			mut, err2 = subtree.InsertExclusionEventsForSubtree(w, "SRC", "/folder")
			return err2
		})
	}); err != nil {
		t.Fatal(err)
	}
	if mut.Affected != 1 || mut.Files != 1 || mut.PendingBytes != 50 {
		t.Fatalf("exclude mut=%+v want only pending file (bytes=50)", mut)
	}
	ae, _ := pull.GetNodeByID(database, "SRC", aeID)
	folder, _ := pull.GetNodeByID(database, "SRC", folderID)
	pending, _ := pull.GetNodeByID(database, "SRC", pendingID)
	if ae.CopyStatus != db.CopyStatusAlreadyExisted || ae.Excluded {
		t.Fatalf("AE sibling mutated: copy=%q excluded=%v", ae.CopyStatus, ae.Excluded)
	}
	if folder.CopyStatus != db.CopyStatusAlreadyExisted || folder.Excluded {
		t.Fatalf("AE folder mutated: copy=%q excluded=%v", folder.CopyStatus, folder.Excluded)
	}
	if pending.CopyStatus != db.CopyStatusExcludedInherited || !pending.Excluded {
		t.Fatalf("pending not excluded: copy=%q", pending.CopyStatus)
	}
}

func TestSetNodeCopyStatusRetryEligibility(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-retry.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/")
	okID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/ok.txt")
	failedID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/failed.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: rootID, Path: "/", Type: db.NodeTypeFolder, Depth: 0},
				{ID: okID, Path: "/ok.txt", ParentPath: "/", ParentID: rootID, Name: "ok.txt", Type: db.NodeTypeFile, Depth: 1},
				{ID: failedID, Path: "/failed.txt", ParentPath: "/", ParentID: rootID, Name: "failed.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: rootID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 0},
				{ID: okID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, EventTime: t0, Depth: 1},
				{ID: failedID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed, EventTime: t0, Depth: 1},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	countEvents := func(id string) int {
		t.Helper()
		conn, err := database.GetDB()
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if err := conn.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM src_status_events WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	beforeOK := countEvents(okID)
	beforeFailed := countEvents(failedID)

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.SetNodeCopyStatus("SRC", okID, db.CopyStatusPending); err != nil {
				return err
			}
			return w.SetNodeCopyStatus("SRC", failedID, db.CopyStatusPending)
		})
	}); err != nil {
		t.Fatal(err)
	}
	if countEvents(okID) != beforeOK {
		t.Fatalf("successful→pending should be no-op; events %d→%d", beforeOK, countEvents(okID))
	}
	if countEvents(failedID) != beforeFailed+1 {
		t.Fatalf("failed→pending should insert; events %d→%d", beforeFailed, countEvents(failedID))
	}
	n, _ := pull.GetNodeByID(database, "SRC", failedID)
	if n.CopyStatus != db.CopyStatusPending {
		t.Fatalf("failed retry copy=%q", n.CopyStatus)
	}
}

func TestSkipDeleteSubtreeAggregates(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/skip-delete.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	rootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/")
	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	okID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/ok.txt")
	pendingCopyID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/pending.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: rootID, Path: "/", Type: db.NodeTypeFolder, Depth: 0},
				{ID: folderID, Path: "/folder", ParentPath: "/", ParentID: rootID, Name: "folder", Type: db.NodeTypeFolder, Depth: 1},
				{ID: okID, Path: "/folder/ok.txt", ParentPath: "/folder", ParentID: folderID, Name: "ok.txt", Type: db.NodeTypeFile, Depth: 2, Size: 40},
				{ID: pendingCopyID, Path: "/folder/pending.txt", ParentPath: "/folder", ParentID: folderID, Name: "pending.txt", Type: db.NodeTypeFile, Depth: 2, Size: 99},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: rootID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending, EventTime: t0, Depth: 0},
				{ID: folderID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending, EventTime: t0, Depth: 1},
				{ID: okID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending, EventTime: t0, Depth: 2},
				{ID: pendingCopyID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, DeleteStatus: db.DeleteStatusPending, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	var mut subtree.SubtreeDeleteMutationResult
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			mut, err2 = subtree.InsertDeleteStatusEventsForSubtree(w, "/folder", db.DeleteStatusSkipped, db.SQLDeleteSubtreeSkipEligible)
			return err2
		})
	}); err != nil {
		t.Fatal(err)
	}
	// folder + ok (copy-complete); pending-copy ineligible
	if mut.Affected != 2 || mut.SelectedBytes != 40 {
		t.Fatalf("skip mut=%+v want affected=2 bytes=40", mut)
	}
	pending, _ := pull.GetNodeByID(database, "SRC", pendingCopyID)
	if pending.DeleteStatus == db.DeleteStatusSkipped {
		t.Fatal("incomplete-copy node should not be skipped")
	}
	ok, _ := pull.GetNodeByID(database, "SRC", okID)
	if ok.DeleteStatus != db.DeleteStatusSkipped {
		t.Fatalf("ok delete=%q want skipped", ok.DeleteStatus)
	}
}

func TestExcludeIdempotentNoDoubleInsert(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/exclude-idem.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	fileID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/a.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{
				{ID: folderID, Path: "/folder", ParentPath: "/", Name: "folder", Type: db.NodeTypeFolder, Depth: 1},
				{ID: fileID, Path: "/folder/a.txt", ParentPath: "/folder", ParentID: folderID, Name: "a.txt", Type: db.NodeTypeFile, Depth: 2, Size: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: folderID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 1},
				{ID: fileID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	countExclusionEvents := func() int {
		t.Helper()
		conn, err := database.GetDB()
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if err := conn.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM src_status_events
WHERE copy_status IN ('excluded_explicit','excluded_inherited')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	var first, second subtree.SubtreeCopyMutationResult
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			first, err2 = subtree.InsertExclusionEventsForSubtree(w, "SRC", "/folder")
			return err2
		})
	}); err != nil {
		t.Fatal(err)
	}
	afterFirst := countExclusionEvents()
	if first.Affected != 2 || afterFirst != 2 {
		t.Fatalf("first exclude: mut=%+v events=%d", first, afterFirst)
	}
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			var err2 error
			second, err2 = subtree.InsertExclusionEventsForSubtree(w, "SRC", "/folder")
			return err2
		})
	}); err != nil {
		t.Fatal(err)
	}
	if second.Affected != 0 {
		t.Fatalf("second exclude should be no-op, mut=%+v", second)
	}
	if got := countExclusionEvents(); got != afterFirst {
		t.Fatalf("double exclude inserted events: %d→%d", afterFirst, got)
	}
}
