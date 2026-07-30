package stats

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestSnapshotDeleteWorkExcludesSkipped(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-skip.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	nodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt", Type: db.NodeTypeFile, Depth: 1, Size: 200, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending},
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/skip.txt"), Path: "/skip.txt", ParentPath: "/", Name: "skip.txt", Type: db.NodeTypeFile, Depth: 1, Size: 400, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped},
	}
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			evs := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				evs = append(evs, db.StatusEvent{ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus, DeleteStatus: n.DeleteStatus, EventTime: time.Now().UnixNano(), Depth: 1})
			}
			return w.BatchInsertSrcStatusEvents(evs)
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
	if totals.Folders != 1 || totals.Files != 2 || totals.Bytes != 300 {
		t.Fatalf("delete_work totals=%+v want folders=1 files=2 bytes=300 (skipped excluded)", totals)
	}
}

func TestSnapshotDeleteWorkAfterPriorInflatedSeal(t *testing.T) {
	// First seal when everything is pending, then skip, then reseal — must shrink.
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-shrink.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idSkip := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/skip.txt")
	nodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt", Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending},
		{ID: idSkip, Path: "/skip.txt", ParentPath: "/", Name: "skip.txt", Type: db.NodeTypeFile, Depth: 1, Size: 400, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending},
	}
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			evs := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				evs = append(evs, db.StatusEvent{ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus, DeleteStatus: n.DeleteStatus, EventTime: time.Now().UnixNano(), Depth: 1})
			}
			return w.BatchInsertSrcStatusEvents(evs)
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
	if totals.Files != 2 || totals.Bytes != 500 {
		t.Fatalf("pre-skip totals=%+v", totals)
	}

	// Skip one file (event + refresh current).
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.SetNodeDeleteStatus("SRC", idSkip, db.DeleteStatusSkipped); err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		t.Fatal(err)
	}
	totals2, err := ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	if err != nil {
		t.Fatal(err)
	}
	if totals2.Files != 1 || totals2.Bytes != 100 {
		t.Fatalf("post-skip reseal totals=%+v want files=1 bytes=100", totals2)
	}
}

func TestSnapshotDeleteWorkIgnoresNonCopyCompletePending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/del-noncomplete.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	nodes := []*db.NodeState{
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/ok.txt"), Path: "/ok.txt", ParentPath: "/", Name: "ok.txt", Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending},
		// discovery-style: delete pending but never copied (excluded from copy)
		{ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/excl.txt"), Path: "/excl.txt", ParentPath: "/", Name: "excl.txt", Type: db.NodeTypeFile, Depth: 1, Size: 900, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit, DeleteStatus: db.DeleteStatusPending},
	}
	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			evs := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				evs = append(evs, db.StatusEvent{ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus, DeleteStatus: n.DeleteStatus, EventTime: time.Now().UnixNano(), Depth: 1})
			}
			return w.BatchInsertSrcStatusEvents(evs)
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
		t.Fatalf("totals=%+v want only copy-complete pending (1 file, 100 bytes)", totals)
	}
}
