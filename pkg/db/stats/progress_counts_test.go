package stats

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"testing"
	"time"
)

func TestGetCopyProgressCountsFromStats(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-progress.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.WriteReviewStatsSnapshot(db.ReviewStatsSnapshot{
				CopyPending:    25,
				CopySuccessful: 70,
				CopyFailed:     5,
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	counts, err := GetPhaseProgressCountsFromStats(database, db.StatsKindCopy)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 25 || counts.Successful != 70 || counts.Failed != 5 {
		t.Fatalf("counts=%+v", counts)
	}
	if got := db.DeterministicProgressPercent(counts.Pending, counts.Successful, counts.Failed, false); got != 75 {
		t.Fatalf("progress=%v want 75", got)
	}
}

func TestGetDeleteProgressCountsEligibleOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/delete-progress.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	copiedPending := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"),
		Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	copiedDeleted := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"),
		Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted,
	}
	copiedFailed := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"),
		Path: "/c.txt", ParentPath: "/", Name: "c.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusFailed,
	}
	copiedSkipped := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/d.txt"),
		Path: "/d.txt", ParentPath: "/", Name: "d.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped,
	}
	// Not copy-successful: must not enter delete progress eligible set.
	notCopied := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/e.txt"),
		Path: "/e.txt", ParentPath: "/", Name: "e.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, DeleteStatus: db.DeleteStatusPending,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{copiedPending, copiedDeleted, copiedFailed, copiedSkipped, notCopied}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, db.StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					DeleteStatus: n.DeleteStatus, EventTime: eventTime, Depth: 1,
				})
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	counts, err := GetDeleteProgressCounts(database)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 1 || counts.Successful != 1 || counts.Failed != 1 {
		t.Fatalf("counts=%+v want pending=1 successful=1 failed=1 (skipped/not-copied excluded)", counts)
	}
	if got := db.DeterministicProgressPercent(counts.Pending, counts.Successful, counts.Failed, false); got != (100.0*2)/3 {
		t.Fatalf("progress=%v want %v", got, (100.0*2)/3)
	}

	deleteCounts, err := GetEligibleDeleteStatusCounts(database)
	if err != nil {
		t.Fatal(err)
	}
	if deleteCounts.Pending != 1 || deleteCounts.Deleted != 1 ||
		deleteCounts.Failed != 1 || deleteCounts.Skipped != 1 {
		t.Fatalf("eligible delete counts=%+v want one in each tracked status", deleteCounts)
	}
}
