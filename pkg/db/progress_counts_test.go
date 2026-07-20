package db

import (
	"context"
	"testing"
	"time"
)

func TestGetCopyProgressCountsFromStats(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/copy-progress.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.WriteReviewStatsSnapshot(ReviewStatsSnapshot{
				CopyPending:    25,
				CopySuccessful: 70,
				CopyFailed:     5,
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	counts, err := database.GetCopyProgressCounts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 25 || counts.Successful != 70 || counts.Failed != 5 {
		t.Fatalf("counts=%+v", counts)
	}
	if got := counts.ProgressPercent(); got != 75 {
		t.Fatalf("progress=%v want 75", got)
	}
}

func TestGetDeleteProgressCountsEligibleOnly(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/delete-progress.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	copiedPending := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/a.txt"),
		Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, DeleteStatus: DeleteStatusPending,
	}
	copiedDeleted := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/b.txt"),
		Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, DeleteStatus: DeleteStatusDeleted,
	}
	copiedFailed := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/c.txt"),
		Path: "/c.txt", ParentPath: "/", Name: "c.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, DeleteStatus: DeleteStatusFailed,
	}
	copiedSkipped := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/d.txt"),
		Path: "/d.txt", ParentPath: "/", Name: "d.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, DeleteStatus: DeleteStatusSkipped,
	}
	// Not copy-successful: must not enter delete progress eligible set.
	notCopied := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/e.txt"),
		Path: "/e.txt", ParentPath: "/", Name: "e.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, DeleteStatus: DeleteStatusPending,
	}

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			nodes := []*NodeState{copiedPending, copiedDeleted, copiedFailed, copiedSkipped, notCopied}
			if err := w.AppenderInsert(tableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, StatusEvent{
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

	counts, err := database.GetDeleteProgressCounts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 1 || counts.Successful != 1 || counts.Failed != 1 {
		t.Fatalf("counts=%+v want pending=1 successful=1 failed=1 (skipped/not-copied excluded)", counts)
	}
	if got := counts.ProgressPercent(); got != (100.0*2)/3 {
		t.Fatalf("progress=%v want %v", got, (100.0*2)/3)
	}

	deleteCounts, err := database.GetEligibleDeleteStatusCounts()
	if err != nil {
		t.Fatal(err)
	}
	if deleteCounts.Pending != 1 || deleteCounts.Deleted != 1 ||
		deleteCounts.Failed != 1 || deleteCounts.Skipped != 1 {
		t.Fatalf("eligible delete counts=%+v want one in each tracked status", deleteCounts)
	}
}
