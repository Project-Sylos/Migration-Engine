package stats

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"testing"
	"time"
)

func TestBytesProgressPercent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		done, total int64
		want        float64
	}{
		{name: "empty total is complete", want: 100},
		{name: "half", done: 50, total: 100, want: 50},
		{name: "all done", done: 100, total: 100, want: 100},
		{name: "none done", done: 0, total: 200, want: 0},
		{name: "fixed total mid", done: 640, total: 1000, want: 64},
		{name: "done past total caps", done: 120, total: 100, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := db.BytesProgressPercent(tt.done, tt.total)
			if got != tt.want {
				t.Fatalf("db.BytesProgressPercent(%d,%d)=%v want %v", tt.done, tt.total, got, tt.want)
			}
		})
	}
}

func TestGetCopyEligibleCountsByTypeAndPendingFileSize(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-eligible.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	folderPending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	filePending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	fileOK := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 250,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
	}
	fileFailed := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 50,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed,
	}
	fileExcluded := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/d.txt"), Path: "/d.txt", ParentPath: "/", Name: "d.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 999,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit,
	}
	fileAlready := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/e.txt"), Path: "/e.txt", ParentPath: "/", Name: "e.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 777,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	folderAlready := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/old"), Path: "/old", ParentPath: "/", Name: "old",
		Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{folderPending, filePending, fileOK, fileFailed, fileExcluded, fileAlready, folderAlready}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, db.StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					EventTime: eventTime, Depth: 1,
				})
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	eligible, err := GetCountsByType(database, db.StatsKindCopy, SelectedEligible)
	if err != nil {
		t.Fatal(err)
	}
	if eligible.Folders != 1 || eligible.Files != 3 {
		t.Fatalf("eligible=%+v want folders=1 files=3 (already_existed/excluded omitted)", eligible)
	}
	if eligible.Total() != 4 {
		t.Fatalf("total=%d want 4", eligible.Total())
	}

	pendingSize, err := GetFileSize(database, db.StatsKindCopy, SelectedPending)
	if err != nil {
		t.Fatal(err)
	}
	if pendingSize != 100 {
		t.Fatalf("pending file size=%d want 100", pendingSize)
	}

	pendingCounts, err := GetCountsByType(database, db.StatsKindCopy, SelectedPending)
	if err != nil {
		t.Fatal(err)
	}
	// folder pending + one pending file (successful/failed/excluded/already_existed omitted)
	if pendingCounts.Folders != 1 || pendingCounts.Files != 1 {
		t.Fatalf("pending counts=%+v want folders=1 files=1", pendingCounts)
	}

	eligibleSize, err := GetFileSize(database, db.StatsKindCopy, SelectedEligible)
	if err != nil {
		t.Fatal(err)
	}
	// pending 100 + successful 250 + failed 50 (excluded + already_existed omitted)
	if eligibleSize != 400 {
		t.Fatalf("eligible file size=%d want 400 (stable; already_existed omitted)", eligibleSize)
	}

	work, err := GetCopyWorkProgressCounts(database)
	if err != nil {
		t.Fatal(err)
	}
	if work.Pending != 2 || work.Successful != 1 || work.Failed != 1 {
		t.Fatalf("work progress=%+v want pending=2 successful=1 failed=1 (already_existed omitted)", work)
	}

	// Fixed total: percent uses done vs eligible size, not done+remaining.
	if got := db.BytesProgressPercent(250, eligibleSize); got != 100.0*250/400 {
		t.Fatalf("bytes mid progress=%v", got)
	}
	if got := db.BytesProgressPercent(eligibleSize, eligibleSize); got != 100 {
		t.Fatalf("bytes complete=%v", got)
	}
}

func TestGetDeleteEligibleCountsByTypeAndPendingFileSize(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/delete-eligible.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	folderPending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	filePending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	fileDeleted := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 200,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted,
	}
	fileFailed := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 50,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusFailed,
	}
	fileSkipped := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/d.txt"), Path: "/d.txt", ParentPath: "/", Name: "d.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 400,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped,
	}
	notCopied := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/e.txt"), Path: "/e.txt", ParentPath: "/", Name: "e.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 800,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, DeleteStatus: db.DeleteStatusPending,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{folderPending, filePending, fileDeleted, fileFailed, fileSkipped, notCopied}
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

	eligible, err := GetCountsByType(database, db.StatsKindDelete, SelectedEligible)
	if err != nil {
		t.Fatal(err)
	}
	if eligible.Folders != 1 || eligible.Files != 3 {
		t.Fatalf("eligible=%+v want folders=1 files=3 (skipped/not-copied excluded)", eligible)
	}

	pendingSize, err := GetFileSize(database, db.StatsKindDelete, SelectedPending)
	if err != nil {
		t.Fatal(err)
	}
	if pendingSize != 100 {
		t.Fatalf("delete pending file size=%d want 100", pendingSize)
	}

	pendingCounts, err := GetCountsByType(database, db.StatsKindDelete, SelectedPending)
	if err != nil {
		t.Fatal(err)
	}
	if pendingCounts.Folders != 1 || pendingCounts.Files != 1 {
		t.Fatalf("delete pending counts=%+v want folders=1 files=1", pendingCounts)
	}

	eligibleSize, err := GetFileSize(database, db.StatsKindDelete, SelectedEligible)
	if err != nil {
		t.Fatal(err)
	}
	// pending 100 + deleted 200 + failed 50 (skipped/not-copied omitted)
	if eligibleSize != 350 {
		t.Fatalf("delete eligible file size=%d want 350", eligibleSize)
	}
}

func TestCompletedForModeMatchesItemsPercent(t *testing.T) {
	t.Parallel()
	c := db.PhaseProgressCounts{Pending: 10, Successful: 80, Failed: 10}
	if c.CompletedForMode(false) != 90 || c.Eligible() != 100 {
		t.Fatalf("normal mode completed/eligible=%d/%d", c.CompletedForMode(false), c.Eligible())
	}
	if c.CompletedForMode(true) != 80 {
		t.Fatalf("retry completed=%d", c.CompletedForMode(true))
	}
	if db.DeterministicProgressPercent(c.Pending, c.Successful, c.Failed, false) != 90 {
		t.Fatal("normal items percent")
	}
	if db.DeterministicProgressPercent(c.Pending, c.Successful, c.Failed, true) != 80 {
		t.Fatal("retry items percent")
	}
}
