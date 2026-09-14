package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
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

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		{Type: db.NodeTypeFile, Depth: 1, Size: 250, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful},
		{Type: db.NodeTypeFile, Depth: 1, Size: 50, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed},
		{Type: db.NodeTypeFile, Depth: 1, Size: 999, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 777, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted},
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted},
	})

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
	if pendingCounts.Folders != 1 || pendingCounts.Files != 1 {
		t.Fatalf("pending counts=%+v want folders=1 files=1", pendingCounts)
	}

	eligibleSize, err := GetFileSize(database, db.StatsKindCopy, SelectedEligible)
	if err != nil {
		t.Fatal(err)
	}
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

	seedSrcDepthStats(t, database, []*db.NodeState{
		{Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 100, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit},
		{Type: db.NodeTypeFile, Depth: 1, Size: 200, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted},
		{Type: db.NodeTypeFile, Depth: 1, Size: 50, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusFailed},
		{Type: db.NodeTypeFile, Depth: 1, Size: 400, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped},
		{Type: db.NodeTypeFile, Depth: 1, Size: 800, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, DeleteStatus: db.DeleteStatusPendingExplicit},
	})

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
	if eligibleSize != 350 {
		t.Fatalf("delete eligible file size=%d want 350", eligibleSize)
	}
}
