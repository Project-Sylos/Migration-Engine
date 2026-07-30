package seal

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestBuildCanonicalReviewStatsDeltasCopyPendingToSuccessful(t *testing.T) {
	t.Parallel()
	jobs := []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Events: []db.StatusEvent{{
			ID:             "n1",
			CopyStatus:     db.CopyStatusSuccessful,
			PrevCopyStatus: db.CopyStatusPending,
			NodeType:       db.NodeTypeFile,
			Size:           10,
		}},
	}}
	deltas := buildCanonicalReviewStatsDeltas(jobs)
	got := map[string]int64{}
	for _, d := range deltas {
		got[d.Key] = d.Delta
	}
	if got[db.ReviewKeyCopyPending] != -1 {
		t.Fatalf("copy/pending delta=%d want -1", got[db.ReviewKeyCopyPending])
	}
	if got[db.ReviewKeyCopySuccessful] != 1 {
		t.Fatalf("copy/successful delta=%d want 1", got[db.ReviewKeyCopySuccessful])
	}
	if got[db.ReviewKeyFiles] != -1 {
		t.Fatalf("files delta=%d want -1", got[db.ReviewKeyFiles])
	}
	if got[db.ReviewKeySizeSelected] != -10 {
		t.Fatalf("size_selected delta=%d want -10", got[db.ReviewKeySizeSelected])
	}
}

func TestBuildCanonicalReviewStatsDeltasEmptyPrevCopyTreatedAsPending(t *testing.T) {
	t.Parallel()
	// PrevCopyStatus unset (common when meta.CopyStatus was empty) must still -1 pending.
	jobs := []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Events: []db.StatusEvent{{
			ID:             "n1",
			CopyStatus:     db.CopyStatusAlreadyExisted,
			PrevCopyStatus: "",
		}},
	}}
	deltas := buildCanonicalReviewStatsDeltas(jobs)
	got := map[string]int64{}
	for _, d := range deltas {
		got[d.Key] = d.Delta
	}
	if got[db.ReviewKeyCopyPending] != -1 {
		t.Fatalf("copy/pending delta=%d want -1 (empty prev → pending)", got[db.ReviewKeyCopyPending])
	}
	// already_existed folds into copy/successful for review keys.
	if got[db.ReviewKeyCopySuccessful] != 1 {
		t.Fatalf("copy/successful delta=%d want 1", got[db.ReviewKeyCopySuccessful])
	}
}

func TestBuildCanonicalReviewStatsDeltasDiscoveryAddsPending(t *testing.T) {
	t.Parallel()
	jobs := []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Nodes:   []*db.NodeState{{ID: "n1", Type: db.NodeTypeFile, Size: 10}},
		Events: []db.StatusEvent{{
			ID:              "n1",
			TraversalStatus: db.StatusSuccessful,
			CopyStatus:      db.CopyStatusPending,
			DeleteStatus:    db.DeleteStatusPending,
		}},
	}}
	deltas := buildCanonicalReviewStatsDeltas(jobs)
	got := map[string]int64{}
	for _, d := range deltas {
		got[d.Key] = d.Delta
	}
	if got[db.ReviewKeyCopyPending] != 1 {
		t.Fatalf("discovery copy/pending=%d want 1", got[db.ReviewKeyCopyPending])
	}
	if got[db.ReviewKeyDeletePending] != 1 {
		t.Fatalf("discovery delete/pending=%d want 1", got[db.ReviewKeyDeletePending])
	}
	if got[db.ReviewKeyFiles] != 1 {
		t.Fatalf("discovery files=%d want 1", got[db.ReviewKeyFiles])
	}
	if got[db.ReviewKeySizeSrc] != 10 {
		t.Fatalf("discovery size_src=%d want 10", got[db.ReviewKeySizeSrc])
	}
	if got[db.ReviewKeySizeSelected] != 10 {
		t.Fatalf("discovery size_selected=%d want 10", got[db.ReviewKeySizeSelected])
	}
}

func TestBuildCanonicalReviewStatsDeltasDiscoveryFolders(t *testing.T) {
	t.Parallel()
	jobs := []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Nodes:   []*db.NodeState{{ID: "f1", Type: db.NodeTypeFolder}},
		Events: []db.StatusEvent{{
			ID:              "f1",
			TraversalStatus: db.StatusSuccessful,
			CopyStatus:      db.CopyStatusPending,
		}},
	}}
	got := deltaMap(buildCanonicalReviewStatsDeltas(jobs))
	if got[db.ReviewKeyFolders] != 1 {
		t.Fatalf("discovery folders=%d want 1", got[db.ReviewKeyFolders])
	}
	if got[db.ReviewKeyFiles] != 0 {
		t.Fatalf("discovery files=%d want 0", got[db.ReviewKeyFiles])
	}
}

func TestBuildCanonicalReviewStatsDeltasSelectedSizes(t *testing.T) {
	t.Parallel()
	// Discovery file: +size_selected, not yet delete-selected.
	jobs := []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Nodes:   []*db.NodeState{{ID: "f1", Type: db.NodeTypeFile, Size: 100}},
		Events: []db.StatusEvent{{
			ID: "f1", CopyStatus: db.CopyStatusPending, DeleteStatus: db.DeleteStatusPending, Size: 100,
		}},
	}}
	got := deltaMap(buildCanonicalReviewStatsDeltas(jobs))
	if got[db.ReviewKeySizeSelected] != 100 {
		t.Fatalf("discovery size_selected=%d want 100", got[db.ReviewKeySizeSelected])
	}
	if got[db.ReviewKeySizeDeleteSelected] != 0 {
		t.Fatalf("discovery size_delete_selected=%d want 0", got[db.ReviewKeySizeDeleteSelected])
	}

	// Copy complete: -size_selected, +size_delete_selected.
	jobs = []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Events: []db.StatusEvent{{
			ID:             "f1",
			CopyStatus:     db.CopyStatusSuccessful,
			PrevCopyStatus: db.CopyStatusPending,
			Size:           100,
		}},
	}}
	got = deltaMap(buildCanonicalReviewStatsDeltas(jobs))
	if got[db.ReviewKeySizeSelected] != -100 {
		t.Fatalf("copy-complete size_selected=%d want -100", got[db.ReviewKeySizeSelected])
	}
	if got[db.ReviewKeySizeDeleteSelected] != 100 {
		t.Fatalf("copy-complete size_delete_selected=%d want 100", got[db.ReviewKeySizeDeleteSelected])
	}

	// Delete complete: -size_delete_selected.
	jobs = []SealJob{{
		Table:   "SRC",
		Pending: discoveryJobStatsSentinel,
		Events: []db.StatusEvent{{
			ID:               "f1",
			CopyStatus:       db.CopyStatusSuccessful,
			PrevCopyStatus:   db.CopyStatusSuccessful,
			DeleteStatus:     db.DeleteStatusDeleted,
			PrevDeleteStatus: db.DeleteStatusPending,
			Size:             100,
		}},
	}}
	got = deltaMap(buildCanonicalReviewStatsDeltas(jobs))
	if got[db.ReviewKeySizeDeleteSelected] != -100 {
		t.Fatalf("delete-complete size_delete_selected=%d want -100", got[db.ReviewKeySizeDeleteSelected])
	}
}

func deltaMap(deltas []db.ReviewStatsDelta) map[string]int64 {
	got := map[string]int64{}
	for _, d := range deltas {
		got[d.Key] = d.Delta
	}
	return got
}
