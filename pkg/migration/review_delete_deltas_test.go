// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestDeleteSkippedStatusTransitionDeltas(t *testing.T) {
	t.Parallel()

	deltas := make(map[string]int64)
	addReviewDeltaForDeleteStatusTransition(
		deltas,
		db.DeleteStatusPendingExplicit,
		db.DeleteStatusSkipped,
	)
	if deltas[DeltaDeletePending] != -1 || deltas[DeltaDeleteSkipped] != 1 {
		t.Fatalf("pending -> skipped deltas = %#v", deltas)
	}

	deltas = make(map[string]int64)
	addReviewDeltaForDeleteStatusTransition(
		deltas,
		db.DeleteStatusSkipped,
		db.DeleteStatusPendingExplicit,
	)
	if deltas[DeltaDeleteSkipped] != -1 || deltas[DeltaDeletePending] != 1 {
		t.Fatalf("skipped -> pending deltas = %#v", deltas)
	}
}

func TestCopyReviewOmitsPending(t *testing.T) {
	t.Parallel()
	raw := ReviewStatsRaw{CopyPending: 2, CopyPendingRetry: 0, CopyFailed: 2, TraversalFailed: 0}

	review := raw.ToPathReviewStats(PhaseCopyReview)
	if review.PendingCount != nil {
		t.Fatalf("copy post-review must omit pendingCount, got %d", *review.PendingCount)
	}
	if review.FailedCount != 2 {
		t.Fatalf("copy review failed = %d, want 2", review.FailedCount)
	}
	if review.PendingRetriesCount != 0 {
		t.Fatalf("copy review pending retry = %d, want 0 (nothing marked)", review.PendingRetriesCount)
	}

	marked := ReviewStatsRaw{CopyPending: 2, CopyPendingRetry: 2, CopyFailed: 0}
	markedReview := marked.ToPathReviewStats(PhaseCopyReview)
	if markedReview.PendingCount != nil || markedReview.PendingRetriesCount != 2 {
		t.Fatalf("marked copy review pending/retry = %v/%d, want omit/2",
			markedReview.PendingCount, markedReview.PendingRetriesCount)
	}
}

func TestTraversalReviewOmitsPending(t *testing.T) {
	t.Parallel()
	raw := ReviewStatsRaw{CopyPending: 2, TraversalPendingRetry: 2, TraversalFailed: 4}
	review := raw.ToPathReviewStats(PhaseTraversalReview)
	if review.PendingCount != nil {
		t.Fatalf("traversal must omit pendingCount, got %d", *review.PendingCount)
	}
	if review.FailedCount != 4 {
		t.Fatalf("traversal failed = %d, want 4", review.FailedCount)
	}
	if review.PendingRetriesCount != 2 {
		t.Fatalf("traversal pending retry = %d, want 2", review.PendingRetriesCount)
	}
}

func TestTraversalReviewNegativePendingRetryDoesNotInflatePending(t *testing.T) {
	t.Parallel()
	raw := ReviewStatsRaw{CopyPending: 4, TraversalPendingRetry: -2}
	review := raw.ToPathReviewStats(PhaseTraversalReview)
	if review.PendingCount != nil {
		t.Fatalf("traversal must omit pendingCount, got %d", *review.PendingCount)
	}
	if review.PendingRetriesCount != 0 {
		t.Fatalf("traversal pending retry = %d, want 0 (clamped)", review.PendingRetriesCount)
	}
}

func TestTraversalReviewFailedDoesNotAffectPendingColumn(t *testing.T) {
	t.Parallel()
	raw := ReviewStatsRaw{CopyPending: 1, TraversalFailed: 4}
	review := raw.ToPathReviewStats(PhaseTraversalReview)
	if review.PendingCount != nil {
		t.Fatalf("traversal must omit pendingCount, got %d", *review.PendingCount)
	}
	if review.FailedCount != 4 {
		t.Fatalf("traversal failed = %d, want 4", review.FailedCount)
	}
}

func TestCopyReviewFailedDoesNotCountAsPending(t *testing.T) {
	t.Parallel()
	// After copy: only failures remain. No Pending column; retries stay 0.
	raw := ReviewStatsRaw{CopyPending: 0, CopyPendingRetry: 0, CopyFailed: 2}
	review := raw.ToPathReviewStats(PhaseCopyReview)
	if review.PendingCount != nil || review.PendingRetriesCount != 0 || review.FailedCount != 2 {
		t.Fatalf("copy review pending/retry/failed = %v/%d/%d, want omit/0/2",
			review.PendingCount, review.PendingRetriesCount, review.FailedCount)
	}
}

func TestDeleteReviewProjectsPendingAsRetry(t *testing.T) {
	t.Parallel()
	raw := ReviewStatsRaw{DeletePending: 3, DeleteFailed: 2}

	active := raw.ToPathReviewStats(PhaseDeleting)
	if active.PendingCount == nil || *active.PendingCount != 3 || active.PendingRetriesCount != 0 {
		t.Fatalf("active delete pending counts = %v/%d, want 3/0", active.PendingCount, active.PendingRetriesCount)
	}

	review := raw.ToPathReviewStats(PhaseDeleteReview)
	if review.PendingCount != nil || review.PendingRetriesCount != 3 {
		t.Fatalf("delete review must omit pendingCount and project retry=%d, got pending=%v retry=%d",
			3, review.PendingCount, review.PendingRetriesCount)
	}
}

func TestAddDeleteSelectedSizeDelta(t *testing.T) {
	t.Parallel()
	file := &db.NodeState{
		Type: db.NodeTypeFile, Size: 250,
		CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit,
	}
	deltas := make(map[string]int64)
	addDeleteSelectedSizeDelta(deltas, file, db.DeleteStatusPendingExplicit, db.DeleteStatusSkipped)
	if deltas[DeltaSizeSelected] != -250 || deltas[DeltaSizeDeleteSelected] != -250 {
		t.Fatalf("pending->skipped size deltas = %#v", deltas)
	}
	if deltas[DeltaFiles] != -1 {
		t.Fatalf("pending->skipped files delta = %#v", deltas)
	}

	deltas = make(map[string]int64)
	addDeleteSelectedSizeDelta(deltas, file, db.DeleteStatusSkipped, db.DeleteStatusPendingExplicit)
	if deltas[DeltaSizeSelected] != 250 || deltas[DeltaSizeDeleteSelected] != 250 {
		t.Fatalf("skipped->pending size deltas = %#v", deltas)
	}
	if deltas[DeltaFiles] != 1 {
		t.Fatalf("skipped->pending files delta = %#v", deltas)
	}

	deltas = make(map[string]int64)
	folder := &db.NodeState{
		Type: db.NodeTypeFolder, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPendingExplicit,
	}
	addDeleteSelectedSizeDelta(deltas, folder, db.DeleteStatusPendingExplicit, db.DeleteStatusSkipped)
	if deltas[DeltaFolders] != -1 || deltas[DeltaSizeSelected] != 0 {
		t.Fatalf("folder skip deltas = %#v", deltas)
	}

	// Not copy-complete: no size delta.
	deltas = make(map[string]int64)
	pendingCopy := &db.NodeState{Type: db.NodeTypeFile, Size: 250, CopyStatus: db.CopyStatusPending}
	addDeleteSelectedSizeDelta(deltas, pendingCopy, db.DeleteStatusPendingExplicit, db.DeleteStatusSkipped)
	if len(deltas) != 0 {
		t.Fatalf("expected no deltas for non-complete copy, got %#v", deltas)
	}
}
