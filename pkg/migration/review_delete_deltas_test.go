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
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusPendingExplicit, -1)
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusSkipped, 1)
	if deltas[DeltaDeletePending] != -1 || deltas[DeltaDeleteSkipped] != 1 {
		t.Fatalf("pending -> skipped deltas = %#v", deltas)
	}

	deltas = make(map[string]int64)
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusSkipped, -1)
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusPendingExplicit, 1)
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
