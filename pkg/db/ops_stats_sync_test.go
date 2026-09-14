// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"
)

func TestReviewStatDeltasStayOnOps(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/review-deltas.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	deltas := []ReviewStatsDelta{{Key: ReviewKeyTraversalFailed, Delta: -1}, {Key: ReviewKeyTraversalPendingRetry, Delta: 1}}
	if err := database.ApplyReviewStatsDeltas(deltas); err != nil {
		t.Fatal(err)
	}
	failed, err := database.Ops().GetStat(ReviewKeyTraversalFailed)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := database.Ops().GetStat(ReviewKeyTraversalPendingRetry)
	if err != nil {
		t.Fatal(err)
	}
	if failed != -1 || pending != 1 {
		t.Fatalf("ops stats failed=%d pending=%d", failed, pending)
	}
}
