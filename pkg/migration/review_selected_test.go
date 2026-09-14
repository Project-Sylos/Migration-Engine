// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

func TestReviewSelectedSpec(t *testing.T) {
	t.Parallel()
	cases := []struct {
		phase, view string
		kind        db.StatsKind
		pop         stats.SelectedPopulation
	}{
		{PhaseTraversalReview, "", db.StatsKindCopy, stats.SelectedPending},
		{PhaseTraversalSuspended, "", db.StatsKindCopy, stats.SelectedPending},
		{PhaseTraversalReview, "copy-plan", db.StatsKindCopy, stats.SelectedPending},
		{PhaseCopyReview, "", db.StatsKindCopy, stats.SelectedEligible},
		{PhaseCopying, "", db.StatsKindCopy, stats.SelectedEligible},
		{PhaseCopySuspended, "", db.StatsKindCopy, stats.SelectedEligible},
		{PhaseCopyReview, "source-cleanup", db.StatsKindDelete, stats.SelectedPending},
		{PhaseDeleteReview, "", db.StatsKindDelete, stats.SelectedPending},
		{PhaseDeleting, "", db.StatsKindDelete, stats.SelectedPending},
	}
	for _, tc := range cases {
		got := reviewSelectedSpec(tc.phase, tc.view)
		if got.Kind != tc.kind || got.Population != tc.pop {
			t.Fatalf("phase=%q view=%q got %+v want kind=%v pop=%v",
				tc.phase, tc.view, got, tc.kind, tc.pop)
		}
	}
}
