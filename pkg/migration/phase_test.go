// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "testing"

func TestCanTransitionInProgressToSuspended(t *testing.T) {
	cases := []struct {
		from, to string
	}{
		{PhaseTraversing, PhaseTraversalSuspended},
		{PhaseCopying, PhaseCopySuspended},
		{PhaseDeleting, PhaseDeleteSuspended},
	}
	for _, tc := range cases {
		if !canTransition(tc.from, tc.to) {
			t.Fatalf("expected %s -> %s allowed", tc.from, tc.to)
		}
	}
}

func TestCanTransitionIdempotentSamePhase(t *testing.T) {
	for _, p := range []string{PhaseCopying, PhaseTraversing, PhaseDeleting, PhaseCopySuspended} {
		if !canTransition(p, p) {
			t.Fatalf("expected %s -> %s allowed", p, p)
		}
	}
}
