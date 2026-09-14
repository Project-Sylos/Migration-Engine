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

func TestCanTransitionInProgressToAborted(t *testing.T) {
	cases := []struct {
		from, to string
	}{
		{PhaseTraversing, PhaseAborted},
		{PhaseCopying, PhaseAborted},
		{PhaseDeleting, PhaseAborted},
	}
	for _, tc := range cases {
		if !canTransition(tc.from, tc.to) {
			t.Fatalf("expected %s -> %s allowed", tc.from, tc.to)
		}
	}
	if canTransition(PhaseAborted, PhaseTraversing) {
		t.Fatal("aborted must not resume to traversing")
	}
}

func TestCanTransitionFinalizePhases(t *testing.T) {
	allowed := []struct{ from, to string }{
		{PhaseTraversing, PhaseTraversalFinalizing},
		{PhaseTraversalFinalizing, PhaseTraversalReview},
		{PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed},
		{PhaseTraversalFinalizeFailed, PhaseTraversalFinalizing},
		{PhaseCopying, PhaseCopyFinalizing},
		{PhaseCopyFinalizing, PhaseCopyReview},
		{PhaseCopyFinalizing, PhaseCopyFinalizeFailed},
		{PhaseCopyFinalizeFailed, PhaseCopyFinalizing},
		{PhaseDeleting, PhaseDeleteFinalizing},
		{PhaseDeleteFinalizing, PhaseDeleteReview},
		{PhaseDeleteFinalizing, PhaseDeleteFinalizeFailed},
		{PhaseDeleteFinalizeFailed, PhaseDeleteFinalizing},
	}
	for _, tc := range allowed {
		if !canTransition(tc.from, tc.to) {
			t.Fatalf("expected %s -> %s allowed", tc.from, tc.to)
		}
	}
	denied := []struct{ from, to string }{
		{PhaseTraversing, PhaseTraversalReview},
		{PhaseCopying, PhaseCopyReview},
		{PhaseDeleting, PhaseDeleteReview},
		{PhaseTraversalFinalizeFailed, PhaseTraversalReview},
		{PhaseCopyFinalizeFailed, PhaseCopying},
		{PhaseDeleteFinalizeFailed, PhaseDeleteSuspended},
	}
	for _, tc := range denied {
		if canTransition(tc.from, tc.to) {
			t.Fatalf("expected %s -> %s denied", tc.from, tc.to)
		}
	}
}

func TestIsFinalizePhaseHelpers(t *testing.T) {
	if !IsFinalizingPhase(PhaseCopyFinalizing) || IsFinalizingPhase(PhaseCopying) {
		t.Fatal("IsFinalizingPhase")
	}
	if !IsFinalizeFailedPhase(PhaseDeleteFinalizeFailed) || IsFinalizeFailedPhase("failed") {
		t.Fatal("IsFinalizeFailedPhase")
	}
}
