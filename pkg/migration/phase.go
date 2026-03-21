// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "fmt"

// Phase is the migration lifecycle state, stored as the canonical status string.
type Phase string

const (
	PhaseCreated    Phase = "Roots-Set"             // User has set root folders and services (initial state)
	PhaseFiltersSet Phase = "Filters-Set"           // Filters configured, ready for traversal (also used for retry)
	PhaseTraversing Phase = "Traversal-In-Progress" // Traversal is currently running
	PhaseReview     Phase = "Awaiting-Path-Review"  // Traversal finished, user can review results and see copy plan
	PhaseCopying    Phase = "Copy-In-Progress"      // Copy phase is currently running
	PhaseCompleted  Phase = "Complete"              // Migration completed successfully
	PhaseSuspended  Phase = "Suspended"             // Migration suspended (can be resumed)
)

func (p Phase) String() string { return string(p) }

func ParsePhase(v string) (Phase, error) {
	switch Phase(v) {
	case PhaseCreated, PhaseFiltersSet, PhaseTraversing, PhaseReview,
		PhaseCopying, PhaseCompleted, PhaseSuspended:
		return Phase(v), nil
	default:
		return PhaseCreated, fmt.Errorf("unknown phase %q", v)
	}
}

func canTransition(from, to Phase) bool {
	if from == to {
		return true
	}
	switch from {
	case PhaseCreated:
		return to == PhaseTraversing
	case PhaseTraversing:
		return to == PhaseReview
	case PhaseReview:
		return to == PhaseCopying || to == PhaseTraversing
	case PhaseCopying:
		return to == PhaseCompleted
	case PhaseCompleted:
		return false
	default:
		return false
	}
}
