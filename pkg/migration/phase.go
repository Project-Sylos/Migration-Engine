// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "fmt"

// Phase is the migration lifecycle state, stored as the canonical status string (lowercase-with-hyphens).
const (
	PhaseCreated         string = "roots-set"                  // Initial state; after user sets roots. TODO: when filter creation module lands, transition to filters-set before traversal.
	PhaseFiltersSet      string = "filters-set"                // Ready for traversal
	PhaseTraversing      string = "traversal-in-progress"      // Traversal running
	PhaseTraversalReview string = "awaiting-traversal-review"  // Traversal done, user can review; can retry traversal or start copy
	PhaseCopying         string = "copy-in-progress"           // Copy phase running
	PhaseCopyReview      string = "awaiting-copy-review"       // Copy done, user can review; can retry copy
)

// ParsePhase parses a phase string from the DB. Accepts new lowercase-with-hyphens values and legacy PascalCase values.
func ParsePhase(v string) (string, error) {
	switch v {
	case PhaseCreated:
		return PhaseCreated, nil
	case PhaseFiltersSet:
		return PhaseFiltersSet, nil
	case PhaseTraversing:
		return PhaseTraversing, nil
	case PhaseTraversalReview:
		return PhaseTraversalReview, nil
	case PhaseCopying:
		return PhaseCopying, nil
	case PhaseCopyReview:
		return PhaseCopyReview, nil
	default:
		return PhaseCreated, fmt.Errorf("unknown phase %q", v)
	}
}

// canTransition returns true if a phase transition is allowed.
// For now, allow roots-set (PhaseCreated) to transition directly to traversal-in-progress
// as well as to filters-set. TODO: When we have the filter creation module, adjust this logic accordingly.
func canTransition(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case PhaseCreated:
		// Allow transition to either filters-set (intended) or directly to traversal-in-progress (temporary).
		return to == PhaseFiltersSet || to == PhaseTraversing // TODO: tighten this up when filter creation lands
	case PhaseFiltersSet:
		return to == PhaseTraversing
	case PhaseTraversing:
		return to == PhaseTraversalReview
	case PhaseTraversalReview:
		return to == PhaseTraversing || to == PhaseCopying
	case PhaseCopying:
		return to == PhaseCopyReview
	case PhaseCopyReview:
		return to == PhaseCopying
	default:
		return false
	}
}
