// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"slices"
)

// Phase is the migration lifecycle state, stored as the canonical status string (lowercase-with-hyphens).
const (
	PhaseCreated              string = "roots-set"                 // Initial state; after user sets roots. TODO: when filter creation module lands, transition to filters-set before traversal.
	PhaseFiltersSet           string = "filters-set"               // Ready for traversal
	PhaseTraversing           string = "traversal-in-progress"     // Traversal running
	PhaseTraversalSuspended   string = "traversal-suspended"       // Traversal stopped via soft suspend; resume with StartTraversal
	PhaseTraversalReview      string = "awaiting-traversal-review" // Traversal done, user can review; can retry traversal or start copy
	PhaseCopying              string = "copy-in-progress"          // Copy phase running
	PhaseCopySuspended        string = "copy-suspended"            // Copy stopped via soft suspend; resume with StartCopy
	PhaseCopyReview           string = "awaiting-copy-review"      // Copy done, user can review; can retry copy
)

// ParsePhase parses a canonical phase string from the DB (lowercase-with-hyphens).
func ParsePhase(v string) (string, error) {
	validPhases := []string{
		PhaseCreated,
		PhaseFiltersSet,
		PhaseTraversing,
		PhaseTraversalSuspended,
		PhaseTraversalReview,
		PhaseCopying,
		PhaseCopySuspended,
		PhaseCopyReview,
	}
	if slices.Contains(validPhases, v) {
		return v, nil
	}
	return PhaseCreated, fmt.Errorf("unknown phase %q", v)
}


// canTransition returns true if a phase transition is allowed.
// For now, allow roots-set (PhaseCreated) to transition directly to traversal-in-progress
// as well as to filters-set. TODO: When we have the filter creation module, adjust this logic accordingly.
//
// from == to allows idempotent transitions (e.g. StartCopy while already copy-in-progress).
func canTransition(from, to string) bool {
	if from == to {
		return true
	}

	// This section kind of feels like tribal knowledge wizardry. 
	// We should probably have some note that explains WHY certain phases are allowed to transition to other certain phases. But oh well. 
	// TODO: Explain this better please.
	switch from {
	case PhaseCreated:
		// Allow transition to either filters-set (intended) or directly to traversal-in-progress (temporary).
		return to == PhaseFiltersSet || to == PhaseTraversing // TODO: tighten this up when filter creation lands
	case PhaseFiltersSet:
		return to == PhaseTraversing
	case PhaseTraversing:
		return to == PhaseTraversalReview || to == PhaseTraversalSuspended
	case PhaseTraversalSuspended:
		return to == PhaseTraversing
	case PhaseTraversalReview:
		return to == PhaseTraversing || to == PhaseCopying
	case PhaseCopying:
		return to == PhaseCopyReview || to == PhaseCopySuspended
	case PhaseCopySuspended:
		return to == PhaseCopying
	case PhaseCopyReview:
		return to == PhaseCopying
	default:
		return false
	}
}
