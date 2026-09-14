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
	PhaseTraversalFinalizing  string = "traversal-finalizing"      // Queues done; seal stop, indexes, checkpoint in progress
	PhaseTraversalFinalizeFailed string = "traversal-finalize-failed" // Durable teardown failed; retry without re-running workers
	PhaseTraversalReview      string = "awaiting-traversal-review" // Traversal done, user can review; can retry traversal or start copy
	PhaseCopying              string = "copy-in-progress"          // Copy phase running
	PhaseCopySuspended        string = "copy-suspended"            // Copy stopped via soft suspend; resume with StartCopy
	PhaseCopyFinalizing       string = "copy-finalizing"
	PhaseCopyFinalizeFailed   string = "copy-finalize-failed"
	PhaseCopyReview           string = "awaiting-copy-review" // Copy done, user can review; can retry copy
	PhaseDeleting             string = "delete-in-progress"   // Delete phase running
	PhaseDeleteSuspended      string = "delete-suspended"     // Delete stopped via soft suspend
	PhaseDeleteFinalizing     string = "delete-finalizing"
	PhaseDeleteFinalizeFailed string = "delete-finalize-failed"
	PhaseDeleteReview         string = "awaiting-delete-review" // Delete done, user can review; can retry delete
	PhaseAborted              string = "aborted"               // Force-stopped; not resumable
)

// ParsePhase parses a canonical phase string from the DB (lowercase-with-hyphens).
func ParsePhase(v string) (string, error) {
	validPhases := []string{
		PhaseCreated,
		PhaseFiltersSet,
		PhaseTraversing,
		PhaseTraversalSuspended,
		PhaseTraversalFinalizing,
		PhaseTraversalFinalizeFailed,
		PhaseTraversalReview,
		PhaseCopying,
		PhaseCopySuspended,
		PhaseCopyFinalizing,
		PhaseCopyFinalizeFailed,
		PhaseCopyReview,
		PhaseDeleting,
		PhaseDeleteSuspended,
		PhaseDeleteFinalizing,
		PhaseDeleteFinalizeFailed,
		PhaseDeleteReview,
		PhaseAborted,
	}
	if slices.Contains(validPhases, v) {
		return v, nil
	}
	return PhaseCreated, fmt.Errorf("unknown phase %q", v)
}

// IsFinalizeFailedPhase reports whether phase is a durable-teardown failure state.
func IsFinalizeFailedPhase(phase string) bool {
	return phase == PhaseTraversalFinalizeFailed || phase == PhaseCopyFinalizeFailed || phase == PhaseDeleteFinalizeFailed
}

// IsFinalizingPhase reports whether phase is durable teardown in progress.
func IsFinalizingPhase(phase string) bool {
	return phase == PhaseTraversalFinalizing || phase == PhaseCopyFinalizing || phase == PhaseDeleteFinalizing
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

	switch from {
	case PhaseCreated:
		return to == PhaseFiltersSet || to == PhaseTraversing
	case PhaseFiltersSet:
		return to == PhaseTraversing
	case PhaseTraversing:
		return to == PhaseTraversalFinalizing || to == PhaseTraversalSuspended || to == PhaseAborted
	case PhaseTraversalSuspended:
		return to == PhaseTraversing
	case PhaseTraversalFinalizing:
		return to == PhaseTraversalReview || to == PhaseTraversalFinalizeFailed || to == PhaseAborted
	case PhaseTraversalFinalizeFailed:
		return to == PhaseTraversalFinalizing
	case PhaseTraversalReview:
		return to == PhaseTraversing || to == PhaseCopying
	case PhaseCopying:
		return to == PhaseCopyFinalizing || to == PhaseCopySuspended || to == PhaseAborted
	case PhaseCopySuspended:
		return to == PhaseCopying
	case PhaseCopyFinalizing:
		return to == PhaseCopyReview || to == PhaseCopyFinalizeFailed || to == PhaseAborted
	case PhaseCopyFinalizeFailed:
		return to == PhaseCopyFinalizing
	case PhaseCopyReview:
		return to == PhaseCopying || to == PhaseDeleting
	case PhaseDeleting:
		return to == PhaseDeleteFinalizing || to == PhaseDeleteSuspended || to == PhaseAborted
	case PhaseDeleteSuspended:
		return to == PhaseDeleting
	case PhaseDeleteFinalizing:
		return to == PhaseDeleteReview || to == PhaseDeleteFinalizeFailed || to == PhaseAborted
	case PhaseDeleteFinalizeFailed:
		return to == PhaseDeleteFinalizing
	case PhaseDeleteReview:
		return to == PhaseDeleting
	case PhaseAborted:
		return false
	default:
		return false
	}
}
