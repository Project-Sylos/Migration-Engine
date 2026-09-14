// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

// reviewSelectedSpec chooses copy vs delete and pending vs eligible for Path Review
// Folders/Files/Selected. Copy-plan and traversal use copy pending; source-cleanup /
// delete use delete pending; copy results use eligible (legacy progress denominators).
func reviewSelectedSpec(phase, view string) stats.ReviewSelectedSpec {
	if view == "source-cleanup" ||
		phase == PhaseDeleting || phase == PhaseDeleteSuspended || phase == PhaseDeleteReview {
		return stats.ReviewSelectedSpec{Kind: db.StatsKindDelete, Population: stats.SelectedPending}
	}
	if view == "copy-plan" {
		return stats.ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: stats.SelectedPending}
	}
	switch phase {
	case PhaseCopying, PhaseCopySuspended, PhaseCopyReview:
		return stats.ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: stats.SelectedEligible}
	default:
		return stats.ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: stats.SelectedPending}
	}
}
