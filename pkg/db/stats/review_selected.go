// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import "codeberg.org/Sylos/Migration-Engine/pkg/db"

// SelectedPopulation chooses which SRC rows Folders/Files/Selected size represent
// on Path Review. Pending = still queued for the next action; Eligible = full
// work set for that phase (includes successful/failed/in_progress as defined by
// GetCountsByType / GetFileSize with SelectedEligible).
type SelectedPopulation int

const (
	SelectedPending SelectedPopulation = iota
	SelectedEligible
)

// ReviewSelectedSpec describes how to overlay Path Review folder/file/selected counts.
type ReviewSelectedSpec struct {
	Kind       db.StatsKind
	Population SelectedPopulation
}

// ReviewSelectedOverlay is Folders/Files/Selected bytes for the Path Review footer.
type ReviewSelectedOverlay struct {
	Folders       int64
	Files         int64
	SelectedBytes int64
}

// OverlayReviewSelected loads folder/file counts and selected file bytes for the given spec.
func OverlayReviewSelected(database *db.DB, spec ReviewSelectedSpec) (ReviewSelectedOverlay, error) {
	var out ReviewSelectedOverlay
	if database == nil {
		return out, nil
	}
	counts, err := GetCountsByType(database, spec.Kind, spec.Population)
	if err != nil {
		return out, err
	}
	size, err := GetFileSize(database, spec.Kind, spec.Population)
	if err != nil {
		return out, err
	}
	out.Folders = counts.Folders
	out.Files = counts.Files
	out.SelectedBytes = size
	return out, nil
}
