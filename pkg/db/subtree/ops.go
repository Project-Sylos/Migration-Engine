// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package subtree

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// SubtreeCopyMutationResult aggregates copy-phase exclude/unexclude subtree writes.
type SubtreeCopyMutationResult struct {
	Affected     int64
	Folders      int64
	Files        int64
	PendingBytes int64
}

// SubtreeDeleteMutationResult aggregates delete-phase cascade subtree writes.
type SubtreeDeleteMutationResult struct {
	Affected      int64
	Folders       int64
	Files         int64
	SelectedBytes int64
}

// PropagateCopyFailure marks pending copy descendants under parentPath as failed.
func PropagateCopyFailure(database *db.DB, parentPath string) (opsdb.SubtreeMutationResult, error) {
	var out opsdb.SubtreeMutationResult
	if database == nil || database.Ops() == nil {
		return out, nil
	}
	mut, err := database.Ops().PropagateCopyFailureUnderPath(parentPath)
	if err != nil {
		return out, err
	}
	if mut.Affected > 0 {
		deltas := []db.ReviewStatsDelta{
			{Key: db.ReviewKeyCopyPending, Delta: -mut.Affected},
			{Key: db.ReviewKeyCopyFailed, Delta: mut.Affected},
		}
		if mut.SelectedBytes != 0 {
			deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeySizeSelected, Delta: -mut.SelectedBytes})
		}
		_ = database.ApplyReviewStatsDeltas(deltas)
	}
	return mut, nil
}

// CascadeDeleteUnderPath marks pending* delete descendants under rootPath as deleted.
func CascadeDeleteUnderPath(database *db.DB, rootPath string) (SubtreeDeleteMutationResult, error) {
	var legacy SubtreeDeleteMutationResult
	if database == nil || database.Ops() == nil {
		return legacy, nil
	}
	mut, err := database.Ops().CascadeDeleteUnderPath(rootPath)
	if err != nil {
		return legacy, err
	}
	legacy = mutationToLegacy(mut)
	if mut.Affected > 0 {
		deltas := []db.ReviewStatsDelta{
			{Key: db.ReviewKeyDeletePending, Delta: -mut.Affected},
			{Key: db.ReviewKeyDeleteDeleted, Delta: mut.Affected},
		}
		if mut.SelectedBytes != 0 {
			deltas = append(deltas,
				db.ReviewStatsDelta{Key: db.ReviewKeySizeDeleteSelected, Delta: -mut.SelectedBytes},
				db.ReviewStatsDelta{Key: db.ReviewKeySizeSrc, Delta: -mut.SelectedBytes},
			)
		}
		_ = database.ApplyReviewStatsDeltas(deltas)
	}
	return legacy, nil
}

func mutationToLegacy(m opsdb.SubtreeMutationResult) SubtreeDeleteMutationResult {
	return SubtreeDeleteMutationResult{
		Affected:      m.Affected,
		Folders:       m.Folders,
		Files:         m.Files,
		SelectedBytes: m.SelectedBytes,
	}
}
