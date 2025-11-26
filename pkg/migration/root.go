// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
)

// RootSeedSummary captures verification details after root task seeding.
type RootSeedSummary struct {
	SrcRoots int
	DstRoots int
}

// SeedRootTasks inserts the supplied source and destination root folders into BadgerDB.
// The folders should already contain root-relative metadata (LocationPath="/", DepthLevel=0).
func SeedRootTasks(srcRoot fsservices.Folder, dstRoot fsservices.Folder, badgerDB *db.DB) (RootSeedSummary, error) {
	if badgerDB == nil {
		return RootSeedSummary{}, fmt.Errorf("badgerDB cannot be nil")
	}

	if srcRoot.Id == "" || dstRoot.Id == "" {
		return RootSeedSummary{}, fmt.Errorf("source and destination root folders must have an Id")
	}

	if err := queue.SeedRootTasks(srcRoot, dstRoot, badgerDB); err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to seed root tasks: %w", err)
	}

	var summary RootSeedSummary

	// Count root tasks from BadgerDB
	srcPrefix := db.PrefixForStatus("src", 0, db.TraversalStatusPending)
	dstPrefix := db.PrefixForStatus("dst", 0, db.TraversalStatusPending)

	srcCount, err := badgerDB.CountByPrefix(srcPrefix)
	if err == nil {
		summary.SrcRoots = srcCount
	}

	dstCount, err := badgerDB.CountByPrefix(dstPrefix)
	if err == nil {
		summary.DstRoots = dstCount
	}

	return summary, nil
}
