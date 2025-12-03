// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// RootSeedSummary captures verification details after root task seeding.
type RootSeedSummary struct {
	SrcRoots int
	DstRoots int
}

// SeedRootTasks inserts the supplied source and destination root folders into BoltDB.
// The folders should already contain root-relative metadata (LocationPath="/", DepthLevel=0).
func SeedRootTasks(srcRoot types.Folder, dstRoot types.Folder, boltDB *db.DB) (RootSeedSummary, error) {
	if boltDB == nil {
		return RootSeedSummary{}, fmt.Errorf("boltDB cannot be nil")
	}

	if srcRoot.Id == "" || dstRoot.Id == "" {
		return RootSeedSummary{}, fmt.Errorf("source and destination root folders must have an Id")
	}

	if err := queue.SeedRootTasks(srcRoot, dstRoot, boltDB); err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to seed root tasks: %w", err)
	}

	var summary RootSeedSummary

	// Count root tasks from BoltDB
	srcCount, err := boltDB.CountByPrefix("SRC", 0, db.StatusPending)
	if err == nil {
		summary.SrcRoots = srcCount
	}

	dstCount, err := boltDB.CountByPrefix("DST", 0, db.StatusPending)
	if err == nil {
		summary.DstRoots = dstCount
	}

	return summary, nil
}
