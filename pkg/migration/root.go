// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// RootSeedSummary captures verification details after root task seeding.
type RootSeedSummary struct {
	SrcRoots int
	DstRoots int
}

// SeedRootTasks inserts the supplied source and destination root folders into the database.
// The folders should already contain root-relative metadata (LocationPath="/", DepthLevel=0).
func SeedRootTasks(srcRoot types.Folder, dstRoot types.Folder, database *db.DB) (RootSeedSummary, error) {
	if database == nil {
		return RootSeedSummary{}, fmt.Errorf("database cannot be nil")
	}

	if srcRoot.ServiceID == "" || dstRoot.ServiceID == "" {
		return RootSeedSummary{}, fmt.Errorf("source and destination root folders must have a ServiceID")
	}

	if err := queue.SeedRootTasks(srcRoot, dstRoot, database); err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to seed root tasks: %w", err)
	}

	var summary RootSeedSummary
	// Stats for depth 0 are updated at seal; until then use counts from stats table or 1/1 after seeding roots
	c, err := database.GetStatsCountAtDepth("SRC", 0, db.StatsKey(db.StatsKindTraversal,db.StatusPending))
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to get stats count at depth: %w", err)
	}
	summary.SrcRoots = int(c)
	if summary.SrcRoots == 0 {
		summary.SrcRoots = 1 // we just inserted the root
	}
	c, err = database.GetStatsCountAtDepth("DST", 0, db.StatsKey(db.StatsKindTraversal,db.StatusPending))
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to get stats count at depth: %w", err)
	}
	summary.DstRoots = int(c)
	if summary.DstRoots == 0 {
		summary.DstRoots = 1
	}
	return summary, nil
}
