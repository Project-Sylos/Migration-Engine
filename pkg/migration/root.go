// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// RootSeedSummary captures verification details after root task seeding.
type RootSeedSummary struct {
	SrcRoots int
	DstRoots int
}

// SeedRootTasks inserts the supplied source and destination root folders into the database.
func SeedRootTasks(srcRoot types.Folder, dstRoot types.Folder, database *db.DB) (RootSeedSummary, error) {
	return SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{})
}

// SeedRootTasksWithPreparation seeds roots and optionally injects UI-reviewed depth-1 children.
func SeedRootTasksWithPreparation(srcRoot, dstRoot types.Folder, database *db.DB, prep RootPreparation) (RootSeedSummary, error) {
	if database == nil {
		return RootSeedSummary{}, fmt.Errorf("database cannot be nil")
	}

	if srcRoot.ServiceID == "" || dstRoot.ServiceID == "" {
		return RootSeedSummary{}, fmt.Errorf("source and destination root folders must have a ServiceID")
	}

	if err := queue.SeedRootTasksPrepared(srcRoot, dstRoot, database, prep.SourcePrepared, prep.DestPrepared); err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to seed root tasks: %w", err)
	}

	if prep.SourcePrepared || prep.DestPrepared {
		srcKids := toPreparedChildren(prep.SourceChildren)
		dstKids := toPreparedChildren(prep.DestChildren)
		if err := queue.SeedPreparedDepth1Children(database, srcKids, dstKids); err != nil {
			return RootSeedSummary{}, fmt.Errorf("failed to seed prepared children: %w", err)
		}
	}

	var summary RootSeedSummary
	srcKey := db.StatsKey(db.StatsKindTraversal, db.StatusPending)
	if prep.SourcePrepared {
		srcKey = db.StatsKey(db.StatsKindTraversal, db.StatusSuccessful)
	}
	c, err := stats.GetStatsCountAtDepth(database, "SRC", 0, srcKey)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to get stats count at depth: %w", err)
	}
	summary.SrcRoots = int(c)
	if summary.SrcRoots == 0 {
		summary.SrcRoots = 1
	}
	dstKey := db.StatsKey(db.StatsKindTraversal, db.StatusPending)
	if prep.DestPrepared {
		dstKey = db.StatsKey(db.StatsKindTraversal, db.StatusSuccessful)
	}
	c, err = stats.GetStatsCountAtDepth(database, "DST", 0, dstKey)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to get stats count at depth: %w", err)
	}
	summary.DstRoots = int(c)
	if summary.DstRoots == 0 {
		summary.DstRoots = 1
	}
	return summary, nil
}

func toPreparedChildren(in []RootChildSeed) []queue.PreparedChild {
	if len(in) == 0 {
		return nil
	}
	out := make([]queue.PreparedChild, 0, len(in))
	for _, c := range in {
		out = append(out, queue.PreparedChild{
			ServiceID: c.ServiceID,
			Name:      c.Name,
			Type:      c.Type,
			Size:      c.Size,
			MTime:     c.MTime,
			Excluded:  c.Excluded,
			DstOnly:   c.DstOnly,
		})
	}
	return out
}
