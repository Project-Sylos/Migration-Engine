// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// RootSeedSummary captures verification details after root task seeding.
type RootSeedSummary struct {
	SrcRoots int
	DstRoots int
}

// SeedRootTasksWithPreparation seeds roots and optionally injects UI-reviewed depth-1 children.
// Path-check args mirror traversal seal: same ResolvePathCheckTarget + ApplyGPLToSRCChildren on SRC kids.
func SeedRootTasksWithPreparation(srcRoot, dstRoot types.Folder, database *db.DB, prep RootPreparation, srcProvider, dstProvider, pathCheckProfile string, windowsCompat bool, rules *filter.CompiledRuleset) (RootSeedSummary, error) {
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
		checkTarget := gpl.ResolvePathCheckTarget(srcProvider, dstProvider, pathCheckProfile)
		skipChecks := checkTarget == ""
		target := gpl.GPLTargetFromProvider(checkTarget)
		if err := queue.SeedPreparedDepth1Children(database, srcKids, dstKids, func(srcNodes []*db.NodeState) {
			// Root-relative children: parent path_len is 0 (same as listing under "/").
			gpl.ApplyGPLToSRCChildren(database, target, 0, srcNodes, skipChecks, windowsCompat)
		}, rules); err != nil {
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
			ServiceID:   c.ServiceID,
			Name:        c.Name,
			Type:        c.Type,
			Size:        c.Size,
			MTime:       c.MTime,
			Excluded:    c.Excluded,
			DstOnly:     c.DstOnly,
			Children:    toPreparedChildren(c.Children),
			IncludeOnly: append([]string(nil), c.IncludeOnly...),
		})
	}
	return out
}
