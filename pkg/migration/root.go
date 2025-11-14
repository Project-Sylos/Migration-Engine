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

// SeedRootTasks inserts the supplied source and destination root folders into the database.
// The folders should already contain root-relative metadata (LocationPath="/", DepthLevel=0).
func SeedRootTasks(database *db.DB, srcRoot fsservices.Folder, dstRoot fsservices.Folder) (RootSeedSummary, error) {
	if database == nil {
		return RootSeedSummary{}, fmt.Errorf("database cannot be nil")
	}

	if srcRoot.Id == "" || dstRoot.Id == "" {
		return RootSeedSummary{}, fmt.Errorf("source and destination root folders must have an Id")
	}

	if err := queue.SeedRootTasks(database, srcRoot, dstRoot); err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to seed root tasks: %w", err)
	}

	var summary RootSeedSummary

	rows, err := database.Query("SELECT COUNT(*) FROM src_nodes WHERE depth_level = 0")
	if err == nil && rows != nil {
		if rows.Next() {
			_ = rows.Scan(&summary.SrcRoots)
		}
		rows.Close()
	}

	rows, err = database.Query("SELECT COUNT(*) FROM dst_nodes WHERE depth_level = 0")
	if err == nil && rows != nil {
		if rows.Next() {
			_ = rows.Scan(&summary.DstRoots)
		}
		rows.Close()
	}

	return summary, nil
}
