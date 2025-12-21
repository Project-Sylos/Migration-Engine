// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// RootSeedSummary captures verification details after root task seeding.
type RootSeedSummary struct {
	SrcRoots int
	DstRoots int
}

// SeedRootTasks inserts the supplied source and destination root folders into BoltDB.
// The folders should already contain root-relative metadata (LocationPath="/", DepthLevel=0).
func SeedRootTasks(srcRoot types.Folder, dstRoot types.Folder, dbOrStore any) (RootSeedSummary, error) {
	if dbOrStore == nil {
		return RootSeedSummary{}, fmt.Errorf("database instance cannot be nil")
	}

	if srcRoot.ServiceID == "" || dstRoot.ServiceID == "" {
		return RootSeedSummary{}, fmt.Errorf("source and destination root folders must have a ServiceID")
	}

	// queue.SeedRootTasks now requires *store.Store
	storeInstance, ok := dbOrStore.(*store.Store)
	if !ok {
		return RootSeedSummary{}, fmt.Errorf("dbOrStore must be *store.Store")
	}
	if err := queue.SeedRootTasks(srcRoot, dstRoot, storeInstance); err != nil {
		return RootSeedSummary{}, fmt.Errorf("failed to seed root tasks: %w", err)
	}

	// Return summary with known root counts
	return RootSeedSummary{
		SrcRoots: 1,
		DstRoots: 1,
	}, nil
}
