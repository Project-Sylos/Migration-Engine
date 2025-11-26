// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// SeedRootTask inserts the initial root folder task into BadgerDB to kickstart traversal.
// For src: sets traversal_status='Pending' and copy_status='Pending'
// For dst: sets traversal_status='Pending'
func SeedRootTask(queueType string, rootFolder fsservices.Folder, badgerDB *db.DB) error {
	if badgerDB == nil {
		return fmt.Errorf("badgerDB cannot be nil")
	}

	// Create NodeState from root folder
	state := &db.NodeState{
		ID:         rootFolder.Id,
		ParentID:   rootFolder.ParentId,
		ParentPath: "", // Root has no parent
		Name:       rootFolder.DisplayName,
		Path:       fsservices.NormalizeLocationPath(rootFolder.LocationPath),
		Type:       rootFolder.Type,
		Size:       0, // Folders have no size
		MTime:      rootFolder.LastUpdated,
		Depth:      0, // Root is always depth 0
		CopyNeeded: false,
		Status:     "Pending",
	}

	// Determine copy status
	copyStatus := db.CopyStatusPending
	if queueType == "dst" {
		copyStatus = "" // DST nodes don't use copy_status
	}

	// Use BatchInsertNodes to write to BadgerDB
	indexPrefix := db.IndexPrefixSrcChildren
	if queueType == "dst" {
		indexPrefix = db.IndexPrefixDstChildren
	}

	ops := []db.InsertOperation{
		{
			QueueType:       queueType,
			Level:           0,
			TraversalStatus: db.TraversalStatusPending,
			CopyStatus:      copyStatus,
			State:           state,
			IndexPrefix:     indexPrefix,
		},
	}

	if err := db.BatchInsertNodes(badgerDB, ops); err != nil {
		return fmt.Errorf("failed to seed root task to BadgerDB for %s: %w", queueType, err)
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Seeded root task: %s (path: %s)", rootFolder.DisplayName, rootFolder.LocationPath),
			"seeding",
			queueType,
		)
	}

	return nil
}

// SeedRootTasks is a convenience function to seed both src and dst root tasks at once.
// Writes root tasks to BadgerDB.
func SeedRootTasks(srcRoot fsservices.Folder, dstRoot fsservices.Folder, badgerDB *db.DB) error {
	if err := SeedRootTask("src", srcRoot, badgerDB); err != nil {
		return fmt.Errorf("failed to seed src root: %w", err)
	}

	if err := SeedRootTask("dst", dstRoot, badgerDB); err != nil {
		return fmt.Errorf("failed to seed dst root: %w", err)
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			"Successfully seeded both src and dst root tasks",
			"seeding",
			"main",
		)
	}

	return nil
}
