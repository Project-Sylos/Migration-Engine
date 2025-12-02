// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// SeedRootTask inserts the initial root folder task into BoltDB to kickstart traversal.
// For src: sets traversal_status='Pending' and copy_status='Pending'
// For dst: sets traversal_status='Pending'
// Ensures stats bucket exists and is updated with root task counts.
func SeedRootTask(queueType string, rootFolder fsservices.Folder, boltDB *db.DB) error {
	if boltDB == nil {
		return fmt.Errorf("boltDB cannot be nil")
	}

	// Ensure stats bucket exists before seeding
	// This is critical because root seeding happens before queues are initialized
	if err := boltDB.EnsureStatsBucket(); err != nil {
		return fmt.Errorf("failed to ensure stats bucket exists: %w", err)
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

	// Populate traversal status in the NodeState metadata
	state.TraversalStatus = db.StatusPending

	// Use BatchInsertNodes to write to BoltDB
	// BatchInsertNodes now updates stats automatically
	ops := []db.InsertOperation{
		{
			QueueType: queueType,
			Level:     0,
			Status:    db.StatusPending,
			State:     state,
		},
	}

	if err := db.BatchInsertNodes(boltDB, ops); err != nil {
		return fmt.Errorf("failed to seed root task to BoltDB for %s: %w", queueType, err)
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
// Writes root tasks to BoltDB.
func SeedRootTasks(srcRoot fsservices.Folder, dstRoot fsservices.Folder, boltDB *db.DB) error {
	if err := SeedRootTask("SRC", srcRoot, boltDB); err != nil {
		return fmt.Errorf("failed to seed src root: %w", err)
	}

	if err := SeedRootTask("DST", dstRoot, boltDB); err != nil {
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
