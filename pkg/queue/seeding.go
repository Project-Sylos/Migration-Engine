// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// SeedRootTask seeds a single root task into the database using Store API.
func SeedRootTask(s *store.Store, queueType string, root types.Folder) error {
	// Create node state
	state := &bolt.NodeState{
		ID:              root.ID(),
		ServiceID:       root.ServiceID,
		ParentID:        "",
		Name:            root.Name(),
		Path:            root.LocationPath,
		ParentPath:      "",
		Type:            "folder",
		Depth:           0,
		TraversalStatus: bolt.StatusPending,
		CopyStatus:      "", // Empty copy status for root
	}

	// Register node using Store API (handles all indexes, stats, mappings automatically)
	if err := s.RegisterNode(queueType, 0, bolt.StatusPending, state); err != nil {
		return fmt.Errorf("failed to register root node: %w", err)
	}

	return nil
}

// SeedRootTasks seeds both source and destination root tasks.
func SeedRootTasks(srcRoot types.Folder, dstRoot types.Folder, s *store.Store) error {
	// Seed source root
	if err := SeedRootTask(s, "SRC", srcRoot); err != nil {
		return fmt.Errorf("failed to seed source root: %w", err)
	}

	// Seed destination root
	if err := SeedRootTask(s, "DST", dstRoot); err != nil {
		return fmt.Errorf("failed to seed destination root: %w", err)
	}

	// Create join mapping between SRC and DST roots
	// RegisterNode handles join mappings internally, or use SetJoinMapping() for explicit control
	if err := s.SetJoinMapping(srcRoot.ID(), dstRoot.ID()); err != nil {
		// Non-fatal - join mapping may already exist or RegisterNode handled it
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("Note: join mapping setup: %v", err), "queue", "seeding", "seeding")
		}
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", fmt.Sprintf("Seeded root tasks: SRC=%s, DST=%s", srcRoot.ID(), dstRoot.ID()), "queue", "seeding", "seeding")
	}

	return nil
}

// SeedRootTaskWithSrcID seeds a root task with a specific source ID (for testing).
func SeedRootTaskWithSrcID(s *store.Store, queueType string, root types.Folder, srcID string) error {
	// Create node state
	state := &bolt.NodeState{
		ID:              root.ID(),
		ServiceID:       root.ServiceID,
		ParentID:        "",
		Name:            root.Name(),
		Path:            root.LocationPath,
		ParentPath:      "",
		Type:            "folder",
		Depth:           0,
		TraversalStatus: bolt.StatusPending,
		CopyStatus:      "", // Empty copy status for root
	}

	// Register node using Store API
	if err := s.RegisterNode(queueType, 0, bolt.StatusPending, state); err != nil {
		return fmt.Errorf("failed to register root node: %w", err)
	}

	return nil
}
