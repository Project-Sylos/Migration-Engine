// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
)

// SeedRootTask inserts the initial root folder task into the database to kickstart traversal.
// For src_nodes: sets traversal_status='Pending' and copy_status='Pending'
// For dst_nodes: sets traversal_status='Pending'
func SeedRootTask(database *db.DB, tableName string, rootFolder fsservices.Folder) error {
	if tableName == "src_nodes" {
		// Source nodes have both traversal_status and copy_status
		err := database.Write(
			tableName,
			rootFolder.Id,           // id
			rootFolder.ParentId,     // parent_id (empty for root)
			rootFolder.DisplayName,  // name
			rootFolder.LocationPath, // path
			"",                      // ParentPatharent_path
			rootFolder.Type,         // type
			0,                       // depth_level (root is always 0)
			nil,                     // size (folders have no size)
			rootFolder.LastUpdated,  // last_updated
			"Pending",               // traversal_status
			"Pending",               // copy_status
		)
		if err != nil {
			return fmt.Errorf("failed to seed root task for %s: %w", tableName, err)
		}
	} else {
		// Destination nodes only have traversal_status
		err := database.Write(
			tableName,
			rootFolder.Id,           // id
			rootFolder.ParentId,     // parent_id (empty for root)
			rootFolder.DisplayName,  // name
			rootFolder.LocationPath, // path
			"",                      // parent_path
			rootFolder.Type,         // type
			0,                       // depth_level (root is always 0)
			nil,                     // size (folders have no size)
			rootFolder.LastUpdated,  // last_updated
			"Pending",               // traversal_status
		)
		if err != nil {
			return fmt.Errorf("failed to seed root task for %s: %w", tableName, err)
		}
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Seeded root task: %s (path: %s)", rootFolder.DisplayName, rootFolder.LocationPath),
			"seeding",
			tableName,
		)
	}

	return nil
}

// SeedRootTasks is a convenience function to seed both src and dst root tasks at once.
func SeedRootTasks(database *db.DB, srcRoot fsservices.Folder, dstRoot fsservices.Folder) error {
	if err := SeedRootTask(database, "src_nodes", srcRoot); err != nil {
		return fmt.Errorf("failed to seed src root: %w", err)
	}

	if err := SeedRootTask(database, "dst_nodes", dstRoot); err != nil {
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
