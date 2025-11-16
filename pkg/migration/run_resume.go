// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
)

// RunMigrationResume resumes a partially completed migration by reconstructing queue state from the database.
func RunMigrationResume(cfg MigrationConfig, status MigrationStatus) (RuntimeStats, error) {
	if cfg.Database == nil {
		return RuntimeStats{}, fmt.Errorf("database cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 10
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.CoordinatorLead <= 0 {
		cfg.CoordinatorLead = 3
	}
	if cfg.SrcServiceName == "" {
		cfg.SrcServiceName = "Source"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.StartupDelay == 0 {
		cfg.StartupDelay = 2 * time.Second
	}
	if cfg.ProgressTick == 0 {
		cfg.ProgressTick = 500 * time.Millisecond
	}

	if cfg.LogAddress != "" && !cfg.SkipListener {
		if err := logservice.StartListener(cfg.LogAddress); err != nil {
			fmt.Printf("Warning: failed to start log listener at %s: %v\n", cfg.LogAddress, err)
		}
		time.Sleep(cfg.StartupDelay)
	}

	if cfg.LogAddress != "" {
		if err := logservice.InitGlobalLogger(cfg.Database, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize global logger: %w", err)
		}
	}

	// Determine round windows for reconstruction
	minPendingDepthSrc := 0
	if status.MinPendingDepthSrc != nil {
		minPendingDepthSrc = *status.MinPendingDepthSrc
	}

	minPendingDepthDst := -1
	dstNeedsResume := status.DstPending > 0
	if dstNeedsResume && status.MinPendingDepthDst != nil {
		minPendingDepthDst = *status.MinPendingDepthDst
	}

	// Query max depth reached for src
	maxDepthSrc, err := queryMaxDepth(cfg.Database, "src_nodes")
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("failed to query max src depth: %w", err)
	}
	if maxDepthSrc < minPendingDepthSrc {
		maxDepthSrc = minPendingDepthSrc
	}

	// Compute src round window: if dst needs resume, ensure src includes the round dst needs to query
	srcRoundStart := minPendingDepthSrc
	srcRoundEnd := maxDepthSrc
	if dstNeedsResume && minPendingDepthDst >= 0 {
		// DST on round R queries src tasks from round R+1
		// Also ensure src is at least coordinatorLead rounds ahead for future advancement
		requiredSrcRoundForDst := minPendingDepthDst + 1                    // DST needs this round to query
		requiredSrcRoundForLead := minPendingDepthDst + cfg.CoordinatorLead // For coordinator constraint
		requiredSrcRound := requiredSrcRoundForDst
		if requiredSrcRoundForLead > requiredSrcRound {
			requiredSrcRound = requiredSrcRoundForLead
		}
		if requiredSrcRound > srcRoundEnd {
			srcRoundEnd = requiredSrcRound
		}
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("info",
			fmt.Sprintf("Resuming migration: src rounds [%d-%d], dst frontier: %d", srcRoundStart, srcRoundEnd, minPendingDepthDst),
			"migration",
			"resume",
		)
	}

	coordinator := queue.NewQueueCoordinator(cfg.CoordinatorLead)

	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.Initialize(cfg.Database, "src_nodes", cfg.SrcAdapter, nil)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.Initialize(cfg.Database, "dst_nodes", cfg.DstAdapter, srcQueue)

	// Reconstruct src RoundQueues
	if err := reconstructSrcQueues(cfg.Database, srcQueue, srcRoundStart, srcRoundEnd); err != nil {
		return RuntimeStats{}, fmt.Errorf("failed to reconstruct src queues: %w", err)
	}

	// Set src queue round to the lowest pending depth
	srcQueue.SetRound(minPendingDepthSrc)
	coordinator.UpdateSrcRound(minPendingDepthSrc)

	// Reconstruct dst RoundQueues if needed
	if dstNeedsResume && minPendingDepthDst >= 0 {
		if err := reconstructDstQueues(cfg.Database, dstQueue, srcQueue, minPendingDepthDst); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to reconstruct dst queues: %w", err)
		}

		dstQueue.SetRound(minPendingDepthDst)
		coordinator.UpdateDstRound(minPendingDepthDst)
	} else {
		// DST is already completed, mark it as such
		coordinator.UpdateDstCompleted()
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "DST queue already completed, only resuming SRC", "migration", "resume")
		}
	}

	progressTicker := time.NewTicker(cfg.ProgressTick)
	defer progressTicker.Stop()

	start := time.Now()

	for {
		if srcQueue.IsExhausted() && dstQueue.IsExhausted() {
			srcStats := srcQueue.Stats()
			dstStats := dstQueue.Stats()
			fmt.Println("\nMigration complete!")
			return RuntimeStats{
				Duration: time.Since(start),
				Src:      srcStats,
				Dst:      dstStats,
			}, nil
		}

		select {
		case <-progressTicker.C:
			srcStats := srcQueue.Stats()
			dstStats := dstQueue.Stats()
			fmt.Printf("\r  Src: Round %d (Pending:%d InProgress:%d Workers:%d) | Dst: Round %d (Pending:%d InProgress:%d Workers:%d)   ",
				srcStats.Round, srcStats.Pending, srcStats.InProgress, srcStats.Workers,
				dstStats.Round, dstStats.Pending, dstStats.InProgress, dstStats.Workers)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// reconstructSrcQueues rebuilds src RoundQueues for the specified round range.
// For each round, it loads pending folders as pending tasks and successful folders
// as successful tasks (with DiscoveredChildren reconstructed from DB).
func reconstructSrcQueues(database *db.DB, srcQueue *queue.Queue, roundStart, roundEnd int) error {
	for round := roundStart; round <= roundEnd; round++ {
		// Load pending folders at this depth
		pendingFolders, err := loadFoldersByDepthAndStatus(database, "src_nodes", round, "Pending")
		if err != nil {
			return fmt.Errorf("failed to load pending src folders at depth %d: %w", round, err)
		}

		// Load successful folders at this depth (needed for dst to query expected children)
		successfulFolders, err := loadFoldersByDepthAndStatus(database, "src_nodes", round, "Successful")
		if err != nil {
			return fmt.Errorf("failed to load successful src folders at depth %d: %w", round, err)
		}

		// Add pending tasks
		for _, folder := range pendingFolders {
			task := &queue.TaskBase{
				Type:   queue.TaskTypeSrcTraversal,
				Folder: folder,
				Round:  round,
			}
			if !srcQueue.AddToRoundPending(round, task) {
				// Task already exists, skip
				continue
			}
		}

		// Add successful tasks with reconstructed DiscoveredChildren
		for _, folder := range successfulFolders {
			children, err := reconstructDiscoveredChildren(database, "src_nodes", folder.LocationPath)
			if err != nil {
				return fmt.Errorf("failed to reconstruct children for %s: %w", folder.LocationPath, err)
			}

			task := &queue.TaskBase{
				Type:               queue.TaskTypeSrcTraversal,
				Folder:             folder,
				Round:              round,
				Status:             "successful",
				DiscoveredChildren: children,
			}
			if !srcQueue.AddToRoundSuccessful(round, task) {
				// Task already exists, skip
				continue
			}
		}
	}

	return nil
}

// reconstructDstQueues rebuilds dst RoundQueues starting from the frontier round.
// For each pending dst folder, it reconstructs ExpectedFolders/ExpectedFiles from src.
func reconstructDstQueues(database *db.DB, dstQueue *queue.Queue, srcQueue *queue.Queue, dstFrontier int) error {
	// Load pending dst folders at the frontier round
	pendingFolders, err := loadFoldersByDepthAndStatus(database, "dst_nodes", dstFrontier, "Pending")
	if err != nil {
		return fmt.Errorf("failed to load pending dst folders at depth %d: %w", dstFrontier, err)
	}

	// Also load successful dst folders at this round (for consistency)
	successfulFolders, err := loadFoldersByDepthAndStatus(database, "dst_nodes", dstFrontier, "Successful")
	if err != nil {
		return fmt.Errorf("failed to load successful dst folders at depth %d: %w", dstFrontier, err)
	}

	// Add pending tasks with expected children from src
	for _, folder := range pendingFolders {
		// Get expected children from src (src round = dst round + 1, because src is one round ahead)
		srcRound := dstFrontier + 1
		srcTask := srcQueue.GetSuccessfulTask(srcRound, folder.LocationPath)
		if srcTask == nil {
			// Fallback: query directly from DB
			expectedFolders, expectedFiles, err := queue.LoadExpectedChildren(database, folder.LocationPath)
			if err != nil {
				return fmt.Errorf("failed to load expected children for %s: %w", folder.LocationPath, err)
			}

			task := &queue.TaskBase{
				Type:            queue.TaskTypeDstTraversal,
				Folder:          folder,
				Round:           dstFrontier,
				ExpectedFolders: expectedFolders,
				ExpectedFiles:   expectedFiles,
			}
			if !dstQueue.AddToRoundPending(dstFrontier, task) {
				// Task already exists, skip
				continue
			}
		} else {
			// Use src task's discovered children
			var expectedFolders []fsservices.Folder
			var expectedFiles []fsservices.File
			for _, child := range srcTask.DiscoveredChildren {
				if child.IsFile {
					expectedFiles = append(expectedFiles, child.File)
				} else {
					expectedFolders = append(expectedFolders, child.Folder)
				}
			}

			task := &queue.TaskBase{
				Type:            queue.TaskTypeDstTraversal,
				Folder:          folder,
				Round:           dstFrontier,
				ExpectedFolders: expectedFolders,
				ExpectedFiles:   expectedFiles,
			}
			if !dstQueue.AddToRoundPending(dstFrontier, task) {
				// Task already exists, skip
				continue
			}
		}
	}

	// Add successful tasks (for stats/consistency)
	for _, folder := range successfulFolders {
		children, err := reconstructDiscoveredChildren(database, "dst_nodes", folder.LocationPath)
		if err != nil {
			return fmt.Errorf("failed to reconstruct dst children for %s: %w", folder.LocationPath, err)
		}

		task := &queue.TaskBase{
			Type:               queue.TaskTypeDstTraversal,
			Folder:             folder,
			Round:              dstFrontier,
			Status:             "successful",
			DiscoveredChildren: children,
		}
		if !dstQueue.AddToRoundSuccessful(dstFrontier, task) {
			// Task already exists, skip
			continue
		}
	}

	return nil
}

// loadFoldersByDepthAndStatus loads folders from the specified table at the given depth and status.
func loadFoldersByDepthAndStatus(database *db.DB, table string, depth int, status string) ([]fsservices.Folder, error) {
	query := fmt.Sprintf(
		"SELECT id, parent_id, name, path, parent_path, type, depth_level, size, last_updated FROM %s WHERE type = 'folder' AND depth_level = ? AND traversal_status = ?",
		table,
	)
	rows, err := database.Query(query, depth, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var folders []fsservices.Folder
	for rows.Next() {
		var (
			id, parentID, name, path, parentPath, nodeType string
			depthLevel                                     int
			size                                           sql.NullInt64
			lastUpdated                                    sql.NullString
		)
		if err := rows.Scan(&id, &parentID, &name, &path, &parentPath, &nodeType, &depthLevel, &size, &lastUpdated); err != nil {
			return nil, err
		}
		if nodeType != fsservices.NodeTypeFolder {
			continue
		}

		folder := fsservices.Folder{
			Id:           id,
			ParentId:     parentID,
			ParentPath:   parentPath,
			DisplayName:  name,
			LocationPath: path,
			DepthLevel:   depthLevel,
			Type:         fsservices.NodeTypeFolder,
		}
		if lastUpdated.Valid {
			folder.LastUpdated = lastUpdated.String
		}
		folders = append(folders, folder)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return folders, nil
}

// reconstructDiscoveredChildren queries the database to rebuild DiscoveredChildren for a completed task.
func reconstructDiscoveredChildren(database *db.DB, table string, parentPath string) ([]queue.ChildResult, error) {
	query := fmt.Sprintf(
		"SELECT id, parent_id, name, path, parent_path, type, depth_level, size, last_updated, traversal_status FROM %s WHERE parent_path = ?",
		table,
	)
	rows, err := database.Query(query, parentPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []queue.ChildResult
	for rows.Next() {
		var (
			id, parentID, name, path, nodeParentPath, nodeType, traversalStatus string
			depthLevel                                                          int
			size                                                                sql.NullInt64
			lastUpdated                                                         sql.NullString
		)
		if err := rows.Scan(&id, &parentID, &name, &path, &nodeParentPath, &nodeType, &depthLevel, &size, &lastUpdated, &traversalStatus); err != nil {
			return nil, err
		}

		last := ""
		if lastUpdated.Valid {
			last = lastUpdated.String
		}

		switch nodeType {
		case fsservices.NodeTypeFolder:
			children = append(children, queue.ChildResult{
				Folder: fsservices.Folder{
					Id:           id,
					ParentId:     parentID,
					ParentPath:   nodeParentPath,
					DisplayName:  name,
					LocationPath: path,
					LastUpdated:  last,
					DepthLevel:   depthLevel,
					Type:         fsservices.NodeTypeFolder,
				},
				Status: traversalStatus,
				IsFile: false,
			})
		case fsservices.NodeTypeFile:
			fileSize := int64(0)
			if size.Valid {
				fileSize = size.Int64
			}
			children = append(children, queue.ChildResult{
				File: fsservices.File{
					Id:           id,
					ParentId:     parentID,
					ParentPath:   nodeParentPath,
					DisplayName:  name,
					LocationPath: path,
					LastUpdated:  last,
					DepthLevel:   depthLevel,
					Size:         fileSize,
					Type:         fsservices.NodeTypeFile,
				},
				Status: traversalStatus,
				IsFile: true,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return children, nil
}

// queryMaxDepth returns the maximum depth_level found in the specified table.
func queryMaxDepth(database *db.DB, table string) (int, error) {
	query := fmt.Sprintf("SELECT MAX(depth_level) FROM %s", table)
	rows, err := database.Query(query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, nil
	}

	var maxDepth sql.NullInt64
	if err := rows.Scan(&maxDepth); err != nil {
		return 0, err
	}

	if !maxDepth.Valid {
		return 0, nil
	}

	return int(maxDepth.Int64), nil
}
