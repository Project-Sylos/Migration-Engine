// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
)

// MigrationConfig encapsulates the inputs required to execute a traversal migration.
type MigrationConfig struct {
	Database *db.DB

	SrcAdapter     fsservices.FSAdapter
	DstAdapter     fsservices.FSAdapter
	SrcRoot        fsservices.Folder
	DstRoot        fsservices.Folder
	SrcServiceName string

	WorkerCount     int
	MaxRetries      int
	CoordinatorLead int

	LogAddress   string
	LogLevel     string
	SkipListener bool
	StartupDelay time.Duration
	ProgressTick time.Duration

	// Resume state: if provided, migration will resume from existing state instead of starting fresh
	ResumeStatus *MigrationStatus

	// Config YAML management
	ConfigPath string
	YAMLConfig *MigrationConfigYAML

	// Shutdown context: if provided, migration will check for cancellation and perform force shutdown
	ShutdownContext context.Context
}

// RuntimeStats captures high-level metrics collected after a migration run.
type RuntimeStats struct {
	Duration time.Duration
	Src      queue.QueueStats
	Dst      queue.QueueStats
}

// RunMigration orchestrates queue setup, logging, task seeding, and completion monitoring.
func RunMigration(cfg MigrationConfig) (RuntimeStats, error) {
	if cfg.Database == nil {
		return RuntimeStats{}, fmt.Errorf("database cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}
	if cfg.SrcRoot.Id == "" || cfg.DstRoot.Id == "" {
		return RuntimeStats{}, fmt.Errorf("source and destination root folders must include an Id")
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

	// Initialize logger if not already initialized (logger should be managed at a higher level)
	if cfg.LogAddress != "" && logservice.LS == nil {
		if err := logservice.InitGlobalLogger(cfg.Database, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize global logger: %w", err)
		}
	}

	coordinator := queue.NewQueueCoordinator(cfg.CoordinatorLead)

	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.InitializeWithContext(cfg.Database, "src_nodes", cfg.SrcAdapter, nil, cfg.ShutdownContext)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.InitializeWithContext(cfg.Database, "dst_nodes", cfg.DstAdapter, srcQueue, cfg.ShutdownContext)

	// Initialize queues: either resume from existing state or seed fresh root tasks
	if cfg.ResumeStatus != nil && cfg.ResumeStatus.HasPending() {
		// Resume: reconstruct queue state from database
		if err := initializeQueuesFromResume(cfg, srcQueue, dstQueue, coordinator, *cfg.ResumeStatus); err != nil {
			return RuntimeStats{}, err
		}
	} else {
		// Fresh: seed root tasks from database
		if err := initializeQueuesFromFresh(cfg, srcQueue, dstQueue); err != nil {
			return RuntimeStats{}, err
		}
	}

	// Update config: Traversal started
	if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
		status, statusErr := InspectMigrationStatus(cfg.Database)
		if statusErr == nil {
			srcStats := srcQueue.Stats()
			dstStats := dstQueue.Stats()
			UpdateConfigFromStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
			cfg.YAMLConfig.State.Status = "running"
			_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
		}
	}

	progressTicker := time.NewTicker(cfg.ProgressTick)
	defer progressTicker.Stop()

	start := time.Now()

	// Track last known rounds to detect actual advancement
	lastSrcRound := -1
	lastDstRound := -1
	tickCount := 0

	for {
		// Check for force shutdown (context cancellation)
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				// Force shutdown: pause queues, checkpoint DB, and save suspended state
				// Pause queues to stop new task leasing (workers will exit via shutdown context)
				srcQueue.Pause()
				dstQueue.Pause()

				// Give workers a moment to finish current tasks (they check shutdown context in their loop)
				// This is non-blocking - workers will exit when they check context in next iteration
				time.Sleep(200 * time.Millisecond)

				srcStats := srcQueue.Stats()
				dstStats := dstQueue.Stats()

				// Checkpoint database to ensure WAL is flushed
				if err := cfg.Database.Checkpoint(); err != nil {
					fmt.Printf("Warning: failed to checkpoint database during shutdown: %v\n", err)
				}

				// Update YAML config with suspended status and current state
				if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
					status, statusErr := InspectMigrationStatus(cfg.Database)
					if statusErr == nil {
						SetSuspendedStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
						_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
					}
				}

				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, errors.New("migration suspended by force shutdown")
			default:
				// Continue normal execution
			}
		}

		if srcQueue.IsExhausted() && dstQueue.IsExhausted() {
			srcStats := srcQueue.Stats()
			dstStats := dstQueue.Stats()
			fmt.Println("\nMigration complete!")

			// Update config YAML with final state
			if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
				status, statusErr := InspectMigrationStatus(cfg.Database)
				if statusErr == nil {
					UpdateConfigFromStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
					// Save final config
					_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
				}
			}

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

			// Update config YAML when rounds actually advance (milestone detection)
			roundAdvanced := false
			if srcStats.Round != lastSrcRound || dstStats.Round != lastDstRound {
				roundAdvanced = true
				lastSrcRound = srcStats.Round
				lastDstRound = dstStats.Round
			}

			tickCount++
			// Update config on round advancement or periodically (every 10 ticks)
			shouldUpdate := roundAdvanced || (tickCount%10 == 0)

			if cfg.YAMLConfig != nil && cfg.ConfigPath != "" && shouldUpdate {
				status, statusErr := InspectMigrationStatus(cfg.Database)
				if statusErr == nil {
					UpdateConfigFromStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
					// Save config (ignore errors to avoid disrupting migration)
					_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
				}
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// initializeQueuesFromFresh seeds root tasks for a fresh migration
func initializeQueuesFromFresh(cfg MigrationConfig, srcQueue *queue.Queue, dstQueue *queue.Queue) error {
	// Load root tasks from database and seed into queues
	srcRootFolders, err := queue.LoadRootFolders(cfg.Database, "src_nodes")
	if err != nil {
		return fmt.Errorf("failed to load src root folders: %w", err)
	}
	if len(srcRootFolders) == 0 {
		return fmt.Errorf("no src root folders found in database; ensure seeding completed")
	}
	if len(srcRootFolders) > 1 {
		return fmt.Errorf("expected exactly one src root folder, found %d", len(srcRootFolders))
	}

	// Seed SRC root task from database
	srcRootTask := &queue.TaskBase{
		Type:   queue.TaskTypeSrcTraversal,
		Folder: srcRootFolders[0],
		Round:  0,
	}
	if !srcQueue.Add(srcRootTask) {
		return fmt.Errorf("failed to enqueue source root task")
	}

	// Wait for SRC to complete round 0, then seed DST root with expected children
	if err := waitForSrcRoundAdvance(srcQueue, 0, cfg.ProgressTick); err != nil {
		return err
	}

	// Load DST root from database
	dstRootFolders, err := queue.LoadRootFolders(cfg.Database, "dst_nodes")
	if err != nil {
		return fmt.Errorf("failed to load dst root folders: %w", err)
	}
	if len(dstRootFolders) == 0 {
		return fmt.Errorf("no dst root folders found in database; ensure seeding completed")
	}
	if len(dstRootFolders) > 1 {
		return fmt.Errorf("expected exactly one dst root folder, found %d", len(dstRootFolders))
	}

	// Get SRC root task from successful queue to extract expected children
	return seedDstQueueRoot(srcQueue, dstQueue, dstRootFolders[0])
}

// initializeQueuesFromResume reconstructs queue state from database for a resume migration
func initializeQueuesFromResume(cfg MigrationConfig, srcQueue *queue.Queue, dstQueue *queue.Queue, coordinator *queue.QueueCoordinator, status MigrationStatus) error {
	// Find minimum pending depths for src and dst
	minPendingDepthSrc := 0
	if status.MinPendingDepthSrc != nil {
		minPendingDepthSrc = *status.MinPendingDepthSrc
	}

	minPendingDepthDst := -1
	dstNeedsResume := status.DstPending > 0
	if dstNeedsResume && status.MinPendingDepthDst != nil {
		minPendingDepthDst = *status.MinPendingDepthDst
	}

	// Query max depth reached for src to determine reconstruction range
	maxDepthSrc, err := queryMaxDepth(cfg.Database, "src_nodes")
	if err != nil {
		return fmt.Errorf("failed to query max src depth: %w", err)
	}
	if maxDepthSrc < minPendingDepthSrc {
		maxDepthSrc = minPendingDepthSrc
	}

	// Ensure src covers rounds dst needs for future advancement
	// DST on round R needs src successful tasks from round R+1 when it completes
	// Also need src to be at least coordinatorLead rounds ahead
	if dstNeedsResume && minPendingDepthDst >= 0 {
		requiredForDst := minPendingDepthDst + 1
		requiredForLead := minPendingDepthDst + cfg.CoordinatorLead
		requiredRound := requiredForDst
		if requiredForLead > requiredRound {
			requiredRound = requiredForLead
		}
		if requiredRound > maxDepthSrc {
			maxDepthSrc = requiredRound
		}
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log("info",
			fmt.Sprintf("Resuming migration: src pending from round %d, max depth %d; dst frontier: %d", minPendingDepthSrc, maxDepthSrc, minPendingDepthDst),
			"migration",
			"resume",
		)
	}

	// Reconstruct src RoundQueues:
	// - Pending tasks from minPendingDepthSrc onwards (src needs to continue work)
	// - Successful tasks for all rounds that dst needs for coordination
	//   * DST tasks at round R need expected children from src round R (same round)
	//   * When dst completes round R, it needs src successful tasks from round R+1
	srcReconstructStart := minPendingDepthSrc
	srcReconstructEnd := maxDepthSrc

	// Ensure we have src successful tasks for rounds dst needs
	// DST at round R needs src successful tasks from:
	// - Round R (for current tasks' expected children when reconstructing)
	// - Round R+1 (for future tasks when dst completes round R)
	if dstNeedsResume && minPendingDepthDst >= 0 {
		// We need src successful tasks for both round R and R+1
		// Start from the earlier of these rounds if it's before minPendingDepthSrc
		earliestNeededRound := minPendingDepthDst
		if earliestNeededRound < srcReconstructStart {
			srcReconstructStart = earliestNeededRound
		}
		// Ensure we cover at least up to R+1
		if minPendingDepthDst+1 > srcReconstructEnd {
			srcReconstructEnd = minPendingDepthDst + 1
		}
	}

	// Reconstruct src queues (pending + successful) for the range
	if err := reconstructSrcQueues(cfg.Database, srcQueue, srcReconstructStart, srcReconstructEnd); err != nil {
		return fmt.Errorf("failed to reconstruct src queues: %w", err)
	}

	// Build bridge of successful src tasks for rounds before minPendingDepthSrc that dst needs
	// DST at round R needs src successful tasks from:
	// - Round R (for reconstructing current dst tasks at round R)
	// - Round R+1 (for when dst completes round R and creates tasks for round R+1)
	// If src has no pending tasks in those rounds, we need to bridge the gap
	if dstNeedsResume && minPendingDepthDst >= 0 {
		// Bridge should cover rounds from minPendingDepthDst up to minPendingDepthSrc - 1
		// This ensures dst can find src tasks for current round and next round
		bridgeStart := minPendingDepthDst
		if bridgeStart < minPendingDepthSrc {
			bridgeEnd := minPendingDepthSrc - 1
			if bridgeEnd >= bridgeStart {
				if logservice.LS != nil {
					_ = logservice.LS.Log("info",
						fmt.Sprintf("Building src bridge: rounds [%d-%d] (successful tasks for dst rounds %d-%d)", bridgeStart, bridgeEnd, minPendingDepthDst, minPendingDepthDst+1),
						"migration",
						"resume",
					)
				}
				if err := reconstructSrcQueuesBridge(cfg.Database, srcQueue, bridgeStart, bridgeEnd); err != nil {
					return fmt.Errorf("failed to reconstruct src bridge queues: %w", err)
				}
			}
		}
	}

	// Set src queue round and coordinator state
	srcQueue.SetRound(minPendingDepthSrc)
	coordinator.UpdateSrcRound(minPendingDepthSrc)

	// Reconstruct dst RoundQueues if needed
	if dstNeedsResume && minPendingDepthDst >= 0 {
		// Prevent workers from leasing until coordinator confirms dst can safely resume
		dstQueue.Pause()

		// Ensure SRC has (re)materialized the expected-children buffer for this dst frontier.
		// When the previous run aborted before SRC finished the corresponding round, the DB
		// won't have any children yet. We need to let SRC re-run that round and insert its
		// discoveries before we reconstruct dst tasks, otherwise they'll be seeded with empty
		// expectations and immediately mark every child as NotOnSrc.
		if srcQueue.Round() <= minPendingDepthDst {
			if err := waitForSrcRoundAdvance(srcQueue, minPendingDepthDst, cfg.ProgressTick); err != nil {
				return err
			}
		}

		if err := reconstructDstQueues(cfg.Database, dstQueue, srcQueue, minPendingDepthDst); err != nil {
			return fmt.Errorf("failed to reconstruct dst queues: %w", err)
		}

		dstQueue.SetRound(minPendingDepthDst)
		coordinator.UpdateDstRound(minPendingDepthDst)

		// Mirror the coordinator gating applied during steady-state round advancement:
		// dst must stay idle until src is sufficiently ahead for this frontier.
		dstQueue.WaitForCoordinatorGate("resume initialization")
		dstQueue.Resume()
	} else {
		// DST is already completed
		coordinator.UpdateDstCompleted()
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "DST queue already completed, only resuming SRC", "migration", "resume")
		}
	}

	return nil
}

func seedDstQueueRoot(srcQueue *queue.Queue, dstQueue *queue.Queue, root fsservices.Folder) error {
	srcRootTask := srcQueue.GetSuccessfulTask(0, root.LocationPath)
	if srcRootTask == nil {
		srcRootTask = srcQueue.GetSuccessfulTask(0, "/")
	}
	if srcRootTask == nil {
		return fmt.Errorf("source root task not available for seeding destination expected children")
	}

	// Extract expected children from SRC task's discovered children
	var expectedFolders []fsservices.Folder
	var expectedFiles []fsservices.File
	for _, srcChild := range srcRootTask.DiscoveredChildren {
		if srcChild.IsFile {
			expectedFiles = append(expectedFiles, srcChild.File)
		} else {
			expectedFolders = append(expectedFolders, srcChild.Folder)
		}
	}

	// Inject task directly into queue round 0 with expected children
	dstTask := &queue.TaskBase{
		Type:            queue.TaskTypeDstTraversal,
		Folder:          root,
		ExpectedFolders: expectedFolders,
		ExpectedFiles:   expectedFiles,
		Round:           0,
	}

	if !dstQueue.Add(dstTask) {
		return fmt.Errorf("failed to enqueue destination root task")
	}

	return nil
}

// Helper functions for resume reconstruction

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

// reconstructSrcQueuesBridge rebuilds successful src RoundQueues for the specified round range.
// This is used to build a "bridge" of successful tasks that dst needs to query, even though
// src has no pending tasks in those rounds. Only reconstructs successful tasks (no pending).
func reconstructSrcQueuesBridge(database *db.DB, srcQueue *queue.Queue, bridgeRoundStart, bridgeRoundEnd int) error {
	for round := bridgeRoundStart; round <= bridgeRoundEnd; round++ {
		// Load successful folders at this depth (bridge only has successful tasks, no pending)
		successfulFolders, err := loadFoldersByDepthAndStatus(database, "src_nodes", round, "Successful")
		if err != nil {
			return fmt.Errorf("failed to load successful src folders at depth %d: %w", round, err)
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
		// Get expected children from src at the same round as dst
		// DST tasks at round N need expected children from SRC tasks at round N
		// (The +1 rule applies when dst COMPLETES round N and creates tasks for round N+1, not for reconstructing round N)
		srcRound := dstFrontier
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
			ParentPath:   fsservices.NormalizeParentPath(parentPath),
			DisplayName:  name,
			LocationPath: fsservices.NormalizeLocationPath(path),
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
	normalizedParent := fsservices.NormalizeLocationPath(parentPath)

	query := fmt.Sprintf(
		"SELECT id, parent_id, name, path, parent_path, type, depth_level, size, last_updated, traversal_status FROM %s WHERE parent_path IN (?)",
		table,
	)
	rows, err := database.Query(query, normalizedParent)
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
					ParentPath:   fsservices.NormalizeParentPath(nodeParentPath),
					DisplayName:  name,
					LocationPath: fsservices.NormalizeLocationPath(path),
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
					ParentPath:   fsservices.NormalizeParentPath(nodeParentPath),
					DisplayName:  name,
					LocationPath: fsservices.NormalizeLocationPath(path),
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

func waitForSrcRoundAdvance(srcQueue *queue.Queue, round int, pollInterval time.Duration) error {
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for source queue to advance beyond round %d", round)
		case <-ticker.C:
			if srcQueue.IsExhausted() {
				return fmt.Errorf("source queue exhausted before completing round %d", round)
			}

			if srcQueue.Round() > round {
				return nil
			}
		}
	}
}
