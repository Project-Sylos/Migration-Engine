// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
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

	if cfg.LogAddress != "" {
		if err := logservice.InitGlobalLogger(cfg.Database, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize global logger: %w", err)
		}
	}

	coordinator := queue.NewQueueCoordinator(cfg.CoordinatorLead)

	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.Initialize(cfg.Database, "src_nodes", cfg.SrcAdapter, nil)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.Initialize(cfg.Database, "dst_nodes", cfg.DstAdapter, srcQueue)

	// Load root tasks from database and seed into queues
	srcRootFolders, err := queue.LoadRootFolders(cfg.Database, "src_nodes")
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("failed to load src root folders: %w", err)
	}
	if len(srcRootFolders) == 0 {
		return RuntimeStats{}, fmt.Errorf("no src root folders found in database; ensure seeding completed")
	}
	if len(srcRootFolders) > 1 {
		return RuntimeStats{}, fmt.Errorf("expected exactly one src root folder, found %d", len(srcRootFolders))
	}

	// Seed SRC root task from database
	srcRootTask := &queue.TaskBase{
		Type:   queue.TaskTypeSrcTraversal,
		Folder: srcRootFolders[0],
		Round:  0,
	}
	if !srcQueue.Add(srcRootTask) {
		return RuntimeStats{}, fmt.Errorf("failed to enqueue source root task")
	}

	// Wait for SRC to complete round 0, then seed DST root with expected children
	if err := waitForSrcRoundAdvance(srcQueue, 0, cfg.ProgressTick); err != nil {
		return RuntimeStats{}, err
	}

	// Load DST root from database
	dstRootFolders, err := queue.LoadRootFolders(cfg.Database, "dst_nodes")
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("failed to load dst root folders: %w", err)
	}
	if len(dstRootFolders) == 0 {
		return RuntimeStats{}, fmt.Errorf("no dst root folders found in database; ensure seeding completed")
	}
	if len(dstRootFolders) > 1 {
		return RuntimeStats{}, fmt.Errorf("expected exactly one dst root folder, found %d", len(dstRootFolders))
	}

	// Get SRC root task from successful queue to extract expected children
	if err := seedDstQueueRoot(srcQueue, dstQueue, dstRootFolders[0]); err != nil {
		return RuntimeStats{}, err
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
