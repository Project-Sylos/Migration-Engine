// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
)

// isPortInUse checks if a UDP port is already in use by attempting to bind to it.
// Returns true if the port is already in use, false otherwise.
func isPortInUse(addr string) bool {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return false
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return true // Port is in use if we can't bind to it
	}
	conn.Close()
	return false // Port is available
}

// closeSpectraAdapters closes SpectraFS adapters, ensuring the underlying SDK instance
// is only closed once even if multiple adapters share the same instance.
func closeSpectraAdapters(srcAdapter, dstAdapter fsservices.FSAdapter, ctx context.Context) {
	var srcFS, dstFS *fsservices.SpectraFS
	var ok bool

	// Check if adapters are SpectraFS instances
	if srcAdapter != nil {
		srcFS, ok = srcAdapter.(*fsservices.SpectraFS)
		if !ok {
			srcFS = nil
		}
	}
	if dstAdapter != nil {
		dstFS, ok = dstAdapter.(*fsservices.SpectraFS)
		if !ok {
			dstFS = nil
		}
	}

	// If both are SpectraFS, check if they share the same SDK instance
	if srcFS != nil && dstFS != nil {
		if srcFS.GetSDKInstance() == dstFS.GetSDKInstance() {
			// Same instance - only close once
			closeDone := make(chan error, 1)
			go func() {
				closeDone <- srcFS.Close()
			}()
			select {
			case err := <-closeDone:
				if err != nil {
					fmt.Printf("Warning: failed to close Spectra SDK instance: %v\n", err)
				}
			case <-ctx.Done():
				fmt.Printf("⚠️  Spectra close timeout - skipping\n")
			}
			return
		}
	}

	// Different instances or only one is SpectraFS - close each separately
	if srcFS != nil {
		closeDone := make(chan error, 1)
		go func() {
			closeDone <- srcFS.Close()
		}()
		select {
		case err := <-closeDone:
			if err != nil {
				fmt.Printf("Warning: failed to close Spectra source adapter: %v\n", err)
			}
		case <-ctx.Done():
			fmt.Printf("⚠️  Spectra source close timeout - skipping\n")
		}
	}

	if dstFS != nil {
		closeDone := make(chan error, 1)
		go func() {
			closeDone <- dstFS.Close()
		}()
		select {
		case err := <-closeDone:
			if err != nil {
				fmt.Printf("Warning: failed to close Spectra destination adapter: %v\n", err)
			}
		case <-ctx.Done():
			fmt.Printf("⚠️  Spectra destination close timeout - skipping\n")
		}
	}
}

// MigrationConfig encapsulates the inputs required to execute a traversal migration.
type MigrationConfig struct {
	// BoltPath is the file path where BoltDB will store its data.
	// This is only used if BoltDB is not provided.
	BoltPath string
	// BoltDB is the BoltDB instance to use. If provided, BoltPath is ignored.
	// If not provided, a new instance will be opened using BoltPath.
	BoltDB *db.DB

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
	if cfg.BoltDB == nil && cfg.BoltPath == "" {
		return RuntimeStats{}, fmt.Errorf("either BoltDB or BoltPath must be provided")
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
		// Check if port is already in use (from previous process)
		// This prevents spawning multiple terminal windows across separate test runs
		if !isPortInUse(cfg.LogAddress) {
			if err := logservice.StartListener(cfg.LogAddress); err != nil {
				fmt.Printf("Warning: failed to start log listener at %s: %v\n", cfg.LogAddress, err)
			} else {
				time.Sleep(cfg.StartupDelay)
			}
		}
		// If port is in use, listener is already running from previous process - skip spawning
	}

	coordinator := queue.NewQueueCoordinator(cfg.CoordinatorLead)

	// BoltDB instance should be passed in via MigrationConfig.BoltDB
	// If not provided, open a new one (for backward compatibility with tests)
	var boltDB *db.DB
	var err error
	if cfg.BoltDB != nil {
		boltDB = cfg.BoltDB
	} else {
		// Fallback: open BoltDB if not provided (shouldn't happen in normal flow)
		boltOpts := db.DefaultOptions()
		boltOpts.Path = cfg.BoltPath
		boltDB, err = db.Open(boltOpts)
		if err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to open BoltDB: %w", err)
		}
		// Only defer close if we opened it here
		defer func() {
			if err := boltDB.Close(); err != nil {
				fmt.Printf("Warning: failed to close BoltDB: %v\n", err)
			}
		}()
	}

	// BoltDB is now the primary and only database

	// Initialize logger if not already initialized (logger should be managed at a higher level)
	if cfg.LogAddress != "" && logservice.LS == nil {
		if err := logservice.InitGlobalLogger(boltDB, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize global logger: %w", err)
		}
	}

	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.InitializeWithContext(boltDB, cfg.SrcAdapter, nil, cfg.ShutdownContext)
	defer srcQueue.Close()

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.InitializeWithContext(boltDB, cfg.DstAdapter, srcQueue, cfg.ShutdownContext)
	defer dstQueue.Close()

	// Initialize queues from YAML config state
	if err := initializeQueues(cfg, srcQueue, dstQueue, coordinator); err != nil {
		return RuntimeStats{}, err
	}

	// Update config: Traversal started
	if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
		status, statusErr := InspectMigrationStatus(boltDB)
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
				// Use timeout context to prevent hanging on cleanup operations
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cleanupCancel()

				// Pause queues to stop new task leasing (workers will exit via shutdown context)
				srcQueue.Pause()
				dstQueue.Pause()

				// Give workers a moment to finish current tasks (they check shutdown context in their loop)
				// This is non-blocking - workers will exit when they check context in next iteration
				select {
				case <-time.After(200 * time.Millisecond):
					// Continue with cleanup
				case <-cleanupCtx.Done():
					// Timeout - skip cleanup and exit immediately
					fmt.Printf("⚠️  Cleanup timeout - exiting immediately to prevent hang\n")
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcQueue.Stats(),
						Dst:      dstQueue.Stats(),
					}, errors.New("migration suspended by force shutdown (cleanup timeout)")
				}

				srcStats := srcQueue.Stats()
				dstStats := dstQueue.Stats()

				// BoltDB doesn't need checkpointing - data is already persisted

				// Close Spectra adapters to ensure proper cleanup (only close once if they share the same SDK instance)
				closeSpectraAdapters(cfg.SrcAdapter, cfg.DstAdapter, cleanupCtx)

				// Update YAML config with suspended status and current state (with timeout)
				if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
					yamlDone := make(chan error, 1)
					go func() {
						status, statusErr := InspectMigrationStatus(boltDB)
						if statusErr == nil {
							SetSuspendedStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
							yamlDone <- SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
						} else {
							yamlDone <- statusErr
						}
					}()
					select {
					case err := <-yamlDone:
						if err != nil {
							fmt.Printf("Warning: failed to save YAML config during shutdown: %v\n", err)
						}
					case <-cleanupCtx.Done():
						fmt.Printf("⚠️  YAML save timeout - skipping config save\n")
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
				status, statusErr := InspectMigrationStatus(boltDB)
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
			srcRoundStats := srcQueue.RoundStats(srcStats.Round)
			dstRoundStats := dstQueue.RoundStats(dstStats.Round)

			srcExpected := 0
			srcCompleted := 0
			if srcRoundStats != nil {
				srcExpected = srcRoundStats.Expected
				srcCompleted = srcRoundStats.Completed
			}

			dstExpected := 0
			dstCompleted := 0
			if dstRoundStats != nil {
				dstExpected = dstRoundStats.Expected
				dstCompleted = dstRoundStats.Completed
			}

			fmt.Printf("\r  Src: Round %d (Pending:%d InProgress:%d Workers:%d Expected:%d Completed:%d) | Dst: Round %d (Pending:%d InProgress:%d Workers:%d Expected:%d Completed:%d)   ",
				srcStats.Round, srcStats.Pending, srcStats.InProgress, srcStats.Workers, srcExpected, srcCompleted,
				dstStats.Round, dstStats.Pending, dstStats.InProgress, dstStats.Workers, dstExpected, dstCompleted)

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
				status, statusErr := InspectMigrationStatus(boltDB)
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

// initializeQueues sets up src/dst queues from YAML config state.
// Reads last_round_src/dst from config, sets queue rounds, calls PullTasks.
// PullTasks blocks on coordinator gate internally, so no extra coordination needed.
func initializeQueues(cfg MigrationConfig, srcQueue *queue.Queue, dstQueue *queue.Queue, coordinator *queue.QueueCoordinator) error {
	// Parse state from YAML config
	srcRound := 0
	dstRound := 0
	if cfg.YAMLConfig != nil && cfg.YAMLConfig.State.LastRoundSrc != nil {
		srcRound = *cfg.YAMLConfig.State.LastRoundSrc
	}
	if cfg.YAMLConfig != nil && cfg.YAMLConfig.State.LastRoundDst != nil {
		dstRound = *cfg.YAMLConfig.State.LastRoundDst
	}

	// BoltDB is now the primary database - no SQLite seeding needed

	// Set queue rounds
	srcQueue.SetRound(srcRound)
	dstQueue.SetRound(dstRound)

	// Initialize Expected count for round 0 (root task exists)
	if srcRound == 0 {
		srcQueue.IncrementExpected(0, 1)
	}
	if dstRound == 0 {
		dstQueue.IncrementExpected(0, 1)
	}

	// Update coordinator state
	coordinator.UpdateSrcRound(srcRound)
	if dstRound >= 0 {
		coordinator.UpdateDstRound(dstRound)
	} else {
		coordinator.UpdateDstCompleted()
	}

	// Pull tasks from BoltDB into queue buffers (blocks on coordinator internally)
	srcQueue.PullTasks(true)
	dstQueue.PullTasks(true)

	if logservice.LS != nil {
		_ = logservice.LS.Log("info",
			fmt.Sprintf("Initialized queues: src round %d, dst round %d", srcRound, dstRound),
			"migration",
			"init",
		)
	}

	return nil
}
