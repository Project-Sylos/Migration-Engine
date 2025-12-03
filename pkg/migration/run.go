// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-FS/pkg/fs"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// MigrationConfig is the configuration passed to RunMigration.
type MigrationConfig struct {
	BoltDB          *db.DB
	BoltPath        string
	SrcAdapter      types.FSAdapter
	DstAdapter      types.FSAdapter
	SrcRoot         types.Folder
	DstRoot         types.Folder
	SrcServiceName  string
	WorkerCount     int
	MaxRetries      int
	CoordinatorLead int
	LogAddress      string
	LogLevel        string
	SkipListener    bool
	StartupDelay    time.Duration
	ProgressTick    time.Duration
	ResumeStatus    *MigrationStatus
	ConfigPath      string
	YAMLConfig      *MigrationConfigYAML
	ShutdownContext context.Context
}

// RuntimeStats captures execution statistics at the end of a migration run.
type RuntimeStats struct {
	Duration time.Duration
	Src      queue.QueueStats
	Dst      queue.QueueStats
}

// getQueueStats builds QueueStats from coordinator and queue queries (non-blocking)
func getQueueStats(coordinator *queue.QueueCoordinator, boltDB *db.DB) (queue.QueueStats, queue.QueueStats) {
	srcRound := coordinator.GetSrcRound()
	dstRound := coordinator.GetDstRound()

	// Get pending counts from DB for current round (non-blocking, uses View transaction)
	srcPending := 0
	dstPending := 0
	if boltDB != nil {
		srcPendingCount, _ := boltDB.CountByPrefix("SRC", srcRound, db.StatusPending)
		srcPending = srcPendingCount
		dstPendingCount, _ := boltDB.CountByPrefix("DST", dstRound, db.StatusPending)
		dstPending = dstPendingCount
	}

	return queue.QueueStats{
			Name:    "src",
			Round:   srcRound,
			Pending: srcPending,
		}, queue.QueueStats{
			Name:    "dst",
			Round:   dstRound,
			Pending: dstPending,
		}
}

// RunMigration executes the migration traversal using the provided configuration.
func RunMigration(cfg MigrationConfig) (RuntimeStats, error) {
	if cfg.BoltDB == nil {
		return RuntimeStats{}, fmt.Errorf("boltDB cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	boltDB := cfg.BoltDB

	// Initialize log service and start listener
	if !cfg.SkipListener && cfg.LogAddress != "" {
		// Start listener terminal FIRST so it's ready before we send logs
		if err := logservice.StartListener(cfg.LogAddress); err != nil {
			// Non-fatal: continue without listener
			// Don't return error - migration can still proceed
		} else {
			// Give the listener terminal time to start up before sending logs
			// Use StartupDelay if provided, otherwise default to 500ms
			startupDelay := cfg.StartupDelay
			if startupDelay <= 0 {
				startupDelay = 500 * time.Millisecond
			}
			time.Sleep(startupDelay)
		}
		// Now initialize logger (which sends test log)
		if err := logservice.InitGlobalLogger(boltDB, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Create coordinator for round advancement gates
	coordinator := queue.NewQueueCoordinator()

	// Set up YAML update callback for automatic config updates on round advance
	if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
		coordinator.SetYAMLUpdateCallback(func(srcRound, dstRound int) {
			// Thread-safe YAML update in background goroutine
			go func() {
				status, err := InspectMigrationStatus(boltDB)
				if err == nil {
					UpdateConfigFromStatus(cfg.YAMLConfig, status, srcRound, dstRound)
					_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
				}
			}()
		})
	}

	// Create queues
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.InitializeWithContext(boltDB, cfg.SrcAdapter, nil, cfg.ShutdownContext)
	// Note: Queues clean themselves up when they complete (Run() exits when state=QueueStateCompleted)
	// We only need to explicitly close for forced shutdowns, which is handled via Pause() + shutdown context

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.InitializeWithContext(boltDB, cfg.DstAdapter, srcQueue, cfg.ShutdownContext)
	// Note: Queues clean themselves up when they complete (Run() exits when state=QueueStateCompleted)
	// We only need to explicitly close for forced shutdowns, which is handled via Pause() + shutdown context

	// Initialize queues from YAML config state
	if err := initializeQueues(cfg, srcQueue, dstQueue, coordinator); err != nil {
		return RuntimeStats{}, err
	}

	// Give queues a moment to start their Run() goroutines
	time.Sleep(100 * time.Millisecond)

	// Set up stats channels for UDP logging (after queues are running)
	srcStatsChan := make(chan queue.QueueStats, 10)
	dstStatsChan := make(chan queue.QueueStats, 10)
	srcQueue.SetStatsChannel(srcStatsChan)
	dstQueue.SetStatsChannel(dstStatsChan)

	// Start stats consumer goroutine for progress updates (fmt output, not log service)
	// Accumulate stats from both channels and print them together
	go func() {
		var lastSrcStats *queue.QueueStats
		var lastDstStats *queue.QueueStats

		for {
			select {
			case srcStats := <-srcStatsChan:
				lastSrcStats = &srcStats
				if lastDstStats != nil {
					// Get round stats for full format
					srcRoundStats := srcQueue.RoundStats(lastSrcStats.Round)
					dstRoundStats := dstQueue.RoundStats(lastDstStats.Round)

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
						lastSrcStats.Round, lastSrcStats.Pending, lastSrcStats.InProgress, lastSrcStats.Workers, srcExpected, srcCompleted,
						lastDstStats.Round, lastDstStats.Pending, lastDstStats.InProgress, lastDstStats.Workers, dstExpected, dstCompleted)
				}
			case dstStats := <-dstStatsChan:
				lastDstStats = &dstStats
				if lastSrcStats != nil {
					// Get round stats for full format
					srcRoundStats := srcQueue.RoundStats(lastSrcStats.Round)
					dstRoundStats := dstQueue.RoundStats(lastDstStats.Round)

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
						lastSrcStats.Round, lastSrcStats.Pending, lastSrcStats.InProgress, lastSrcStats.Workers, srcExpected, srcCompleted,
						lastDstStats.Round, lastDstStats.Pending, lastDstStats.InProgress, lastDstStats.Workers, dstExpected, dstCompleted)
				}
			}
		}
	}()

	// Update config: Traversal started
	if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
		status, statusErr := InspectMigrationStatus(boltDB)
		if statusErr == nil {
			UpdateConfigFromStatus(cfg.YAMLConfig, status, coordinator.GetSrcRound(), coordinator.GetDstRound())
			cfg.YAMLConfig.State.Status = "running"
			_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
		}
	}

	// Ensure ProgressTick is positive (default to 1 second if not set)
	progressTick := cfg.ProgressTick
	if progressTick <= 0 {
		progressTick = 1 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
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
					srcStats, dstStats := getQueueStats(coordinator, boltDB)
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcStats,
						Dst:      dstStats,
					}, errors.New("migration suspended by force shutdown (cleanup timeout)")
				}

				// Get stats directly (non-blocking)
				srcStats, dstStats := getQueueStats(coordinator, boltDB)

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

		// Check exhaustion status from coordinator (source of truth)
		// Queues call MarkSrcCompleted()/MarkDstCompleted() on coordinator when done
		bothCompleted := coordinator.IsCompleted("both")

		if bothCompleted {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug",
					"Migration loop: Both queues completed, exiting migration",
					"migration", "run", "run")
			}

			// Get stats directly (non-blocking)
			srcStats, dstStats := getQueueStats(coordinator, boltDB)

			fmt.Println("\nMigration complete!")

			// Update config YAML with final state (fire-and-forget to avoid blocking)
			if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
				// Fire-and-forget - don't block completion on config save
				go func() {
					// Use a context with timeout to prevent indefinite blocking
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					done := make(chan error, 1)
					go func() {
						if logservice.LS != nil {
							_ = logservice.LS.Log("debug", "Starting YAML config save...", "migration", "run", "run")
						}
						status, statusErr := InspectMigrationStatus(boltDB)
						if statusErr != nil {
							if logservice.LS != nil {
								_ = logservice.LS.Log("warning", fmt.Sprintf("InspectMigrationStatus error: %v", statusErr), "migration", "run", "run")
							}
							done <- statusErr
							return
						}
						if logservice.LS != nil {
							_ = logservice.LS.Log("debug", "InspectMigrationStatus completed", "migration", "run", "run")
						}
						UpdateConfigFromStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
						done <- SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
					}()

					select {
					case err := <-done:
						if err != nil {
							if logservice.LS != nil {
								_ = logservice.LS.Log("warning", fmt.Sprintf("Config save failed: %v", err), "migration", "run", "run")
							}
						} else {
							if logservice.LS != nil {
								_ = logservice.LS.Log("debug", "YAML config save completed", "migration", "run", "run")
							}
						}
					case <-ctx.Done():
						if logservice.LS != nil {
							_ = logservice.LS.Log("warning", "Config save timeout (fire-and-forget)", "migration", "run", "run")
						}
					}
				}()
			}

			if logservice.LS != nil {
				_ = logservice.LS.Log("debug", "About to return from RunMigration", "migration", "run", "run")
			}

			// Stop progress ticker to prevent any more ticks
			progressTicker.Stop()

			// Close log service before returning (flush logs) - with timeout to prevent hanging
			if logservice.LS != nil {
				// Use a timeout context to prevent indefinite blocking
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer closeCancel()

				closeDone := make(chan struct{}, 1)
				go func() {
					_ = logservice.LS.Close()
					closeDone <- struct{}{}
				}()

				select {
				case <-closeDone:
				case <-closeCtx.Done():
					// Timeout - log service close is taking too long, continue anyway
					// (flushes should only take a few ms, so 1s timeout is generous)
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
			// Re-check exhaustion from coordinator (queues might have completed during tick)
			if coordinator.IsCompleted("both") {
				// Get stats directly (non-blocking)
				srcStats, dstStats := getQueueStats(coordinator, boltDB)
				fmt.Println("\nMigration complete!")

				// Update config YAML with final state (fire-and-forget to avoid blocking)
				if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
					// Fire-and-forget - don't block completion on config save
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()

						done := make(chan error, 1)
						go func() {
							status, statusErr := InspectMigrationStatus(boltDB)
							if statusErr == nil {
								UpdateConfigFromStatus(cfg.YAMLConfig, status, srcStats.Round, dstStats.Round)
								done <- SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
							} else {
								done <- statusErr
							}
						}()

						select {
						case <-done:
							// Config saved (ignore errors)
						case <-ctx.Done():
							// Timeout - fire and forget
						}
					}()
				}

				// Stop progress ticker to prevent any more ticks
				progressTicker.Stop()

				// Close log service before returning (flush logs) - with timeout to prevent hanging
				if logservice.LS != nil {
					// Use a timeout context to prevent indefinite blocking
					closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer closeCancel()

					closeDone := make(chan struct{}, 1)
					go func() {
						_ = logservice.LS.Close()
						closeDone <- struct{}{}
					}()

					select {
					case <-closeDone:
					case <-closeCtx.Done():
						// Timeout - log service close is taking too long, continue anyway
						// (flushes should only take a few ms, so 1s timeout is generous)
					}
				}

				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, nil
			}

			// Get stats directly (non-blocking queries)
			srcStats, dstStats := getQueueStats(coordinator, boltDB)

			// Stats are printed via the channel listener goroutine, not here
			// This section only updates config YAML

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
			// Debug: Log when taking default case
			if bothCompleted {
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// initializeQueues sets up src/dst queues from YAML config state.
// Reads last_round_src/dst from config, sets queue rounds.
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
	if coordinator != nil {
		coordinator.UpdateSrcRound(srcRound)
		if dstRound >= 0 {
			coordinator.UpdateDstRound(dstRound)
		} else {
			coordinator.MarkDstCompleted()
		}
	}

	// Don't pull tasks here - let Run() handle the initial pull
	// DST will check coordinator when it needs to advance

	if logservice.LS != nil {
		_ = logservice.LS.Log("info",
			fmt.Sprintf("Initialized queues: src round %d, dst round %d", srcRound, dstRound),
			"migration",
			"init",
		)
	}

	return nil
}

// closeSpectraAdapters closes Spectra adapters if they are SpectraFS instances.
// Handles shared SDK instances by only closing once.
func closeSpectraAdapters(srcAdapter, dstAdapter types.FSAdapter, ctx context.Context) {
	// Check if adapters are Spectra instances
	srcSpectra, srcIsSpectra := srcAdapter.(*fs.SpectraFS)
	dstSpectra, dstIsSpectra := dstAdapter.(*fs.SpectraFS)

	if srcIsSpectra && dstIsSpectra {
		// Both are Spectra - check if they share the same SDK instance
		if srcSpectra != nil && dstSpectra != nil && srcSpectra.GetSDKInstance() == dstSpectra.GetSDKInstance() {
			// Same SDK - only close once
			done := make(chan struct{}, 1)
			go func() {
				if srcSpectra.GetSDKInstance() != nil {
					_ = srcSpectra.Close()
				}
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-ctx.Done():
				// Timeout - skip
			}
		} else {
			// Different SDK instances - close both
			done := make(chan struct{}, 2)
			go func() {
				if srcSpectra != nil {
					_ = srcSpectra.Close()
				}
				done <- struct{}{}
			}()
			go func() {
				if dstSpectra != nil {
					_ = dstSpectra.Close()
				}
				done <- struct{}{}
			}()
			// Wait for both or timeout
			completed := 0
			for completed < 2 {
				select {
				case <-done:
					completed++
				case <-ctx.Done():
					return
				}
			}
		}
	} else {
		// Close individually if they're Spectra
		if srcIsSpectra && srcSpectra != nil {
			done := make(chan struct{}, 1)
			go func() {
				_ = srcSpectra.Close()
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-ctx.Done():
			}
		}
		if dstIsSpectra && dstSpectra != nil {
			done := make(chan struct{}, 1)
			go func() {
				_ = dstSpectra.Close()
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-ctx.Done():
			}
		}
	}
}
