// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
	bolt "go.etcd.io/bbolt"
)

// CopyPhaseConfig configures the copy phase execution.
type CopyPhaseConfig struct {
	BoltDB          *db.DB
	SrcAdapter      types.FSAdapter
	DstAdapter      types.FSAdapter
	WorkerCount     int
	MaxRetries      int
	LogAddress      string
	LogLevel        string
	SkipListener    bool
	StartupDelay    time.Duration
	ProgressTick    time.Duration
	ShutdownContext context.Context
	YAMLConfig      *MigrationConfigYAML // Optional: YAML config for status updates
	ConfigPath      string               // Optional: Path to YAML config file for status updates
}

// RunCopyPhase executes the copy phase (two-pass: folders then files).
// This should be called after traversal, review, and ETL back to BoltDB.
func RunCopyPhase(cfg CopyPhaseConfig) (queue.QueueStats, error) {
	boltDB := cfg.BoltDB
	if boltDB == nil {
		return queue.QueueStats{}, fmt.Errorf("BoltDB must be provided")
	}

	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return queue.QueueStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	// Initialize log service if address provided
	if cfg.LogAddress != "" {
		if !cfg.SkipListener {
			if err := logservice.StartListener(cfg.LogAddress); err != nil {
				// Non-fatal: continue without listener
			} else {
				startupDelay := cfg.StartupDelay
				if startupDelay <= 0 {
					startupDelay = 500 * time.Millisecond
				}
				time.Sleep(startupDelay)
			}
		}
		if err := logservice.InitGlobalLogger(boltDB, cfg.LogAddress, cfg.LogLevel); err != nil {
			return queue.QueueStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Create copy queue (single queue, not dual like traversal)
	copyQueue := queue.NewQueue("copy", cfg.MaxRetries, cfg.WorkerCount, nil) // No coordinator needed for copy
	copyQueue.SetMode(queue.QueueModeCopy)
	copyQueue.SetCopyPass(1) // Start with pass 1 (folders)

	// Find minimum level with pending copy tasks (skip round 0)
	// Start at round 1 since round 0 (root) is skipped
	// Use -1 as sentinel to indicate we haven't found any pending level yet
	minLevel := -1
	levels, err := boltDB.GetAllLevels("SRC")
	if err == nil && len(levels) > 0 {
		// Find minimum level with pending copy tasks (start with folders since pass 1 is folders)
		for _, level := range levels {
			if level == 0 {
				continue // Skip round 0
			}
			// Check folder tasks (pass 1 starts with folders)
			hasPending, err := boltDB.HasCopyStatusBucketItems(level, db.NodeTypeFolder, db.CopyStatusPending)
			if err == nil && hasPending {
				// First pending level found OR current level is smaller than what we've found
				if minLevel == -1 || level < minLevel {
					minLevel = level
				}
			}
		}
	}

	// If no pending levels found, default to level 1
	if minLevel == -1 {
		minLevel = 1
	}
	copyQueue.SetRound(minLevel) // Set initial round

	// CRITICAL: Ensure root folder (level 0) has join-lookup mapping
	// Items at level 1 will look up their parent (root) in the join-lookup table
	// We need to ensure this mapping exists before starting the copy phase
	// Use a single transaction to check and create the mapping to avoid deadlock
	err = boltDB.Update(func(tx *bolt.Tx) error {
		// Get SRC root node
		srcNodesBucket := db.GetNodesBucket(tx, "SRC")
		if srcNodesBucket == nil {
			return fmt.Errorf("SRC nodes bucket not found")
		}

		// Find root node (depth 0)
		var srcRootID, dstRootID string
		cursor := srcNodesBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if v == nil {
				continue
			}
			srcNode, err := db.DeserializeNodeState(v)
			if err == nil && srcNode != nil && srcNode.Depth == 0 {
				srcRootID = srcNode.ID
				break
			}
		}

		if srcRootID == "" {
			return fmt.Errorf("could not find SRC root node")
		}

		// Get DST root node
		dstNodesBucket := db.GetNodesBucket(tx, "DST")
		if dstNodesBucket == nil {
			return fmt.Errorf("DST nodes bucket not found")
		}

		cursor = dstNodesBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if v == nil {
				continue
			}
			dstNode, err := db.DeserializeNodeState(v)
			if err == nil && dstNode != nil && dstNode.Depth == 0 {
				dstRootID = dstNode.ID
				break
			}
		}

		if dstRootID == "" {
			return fmt.Errorf("could not find DST root node")
		}

		// Check if mapping already exists
		joinBucket := db.GetJoinLookupBucket(tx)
		if joinBucket != nil {
			existing := joinBucket.Get([]byte(srcRootID))
			if existing != nil {
				return nil // Mapping already exists
			}
		}

		// Create mapping
		joinBucket, err := db.GetOrCreateJoinLookupBucket(tx)
		if err != nil {
			return fmt.Errorf("failed to get join-lookup bucket: %w", err)
		}

		if err := joinBucket.Put([]byte(srcRootID), []byte(dstRootID)); err != nil {
			return fmt.Errorf("failed to store mapping: %w", err)
		}

		return nil
	})

	if err != nil {
		// Log error but don't fail - join-lookup may already exist from traversal
		if logservice.LS != nil {
			_ = logservice.LS.Log("warn", fmt.Sprintf("Failed to ensure root join-lookup mapping: %v", err), "migration", "copy", "copy")
		}
	}

	// Update config: Copy phase started
	if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
		SetStatusCopyInProgress(cfg.YAMLConfig)
		_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
	}

	// Initialize copy queue with both source and destination adapters
	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	copyQueue.InitializeCopyWithContext(boltDB, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)

	// Create observer for stats publishing
	observer := queue.NewQueueObserver(boltDB, 200*time.Millisecond)
	observer.Start()
	defer observer.Stop()

	// Register copy queue with observer
	observer.RegisterQueue("copy", copyQueue)
	copyQueue.SetObserver(observer)

	// Set up stats channel for progress updates
	statsChan := make(chan queue.QueueStats, 10)
	copyQueue.SetStatsChannel(statsChan)

	// Start progress ticker
	progressTick := cfg.ProgressTick
	if progressTick <= 0 {
		progressTick = 2 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()

	// Start stats consumer goroutine
	go func() {
		var lastStats *queue.QueueStats
		for {
			select {
			case stats := <-statsChan:
				lastStats = &stats
			case <-progressTicker.C:
				if lastStats != nil {
					copyPass := copyQueue.GetCopyPass()
					passName := "folders"
					if copyPass == 2 {
						passName = "files"
					}
					// Get round stats for expected/completed counts (similar to traversal)
					roundStats := copyQueue.RoundStats(lastStats.Round)
					expected := 0
					completed := 0
					if roundStats != nil {
						expected = roundStats.Expected
						completed = roundStats.Completed
					}
					fmt.Printf("\r  Copy: Pass %d (%s) Round %d (Pending:%d InProgress:%d Workers:%d Expected:%d Completed:%d)   ",
						copyPass, passName, lastStats.Round, lastStats.Pending, lastStats.InProgress, lastStats.Workers, expected, completed)
				}
			}
		}
	}()

	// Start queue Run() goroutine
	go copyQueue.Run()

	// Wait for copy phase completion
	start := time.Now()
	lastRound := minLevel // Initialize to starting round
	tickCount := 0
	for {
		// Check for shutdown
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				copyQueue.Pause()
				stats := copyQueue.Stats()

				// Update YAML config with suspended status
				if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
					status, statusErr := InspectMigrationStatus(boltDB)
					if statusErr == nil {
						// For copy phase, we only track one round (copy queue round)
						// Use the current round for both src and dst in status update
						currentRound := stats.Round
						SetSuspendedStatus(cfg.YAMLConfig, status, currentRound, currentRound)
						_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
					}
				}

				return queue.QueueStats{}, fmt.Errorf("copy phase shutdown requested")
			default:
			}
		}

		// Check if queue is completed
		if copyQueue.IsExhausted() {
			stats := copyQueue.Stats()

			// Stop progress ticker
			progressTicker.Stop()

			// Update config YAML with final state (fire-and-forget to avoid blocking)
			if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					done := make(chan error, 1)
					go func() {
						status, statusErr := InspectMigrationStatus(boltDB)
						if statusErr != nil {
							if logservice.LS != nil {
								_ = logservice.LS.Log("warning", fmt.Sprintf("InspectMigrationStatus error: %v", statusErr), "migration", "copy", "copy")
							}
							done <- statusErr
							return
						}
						// For copy phase, use current round for both src and dst
						currentRound := stats.Round
						UpdateConfigFromStatus(cfg.YAMLConfig, status, currentRound, currentRound)
						SetStatusComplete(cfg.YAMLConfig)
						done <- SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
					}()

					select {
					case err := <-done:
						if err != nil {
							if logservice.LS != nil {
								_ = logservice.LS.Log("warning", fmt.Sprintf("Config save failed: %v", err), "migration", "copy", "copy")
							}
						}
					case <-ctx.Done():
						if logservice.LS != nil {
							_ = logservice.LS.Log("warning", "Config save timeout (fire-and-forget)", "migration", "copy", "copy")
						}
					}
				}()
			}

			// Close log service
			if logservice.LS != nil {
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
				}
			}

			fmt.Printf("\nCopy phase complete! Duration: %v\n", time.Since(start))
			return stats, nil
		}

		// Update config periodically (every 10 iterations) or on round advancement
		// Track round advancement for YAML updates
		currentRound := copyQueue.Round()
		roundAdvanced := false
		if currentRound != lastRound {
			roundAdvanced = true
			lastRound = currentRound
		}

		tickCount++
		shouldUpdate := roundAdvanced || (tickCount%10 == 0)
		if cfg.YAMLConfig != nil && cfg.ConfigPath != "" && shouldUpdate {
			status, statusErr := InspectMigrationStatus(boltDB)
			if statusErr == nil {
				// For copy phase, use current round for both src and dst
				UpdateConfigFromStatus(cfg.YAMLConfig, status, currentRound, currentRound)
				// Save config (ignore errors to avoid disrupting migration)
				_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
			}
		}

		// Sleep briefly before checking again
		time.Sleep(100 * time.Millisecond)
	}
}
