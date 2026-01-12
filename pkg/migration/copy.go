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
	fmt.Printf("[Copy Init] Found %d levels in SRC\n", len(levels))
	if err == nil && len(levels) > 0 {
		// Find minimum level with pending copy tasks (start with folders since pass 1 is folders)
		for _, level := range levels {
			if level == 0 {
				continue // Skip round 0
			}
			// Check folder tasks (pass 1 starts with folders)
			hasPending, err := boltDB.HasCopyStatusBucketItems(level, db.NodeTypeFolder, db.CopyStatusPending)
			if err == nil {
				count, countErr := boltDB.CountCopyStatusBucket(level, db.NodeTypeFolder, db.CopyStatusPending)
				if countErr == nil {
					fmt.Printf("[Copy Init] Level %d: hasPending=%v, count=%d (folders)\n", level, hasPending, count)
				} else {
					fmt.Printf("[Copy Init] Level %d: hasPending=%v, count=error(%v) (folders)\n", level, hasPending, countErr)
				}
				if hasPending {
					// First pending level found OR current level is smaller than what we've found
					if minLevel == -1 || level < minLevel {
						minLevel = level
						fmt.Printf("[Copy Init] Set minLevel to %d\n", minLevel)
					}
				}
			} else {
				fmt.Printf("[Copy Init] Level %d: error checking pending=%v\n", level, err)
			}
		}
	} else {
		fmt.Printf("[Copy Init] Error getting levels or no levels found: err=%v, levels=%v\n", err, levels)
	}

	// If no pending levels found, default to level 1
	if minLevel == -1 {
		minLevel = 1
		fmt.Printf("[Copy Init] No pending levels found, defaulting to level 1\n")
	}
	fmt.Printf("[Copy Init] Starting at round %d (pass 1: folders)\n", minLevel)
	copyQueue.SetRound(minLevel) // Set initial round

	// CRITICAL: Ensure root folder (level 0) has join-lookup mapping
	// Items at level 1 will look up their parent (root) in the join-lookup table
	// We need to ensure this mapping exists before starting the copy phase
	// Use a single transaction to check and create the mapping to avoid deadlock
	fmt.Printf("[Copy Init] Ensuring root folder join-lookup mapping exists\n")
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
				fmt.Printf("[Copy Init] Root join-lookup mapping already exists: SRC=%s → DST=%s\n", srcRootID, string(existing))
				return nil
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

		fmt.Printf("[Copy Init] Created root join-lookup mapping: SRC=%s → DST=%s\n", srcRootID, dstRootID)
		return nil
	})

	if err != nil {
		fmt.Printf("[Copy Init] ERROR: Failed to ensure root join-lookup mapping: %v\n", err)
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
					fmt.Printf("[Copy Phase - Pass %d (%s)] Round: %d, Pending: %d, In-Progress: %d, Total: %d\n",
						copyPass, passName, lastStats.Round, lastStats.Pending, lastStats.InProgress, lastStats.TotalTracked)
				}
			}
		}
	}()

	// Start queue Run() goroutine
	go copyQueue.Run()

	// Wait for copy phase completion
	start := time.Now()
	for {
		// Check for shutdown
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				copyQueue.Pause()
				return queue.QueueStats{}, fmt.Errorf("copy phase shutdown requested")
			default:
			}
		}

		// Check if queue is completed
		if copyQueue.IsExhausted() {
			stats := copyQueue.Stats()

			// Stop progress ticker
			progressTicker.Stop()

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

		// Sleep briefly before checking again
		time.Sleep(100 * time.Millisecond)
	}
}
