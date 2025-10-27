// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
	"github.com/Project-Sylos/Migration-Engine/internal/queue"
	"github.com/Project-Sylos/Spectra/sdk"
)

func main() {
	fmt.Println("=== Spectra Migration Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	// 1. Setup phase
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	database, spectraFS, err := setupTest()
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	defer database.Close()
	fmt.Println()

	// 2. Migration phase
	fmt.Println("🚀 Phase 2: Migration")
	fmt.Println("=====================")
	if err := runMigration(database, spectraFS); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	fmt.Println()

	// 3. Verification phase
	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	if err := verifyMigration(database); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	return nil
}

func runMigration(database *db.DB, spectraFS *sdk.SpectraFS) error {
	// Start logging service
	fmt.Println("Starting logging service...")
	if err := logservice.StartListener("127.0.0.1:8080"); err != nil {
		fmt.Printf("Warning: failed to start listener: %v\n", err)
	}
	time.Sleep(2 * time.Second)
	fmt.Println("✓ Log service started")
	fmt.Println()

	// Create filesystem adapters
	fmt.Println("Creating filesystem adapters...")
	srcAdapter, err := fsservices.NewSpectraFS(spectraFS, "p-root")
	if err != nil {
		return fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fsservices.NewSpectraFS(spectraFS, "s1-root")
	if err != nil {
		return fmt.Errorf("failed to create dst adapter: %w", err)
	}

	srcContext := fsservices.NewServiceContext("Spectra-Src", srcAdapter)
	dstContext := fsservices.NewServiceContext("Spectra-Dst", dstAdapter)
	fmt.Println("✓ Adapters ready")
	fmt.Println()

	// Create and configure coordinator
	fmt.Println("Starting coordinator...")
	coordinator := queue.NewCoordinator(
		database,
		*srcContext,
		*dstContext,
		3,    // maxRetries
		1000, // batchSize
	)

	coordinator.AddWorkers("src", 3)
	coordinator.AddWorkers("dst", 3)

	if err := coordinator.Start(); err != nil {
		return fmt.Errorf("failed to start coordinator: %w", err)
	}
	fmt.Println("✓ Coordinator started (3 src + 3 dst workers)")
	fmt.Println()

	// Wait for migration to complete (coordinator handles everything)
	fmt.Println("Migration in progress (check log service window for details)...")
	fmt.Println("Waiting for completion...")

	for {
		time.Sleep(500 * time.Millisecond)

		// Check if migration is complete
		if coordinator.IsComplete() {
			break
		}

		// Print progress
		srcStats, _ := coordinator.QueueStats("src")
		dstStats, _ := coordinator.QueueStats("dst")
		fmt.Printf("\r  Src: Round %d (P:%d IP:%d) | Dst: Round %d (P:%d IP:%d)   ",
			srcStats.Round, srcStats.Pending, srcStats.InProgress,
			dstStats.Round, dstStats.Pending, dstStats.InProgress)
	}

	fmt.Println("\n✓ Migration completed")

	return coordinator.Stop()
}
