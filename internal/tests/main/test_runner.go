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
	if err := logservice.StartListener("127.0.0.1:8081"); err != nil {
		fmt.Printf("Warning: failed to start listener: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	// Initialize global logger
	if err := logservice.InitGlobalLogger(database, "127.0.0.1:8081", "debug"); err != nil {
		return fmt.Errorf("failed to initialize global logger: %w", err)
	}
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
	fmt.Println("✓ Adapters ready")
	fmt.Println()

	// Create and initialize queues - workers start automatically
	fmt.Println("Creating source queue...")
	srcQueue := queue.NewQueue("src", 3, 1000, 1)
	srcQueue.Initialize(database, "src_nodes", srcAdapter, nil, nil)
	fmt.Println("✓ Source queue initialized with 1 worker")

	fmt.Println("Creating destination queue...")
	dstQueue := queue.NewQueue("dst", 3, 1000, 1)
	// dst needs src context to query src_nodes for expected children
	dstQueue.Initialize(database, "dst_nodes", dstAdapter, srcContext, srcQueue)
	fmt.Println("✓ Destination queue initialized with 1 worker")
	fmt.Println()

	// Wait for migration to complete (queues manage themselves)
	fmt.Println("Migration in progress (check log window for details)...")
	fmt.Println("Waiting for completion...")

	for {
		// Check if migration is complete
		if srcQueue.IsExhausted() && dstQueue.IsExhausted() {
			fmt.Println("\nMigration complete!")
			return nil
		}

		// Print progress
		srcStats := srcQueue.Stats()
		dstStats := dstQueue.Stats()
		fmt.Printf("\r  Src: Round %d (P:%d IP:%d W:%d) | Dst: Round %d (P:%d IP:%d W:%d)   ",
			srcStats.Round, srcStats.Pending, srcStats.InProgress, srcStats.Workers,
			dstStats.Round, dstStats.Pending, dstStats.InProgress, dstStats.Workers)

		time.Sleep(500 * time.Millisecond)
	}
}
