// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/copy/shared"
)

func main() {
	fmt.Println("=== Copy Phase Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	// Setup adapters and open pre-provisioned DB (RemoveExisting: false)
	database, srcAdapter, dstAdapter, err := shared.SetupCopyTest(false, false)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	defer database.Close()
	fmt.Println()

	fmt.Println("🚀 Phase 2: Copy Phase")
	fmt.Println("======================")
	// Run copy phase
	stats, err := migration.RunCopyPhase(migration.CopyPhaseConfig{
		DuckDB:          database,
		SrcAdapter:      srcAdapter,
		DstAdapter:      dstAdapter,
		WorkerCount:     10,
		MaxRetries:      3,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		SkipListener:    false,
		StartupDelay:    3 * time.Second,
		ProgressTick:    2 * time.Second,
		ShutdownContext: nil,
	})
	if err != nil {
		return fmt.Errorf("copy phase failed: %w", err)
	}
	fmt.Println()

	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	shared.PrintCopyVerification(stats)
	if err := shared.VerifyCopyCompletion(database); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	return nil
}
