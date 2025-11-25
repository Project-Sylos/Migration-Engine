// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/shared"
)

func main() {
	resumeOnly := flag.Bool("resume", false, "Resume existing migration (skip initial run)")
	flag.Parse()

	fmt.Println("=== Spectra Migration Resumption Test Runner ===")
	fmt.Println()

	var err error
	if *resumeOnly {
		err = runResumeTest()
	} else {
		err = runInitialTest()
	}

	if err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

// runInitialTest runs the initial migration that will be killed midway.
func runInitialTest() error {
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	// Clean both Spectra DB and migration DB for fresh start
	cfg, err := shared.SetupTest(true, true)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	fmt.Println()

	fmt.Println("🚀 Phase 2: Starting Migration (will run until killed)")
	fmt.Println("=======================================================")
	fmt.Println("Migration will be killed externally after some progress...")
	fmt.Println()

	// Use StartMigration for async execution so we can be killed
	controller := migration.StartMigration(cfg)

	// Wait for migration to complete or be killed
	// In normal test, this would be killed externally (via signal or Stop-Job)
	// But we'll also check if it completes normally
	<-controller.Done()
	// Migration completed or was killed
	result, err := controller.Wait()

	// Check if it was a clean shutdown
	if err != nil && err.Error() == "migration suspended by force shutdown" {
		fmt.Println()
		fmt.Println("✓ Migration was cleanly suspended")
		fmt.Printf("  Progress: SRC Round %d, DST Round %d\n",
			result.Runtime.Src.Round, result.Runtime.Dst.Round)
		fmt.Println("  State saved for resumption")
		return nil // This is expected when killed
	}

	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// Migration completed normally (shouldn't happen in test, but handle it)
	fmt.Println()
	fmt.Println("⚠️  Migration completed normally (wasn't killed)")
	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	shared.PrintVerification(result)
	return nil
}

// runResumeTest resumes the migration from the suspended state.
func runResumeTest() error {
	fmt.Println("📋 Phase 1: Setup (Loading existing config)")
	fmt.Println("===========================================")
	// Don't clean Spectra DB - we need it to persist for resumption
	// Don't remove migration database - we want to resume
	cfg, err := shared.SetupTest(false, false)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	fmt.Println("✓ Config loaded")
	fmt.Println("✓ Spectra DB preserved (nodes persist across resumption)")
	fmt.Println("✓ Migration DB preserved (state persists for resumption)")
	fmt.Println("✓ Will resume from existing migration database state")
	fmt.Println()

	fmt.Println("🔄 Phase 2: Resuming Migration")
	fmt.Println("===============================")

	// LetsMigrate will automatically detect and resume from suspended state
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration resumption failed: %w", err)
	}

	fmt.Println()
	fmt.Println("✓ Migration resumed and completed successfully")
	fmt.Println()

	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	shared.PrintVerification(result)

	return nil
}
