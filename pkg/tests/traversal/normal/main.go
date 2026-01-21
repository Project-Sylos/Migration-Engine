// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/traversal/shared"
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
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	// Clean both Spectra DB and migration DB for fresh test
	cfg, err := shared.SetupTest(true, true)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	fmt.Println()

	fmt.Println("🚀 Phase 2: Migration")
	fmt.Println("=====================")
	// Note: In ModeStandalone, ME will close the DB automatically
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	fmt.Println()

	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	shared.PrintVerification(result)

	// DB is already closed by ME in ModeStandalone mode
	return nil
}
