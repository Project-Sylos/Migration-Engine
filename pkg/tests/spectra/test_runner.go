// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
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
	cfg, err := setupTest()
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	fmt.Println()

	fmt.Println("🚀 Phase 2: Migration")
	fmt.Println("=====================")
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	fmt.Println()

	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	printVerification(result)

	return nil
}
