// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/shared"
)

func main() {
	fmt.Println("=== Local Filesystem Migration Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	// Get user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get user home directory: %w", err)
	}

	fmt.Printf("Home directory: %s\n", homeDir)

	// Set up paths
	// srcPath := filepath.Join(homeDir, "Documents")
	// dstPath := filepath.Join(homeDir, "Downloads")

	srcPath := "C:\\Program Files (x86)"
	dstPath := "C:\\Program Files"

	// Normalize paths to absolute
	srcPath, err = filepath.Abs(srcPath)
	if err != nil {
		return fmt.Errorf("failed to resolve source path: %w", err)
	}

	dstPath, err = filepath.Abs(dstPath)
	if err != nil {
		return fmt.Errorf("failed to resolve destination path: %w", err)
	}

	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	fmt.Printf("Source: %s\n", srcPath)
	fmt.Printf("Destination: %s\n", dstPath)

	// Verify paths exist before proceeding
	if _, err := os.Stat(srcPath); err != nil {
		return fmt.Errorf("source path does not exist: %s (error: %w)", srcPath, err)
	}

	if _, err := os.Stat(dstPath); err != nil {
		return fmt.Errorf("destination path does not exist: %s (error: %w)\nHint: Create the folder or use a different destination path", dstPath, err)
	}

	fmt.Println()

	// Clean migration DB for fresh test
	cfg, err := shared.SetupLocalTest(srcPath, dstPath, true)
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
	shared.PrintVerification(result)

	return nil
}
