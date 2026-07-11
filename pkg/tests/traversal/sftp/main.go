// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/traversal/shared"
)

func main() {
	fmt.Println("=== SFTP Traversal Migration Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	srcPath := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_SRC"))
	dstPath := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_DST"))
	if srcPath == "" || dstPath == "" {
		return fmt.Errorf("SYLOS_SFTP_TEST_SRC and SYLOS_SFTP_TEST_DST must be set")
	}

	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	fmt.Printf("Source: %s\n", srcPath)
	fmt.Printf("Destination: %s\n", dstPath)
	fmt.Println()

	cfg, err := shared.SetupSftpTest(srcPath, dstPath, true)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	fmt.Println("🚀 Phase 2: Migration")
	fmt.Println("=====================")
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	shared.PrintVerification(result)
	return nil
}
