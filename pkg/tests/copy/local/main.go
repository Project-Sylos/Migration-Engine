// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/copy/shared"
	"github.com/Project-Sylos/Sylos-FS/pkg/fs"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

func main() {
	fmt.Println("=== Local Copy Phase Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	// Get source path (Documents folder) from environment variable (set by PowerShell script)
	// This handles Windows 11's OneDrive Documents folder location correctly
	srcPath := os.Getenv("SYLOS_COPY_TEST_SRC")
	if srcPath == "" {
		// Fallback: try to construct from home directory (may not work on Windows 11 with OneDrive)
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("source path not provided - SYLOS_COPY_TEST_SRC environment variable must be set, and failed to get home directory: %w", err)
		}
		srcPath = filepath.Join(homeDir, "Documents")
	}
	srcPath, err := filepath.Abs(srcPath)
	if err != nil {
		return fmt.Errorf("failed to resolve source path: %w", err)
	}

	// Get destination path from environment variable (set by PowerShell script)
	dstPath := os.Getenv("SYLOS_COPY_TEST_DST")
	if dstPath == "" {
		return fmt.Errorf("destination path not provided - SYLOS_COPY_TEST_DST environment variable must be set")
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
		return fmt.Errorf("destination path does not exist: %s (error: %w)\nHint: PowerShell script should create this folder", dstPath, err)
	}

	fmt.Println()

	// Phase 1: Run traversal to populate the database
	fmt.Println("🚀 Phase 2: Traversal")
	fmt.Println("=====================")
	// Setup for traversal (using traversal shared setup)
	// We'll use LetsMigrate which runs traversal
	cfg, err := setupTraversalConfig(srcPath, dstPath)
	if err != nil {
		return fmt.Errorf("traversal setup failed: %w", err)
	}

	// Run traversal phase (LetsMigrate runs traversal and closes DB in ModeStandalone)
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("traversal failed: %w", err)
	}
	fmt.Printf("Traversal completed. Processed %d SRC nodes, %d DST nodes\n", result.Verification.SrcTotal, result.Verification.DstTotal)
	fmt.Println()

	// Phase 2: Run copy phase
	// Reopen database (LetsMigrate closed it in ModeStandalone mode)
	fmt.Println("🚀 Phase 3: Copy Phase")
	fmt.Println("======================")
	boltDB, srcAdapter, dstAdapter, err := shared.SetupLocalCopyTest(srcPath, dstPath, false) // Don't remove existing DB
	if err != nil {
		return fmt.Errorf("copy setup failed: %w", err)
	}
	defer boltDB.Close()

	// Run copy phase
	stats, err := migration.RunCopyPhase(migration.CopyPhaseConfig{
		BoltDB:          boltDB,
		SrcAdapter:      srcAdapter,
		DstAdapter:      dstAdapter,
		WorkerCount:     10,
		MaxRetries:      3,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		SkipListener:    true,
		StartupDelay:    1 * time.Second,
		ProgressTick:    2 * time.Second,
		ShutdownContext: nil,
	})
	if err != nil {
		return fmt.Errorf("copy phase failed: %w", err)
	}
	fmt.Println()

	// Phase 3: Verification
	fmt.Println("✓ Phase 4: Verification")
	fmt.Println("========================")
	shared.PrintCopyVerification(stats)
	if err := shared.VerifyCopyCompletion(boltDB); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	return nil
}

// setupTraversalConfig creates a migration config for running traversal phase.
// Uses the same database path as copy tests so copy phase can use the populated DB.
func setupTraversalConfig(srcPath, dstPath string) (migration.Config, error) {
	// Import traversal shared to use SetupLocalTest
	// But we need to use copy/shared database path
	// So we'll create the config manually similar to SetupLocalTest

	// We'll use a helper that creates the config but uses copy/shared DB path
	// Actually, let's just use the traversal shared SetupLocalTest but with a custom DB path
	// Or better: create a minimal config here

	// For now, let's use traversal shared but we need to adjust the DB path
	// Actually, the simplest is to temporarily use traversal/shared DB, then copy it
	// Or just use copy/shared DB path directly

	// Create LocalFS adapters
	srcAdapter, err := fs.NewLocalFS(srcPath)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewLocalFS(dstPath)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	// Create root folder structures
	srcRoot := types.Folder{
		ServiceID:    srcPath,
		ParentId:     filepath.Dir(srcPath),
		ParentPath:   "",
		DisplayName:  filepath.Base(srcPath),
		LocationPath: "/",
		LastUpdated:  time.Now().Format(time.RFC3339),
		DepthLevel:   0,
		Type:         types.NodeTypeFolder,
	}

	dstRoot := types.Folder{
		ServiceID:    dstPath,
		ParentId:     filepath.Dir(dstPath),
		ParentPath:   "",
		DisplayName:  filepath.Base(dstPath),
		LocationPath: "/",
		LastUpdated:  time.Now().Format(time.RFC3339),
		DepthLevel:   0,
		Type:         types.NodeTypeFolder,
	}

	// Open database - use copy/shared DB path
	dbInstance, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           "pkg/tests/copy/shared/main_test.db",
		RemoveExisting: true, // Clean DB for fresh traversal
	})
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to open database: %w", err)
	}

	cfg := migration.Config{
		DatabaseInstance: dbInstance,
		Runtime:          migration.ModeStandalone, // Will close DB after traversal
		Database: migration.DatabaseConfig{
			Path:           "pkg/tests/copy/shared/main_test.db",
			RemoveExisting: true,
		},
		Source: migration.Service{
			Name:    "Local-Src",
			Adapter: srcAdapter,
		},
		Destination: migration.Service{
			Name:    "Local-Dst",
			Adapter: dstAdapter,
		},
		SeedRoots:       true,
		WorkerCount:     10,
		MaxRetries:      3,
		CoordinatorLead: 4,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		SkipListener:    true,
		StartupDelay:    1 * time.Second,
		Verification:    migration.VerifyOptions{},
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}
