// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/copy/shared"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
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
	cfg, err := setupTraversalConfig(srcPath, dstPath)
	if err != nil {
		return fmt.Errorf("traversal setup failed: %w", err)
	}

	manager := migration.NewMigrationManager()
	defer manager.Close()

	dir, id := migration.MigrationDirAndIDFromDBPath(cfg.Database.Path)
	if cfg.Database.RemoveExisting {
		_ = os.Remove(cfg.Database.Path)
	}

	migrationInstance, err := manager.CreateMigration(migration.CreateMigrationConfig{
		MigrationDir: dir,
		MigrationID:  id,
		Name:         id,
		ServiceMetadata: map[string]string{
			"source_name":      cfg.Source.Name,
			"destination_name": cfg.Destination.Name,
		},
		RootConfig: map[string]string{
			"source_root_id":      cfg.Source.Root.ServiceID,
			"destination_root_id": cfg.Destination.Root.ServiceID,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	if _, err := migrationInstance.AddRoots(cfg.Source.Root, cfg.Destination.Root); err != nil {
		return fmt.Errorf("failed to add roots: %w", err)
	}
	if err := assertMigrationPhase(manager, migrationInstance.ID, migration.PhaseFiltersSet); err != nil {
		return err
	}

	runtime, err := migrationInstance.StartTraversal(cfg)
	if err != nil {
		return fmt.Errorf("traversal failed: %w", err)
	}
	if err := assertMigrationPhase(manager, migrationInstance.ID, migration.PhaseTraversalReview); err != nil {
		return err
	}
	fmt.Printf("Traversal completed. SRC tracked %d, DST tracked %d\n", runtime.Src.TotalTracked, runtime.Dst.TotalTracked)
	fmt.Println()

	// Phase 2: Run copy phase via the migration lifecycle so DB phase updates match the API flow.
	fmt.Println("🚀 Phase 3: Copy Phase")
	fmt.Println("======================")
	stats, err := migrationInstance.StartCopy(cfg)
	if err != nil {
		return fmt.Errorf("copy phase failed: %w", err)
	}
	if err := assertMigrationPhase(manager, migrationInstance.ID, migration.PhaseCopyReview); err != nil {
		return err
	}
	fmt.Println()

	// Phase 3: Verification
	fmt.Println("✓ Phase 4: Verification")
	fmt.Println("========================")
	shared.PrintCopyVerification(stats)
	if err := shared.VerifyCopyCompletion(migrationInstance.DB); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	return nil
}

// setupTraversalConfig creates a migration config for running traversal phase.
// Uses the same database path as copy tests so copy phase can use the populated DB.
func setupTraversalConfig(srcPath, dstPath string) (migration.Config, error) {
	// Create LocalFS adapters
	srcAdapter, err := local.NewLocalFS(srcPath)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := local.NewLocalFS(dstPath)
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

	// Open database - use copy/shared DB path (absolute to avoid split-brain across connections)
	dbPath, err := filepath.Abs("pkg/tests/copy/shared/main_test.db")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to resolve DB path: %w", err)
	}
	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
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
		StartupDelay:    2 * time.Second,
		Verification:    migration.VerifyOptions{},
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}

func assertMigrationPhase(manager *migration.MigrationManager, migrationID, expected string) error {
	details, err := manager.GetMigrationDetails(migrationID, "")
	if err != nil {
		return fmt.Errorf("failed to load migration details for %s: %w", migrationID, err)
	}
	if details == nil {
		return fmt.Errorf("migration details missing for %s", migrationID)
	}
	if details.Phase != expected {
		return fmt.Errorf("unexpected migration phase: got %q want %q", details.Phase, expected)
	}
	return nil
}
