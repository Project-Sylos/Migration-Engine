// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/copy/shared"
)

func main() {
	fmt.Println("=== SFTP Copy Phase Test Runner ===")
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

	cfg, srcRoot, dstRoot, err := shared.SetupSftpCopyTest(srcPath, dstPath)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
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
			"source_root_id":      srcRoot.ServiceID,
			"destination_root_id": dstRoot.ServiceID,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	if _, err := migrationInstance.AddRoots(srcRoot, dstRoot); err != nil {
		return fmt.Errorf("failed to add roots: %w", err)
	}
	if err := assertMigrationPhase(manager, migrationInstance.ID, migration.PhaseFiltersSet); err != nil {
		return err
	}

	fmt.Println("🚀 Phase 2: Traversal")
	fmt.Println("=====================")
	runtime, err := migrationInstance.StartTraversal(cfg)
	if err != nil {
		return fmt.Errorf("traversal failed: %w", err)
	}
	if err := assertMigrationPhase(manager, migrationInstance.ID, migration.PhaseTraversalReview); err != nil {
		return err
	}
	fmt.Printf("Traversal completed. SRC tracked %d, DST tracked %d\n", runtime.Src.TotalTracked, runtime.Dst.TotalTracked)
	fmt.Println()

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

	fmt.Println("✓ Phase 4: Verification")
	fmt.Println("========================")
	shared.PrintCopyVerification(stats)
	if err := shared.VerifyCopyCompletion(migrationInstance.DB); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}
	return nil
}

func assertMigrationPhase(manager *migration.MigrationManager, migrationID, expected string) error {
	details, err := manager.GetMigrationDetails(migrationID, "", nil)
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
