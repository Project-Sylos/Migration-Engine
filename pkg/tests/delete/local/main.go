// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/delete/shared"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func main() {
	if err := run(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n✅ TEST PASSED!")
}

func run() error {
	srcPath := os.Getenv("SYLOS_DELETE_TEST_SRC")
	dstPath := os.Getenv("SYLOS_DELETE_TEST_DST")
	if srcPath == "" || dstPath == "" {
		return fmt.Errorf("SYLOS_DELETE_TEST_SRC and SYLOS_DELETE_TEST_DST must be set")
	}
	cfg, err := setupConfig(srcPath, dstPath)
	if err != nil {
		return err
	}
	manager := migration.NewMigrationManager()
	defer manager.Close()

	dir, id := migration.MigrationDirAndIDFromDBPath(cfg.Database.Path)
	if cfg.Database.RemoveExisting {
		_ = os.Remove(cfg.Database.Path)
	}
	mig, err := manager.CreateMigration(migration.CreateMigrationConfig{
		MigrationDir: dir,
		MigrationID:  id,
		Name:         "delete-local",
	})
	if err != nil {
		return err
	}
	if _, err := mig.AddRoots(cfg.Source.Root, cfg.Destination.Root); err != nil {
		return err
	}
	if _, err := mig.StartTraversal(cfg); err != nil {
		return fmt.Errorf("traversal: %w", err)
	}
	if _, err := mig.StartCopy(cfg); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	if _, err := mig.StartDelete(cfg); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if mig.Phase() != migration.PhaseDeleteReview {
		return fmt.Errorf("expected %s, got %s", migration.PhaseDeleteReview, mig.Phase())
	}
	if err := shared.VerifySrcTreeEmpty(srcPath); err != nil {
		return err
	}
	if err := shared.VerifyDeleteCounts(mig.DB, 1); err != nil {
		return err
	}
	return nil
}

func setupConfig(srcPath, dstPath string) (migration.Config, error) {
	srcAdapter, err := local.NewLocalFS(srcPath)
	if err != nil {
		return migration.Config{}, err
	}
	dstAdapter, err := local.NewLocalFS(dstPath)
	if err != nil {
		return migration.Config{}, err
	}
	srcRoot := types.Folder{
		ServiceID: srcPath, LocationPath: "/", DisplayName: filepath.Base(srcPath),
		LastUpdated: time.Now().Format(time.RFC3339), DepthLevel: 0, Type: types.NodeTypeFolder,
	}
	dstRoot := types.Folder{
		ServiceID: dstPath, LocationPath: "/", DisplayName: filepath.Base(dstPath),
		LastUpdated: time.Now().Format(time.RFC3339), DepthLevel: 0, Type: types.NodeTypeFolder,
	}
	dbPath, err := filepath.Abs(filepath.Join(os.TempDir(), "sylos_delete_local_test.db"))
	if err != nil {
		return migration.Config{}, err
	}
	cfg := migration.Config{
		Database:        migration.DatabaseConfig{Path: dbPath, RemoveExisting: true},
		Source:          migration.Service{Name: "Local-Src", Adapter: srcAdapter},
		Destination:       migration.Service{Name: "Local-Dst", Adapter: dstAdapter},
		SeedRoots:         true,
		WorkerCount:       4,
		MaxRetries:        3,
		SkipListener:      true,
		StartupDelay:      500 * time.Millisecond,
	}
	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}
	return cfg, nil
}
