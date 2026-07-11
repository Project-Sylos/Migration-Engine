// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/sftptest"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
)

// SetupSftpTest assembles an SFTP-backed migration configuration for traversal tests.
func SetupSftpTest(srcPath, dstPath string, removeMigrationDB bool) (migration.Config, error) {
	stored, err := sftptest.CredentialsFromEnv()
	if err != nil {
		return migration.Config{}, err
	}

	fmt.Printf("Setting up SFTP migration...\n")
	fmt.Printf("  Source: %s\n", srcPath)
	fmt.Printf("  Destination: %s\n", dstPath)

	srcAdapter, err := sftptest.NewAdapter(stored, srcPath, "test-sftp-src")
	if err != nil {
		return migration.Config{}, fmt.Errorf("create src adapter: %w", err)
	}
	dstAdapter, err := sftptest.NewAdapter(stored, dstPath, "test-sftp-dst")
	if err != nil {
		return migration.Config{}, fmt.Errorf("create dst adapter: %w", err)
	}

	srcRoot := sftptest.RootFolder(srcPath)
	dstRoot := sftptest.RootFolder(dstPath)

	dbPath, err := filepath.Abs("pkg/tests/traversal/shared/main_test.db")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to resolve DB path: %w", err)
	}

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: removeMigrationDB,
		},
		Source: migration.Service{
			Name:       "SFTP-Src",
			Adapter:    srcAdapter,
			ProviderID: cloud.ProviderSFTP,
		},
		Destination: migration.Service{
			Name:       "SFTP-Dst",
			Adapter:    dstAdapter,
			ProviderID: cloud.ProviderSFTP,
		},
		SeedRoots:       true,
		WorkerCount:     10,
		MaxRetries:      3,
		CoordinatorLead: 4,
		SkipListener:    true,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		StartupDelay:    1 * time.Second,
		Verification:    migration.VerifyOptions{AllowNotOnSrc: true},
		Autoscaler:      migration.DefaultAutoscalerConfig(),
	}
	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}
	return cfg, nil
}
