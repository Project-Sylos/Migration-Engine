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
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// SetupSftpCopyTest assembles SFTP adapters for copy integration tests.
func SetupSftpCopyTest(srcPath, dstPath string) (migration.Config, types.Folder, types.Folder, error) {
	stored, err := sftptest.CredentialsFromEnv()
	if err != nil {
		return migration.Config{}, types.Folder{}, types.Folder{}, err
	}

	srcAdapter, err := sftptest.NewAdapter(stored, srcPath, "test-sftp-copy-src")
	if err != nil {
		return migration.Config{}, types.Folder{}, types.Folder{}, fmt.Errorf("create src adapter: %w", err)
	}
	dstAdapter, err := sftptest.NewAdapter(stored, dstPath, "test-sftp-copy-dst")
	if err != nil {
		return migration.Config{}, types.Folder{}, types.Folder{}, fmt.Errorf("create dst adapter: %w", err)
	}

	srcRoot := sftptest.RootFolder(srcPath)
	dstRoot := sftptest.RootFolder(dstPath)

	dbPath, err := filepath.Abs("pkg/tests/copy/shared/main_test.db")
	if err != nil {
		return migration.Config{}, types.Folder{}, types.Folder{}, err
	}

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: true,
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
		ProgressTick:    time.Second,
		Verification:    migration.VerifyOptions{AllowNotOnSrc: true},
	}
	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, types.Folder{}, types.Folder{}, err
	}
	fmt.Printf("SFTP copy test configured: %s -> %s\n", srcPath, dstPath)
	return cfg, srcRoot, dstRoot, nil
}
