// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Spectra/sdk"
)

// setupSpectraFS creates a SpectraFS instance, handling DB cleanup appropriately.
// Since each test run is a separate process, we can't rely on in-memory state.
// Instead, we check if the DB file exists and only clean it if explicitly requested.
// The SDK should load existing data from the DB file if it exists.
func setupSpectraFS(configPath string, cleanDB bool) (*sdk.SpectraFS, error) {
	// Check if DB file exists
	dbExists := false
	if _, err := os.Stat("./spectra.db"); err == nil {
		dbExists = true
	}

	// Clean DB only if explicitly requested
	if cleanDB && dbExists {
		fmt.Println("Cleaning up previous Spectra state...")
		if err := os.Remove("./spectra.db"); err != nil {
			return nil, fmt.Errorf("failed to remove existing Spectra DB: %w", err)
		}
		// Also remove WAL and SHM files if they exist
		os.Remove("./spectra.db-wal")
		os.Remove("./spectra.db-shm")
	}

	// Create new SpectraFS instance
	// The SDK should load existing data from the DB file if it exists (and wasn't cleaned)
	fs, err := sdk.New(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Spectra: %w", err)
	}

	return fs, nil
}

// SetupTest assembles the Spectra-backed migration configuration.
// cleanSpectraDB controls whether to delete the existing Spectra DB (use false for resumption tests).
// removeMigrationDB controls whether to remove the migration database (use false for resumption tests).
func SetupTest(cleanSpectraDB bool, removeMigrationDB bool) (migration.Config, error) {
	fmt.Println("Loading Spectra configuration...")

	// Create SpectraFS instance (SDK should load existing DB data if file exists)
	spectraFS, err := setupSpectraFS("pkg/configs/spectra.json", cleanSpectraDB)
	if err != nil {
		return migration.Config{}, err
	}

	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}

	srcAdapter, err := fsservices.NewSpectraFS(spectraFS, srcRoot.Id, "primary")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fsservices.NewSpectraFS(spectraFS, dstRoot.Id, "s1")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           "pkg/tests/bolt.db",
			RemoveExisting: removeMigrationDB,
		},
		Source: migration.Service{
			Name:    "Spectra-Primary",
			Adapter: srcAdapter,
		},
		Destination: migration.Service{
			Name:    "Spectra-S1",
			Adapter: dstAdapter,
		},
		SeedRoots:       true,
		WorkerCount:     10,
		MaxRetries:      3,
		CoordinatorLead: 4,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		StartupDelay:    3 * time.Second,
		Verification:    migration.VerifyOptions{},
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}

// LoadSpectraRoots fetches the Spectra root nodes and maps them to fsservices.Folder structures.
func LoadSpectraRoots(spectraFS *sdk.SpectraFS) (fsservices.Folder, fsservices.Folder, error) {
	// Get root nodes from Spectra using request structs
	srcRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fsservices.Folder{}, fsservices.Folder{}, fmt.Errorf("failed to get src root from Spectra: %w", err)
	}

	dstRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fsservices.Folder{}, fsservices.Folder{}, fmt.Errorf("failed to get dst root from Spectra: %w", err)
	}

	// Create folder structs
	srcFolder := fsservices.Folder{
		Id:           srcRoot.ID,
		ParentId:     "",
		DisplayName:  srcRoot.Name,
		LocationPath: "/",
		LastUpdated:  srcRoot.LastUpdated.Format(time.RFC3339),
		ParentPath:   "",
		Type:         fsservices.NodeTypeFolder,
	}

	dstFolder := fsservices.Folder{
		Id:           dstRoot.ID,
		ParentId:     "",
		DisplayName:  dstRoot.Name,
		LocationPath: "/",
		LastUpdated:  dstRoot.LastUpdated.Format(time.RFC3339),
		ParentPath:   "",
		Type:         fsservices.NodeTypeFolder,
	}

	return srcFolder, dstFolder, nil
}
