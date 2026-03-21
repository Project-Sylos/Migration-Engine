// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Spectra/sdk"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// isEphemeralMode checks if the Spectra config file specifies ephemeral mode.
func isEphemeralMode(configPath string) (bool, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return false, fmt.Errorf("failed to read config file: %w", err)
	}

	var config struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(configData, &config); err != nil {
		return false, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config.Mode == "ephemeral", nil
}

// SetupSpectraFS creates a SpectraFS instance, handling DB cleanup appropriately.
// Since each test run is a separate process, we can't rely on in-memory state.
// Instead, we check if the DB file exists (from the config) and only clean it if explicitly requested.
// The SDK should load existing data from the DB file if it exists.
// The configPath should point to a JSON config file that contains the db_path.
func SetupSpectraFS(configPath string, cleanDB bool) (*sdk.SpectraFS, error) {
	// Read config to get the actual DB path
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse config to get db_path (simple JSON parsing)
	var config struct {
		Seed struct {
			DBPath string `json:"db_path"`
		} `json:"seed"`
	}
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Resolve DB path the same way the SDK will (for cleanup purposes only)
	// The SDK resolves paths relative to the config file directory
	dbPath := config.Seed.DBPath
	if !filepath.IsAbs(dbPath) {
		configDir := filepath.Dir(configPath)
		dbPath = filepath.Join(configDir, dbPath)
	}

	// Clean DB only if explicitly requested
	if cleanDB {
		// Check if DB file exists
		if _, err := os.Stat(dbPath); err == nil {
			fmt.Println("Cleaning up previous Spectra state...")
			if err := os.Remove(dbPath); err != nil {
				return nil, fmt.Errorf("failed to remove existing Spectra DB (may be locked from previous run): %w", err)
			}
			// Also try to remove lock files (Windows-specific: .db.lock)
			lockPath := dbPath + ".lock"
			err := os.Remove(lockPath) // Ignore errors - lock file might not exist
			if err != nil {
				fmt.Println("error removing lock file", err)
			}
		}
	}

	// Create new SpectraFS instance
	// The SDK should load existing data from the DB file if it exists (and wasn't cleaned)
	fs, err := sdk.New(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Spectra: %w", err)
	}

	return fs, nil
}

// LoadSpectraRoots fetches the Spectra root nodes and maps them to types.Folder structures.
func LoadSpectraRoots(spectraFS *sdk.SpectraFS) (types.Folder, types.Folder, error) {
	// Get root nodes from Spectra using request structs
	srcRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return types.Folder{}, types.Folder{}, fmt.Errorf("failed to get src root from Spectra: %w", err)
	}

	dstRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return types.Folder{}, types.Folder{}, fmt.Errorf("failed to get dst root from Spectra: %w", err)
	}

	// Create folder structs
	srcFolder := types.Folder{
		ServiceID:    srcRoot.ID,
		ParentId:     "",
		DisplayName:  srcRoot.Name,
		LocationPath: "/",
		LastUpdated:  srcRoot.LastUpdated.Format(time.RFC3339),
		ParentPath:   "",
		Type:         types.NodeTypeFolder,
	}

	dstFolder := types.Folder{
		ServiceID:    dstRoot.ID,
		ParentId:     "",
		DisplayName:  dstRoot.Name,
		LocationPath: "/",
		LastUpdated:  dstRoot.LastUpdated.Format(time.RFC3339),
		ParentPath:   "",
		Type:         types.NodeTypeFolder,
	}

	return srcFolder, dstFolder, nil
}

// SetupCopyTest sets up the database and adapters for copy phase testing.
// cleanSpectraDB controls whether to delete the existing Spectra DB (use false for copy tests).
// removeMigrationDB controls whether to remove the migration database (use false to use pre-provisioned DB).
// Returns the database instance, source adapter, destination adapter, and error.
func SetupCopyTest(cleanSpectraDB bool, removeMigrationDB bool) (*db.DB, types.FSAdapter, types.FSAdapter, error) {
	cfg, err := SetupCopyTestConfig(cleanSpectraDB, removeMigrationDB)
	if err != nil {
		return nil, nil, nil, err
	}
	database, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           cfg.Database.Path,
		RemoveExisting: false,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return database, cfg.Source.Adapter, cfg.Destination.Adapter, nil
}

// SetupCopyTestConfig returns a full migration.Config for the copy test (Spectra roots, DB at copy/shared/main_test.db).
// Use with migration.LetsMigrate to run traversal and produce a DuckDB ready for copy-phase tests.
// cleanSpectraDB: false to keep existing Spectra DB. removeMigrationDB: true to create a fresh migration DB.
func SetupCopyTestConfig(cleanSpectraDB bool, removeMigrationDB bool) (migration.Config, error) {
	fmt.Println("Loading Spectra configuration...")

	spectraFS, err := SetupSpectraFS("pkg/tests/copy/shared/spectra.json", cleanSpectraDB)
	if err != nil {
		return migration.Config{}, err
	}

	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}

	isEphemeral, err := isEphemeralMode("pkg/tests/copy/shared/spectra.json")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to check mode: %w", err)
	}

	srcAdapter, err := fs.NewSpectraFS(spectraFS, srcRoot.ServiceID, "primary", isEphemeral)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewSpectraFS(spectraFS, dstRoot.ServiceID, "s1", isEphemeral)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	dbPath, err := filepath.Abs("pkg/tests/copy/shared/main_test.db")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to resolve DB path: %w", err)
	}
	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
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
		SkipListener:    true,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		StartupDelay:    1 * time.Second,
		Verification:    migration.VerifyOptions{},
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}

// SetupLocalCopyTest sets up the database and adapters for local filesystem copy phase testing.
// srcPath and dstPath are absolute paths to the source and destination directories.
// removeMigrationDB controls whether to remove the migration database (use true for fresh test).
// Returns the DuckDB instance, source adapter, destination adapter, and error.
func SetupLocalCopyTest(srcPath, dstPath string, removeMigrationDB bool) (*db.DB, types.FSAdapter, types.FSAdapter, error) {
	fmt.Printf("Setting up local filesystem copy test...\n")
	fmt.Printf("  Source: %s\n", srcPath)
	fmt.Printf("  Destination: %s\n", dstPath)

	// Create LocalFS adapters
	srcAdapter, err := fs.NewLocalFS(srcPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewLocalFS(dstPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	// Verify source path exists and is accessible
	if _, err := os.Stat(srcPath); err != nil {
		return nil, nil, nil, fmt.Errorf("source path does not exist or is not accessible: %s (error: %w)", srcPath, err)
	}

	// Verify destination path exists and is accessible
	// Note: PowerShell script should create this before running the test
	if _, err := os.Stat(dstPath); err != nil {
		return nil, nil, nil, fmt.Errorf("destination path does not exist or is not accessible: %s (error: %w)", dstPath, err)
	}

	// Open database - tests own the lifecycle (absolute path to avoid split-brain across connections)
	dbPath, err := filepath.Abs("pkg/tests/copy/shared/main_test.db")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to resolve DB path: %w", err)
	}
	dbInstance, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: removeMigrationDB,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open database: %w", err)
	}

	return dbInstance, srcAdapter, dstAdapter, nil
}
