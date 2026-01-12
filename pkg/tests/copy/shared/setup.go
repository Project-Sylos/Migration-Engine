// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Spectra/sdk"
	"github.com/Project-Sylos/Sylos-FS/pkg/fs"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

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
			_ = os.Remove(lockPath) // Ignore errors - lock file might not exist
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
// Returns the BoltDB instance, source adapter, destination adapter, and error.
func SetupCopyTest(cleanSpectraDB bool, removeMigrationDB bool) (*db.DB, types.FSAdapter, types.FSAdapter, error) {
	fmt.Println("Loading Spectra configuration...")

	// Create SpectraFS instance (SDK should load existing DB data if file exists)
	spectraFS, err := SetupSpectraFS("pkg/tests/copy/shared/spectra.json", cleanSpectraDB)
	if err != nil {
		return nil, nil, nil, err
	}

	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return nil, nil, nil, err
	}

	srcAdapter, err := fs.NewSpectraFS(spectraFS, srcRoot.ServiceID, "primary")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewSpectraFS(spectraFS, dstRoot.ServiceID, "s1")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	// Open database - tests own the lifecycle
	// Use RemoveExisting: false to use pre-provisioned DB
	dbInstance, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           "pkg/tests/copy/shared/main_test.db",
		RemoveExisting: removeMigrationDB,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open database: %w", err)
	}

	return dbInstance, srcAdapter, dstAdapter, nil
}
