// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

// SetupTest assembles the Spectra-backed migration configuration.
// cleanSpectraDB controls whether to delete the existing Spectra DB (use false for resumption tests).
// removeMigrationDB controls whether to remove the migration database (use false for resumption tests).
func SetupTest(cleanSpectraDB bool, removeMigrationDB bool) (migration.Config, error) {
	fmt.Println("Loading Spectra configuration...")

	// Create SpectraFS instance (SDK should load existing DB data if file exists)
	spectraFS, err := SetupSpectraFS("pkg/tests/shared/spectra.json", cleanSpectraDB)
	if err != nil {
		return migration.Config{}, err
	}

	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}

	srcAdapter, err := fs.NewSpectraFS(spectraFS, srcRoot.ServiceID, "primary")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewSpectraFS(spectraFS, dstRoot.ServiceID, "s1")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	// Open database - tests own the lifecycle
	dbPath := "pkg/tests/shared/main_test.db"
	dbInstance, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: removeMigrationDB,
	})
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to open database: %w", err)
	}

	// Use NewConfig() template and customize with builder methods
	cfg := migration.NewConfig().
		WithStore(dbInstance, dbPath).
		WithServices(srcAdapter, dstAdapter, "Spectra-Primary", "Spectra-S1").
		WithRoots(srcRoot, dstRoot)

	// override skip listener to false
	cfg.SkipListener = false
	return *cfg, nil
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

// SetupLocalTest assembles a local filesystem-backed migration configuration.
// srcPath and dstPath are absolute paths to the source and destination directories.
// removeMigrationDB controls whether to remove the migration database (use false for resumption tests).
func SetupLocalTest(srcPath, dstPath string, removeMigrationDB bool) (migration.Config, error) {
	fmt.Printf("Setting up local filesystem migration...\n")
	fmt.Printf("  Source: %s\n", srcPath)
	fmt.Printf("  Destination: %s\n", dstPath)

	// Verify paths exist
	if _, err := os.Stat(srcPath); err != nil {
		return migration.Config{}, fmt.Errorf("source path does not exist or is not accessible: %s (error: %w)", srcPath, err)
	}
	if _, err := os.Stat(dstPath); err != nil {
		return migration.Config{}, fmt.Errorf("destination path does not exist or is not accessible: %s (error: %w)", dstPath, err)
	}

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

	// Open database - tests own the lifecycle
	dbPath := "pkg/tests/shared/main_test.db"
	dbInstance, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: removeMigrationDB,
	})
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to open database: %w", err)
	}

	// Use NewConfig() template and customize with builder methods
	cfg := migration.NewConfig().
		WithStore(dbInstance, dbPath).
		WithServices(srcAdapter, dstAdapter, "Local-Src", "Local-Dst").
		WithRoots(srcRoot, dstRoot)

	return *cfg, nil
}
