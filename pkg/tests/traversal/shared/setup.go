// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Spectra/sdk"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/spectra"
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

// IsChaosEnabled reports whether the Spectra config has chaos.enabled set.
func IsChaosEnabled(configPath string) (bool, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return false, fmt.Errorf("failed to read config file: %w", err)
	}
	var config struct {
		Chaos *struct {
			Enabled bool `json:"enabled"`
		} `json:"chaos"`
	}
	if err := json.Unmarshal(configData, &config); err != nil {
		return false, fmt.Errorf("failed to parse config file: %w", err)
	}
	return config.Chaos != nil && config.Chaos.Enabled, nil
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
	if err := EnsureSpectraAuth(fs); err != nil {
		_ = fs.Close()
		return nil, err
	}

	return fs, nil
}

// EnsureSpectraAuth issues and binds per-world access tokens when auth is enabled.
func EnsureSpectraAuth(fs *sdk.SpectraFS) error {
	if fs == nil || !fs.AuthEnabled() {
		return nil
	}
	worlds := []string{"primary"}
	worlds = append(worlds, fs.GetSecondaryTables()...)
	for _, world := range worlds {
		if _, err := fs.EnsureWorldAuth(world); err != nil {
			return fmt.Errorf("ensure auth for world %s: %w", world, err)
		}
	}
	return nil
}

// SetupTest assembles the Spectra-backed migration configuration.
// cleanSpectraDB controls whether to delete the existing Spectra DB (use false for resumption tests).
// removeMigrationDB controls whether to remove the migration database (use false for resumption tests).
func SetupTest(cleanSpectraDB bool, removeMigrationDB bool) (migration.Config, error) {
	fmt.Println("Loading Spectra configuration...")

	// Create SpectraFS instance (SDK should load existing DB data if file exists)
	spectraFS, err := SetupSpectraFS("pkg/tests/traversal/shared/spectra.json", cleanSpectraDB)
	if err != nil {
		return migration.Config{}, err
	}

	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}

	// Check if we're in ephemeral mode
	isEphemeral, err := isEphemeralMode("pkg/tests/traversal/shared/spectra.json")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to check mode: %w", err)
	}

	srcAdapter, err := spectra.NewSpectraFS(spectraFS, srcRoot.ServiceID, "primary", isEphemeral)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := spectra.NewSpectraFS(spectraFS, dstRoot.ServiceID, "s1", isEphemeral)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

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
		StartupDelay:    1 * time.Second, // you should set this to 3 if you set skip listener to false to account for terminal opening delay
		Verification:    migration.VerifyOptions{},
		Autoscaler:      migration.DefaultAutoscalerConfig(),
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}

// SetupEphemeralTest assembles the Spectra-backed migration configuration using ephemeral mode.
// Ephemeral mode doesn't persist data to a database, so no Spectra DB cleanup is needed.
// removeMigrationDB controls whether to remove the migration database (use false for resumption tests).
func SetupEphemeralTest(removeMigrationDB bool) (migration.Config, error) {
	fmt.Println("Loading Spectra ephemeral configuration...")

	// Create SpectraFS instance using ephemeral config (no DB cleanup needed for ephemeral mode)
	spectraFS, err := sdk.New("pkg/tests/traversal/shared/spectra_ephemeral.json")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to initialize Spectra in ephemeral mode: %w", err)
	}
	if err := EnsureSpectraAuth(spectraFS); err != nil {
		_ = spectraFS.Close()
		return migration.Config{}, err
	}

	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}

	// Create adapters with ephemeral mode enabled.
	// The adapter will pass depth parameter in ListChildren() calls when needed (ephemeral mode only).
	// The adapter gets the parent node's depth via GetNode() and passes it to the SDK.
	srcAdapter, dstAdapter, err := NewSharedSpectraAdapters(spectraFS, srcRoot, dstRoot, true)
	if err != nil {
		return migration.Config{}, err
	}

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
		StartupDelay:    1 * time.Second, // you should set this to 3 if you set skip listener to false to account for terminal opening delay
		Verification: migration.VerifyOptions{
			AllowNotOnSrc: true, // Ephemeral mode allows divergent trees (nodes on dst but not src)
		},
		Autoscaler: migration.DefaultAutoscalerConfig(),
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}

// NewSharedSpectraAdapters creates src/dst adapters sharing one degradation state (same SDK backend).
func NewSharedSpectraAdapters(spectraFS *sdk.SpectraFS, srcRoot, dstRoot types.Folder, ephemeral bool) (*spectra.SpectraFS, *spectra.SpectraFS, error) {
	deg := types.NewFSDegradationState()
	srcAdapter, err := spectra.NewSpectraFS(spectraFS, srcRoot.ServiceID, "primary", ephemeral, spectra.WithDegradationState(deg))
	if err != nil {
		return nil, nil, fmt.Errorf("create src adapter: %w", err)
	}
	dstAdapter, err := spectra.NewSpectraFS(spectraFS, dstRoot.ServiceID, "s1", ephemeral, spectra.WithDegradationState(deg))
	if err != nil {
		return nil, nil, fmt.Errorf("create dst adapter: %w", err)
	}
	return srcAdapter, dstAdapter, nil
}

// SetupEphemeralThrottleTest builds ephemeral Spectra config with chaos rate limits for autoscaler testing.
func SetupEphemeralThrottleTest(removeMigrationDB bool, workerCount int, autoscaler migration.AutoscalerConfig) (migration.Config, error) {
	fmt.Println("Loading Spectra ephemeral throttle configuration...")
	spectraFS, err := sdk.New("pkg/tests/traversal/shared/spectra_ephemeral_autoscaler_throttle.json")
	if err != nil {
		return migration.Config{}, fmt.Errorf("spectra throttle config: %w", err)
	}
	if err := EnsureSpectraAuth(spectraFS); err != nil {
		_ = spectraFS.Close()
		return migration.Config{}, err
	}
	srcRoot, dstRoot, err := LoadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}
	srcAdapter, dstAdapter, err := NewSharedSpectraAdapters(spectraFS, srcRoot, dstRoot, true)
	if err != nil {
		return migration.Config{}, err
	}
	if workerCount <= 0 {
		workerCount = 20
	}
	dbPath, err := filepath.Abs("pkg/tests/traversal/shared/main_test.db")
	if err != nil {
		return migration.Config{}, err
	}
	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: removeMigrationDB,
		},
		Source: migration.Service{
			Name:       "Spectra-Primary",
			Adapter:    srcAdapter,
			ProviderID: "spectra",
		},
		Destination: migration.Service{
			Name:       "Spectra-S1",
			Adapter:    dstAdapter,
			ProviderID: "spectra",
		},
		SeedRoots:       true,
		WorkerCount:     workerCount,
		MaxRetries:      3,
		CoordinatorLead: 4,
		SkipListener:    true,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		StartupDelay:    500 * time.Millisecond,
		ProgressTick:    time.Second,
		Autoscaler:      autoscaler.Resolve(),
		Verification: migration.VerifyOptions{
			AllowNotOnSrc: true,
		},
	}
	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}
	return cfg, nil
}

// LoadSpectraRoots fetches the Spectra root nodes and maps them to types.Folder structures.
func LoadSpectraRoots(spectraFS *sdk.SpectraFS) (types.Folder, types.Folder, error) {
	// Get root nodes from Spectra using request structs
	srcRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID:        "root",
		TableName: "primary",
	})
	if err != nil {
		return types.Folder{}, types.Folder{}, fmt.Errorf("failed to get src root from Spectra: %w", err)
	}

	dstRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID:        "root",
		TableName: "s1",
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

// LocalTestOptions configures SetupLocalTestWithOptions.
type LocalTestOptions struct {
	RemoveMigrationDB bool
	WorkerCount       int
	ProgressTick      time.Duration
	Autoscaler        migration.AutoscalerConfig
}

// SetupLocalTest assembles a local filesystem-backed migration configuration.
// srcPath and dstPath are absolute paths to the source and destination directories.
// removeMigrationDB controls whether to remove the migration database (use false for resumption tests).
func SetupLocalTest(srcPath, dstPath string, removeMigrationDB bool) (migration.Config, error) {
	return SetupLocalTestWithOptions(srcPath, dstPath, LocalTestOptions{
		RemoveMigrationDB: removeMigrationDB,
	})
}

// SetupLocalTestWithOptions assembles a local filesystem-backed migration configuration.
func SetupLocalTestWithOptions(srcPath, dstPath string, opts LocalTestOptions) (migration.Config, error) {
	fmt.Printf("Setting up local filesystem migration...\n")
	fmt.Printf("  Source: %s\n", srcPath)
	fmt.Printf("  Destination: %s\n", dstPath)

	// Create LocalFS adapters
	srcAdapter, err := local.NewLocalFS(srcPath)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := local.NewLocalFS(dstPath)
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	// Verify source path exists and is accessible
	if _, err := os.Stat(srcPath); err != nil {
		return migration.Config{}, fmt.Errorf("source path does not exist or is not accessible: %s (error: %w)", srcPath, err)
	}

	// Verify destination path exists and is accessible
	// Note: We don't create it automatically to avoid accidentally creating directories
	if _, err := os.Stat(dstPath); err != nil {
		return migration.Config{}, fmt.Errorf("destination path does not exist or is not accessible: %s (error: %w)", dstPath, err)
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

	dbPath, err := filepath.Abs("pkg/tests/traversal/shared/main_test.db")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to resolve DB path: %w", err)
	}

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = 10
	}
	progressTick := opts.ProgressTick
	if progressTick <= 0 {
		progressTick = time.Second
	}

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: opts.RemoveMigrationDB,
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
		WorkerCount:     workerCount,
		MaxRetries:      3,
		CoordinatorLead: 4,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		SkipListener:    true,
		StartupDelay:    1 * time.Second,
		ProgressTick:    progressTick,
		Autoscaler:      opts.Autoscaler.Resolve(),
		Verification:    migration.VerifyOptions{AllowNotOnSrc: true},
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}
