// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// Service defines a single filesystem service participating in a migration.
type Service struct {
	Name    string
	Adapter fsservices.FSAdapter
	Root    fsservices.Folder
}

// Config aggregates all of the knobs required to run the migration engine once.
type Config struct {
	Database         DatabaseConfig
	DatabaseInstance *db.DB // BoltDB instance
	CloseDatabase    bool

	Source      Service
	Destination Service

	SeedRoots       bool
	WorkerCount     int
	MaxRetries      int
	CoordinatorLead int

	LogAddress   string
	LogLevel     string
	SkipListener bool
	StartupDelay time.Duration
	ProgressTick time.Duration

	Verification VerifyOptions

	// Config YAML management (internal use)
	ConfigPath string
	YAMLConfig *MigrationConfigYAML

	// ShutdownContext is an optional context for force shutdown control.
	// If not provided, LetsMigrate will create one internally.
	// Set this when using StartMigration for programmatic shutdown control.
	ShutdownContext context.Context
}

// Result captures the outcome of a migration run.
type Result struct {
	RootsSeeded  bool
	RootSummary  RootSeedSummary
	Runtime      RuntimeStats
	Verification VerificationReport
}

// MigrationController provides programmatic control over a running migration.
// It allows you to trigger force shutdown and check migration status.
type MigrationController struct {
	shutdownCancel context.CancelFunc
	shutdownCtx    context.Context
	done           chan struct{}
	result         *Result
	err            error
}

// Shutdown triggers a force shutdown of the migration.
// It checkpoints the database and saves the current state to YAML with "suspended" status.
// This is safe to call multiple times or after the migration has completed.
func (mc *MigrationController) Shutdown() {
	if mc.shutdownCancel != nil {
		mc.shutdownCancel()
	}
}

// Done returns a channel that is closed when the migration completes or is shutdown.
func (mc *MigrationController) Done() <-chan struct{} {
	return mc.done
}

// Wait blocks until the migration completes or is shutdown, then returns the result and error.
func (mc *MigrationController) Wait() (Result, error) {
	<-mc.done
	if mc.result != nil {
		return *mc.result, mc.err
	}
	return Result{}, mc.err
}

// SetRootFolders assigns the source and destination root folders that will seed the migration queues.
// It normalizes required defaults (location path, type, display name) and validates identifiers.
func (c *Config) SetRootFolders(src, dst fsservices.Folder) error {
	normalizedSrc, err := normalizeRootFolder(src)
	if err != nil {
		return fmt.Errorf("source root: %w", err)
	}
	normalizedDst, err := normalizeRootFolder(dst)
	if err != nil {
		return fmt.Errorf("destination root: %w", err)
	}

	c.Source.Root = normalizedSrc
	c.Destination.Root = normalizedDst

	// Update config YAML if it exists
	if c.YAMLConfig != nil && c.ConfigPath != "" {
		UpdateConfigFromRoots(c.YAMLConfig, normalizedSrc, normalizedDst)
		if err := SaveMigrationConfig(c.ConfigPath, c.YAMLConfig); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Warning: failed to update config YAML: %v\n", err)
		}
	}

	return nil
}

// StartMigration starts a migration asynchronously and returns a MigrationController
// that allows programmatic shutdown. Use this when you need to control the migration
// lifecycle or run migrations in the background.
//
// Example:
//
//	controller := migration.StartMigration(cfg)
//	defer controller.Shutdown()
//
//	// Later, trigger shutdown programmatically:
//	controller.Shutdown()
//
//	// Wait for completion:
//	result, err := controller.Wait()
func StartMigration(cfg Config) *MigrationController {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	controller := &MigrationController{
		shutdownCancel: shutdownCancel,
		shutdownCtx:    shutdownCtx,
		done:           done,
	}

	// Run migration in goroutine
	go func() {
		defer close(done)
		// Pass the shutdown context to LetsMigrate
		cfgCopy := cfg
		cfgCopy.ShutdownContext = shutdownCtx
		result, err := letsMigrateWithContext(cfgCopy)
		controller.result = &result
		controller.err = err
	}()

	return controller
}

// LetsMigrate executes setup, traversal, and verification using the supplied configuration.
// This is the synchronous version - it blocks until the migration completes or is shutdown.
// For programmatic shutdown control, use StartMigration instead.
func LetsMigrate(cfg Config) (Result, error) {
	// Create shutdown context if not provided
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	// Start signal handler in background goroutine
	go HandleShutdownSignals(shutdownCancel)

	cfg.ShutdownContext = shutdownCtx
	return letsMigrateWithContext(cfg)
}

// letsMigrateWithContext is the internal implementation that accepts a shutdown context.
func letsMigrateWithContext(cfg Config) (Result, error) {
	var (
		boltDB *db.DB
		err    error
	)

	// Use provided shutdown context, or create one if not provided
	shutdownCtx := cfg.ShutdownContext
	var shutdownCancel context.CancelFunc
	if shutdownCtx == nil {
		shutdownCtx, shutdownCancel = context.WithCancel(context.Background())
		defer shutdownCancel()

		// Start signal handler in background goroutine
		go HandleShutdownSignals(shutdownCancel)
	}

	var wasFresh bool
	if cfg.DatabaseInstance != nil {
		boltDB = cfg.DatabaseInstance
		wasFresh = false // Existing database instance
	} else {
		var setupErr error
		boltDB, wasFresh, setupErr = SetupDatabase(cfg.Database)
		if setupErr != nil {
			return Result{}, setupErr
		}
		cfg.CloseDatabase = true
	}

	// Only inspect migration status if this isn't a fresh database.
	// If we just created it (RemoveExisting=true or directory didn't exist), skip the check.
	var status MigrationStatus
	if wasFresh {
		// Fresh database - no need to check, it's definitely empty
		status = MigrationStatus{}
	} else {
		// Existing database - inspect to decide between fresh run and resume
		var inspectErr error
		status, inspectErr = InspectMigrationStatus(boltDB)
		if inspectErr != nil {
			return Result{}, fmt.Errorf("failed to inspect migration status: %w", inspectErr)
		}
	}

	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return Result{}, fmt.Errorf("source and destination adapters must be provided")
	}

	srcRoot, err := normalizeRootFolder(cfg.Source.Root)
	if err != nil {
		return Result{}, fmt.Errorf("source root: %w", err)
	}
	dstRoot, err := normalizeRootFolder(cfg.Destination.Root)
	if err != nil {
		return Result{}, fmt.Errorf("destination root: %w", err)
	}

	// Determine config path
	configPath := cfg.Database.ConfigPath
	if configPath == "" {
		configPath = ConfigPathFromDatabasePath(cfg.Database.Path)
	}

	// Load or create migration config YAML
	var yamlCfg *MigrationConfigYAML
	if wasFresh {
		// Create new config
		yamlCfg, err = NewMigrationConfigYAML(cfg, status)
		if err != nil {
			return Result{}, fmt.Errorf("failed to create migration config: %w", err)
		}
		// Update with root info
		UpdateConfigFromRoots(yamlCfg, srcRoot, dstRoot)
		// Save initial config
		if err := SaveMigrationConfig(configPath, yamlCfg); err != nil {
			return Result{}, fmt.Errorf("failed to save migration config: %w", err)
		}
	} else {
		// Try to load existing config
		loadedCfg, loadErr := LoadMigrationConfig(configPath)
		if loadErr != nil {
			// If config doesn't exist, create a new one
			yamlCfg, err = NewMigrationConfigYAML(cfg, status)
			if err != nil {
				return Result{}, fmt.Errorf("failed to create migration config: %w", err)
			}
			UpdateConfigFromRoots(yamlCfg, srcRoot, dstRoot)
			if err := SaveMigrationConfig(configPath, yamlCfg); err != nil {
				return Result{}, fmt.Errorf("failed to save migration config: %w", err)
			}
		} else {
			yamlCfg = loadedCfg
			// Update with current root info
			UpdateConfigFromRoots(yamlCfg, srcRoot, dstRoot)
			// Update status (preserves "suspended" status if set)
			UpdateConfigFromStatus(yamlCfg, status, 0, 0)
			if err := SaveMigrationConfig(configPath, yamlCfg); err != nil {
				return Result{}, fmt.Errorf("failed to save migration config: %w", err)
			}

			// If status was "suspended", indicate resume from suspension
			if yamlCfg.State.Status == "suspended" {
				fmt.Println("Resuming from suspended migration state...")
			}
		}
	}

	result := Result{RootsSeeded: cfg.SeedRoots}

	// Decide whether to run a fresh migration (seed + traversal) or resume from
	// an existing in-progress database.
	var runtime RuntimeStats
	var runErr error

	boltPath := cfg.Database.Path
	if boltPath == "" {
		boltPath = "migration.db"
	}
	if abs, err := filepath.Abs(boltPath); err == nil {
		boltPath = abs
	}

	// Helper function to run fresh migration
	runFreshMigration := func() (RuntimeStats, error) {
		if cfg.SeedRoots {
			// Seed root tasks to BoltDB
			summary, err := SeedRootTasks(srcRoot, dstRoot, boltDB)
			if err != nil {
				fmt.Printf("Warning: failed to seed root tasks: %v\n", err)
				return RuntimeStats{}, err
			}
			result.RootSummary = summary

			// Update config: Roots seeded milestone
			if yamlCfg != nil && configPath != "" {
				UpdateConfigFromRoots(yamlCfg, srcRoot, dstRoot)
				yamlCfg.State.Status = "seeded"
				_ = SaveMigrationConfig(configPath, yamlCfg)
			}
		}
		return RunMigration(MigrationConfig{
			BoltDB:          boltDB,
			BoltPath:        boltPath,
			SrcAdapter:      cfg.Source.Adapter,
			DstAdapter:      cfg.Destination.Adapter,
			SrcRoot:         srcRoot,
			DstRoot:         dstRoot,
			SrcServiceName:  cfg.Source.Name,
			WorkerCount:     cfg.WorkerCount,
			MaxRetries:      cfg.MaxRetries,
			CoordinatorLead: cfg.CoordinatorLead,
			LogAddress:      cfg.LogAddress,
			LogLevel:        cfg.LogLevel,
			SkipListener:    cfg.SkipListener,
			StartupDelay:    cfg.StartupDelay,
			ProgressTick:    cfg.ProgressTick,
			ConfigPath:      configPath,
			YAMLConfig:      yamlCfg,
			ShutdownContext: shutdownCtx,
		})
	}

	switch {
	case status.IsEmpty():
		// Fresh run: optionally seed roots, then run normal traversal.
		runtime, runErr = runFreshMigration()

	case status.HasPending():
		// Resume from an in-progress migration (including suspended state).
		// Root seeding is assumed to have been done previously and is skipped here.

		// Validate that root nodes still exist in the filesystem before resuming.
		// If they don't exist (e.g., Spectra DB was reset), we can't safely resume
		// because the migration DB contains stale node IDs.
		if err := validateRootNodesExist(cfg.Source.Adapter, cfg.Destination.Adapter, srcRoot, dstRoot); err != nil {
			fmt.Printf("⚠️  Warning: Root nodes validation failed during resume: %v\n", err)
			fmt.Println("   This likely means the filesystem state was reset (e.g., Spectra DB cleared).")
			fmt.Println("   Treating as fresh start instead of resume to avoid 'node not found' errors.")
			fmt.Println()

			// Update YAML to reflect fresh start
			if yamlCfg != nil {
				yamlCfg.State.Status = "running"
				UpdateConfigFromRoots(yamlCfg, srcRoot, dstRoot)
				_ = SaveMigrationConfig(configPath, yamlCfg)
			}

			// Treat as fresh start instead of resume
			runtime, runErr = runFreshMigration()
		} else {
			// Root nodes exist - safe to resume
			if yamlCfg != nil && yamlCfg.State.Status == "suspended" {
				fmt.Println("Resuming suspended migration from database state...")
			} else {
				fmt.Println("Resuming migration from existing database state...")
			}
			runtime, runErr = RunMigration(MigrationConfig{
				BoltDB:          boltDB,
				BoltPath:        boltPath,
				SrcAdapter:      cfg.Source.Adapter,
				DstAdapter:      cfg.Destination.Adapter,
				SrcRoot:         srcRoot,
				DstRoot:         dstRoot,
				SrcServiceName:  cfg.Source.Name,
				WorkerCount:     cfg.WorkerCount,
				MaxRetries:      cfg.MaxRetries,
				CoordinatorLead: cfg.CoordinatorLead,
				LogAddress:      cfg.LogAddress,
				LogLevel:        cfg.LogLevel,
				SkipListener:    cfg.SkipListener,
				StartupDelay:    cfg.StartupDelay,
				ProgressTick:    cfg.ProgressTick,
				ResumeStatus:    &status,
				ConfigPath:      configPath,
				YAMLConfig:      yamlCfg,
				ShutdownContext: shutdownCtx,
			})
		}

	default:
		// Completed (or failed-only) migration with no pending work. For now we
		// do not re-run traversal automatically; verification below will report
		// success or failure based on the existing DB contents.
		fmt.Println("Existing migration detected with no pending work; skipping traversal.")
	}

	result.Runtime = runtime

	fmt.Printf("[DEBUG] RunMigration returned - runtime duration: %v, error: %v\n", runtime.Duration, runErr)

	// Check if migration was suspended by force shutdown
	isShutdown := runErr != nil && runErr.Error() == "migration suspended by force shutdown"
	if isShutdown {
		// Force shutdown occurred: flush logs
		// Use timeout to prevent hanging
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()

		// BoltDB doesn't need checkpointing - data is already persisted

		// Flush log service if available (with timeout)
		if logservice.LS != nil {
			flushDone := make(chan struct{}, 1)
			go func() {
				logservice.LS.Close()
				flushDone <- struct{}{}
			}()
			select {
			case <-flushDone:
				// Log flush completed
			case <-cleanupCtx.Done():
				fmt.Printf("⚠️  Log service flush timeout - skipping\n")
			}
		}

		// Close Spectra adapters to ensure proper cleanup (only close once if they share the same SDK instance)
		closeSpectraAdapters(cfg.Source.Adapter, cfg.Destination.Adapter, cleanupCtx)

		// Don't run verification on shutdown - just return with suspended state
		fmt.Println("\nMigration suspended by force shutdown. State saved for resumption.")
		return result, nil
	}

	// Verify migration before closing database - verification needs database access
	// Note: We run verification even if migration had errors, to provide full diagnostic info
	// Wrap in timeout to prevent hanging if database operations block
	fmt.Printf("[DEBUG] Starting verification (with 10s timeout)\n")
	verifyDone := make(chan struct {
		report VerificationReport
		err    error
	}, 1)
	go func() {
		fmt.Printf("[DEBUG] VerifyMigration goroutine started\n")
		report, err := VerifyMigration(boltDB, cfg.Verification)
		fmt.Printf("[DEBUG] VerifyMigration returned - error: %v\n", err)
		verifyDone <- struct {
			report VerificationReport
			err    error
		}{report, err}
	}()

	var report VerificationReport
	var verifyErr error
	select {
	case result := <-verifyDone:
		fmt.Printf("[DEBUG] Verification completed\n")
		report = result.report
		verifyErr = result.err
	case <-time.After(10 * time.Second):
		// Timeout - return empty report with error
		fmt.Printf("[DEBUG] Verification timeout after 10 seconds\n")
		verifyErr = fmt.Errorf("verification timeout after 10 seconds")
		report = VerificationReport{}
		fmt.Printf("⚠️  Verification timeout - skipping verification checks\n")
	}

	result.Verification = report
	fmt.Printf("[DEBUG] Verification report assigned to result\n")

	// Note: We do NOT close the log service here - it's a global singleton that should
	// be managed at the application level (e.g., in the API that calls LetsMigrate).
	// Closing it here would cause issues if:
	// 1. Multiple migrations run in sequence
	// 2. The API also tries to close it
	// 3. Verification or other code needs to log after migration completes

	// Close database after verification and log service are done (allows verification and log flushing to access DB)
	fmt.Printf("[DEBUG] Checking if database should be closed (CloseDatabase=%v)\n", cfg.CloseDatabase)
	if cfg.CloseDatabase {
		fmt.Printf("[DEBUG] Setting defer for boltDB.Close()\n")
		defer boltDB.Close()
	}

	// Return migration error if it occurred (verification ran for diagnostics)
	if runErr != nil {
		fmt.Printf("[DEBUG] Returning with runErr: %v\n", runErr)
		return result, runErr
	}

	if verifyErr != nil {
		fmt.Printf("[DEBUG] Returning with verifyErr: %v\n", verifyErr)
		return result, verifyErr
	}

	fmt.Printf("[DEBUG] Checking verification success\n")
	if !result.Verification.Success(cfg.Verification) {
		// Build detailed error message showing what failed
		report := result.Verification
		errMsg := "migration failed: verification checks failed\n"
		errMsg += fmt.Sprintf("  SRC: Total=%d Pending=%d Failed=%d\n",
			report.SrcTotal, report.SrcPending, report.SrcFailed)
		errMsg += fmt.Sprintf("  DST: Total=%d Pending=%d Failed=%d NotOnSrc=%d\n",
			report.DstTotal, report.DstPending, report.DstFailed, report.DstNotOnSrc)
		errMsg += fmt.Sprintf("  Moved Nodes: %d\n", report.NumMovedNodes)

		// Show which checks failed
		if !cfg.Verification.AllowPending && (report.SrcPending > 0 || report.DstPending > 0) {
			errMsg += "  ❌ Pending nodes remain (not allowed)\n"
		}
		if !cfg.Verification.AllowFailed && (report.SrcFailed > 0 || report.DstFailed > 0) {
			errMsg += "  ❌ Failed nodes detected (not allowed)\n"
		}
		if !cfg.Verification.AllowNotOnSrc && report.DstNotOnSrc > 0 {
			errMsg += "  ❌ Nodes found on DST but not on SRC (not allowed)\n"
		}
		if report.NumMovedNodes == 0 {
			errMsg += "  ❌ No nodes were actually migrated\n"
		}

		fmt.Printf("[DEBUG] Verification failed, returning error\n")
		return result, errors.New(errMsg)
	}

	fmt.Printf("[DEBUG] Migration completed successfully, returning result\n")
	return result, nil
}

func normalizeRootFolder(folder fsservices.Folder) (fsservices.Folder, error) {
	if folder.Id == "" {
		return fsservices.Folder{}, fmt.Errorf("folder ID cannot be empty")
	}

	if folder.DisplayName == "" {
		folder.DisplayName = folder.Id
	}
	if folder.LocationPath == "" {
		folder.LocationPath = "/"
	}
	if folder.Type == "" {
		folder.Type = fsservices.NodeTypeFolder
	}

	return folder, nil
}

// validateRootNodesExist checks if the root nodes still exist in the filesystem adapters.
// This is critical for resumption - if nodes don't exist (e.g., Spectra DB was reset),
// resuming would cause "node not found" errors because the migration DB has stale node IDs.
func validateRootNodesExist(srcAdapter, dstAdapter fsservices.FSAdapter, srcRoot, dstRoot fsservices.Folder) error {
	// Try to list children of the source root - if it doesn't exist, this will fail
	_, err := srcAdapter.ListChildren(srcRoot.Id)
	if err != nil {
		return fmt.Errorf("source root node '%s' does not exist in filesystem: %w", srcRoot.Id, err)
	}

	// Try to list children of the destination root - if it doesn't exist, this will fail
	_, err = dstAdapter.ListChildren(dstRoot.Id)
	if err != nil {
		return fmt.Errorf("destination root node '%s' does not exist in filesystem: %w", dstRoot.Id, err)
	}

	return nil
}
