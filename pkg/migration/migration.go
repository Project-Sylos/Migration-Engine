// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
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
	DatabaseInstance *db.DB
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
}

// Result captures the outcome of a migration run.
type Result struct {
	RootsSeeded  bool
	RootSummary  RootSeedSummary
	Runtime      RuntimeStats
	Verification VerificationReport
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
	return nil
}

// LetsMigrate executes setup, traversal, and verification using the supplied configuration.
func LetsMigrate(cfg Config) (Result, error) {
	var (
		database *db.DB
		err      error
	)

	var wasFresh bool
	if cfg.DatabaseInstance != nil {
		database = cfg.DatabaseInstance
		wasFresh = false // Existing database instance - always check status and validate schema
	} else {
		var setupErr error
		database, wasFresh, setupErr = SetupDatabase(cfg.Database)
		if setupErr != nil {
			return Result{}, setupErr
		}
		cfg.CloseDatabase = true
		// Fresh database - no need to validate schema since we just registered the tables
	}

	// Validate core schema for any caller-supplied database instance. This is
	// intentionally strict to avoid resuming or running against a modified or
	// incompatible DB file. Databases created via SetupDatabase are assumed to
	// have the correct schema because we just registered the tables.
	// Skip validation for fresh databases since we just created them correctly.
	if cfg.DatabaseInstance != nil {
		if err := database.ValidateCoreSchema(); err != nil {
			return Result{}, fmt.Errorf("database schema invalid: %w", err)
		}
	}
	// Note: Fresh databases (wasFresh=true) don't need validation since we just registered the tables

	// Only inspect migration status if this isn't a fresh database.
	// If we just created it (RemoveExisting=true or file didn't exist), skip the check.
	var status MigrationStatus
	if wasFresh {
		// Fresh database - no need to check, it's definitely empty
		status = MigrationStatus{}
	} else {
		// Existing database - inspect to decide between fresh run and resume
		var inspectErr error
		status, inspectErr = InspectMigrationStatus(database)
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

	result := Result{RootsSeeded: cfg.SeedRoots}

	// Decide whether to run a fresh migration (seed + traversal) or resume from
	// an existing in-progress database.
	var runtime RuntimeStats
	var runErr error

	switch {
	case status.IsEmpty():
		// Fresh run: optionally seed roots, then run normal traversal.
		if cfg.SeedRoots {
			summary, err := SeedRootTasks(database, srcRoot, dstRoot)
			if err != nil {
				return Result{}, err
			}
			fmt.Printf("Root tasks seeded (src:%d dst:%d)\n", summary.SrcRoots, summary.DstRoots)
			result.RootSummary = summary
		}
		runtime, runErr = RunMigration(MigrationConfig{
			Database:        database,
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
		})

	case status.HasPending():
		// Resume from an in-progress migration. Root seeding is assumed to have
		// been done previously and is skipped here.
		fmt.Println("Resuming migration from existing database state...")
		runtime, runErr = RunMigration(MigrationConfig{
			Database:        database,
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
		})

	default:
		// Completed (or failed-only) migration with no pending work. For now we
		// do not re-run traversal automatically; verification below will report
		// success or failure based on the existing DB contents.
		fmt.Println("Existing migration detected with no pending work; skipping traversal.")
	}

	result.Runtime = runtime

	// Verify migration before closing database - verification needs database access
	// Note: We run verification even if migration had errors, to provide full diagnostic info
	report, verifyErr := VerifyMigration(database, cfg.Verification)
	result.Verification = report

	// Note: We do NOT close the log service here - it's a global singleton that should
	// be managed at the application level (e.g., in the API that calls LetsMigrate).
	// Closing it here would cause issues if:
	// 1. Multiple migrations run in sequence
	// 2. The API also tries to close it
	// 3. Verification or other code needs to log after migration completes

	// Close database after verification and log service are done (allows verification and log flushing to access DB)
	if cfg.CloseDatabase {
		defer database.Close()
	}

	// Return migration error if it occurred (verification ran for diagnostics)
	if runErr != nil {
		return result, runErr
	}

	if verifyErr != nil {
		return result, verifyErr
	}

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
