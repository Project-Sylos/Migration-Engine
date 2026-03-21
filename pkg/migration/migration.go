// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// Service defines a single filesystem service participating in a migration.
type Service struct {
	Name    string
	Adapter types.FSAdapter
	Root    types.Folder
}

// Config aggregates all of the knobs required to run the migration engine once.
type Config struct {
	// Database config (path, etc.). MigrationManager opens and owns this connection lifecycle.
	Database DatabaseConfig

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
	manager        *MigrationManager
	migration      *Migration
}

// Shutdown triggers a force shutdown of the migration.
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
func (c *Config) SetRootFolders(src, dst types.Folder) error {
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
		cfgCopy := cfg
		cfgCopy.ShutdownContext = shutdownCtx
		result, err := LetsMigrate(cfgCopy)
		controller.result = &result
		controller.err = err
	}()

	return controller
}

// LetsMigrate executes setup, traversal, and verification using the supplied configuration.
// This is the synchronous version - it blocks until the migration completes or is shutdown.
// For programmatic shutdown control, use StartMigration instead.
func LetsMigrate(cfg Config) (Result, error) {
	if cfg.ShutdownContext == nil {
		shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
		defer shutdownCancel()
		go HandleShutdownSignals(shutdownCancel)
		cfg.ShutdownContext = shutdownCtx
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

	manager, err := NewMigrationManager(cfg.Database)
	if err != nil {
		return Result{}, err
	}
	defer manager.Close()

	result := Result{RootsSeeded: cfg.SeedRoots}
	migrationInstance, err := manager.CreateMigration(CreateMigrationConfig{
		Name: migrationNameFromConfig(cfg),
		ServiceMetadata: map[string]string{
			"source_name":      cfg.Source.Name,
			"destination_name": cfg.Destination.Name,
		},
		RootConfig: map[string]string{
			"source_root_id":      srcRoot.ServiceID,
			"destination_root_id": dstRoot.ServiceID,
		},
	})
	if err != nil {
		return Result{}, err
	}

	if cfg.SeedRoots {
		summary, err := SeedRootTasks(srcRoot, dstRoot, manager.db)
		if err != nil {
			return Result{}, fmt.Errorf("seed roots: %w", err)
		}
		result.RootSummary = summary
	}

	runtime, runErr := migrationInstance.StartTraversal(cfg)
	result.Runtime = runtime

	// Check if migration was suspended by force shutdown
	isShutdown := runErr != nil && runErr.Error() == "migration suspended by force shutdown"
	if isShutdown {
		// Force shutdown occurred: flush logs
		// Use timeout to prevent hanging
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()

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

		// Don't run verification on shutdown - just return with suspended state
		// Note: Adapters are caller-owned and should not be closed by the engine
		fmt.Println("\nMigration suspended by force shutdown. State saved for resumption.")
		return result, nil
	}

	// Verify migration before closing database - verification needs database access
	// Note: We run verification even if migration had errors, to provide full diagnostic info
	// Wrap in timeout to prevent hanging if database operations block
	verifyDone := make(chan struct {
		report VerificationReport
		err    error
	}, 1)
	go func() {
		report, err := VerifyMigration(manager.db, cfg.Verification)
		verifyDone <- struct {
			report VerificationReport
			err    error
		}{report, err}
	}()

	var report VerificationReport
	var verifyErr error
	select {
	case result := <-verifyDone:
		fmt.Printf("[] Verification completed\n")
		report = result.report
		verifyErr = result.err
	case <-time.After(10 * time.Second):
		// Timeout - return empty report with error
		verifyErr = fmt.Errorf("verification timeout after 10 seconds")
		report = VerificationReport{}
		fmt.Printf("⚠️  Verification timeout - skipping verification checks\n")
	}

	result.Verification = report

	// Note: We do NOT close the log service here - it's a global singleton that should
	// be managed at the application level (e.g., in the API that calls LetsMigrate).
	// Closing it here would cause issues if:
	// 1. Multiple migrations run in sequence
	// 2. The API also tries to close it
	// 3. Verification or other code needs to log after migration completes

	// Return migration error if it occurred (verification ran for diagnostics)
	if runErr != nil {
		return result, runErr
	}

	if verifyErr != nil {
		return result, verifyErr
	}

	if !result.Verification.Success(cfg.Verification) {
		// Build detailed error message showing what failed
		report := result.Verification
		errMsg := "migration failed: verification checks failed\n"
		errMsg += fmt.Sprintf("  SRC: Total=%d Pending=%d Successful=%d Failed=%d\n",
			report.SrcTotal, report.SrcPending, report.SrcSuccessful, report.SrcFailed)
		errMsg += fmt.Sprintf("  DST: Total=%d Pending=%d Successful=%d Failed=%d NotOnSrc=%d\n",
			report.DstTotal, report.DstPending, report.DstSuccessful, report.DstFailed, report.DstNotOnSrc)

		// Show which checks failed
		if !cfg.Verification.AllowPending && (report.SrcPending > 0 || report.DstPending > 0) {
			errMsg += "  ❌ Pending nodes remain (not allowed)\n"
		}
		if !cfg.Verification.AllowNotOnSrc && report.DstNotOnSrc > 0 {
			errMsg += "  ❌ Nodes found on DST but not on SRC (not allowed)\n"
		}

		return result, errors.New(errMsg)
	}

	return result, nil
}

func normalizeRootFolder(folder types.Folder) (types.Folder, error) {
	if folder.ServiceID == "" {
		return types.Folder{}, fmt.Errorf("folder ServiceID cannot be empty")
	}

	if folder.DisplayName == "" {
		folder.DisplayName = folder.ServiceID
	}
	// The actual folder path doesn't matter - the root node is always stored at "/".
	// Child paths will be relative to this root (e.g., "/child", "/child/grandchild").
	folder.LocationPath = "/"
	if folder.Type == "" {
		folder.Type = types.NodeTypeFolder
	}

	return folder, nil
}
