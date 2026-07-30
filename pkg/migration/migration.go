// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const persistedRunConfigVersion = 1

// persistedRunConfig is stored in migrations.root_config_json (JSON). It omits FS adapters and context.
type persistedRunConfig struct {
	V                 int            `json:"v"`
	DatabasePath      string         `json:"databasePath,omitempty"`
	SourceName        string         `json:"sourceName"`
	SourceRoot        types.Folder   `json:"sourceRoot"`
	DestinationName   string         `json:"destinationName"`
	DestinationRoot   types.Folder   `json:"destinationRoot"`
	SourceProviderID  string         `json:"sourceProviderId,omitempty"`
	DestProviderID    string         `json:"destinationProviderId,omitempty"`
	PathCheckTarget   string         `json:"pathCheckTarget,omitempty"`
	WindowsCompat     bool           `json:"windowsCompat,omitempty"`
	SeedRoots         bool           `json:"seedRoots"`
	WorkerCount       int            `json:"workerCount"`
	MaxRetries        int            `json:"maxRetries"`
	CoordinatorLead   int            `json:"coordinatorLead"`
	LogAddress        string         `json:"logAddress"`
	LogLevel          string         `json:"logLevel"`
	SkipListener      bool           `json:"skipListener"`
	StartupDelayNanos int64          `json:"startupDelayNanos"`
	ProgressTickNanos int64          `json:"progressTickNanos"`
	Verification      VerifyOptions  `json:"verification"`
	RemoveExistingDB  bool           `json:"removeExistingDb,omitempty"`
	RequireOpenDB     bool           `json:"requireOpenDb,omitempty"`
}

func persistedRunConfigFrom(cfg Config) persistedRunConfig {
	return persistedRunConfig{
		V:                 persistedRunConfigVersion,
		DatabasePath:      cfg.Database.Path,
		RemoveExistingDB:  cfg.Database.RemoveExisting,
		RequireOpenDB:     cfg.Database.RequireOpen,
		SourceName:        cfg.Source.Name,
		SourceRoot:        cfg.Source.Root,
		DestinationName:   cfg.Destination.Name,
		DestinationRoot:   cfg.Destination.Root,
		SourceProviderID:  cfg.Source.ProviderID,
		DestProviderID:    cfg.Destination.ProviderID,
		PathCheckTarget:   cfg.PathCheckTarget,
		WindowsCompat:     cfg.WindowsCompat,
		SeedRoots:         cfg.SeedRoots,
		WorkerCount:       cfg.WorkerCount,
		MaxRetries:        cfg.MaxRetries,
		CoordinatorLead:   cfg.CoordinatorLead,
		LogAddress:        cfg.LogAddress,
		LogLevel:          cfg.LogLevel,
		SkipListener:      cfg.SkipListener,
		StartupDelayNanos: cfg.StartupDelay.Nanoseconds(),
		ProgressTickNanos: cfg.ProgressTick.Nanoseconds(),
		Verification:      cfg.Verification,
	}
}

// Service defines a single filesystem service participating in a migration.
type Service struct {
	Name           string
	Adapter        types.FSAdapter
	Root           types.Folder
	ProviderID     string // optional: "spectra", "local", "generic"
	BackendGroupID string // optional: shared rate-limit pool id
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

	// PathCheckTarget selects destination-name rules: "none", "auto", or a provider id
	// (e.g. "windows"). Empty is treated as "auto".
	PathCheckTarget string
	// WindowsCompat enables Windows desktop-sync overlays on soft cloud destinations
	// (Dropbox, Box, Egnyte, ShareFile). Default false.
	WindowsCompat bool

	// ShutdownContext is an optional context for force shutdown control.
	ShutdownContext context.Context

	// Autoscaler enables in-engine throughput tuning during traversal/copy.
	// Enabled by default; see AutoscalerConfig.Resolve and DisableAutoscaler.
	Autoscaler AutoscalerConfig

	// RootPreparation is set when the UI reviewed root children before Start discovery.
	RootPreparation RootPreparation
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

	manager := NewMigrationManager()
	defer manager.Close()

	createCfg := CreateMigrationConfig{
		Name: migrationNameFromConfig(cfg),
		ServiceMetadata: map[string]string{
			"source_name":      cfg.Source.Name,
			"destination_name": cfg.Destination.Name,
		},
		RootConfig: map[string]string{
			"source_root_id":      srcRoot.ServiceID,
			"destination_root_id": dstRoot.ServiceID,
		},
	}
	if cfg.Database.Path != "" {
		if cfg.Database.RemoveExisting {
			_ = os.Remove(cfg.Database.Path)
		}
		dir, id := MigrationDirAndIDFromDBPath(cfg.Database.Path)
		createCfg.MigrationDir = dir
		createCfg.MigrationID = id
	}
	migrationInstance, err := manager.CreateMigration(createCfg)
	if err != nil {
		return Result{}, err
	}

	result := Result{RootsSeeded: cfg.SeedRoots}
	if cfg.SeedRoots {
		summary, err := SeedRootTasks(srcRoot, dstRoot, migrationInstance.DB)
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
		report, err := VerifyMigration(migrationInstance.DB, cfg.Verification)
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
