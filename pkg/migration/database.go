// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/queue/mode"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/queue/worker"
)

// DatabaseConfig defines how the migration engine opens a per-migration store for a run or test.
type DatabaseConfig struct {
	// Path is the logical migration path (e.g. {migrationDir}/{id}.db); ops live in {id}.ops/.
	Path string
	// EncryptionKey is unused for Badger ops (kept for API compatibility).
	EncryptionKey []byte
	// RemoveExisting deletes the logical *.db marker, ops dir, and logs sidecar before open.
	RemoveExisting bool
	// RequireOpen determines whether the DB instance must already be open (true) or can be auto-opened (false).
	RequireOpen bool
}

// MigrationDirAndIDFromDBPath splits a migration DB file path into its folder and migration id.
func MigrationDirAndIDFromDBPath(dbPath string) (migrationDir, migrationID string) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		abs = dbPath
	}
	return filepath.Dir(abs), strings.TrimSuffix(filepath.Base(abs), ".db")
}

// MigrationDBPath returns the per-migration logical DB path under the migration folder.
func MigrationDBPath(migrationDir, migrationID string) string {
	return filepath.Join(migrationDir, migrationID+".db")
}

// MigrationStorageExists reports whether a migration has on-disk state (*.db marker, *.ops, or *.logs).
func MigrationStorageExists(migrationDir, migrationID string) bool {
	absDir, err := filepath.Abs(migrationDir)
	if err != nil {
		absDir = migrationDir
	}
	dbPath := MigrationDBPath(absDir, migrationID)
	if info, err := os.Stat(dbPath); err == nil && !info.IsDir() {
		return true
	}
	opsPath := db.MigrationOpsPath(dbPath)
	if info, err := os.Stat(opsPath); err == nil && info.IsDir() {
		return true
	}
	logsPath := db.MigrationLogsPath(dbPath)
	if info, err := os.Stat(logsPath); err == nil && info.IsDir() {
		return true
	}
	return false
}

// SetupDatabase opens the Badger ops store at cfg.Path.
func SetupDatabase(cfg DatabaseConfig) (*db.DB, bool, error) {
	if cfg.Path == "" {
		return nil, false, fmt.Errorf("database path cannot be empty")
	}

	wasFresh := false
	if cfg.RemoveExisting {
		if err := os.Remove(cfg.Path); err != nil && !os.IsNotExist(err) {
			return nil, false, fmt.Errorf("failed to remove database file %s: %w", cfg.Path, err)
		}
		opsDir := db.MigrationOpsPath(cfg.Path)
		if err := os.RemoveAll(opsDir); err != nil {
			return nil, false, fmt.Errorf("failed to remove ops dir %s: %w", opsDir, err)
		}
		logsDir := db.MigrationLogsPath(cfg.Path)
		if err := os.RemoveAll(logsDir); err != nil {
			return nil, false, fmt.Errorf("failed to remove logs dir %s: %w", logsDir, err)
		}
		wasFresh = true
	} else {
		opsDir := db.MigrationOpsPath(cfg.Path)
		if _, err := os.Stat(opsDir); os.IsNotExist(err) {
			wasFresh = true
		}
	}

	opts := db.DefaultOptions()
	opts.Path = cfg.Path
	opts.OpsDir = db.MigrationOpsPath(cfg.Path)
	opts.LogsDir = db.MigrationLogsPath(cfg.Path)
	opts.EncryptionKey = cfg.EncryptionKey
	opts.SealBuffer = &db.SealBufferOptions{}
	database, err := db.Open(opts)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open database %s: %w", cfg.Path, err)
	}
	return database, wasFresh, nil
}
