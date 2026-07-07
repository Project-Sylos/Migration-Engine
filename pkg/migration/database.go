// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// DatabaseConfig defines how the migration engine opens a DuckDB file for a run or test.
type DatabaseConfig struct {
	// Path is the DuckDB file for this migration (e.g. {migrationDir}/{id}.db).
	Path string
	// EncryptionKey enables DuckDB native encryption; nil keeps plaintext (tests).
	EncryptionKey []byte
	// RemoveExisting deletes the database file if it already exists before creating a new database.
	RemoveExisting bool
	// RequireOpen determines whether the DB instance must already be open (true) or can be auto-opened (false).
	// When true (API mode): DB instance must be provided and already open, error if nil/closed.
	// When false (standalone mode): Can auto-open DB if instance is nil or not open.
	RequireOpen bool
}

// MigrationDirAndIDFromDBPath splits a migration DB file path into its folder and migration id.
// For /data/migration-1/migration-1.db returns (/data/migration-1, migration-1).
func MigrationDirAndIDFromDBPath(dbPath string) (migrationDir, migrationID string) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		abs = dbPath
	}
	return filepath.Dir(abs), strings.TrimSuffix(filepath.Base(abs), ".db")
}

// MigrationDBPath returns the per-migration DB path when the API passes the folder for that migration.
// migrationDir is the absolute path to the migration's folder (e.g. data/migration-123); the DB file is migrationDir/{migrationID}.db.
func MigrationDBPath(migrationDir, migrationID string) string {
	return filepath.Join(migrationDir, migrationID+".db")
}

// SetupDatabase opens a DuckDB database at cfg.Path. Returns the DB and whether it was fresh (true if new or removed).
// The caller is responsible for closing the database when done.
func SetupDatabase(cfg DatabaseConfig) (*db.DB, bool, error) {
	if cfg.Path == "" {
		return nil, false, fmt.Errorf("database path cannot be empty")
	}

	wasFresh := false
	if cfg.RemoveExisting {
		if err := os.Remove(cfg.Path); err != nil && !os.IsNotExist(err) {
			return nil, false, fmt.Errorf("failed to remove database file %s: %w", cfg.Path, err)
		}
		wasFresh = true
	} else {
		if _, err := os.Stat(cfg.Path); os.IsNotExist(err) {
			wasFresh = true
		}
	}

	opts := db.DefaultOptions()
	opts.Path = cfg.Path
	opts.EncryptionKey = cfg.EncryptionKey
	opts.SealBuffer = &db.SealBufferOptions{} // async seal with default interval/threshold
	database, err := db.Open(opts)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open database %s: %w", cfg.Path, err)
	}

	// Secondary indexes on node and status-event tables are not created here: BeginTraversalPhase /
	// BeginTraversalPhase drops them before bulk inserts, and EndTraversalPhase (or
	// EnsureBulkPhaseSecondaryIndexes after retry) recreate them. Creating them at open would be
	// redundant for new migrations and wasted work before the first phase.
	return database, wasFresh, nil
}
