// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"os"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// DatabaseConfig defines how the migration engine should prepare its backing store.
type DatabaseConfig struct {
	// Path is the DuckDB file path to create/open (e.g. migration.duckdb).
	Path string
	// RemoveExisting deletes the database file if it already exists before creating a new database.
	RemoveExisting bool
	// RequireOpen determines whether the DB instance must already be open (true) or can be auto-opened (false).
	// When true (API mode): DB instance must be provided and already open, error if nil/closed.
	// When false (standalone mode): Can auto-open DB if instance is nil or not open.
	RequireOpen bool
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
	opts.SealBuffer = &db.SealBufferOptions{} // async seal with default interval/threshold
	database, err := db.Open(opts)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open database %s: %w", cfg.Path, err)
	}

	// Build node and status event indexes up front so traversal/copy queries and joins can use them immediately.
	if err := db.EnsureNodeTableIndexes(database, "src_nodes"); err != nil {
		_ = database.Close()
		return nil, false, fmt.Errorf("failed to ensure src node indexes: %w", err)
	}
	if err := db.EnsureNodeTableIndexes(database, "dst_nodes"); err != nil {
		_ = database.Close()
		return nil, false, fmt.Errorf("failed to ensure dst node indexes: %w", err)
	}
	if err := db.EnsureStatusEventTableIndexes(database, "src_status_events"); err != nil {
		_ = database.Close()
		return nil, false, fmt.Errorf("failed to ensure src status event indexes: %w", err)
	}
	if err := db.EnsureStatusEventTableIndexes(database, "dst_status_events"); err != nil {
		_ = database.Close()
		return nil, false, fmt.Errorf("failed to ensure dst status event indexes: %w", err)
	}
	return database, wasFresh, nil
}
