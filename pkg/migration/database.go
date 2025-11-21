// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

// DatabaseConfig defines how the migration engine should prepare its backing store.
type DatabaseConfig struct {
	// Path is the SQLite database file to create/open.
	Path string
	// RemoveExisting deletes the file if it already exists before creating a new database.
	RemoveExisting bool
	// ConfigPath is the path to the migration config YAML file. If empty, defaults to {Path}.yaml
	ConfigPath string
}

// SetupDatabase removes any existing database (when configured), creates a fresh SQLite
// database, and registers the tables required by the migration engine.
// Returns the database and a boolean indicating whether this is a fresh database (true = fresh).
func SetupDatabase(cfg DatabaseConfig) (*db.DB, bool, error) {
	if cfg.Path == "" {
		return nil, false, fmt.Errorf("database path cannot be empty")
	}

	wasFresh := false
	if cfg.RemoveExisting {
		if err := os.Remove(cfg.Path); err != nil && !os.IsNotExist(err) {
			return nil, false, fmt.Errorf("failed to remove existing database %s: %w", cfg.Path, err)
		}
		wasFresh = true // We removed it, so it's definitely fresh
	} else {
		// Check if database file exists - if not, it's a fresh database
		if _, err := os.Stat(cfg.Path); os.IsNotExist(err) {
			wasFresh = true // File doesn't exist, so it's fresh
		}
	}

	database, err := db.NewDB(cfg.Path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create database %s: %w", cfg.Path, err)
	}

	tables := []db.TableDef{
		db.SrcNodesTable{},
		db.DstNodesTable{},
		db.LogsTable{},
	}

	for _, tbl := range tables {
		if err := database.RegisterTable(tbl); err != nil {
			database.Close()
			return nil, false, fmt.Errorf("failed to register table %s: %w", tbl.Name(), err)
		}
	}

	return database, wasFresh, nil
}
