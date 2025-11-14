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
}

// SetupDatabase removes any existing database (when configured), creates a fresh SQLite
// database, and registers the tables required by the migration engine.
func SetupDatabase(cfg DatabaseConfig) (*db.DB, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("database path cannot be empty")
	}

	if cfg.RemoveExisting {
		if err := os.Remove(cfg.Path); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to remove existing database %s: %w", cfg.Path, err)
		}
	}

	database, err := db.NewDB(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create database %s: %w", cfg.Path, err)
	}

	tables := []db.TableDef{
		db.SrcNodesTable{},
		db.DstNodesTable{},
		db.LogsTable{},
	}

	for _, tbl := range tables {
		if err := database.RegisterTable(tbl); err != nil {
			database.Close()
			return nil, fmt.Errorf("failed to register table %s: %w", tbl.Name(), err)
		}
	}

	return database, nil
}
