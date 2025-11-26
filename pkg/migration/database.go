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
	// Dir is the BadgerDB directory to create/open.
	Path string
	// RemoveExisting deletes the directory if it already exists before creating a new database.
	RemoveExisting bool
	// ConfigPath is the path to the migration config YAML file. If empty, defaults to {Dir}.yaml
	ConfigPath string
}

// SetupDatabase removes any existing database (when configured), creates a fresh BadgerDB
// instance, and prepares it for use by the migration engine.
// Returns the database and a boolean indicating whether this is a fresh database (true = fresh).
func SetupDatabase(cfg DatabaseConfig) (*db.DB, bool, error) {
	if cfg.Path == "" {
		return nil, false, fmt.Errorf("database directory cannot be empty")
	}

	wasFresh := false
	if cfg.RemoveExisting {
		if err := os.RemoveAll(cfg.Path); err != nil && !os.IsNotExist(err) {
			return nil, false, fmt.Errorf("failed to remove existing database directory %s: %w", cfg.Path, err)
		}
		wasFresh = true // We removed it, so it's definitely fresh
	} else {
		// Check if database directory exists - if not, it's a fresh database
		if _, err := os.Stat(cfg.Path); os.IsNotExist(err) {
			wasFresh = true // Directory doesn't exist, so it's fresh
		}
	}

	badgerOpts := db.DefaultOptions()
	badgerOpts.Path = cfg.Path

	database, err := db.Open(badgerOpts)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create database %s: %w", cfg.Path, err)
	}

	return database, wasFresh, nil
}
