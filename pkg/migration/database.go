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
	// Path is the BoltDB file path to create/open.
	Path string
	// RemoveExisting deletes the database file if it already exists before creating a new database.
	RemoveExisting bool
	// ConfigPath is the path to the migration config YAML file. If empty, defaults to {Path}.yaml
	ConfigPath string
}

// removeBoltDatabase removes a BoltDB file.
func removeBoltDatabase(path string) error {
	if path == "" {
		return nil
	}

	// Remove the database file
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove database file %s: %w", path, err)
	}

	return nil
}

// SetupDatabase removes any existing database (when configured), creates a fresh BoltDB
// instance, and prepares it for use by the migration engine.
// Returns the database and a boolean indicating whether this is a fresh database (true = fresh).
func SetupDatabase(cfg DatabaseConfig) (*db.DB, bool, error) {
	if cfg.Path == "" {
		return nil, false, fmt.Errorf("database path cannot be empty")
	}

	wasFresh := false
	if cfg.RemoveExisting {
		if err := removeBoltDatabase(cfg.Path); err != nil {
			return nil, false, err
		}
		wasFresh = true // We removed it, so it's definitely fresh
	} else {
		// Check if database file exists - if not, it's a fresh database
		if _, err := os.Stat(cfg.Path); os.IsNotExist(err) {
			wasFresh = true // File doesn't exist, so it's fresh
		}
	}

	boltOpts := db.DefaultOptions()
	boltOpts.Path = cfg.Path

	database, err := db.Open(boltOpts)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create database %s: %w", cfg.Path, err)
	}

	return database, wasFresh, nil
}
