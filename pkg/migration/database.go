// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"os"

	"github.com/Project-Sylos/Sylos-DB/pkg/store"
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

// SetupDatabase is a helper function for the API/caller to open a store.Store instance.
// The Migration Engine does NOT call this - it requires the Store to be passed in via Config.StoreInstance.
// This function is provided for convenience when the API needs to open the database.
// Returns the store.Store and a boolean indicating whether this is a fresh database (true = fresh).
// The caller is responsible for closing the Store when done.
func SetupDatabase(cfg DatabaseConfig) (*store.Store, bool, error) {
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

	// Open Store (which internally opens bolt.DB)
	storeInstance, err := store.Open(cfg.Path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open store %s: %w", cfg.Path, err)
	}

	return storeInstance, wasFresh, nil
}
