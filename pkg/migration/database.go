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

// DatabaseConfig defines how the migration engine should prepare its backing store.
type DatabaseConfig struct {
	// Path is the BoltDB file path to create/open.
	Path string
	// RemoveExisting deletes the database file if it already exists before creating a new database.
	RemoveExisting bool
	// ConfigPath is the path to the migration config YAML file. If empty, defaults to {Path}.yaml
	ConfigPath string
	// RequireOpen determines whether the DB instance must already be open (true) or can be auto-opened (false).
	// When true (API mode): DB instance must be provided and already open, error if nil/closed.
	// When false (standalone mode): Can auto-open DB if instance is nil or not open.
	// Defaults to false (standalone mode) for backward compatibility.
	RequireOpen bool
}

// logDBPathFromMain derives the dedicated log DB path from the main DB path (e.g. main_test.db -> main_test_logs.db).
func logDBPathFromMain(mainPath string) string {
	mainPath = strings.TrimSpace(mainPath)
	if mainPath == "" {
		return ""
	}
	dir := filepath.Dir(mainPath)
	base := filepath.Base(mainPath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if name == "" {
		return ""
	}
	return filepath.Join(dir, name+"_logs.db")
}

// removeBoltDatabase removes a BoltDB file and its dedicated log DB file (if present).
func removeBoltDatabase(path string) error {
	if path == "" {
		return nil
	}

	// Remove the main database file
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove database file %s: %w", path, err)
	}

	// Remove the dedicated log DB file (e.g. main_test_logs.db)
	logPath := logDBPathFromMain(path)
	if logPath != "" {
		_ = os.Remove(logPath) // Ignore errors - log file might not exist
	}

	return nil
}

// SetupDatabase is a helper function for the API/caller to open a BoltDB instance.
// The Migration Engine does NOT call this - it requires the DB to be passed in via Config.DatabaseInstance.
// This function is provided for convenience when the API needs to open the database.
// Returns the database and a boolean indicating whether this is a fresh database (true = fresh).
// The caller is responsible for closing the database when done.
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
