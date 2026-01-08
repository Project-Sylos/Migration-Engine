// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
)

// DBManager tracks a database instance and whether we opened it (for lifecycle management).
type DBManager struct {
	db     *DB
	opened bool // true if we opened it (standalone mode)
	path   string
}

// IsOpen checks if a DB instance is open by verifying the underlying bolt.DB is not nil.
func IsOpen(db *DB) bool {
	if db == nil {
		return false
	}
	return db.IsOpen()
}

// EnsureOpen ensures a database is open, opening it if needed based on the RequireOpen flag.
// Returns a DBManager that tracks ownership for lifecycle management.
//
// Parameters:
//   - db: The database instance (may be nil or closed)
//   - path: The database file path (required if RequireOpen is false and db is nil/closed)
//   - requireOpen: If true, DB must already be open (error if nil/closed). If false, can auto-open.
//
// Behavior:
//   - If db != nil and IsOpen(db): Use provided DB, return manager with opened = false
//   - If db == nil or not open:
//   - If requireOpen == true: Return error "DB instance is not open and RequireOpen is true"
//   - If requireOpen == false: Open DB using path, return manager with opened = true
func EnsureOpen(db *DB, path string, requireOpen bool) (*DBManager, error) {
	// Check if DB is already open
	if db != nil && IsOpen(db) {
		// Set RequireOpen flag on the provided DB instance
		db.SetRequireOpen(requireOpen)
		return &DBManager{
			db:     db,
			opened: false, // We didn't open it, caller owns it
			path:   db.Path(),
		}, nil
	}

	// DB is nil or not open - check if we're allowed to open it
	if requireOpen {
		return nil, fmt.Errorf("DB instance is not open and RequireOpen is true")
	}

	// Standalone mode: open the DB ourselves
	if path == "" {
		return nil, fmt.Errorf("cannot auto-open DB: Path is required when RequireOpen is false")
	}

	boltOpts := DefaultOptions()
	boltOpts.Path = path

	openedDB, err := Open(boltOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at %s: %w", path, err)
	}

	// Set RequireOpen flag on the DB instance
	openedDB.SetRequireOpen(requireOpen)

	return &DBManager{
		db:     openedDB,
		opened: true, // We opened it, we own it
		path:   path,
	}, nil
}

// CloseIfOwned closes the database only if we opened it (opened == true).
// This should be called in standalone mode to clean up resources.
func CloseIfOwned(manager *DBManager) error {
	if manager == nil {
		return nil
	}

	if !manager.opened {
		// We didn't open it, don't close it
		return nil
	}

	if manager.db == nil {
		return nil
	}

	return manager.db.Close()
}

// GetDB returns the managed database instance.
func (m *DBManager) GetDB() *DB {
	return m.db
}
