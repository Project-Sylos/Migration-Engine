// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package etl

import (
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

// BoltToDuckConfig configures the BoltDB to DuckDB migration
type BoltToDuckConfig struct {
	// BoltDB is an existing BoltDB instance. If nil, BoltPath will be used to open one.
	BoltDB *db.DB
	// BoltPath is the path to the BoltDB file. Required if BoltDB is nil and RequireOpen is false.
	BoltPath string
	// DuckDBPath is the path to the DuckDB file (will be created if it doesn't exist)
	DuckDBPath string
	// Overwrite determines whether to overwrite existing DuckDB file
	Overwrite bool
	// RequireOpen determines whether the DB instance must already be open (true) or can be auto-opened (false).
	// When true (API mode): DB instance must be provided and already open, error if nil/closed.
	// When false (standalone mode): Can auto-open DB if instance is nil or not open.
	// Defaults to false (standalone mode) for backward compatibility.
	RequireOpen bool
	// OnETLStart is an optional callback called when ETL starts (for status updates)
	OnETLStart func() error
	// OnETLComplete is an optional callback called when ETL completes successfully (for status updates)
	OnETLComplete func() error
}

// RunBoltToDuck migrates data from BoltDB to DuckDB.
// You can either pass an existing BoltDB instance via BoltDB, or provide BoltPath to open one.
// The BoltDB instance will be closed automatically if it was opened by this function (standalone mode).
func RunBoltToDuck(cfg BoltToDuckConfig) error {
	// Validate configuration
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("DuckDBPath is required")
	}

	// Ensure DB is open using manager
	dbManager, err := db.EnsureOpen(cfg.BoltDB, cfg.BoltPath, cfg.RequireOpen)
	if err != nil {
		return fmt.Errorf("failed to ensure database is open: %w", err)
	}

	boltDB := dbManager.GetDB()

	// In standalone mode (we opened it), defer closing
	if !cfg.RequireOpen {
		defer func() {
			if closeErr := db.CloseIfOwned(dbManager); closeErr != nil {
				fmt.Printf("Warning: failed to close BoltDB: %v\n", closeErr)
			}
		}()
	}

	// Handle overwrite flag
	if cfg.Overwrite {
		if _, err := os.Stat(cfg.DuckDBPath); err == nil {
			// File exists, remove it
			if err := os.Remove(cfg.DuckDBPath); err != nil {
				return fmt.Errorf("failed to remove existing DuckDB file: %w", err)
			}
		}
	}

	// Call status update callback when ETL starts
	if cfg.OnETLStart != nil {
		if err := cfg.OnETLStart(); err != nil {
			return fmt.Errorf("failed to update status on ETL start: %w", err)
		}
	}

	// Perform migration
	err = MigrateBoltToDuck(boltDB, cfg.DuckDBPath, cfg.Overwrite)
	if err != nil {
		return err
	}

	// Call status update callback when ETL completes successfully
	if cfg.OnETLComplete != nil {
		if err := cfg.OnETLComplete(); err != nil {
			return fmt.Errorf("failed to update status on ETL complete: %w", err)
		}
	}

	return nil
}

// DuckToBoltConfig configures the DuckDB to BoltDB migration
type DuckToBoltConfig struct {
	// DuckDBPath is the path to the DuckDB file
	DuckDBPath string
	// BoltDB is an existing BoltDB instance. If nil, BoltPath will be used to open one.
	BoltDB *db.DB
	// BoltPath is the path to the BoltDB file. Required if BoltDB is nil and RequireOpen is false.
	BoltPath string
	// Overwrite determines whether to overwrite existing BoltDB file
	Overwrite bool
	// RequireOpen determines whether the DB instance must already be open (true) or can be auto-opened (false).
	// When true (API mode): DB instance must be provided and already open, error if nil/closed.
	// When false (standalone mode): Can auto-open DB if instance is nil or not open.
	// Defaults to false (standalone mode) for backward compatibility.
	RequireOpen bool
	// OnETLStart is an optional callback called when ETL starts (for status updates)
	OnETLStart func() error
	// OnETLComplete is an optional callback called when ETL completes successfully (for status updates)
	OnETLComplete func() error
}

// RunDuckToBolt migrates data from DuckDB to BoltDB.
// You can either pass an existing BoltDB instance via BoltDB, or provide BoltPath to open one.
// The BoltDB instance will be closed automatically if it was opened by this function (standalone mode).
func RunDuckToBolt(cfg DuckToBoltConfig) error {
	// Validate configuration
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("DuckDBPath is required")
	}

	// Handle overwrite flag - delete existing BoltDB file if it exists
	if cfg.Overwrite {
		if cfg.BoltDB != nil {
			// If BoltDB instance is provided, we can't delete the file directly
			// The caller should handle this, but we'll proceed
		} else if cfg.BoltPath != "" {
			if _, err := os.Stat(cfg.BoltPath); err == nil {
				// File exists, remove it
				if err := os.Remove(cfg.BoltPath); err != nil {
					return fmt.Errorf("failed to remove existing BoltDB file: %w", err)
				}
			}
		}
	}

	// Ensure DB is open using manager
	dbManager, err := db.EnsureOpen(cfg.BoltDB, cfg.BoltPath, cfg.RequireOpen)
	if err != nil {
		return fmt.Errorf("failed to ensure database is open: %w", err)
	}

	boltDB := dbManager.GetDB()

	// In standalone mode (we opened it), defer closing
	if !cfg.RequireOpen {
		defer func() {
			if closeErr := db.CloseIfOwned(dbManager); closeErr != nil {
				fmt.Printf("Warning: failed to close BoltDB: %v\n", closeErr)
			}
		}()
	}

	// Call status update callback when ETL starts
	if cfg.OnETLStart != nil {
		if err := cfg.OnETLStart(); err != nil {
			return fmt.Errorf("failed to update status on ETL start: %w", err)
		}
	}

	// Perform migration
	err = MigrateDuckToBolt(boltDB, cfg.DuckDBPath)
	if err != nil {
		return err
	}

	// Call status update callback when ETL completes successfully
	if cfg.OnETLComplete != nil {
		if err := cfg.OnETLComplete(); err != nil {
			return fmt.Errorf("failed to update status on ETL complete: %w", err)
		}
	}

	return nil
}
