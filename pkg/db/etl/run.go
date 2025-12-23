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
	// BoltPath is the path to the BoltDB file. Required if BoltDB is nil.
	BoltPath string
	// DuckDBPath is the path to the DuckDB file (will be created if it doesn't exist)
	DuckDBPath string
	// Overwrite determines whether to overwrite existing DuckDB file
	Overwrite bool
}

// RunBoltToDuck migrates data from BoltDB to DuckDB.
// You can either pass an existing BoltDB instance via BoltDB, or provide BoltPath to open one.
// The BoltDB instance will be closed automatically if it was opened by this function.
func RunBoltToDuck(cfg BoltToDuckConfig) error {
	// Validate configuration
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("DuckDBPath is required")
	}

	var boltDB *db.DB
	var openedBoltDB bool
	var err error

	// Use provided BoltDB instance or open one from path
	if cfg.BoltDB != nil {
		boltDB = cfg.BoltDB
	} else {
		if cfg.BoltPath == "" {
			return fmt.Errorf("either BoltDB or BoltPath must be provided")
		}
		// Open BoltDB
		boltOpts := db.DefaultOptions()
		boltOpts.Path = cfg.BoltPath
		boltDB, err = db.Open(boltOpts)
		if err != nil {
			return fmt.Errorf("failed to open BoltDB at %s: %w", cfg.BoltPath, err)
		}
		openedBoltDB = true
	}

	// Defer closing BoltDB if we opened it
	if openedBoltDB {
		defer func() {
			if closeErr := boltDB.Close(); closeErr != nil {
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

	// Perform migration
	return MigrateBoltToDuck(boltDB, cfg.DuckDBPath, cfg.Overwrite)
}

// DuckToBoltConfig configures the DuckDB to BoltDB migration
type DuckToBoltConfig struct {
	// DuckDBPath is the path to the DuckDB file
	DuckDBPath string
	// BoltDB is an existing BoltDB instance. If nil, BoltPath will be used to open one.
	BoltDB *db.DB
	// BoltPath is the path to the BoltDB file. Required if BoltDB is nil.
	BoltPath string
	// Overwrite determines whether to overwrite existing BoltDB file
	Overwrite bool
}

// RunDuckToBolt migrates data from DuckDB to BoltDB.
// This is a stub implementation for now.
func RunDuckToBolt(cfg DuckToBoltConfig) error {
	// TODO: Implement DuckDB to BoltDB migration
	return fmt.Errorf("DuckDB to BoltDB migration is not yet implemented")
}
