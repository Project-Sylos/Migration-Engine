// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Project-Sylos/Migration-Engine/pkg/db/etl"
)

func main() {
	fmt.Println("=== BoltDB to DuckDB ETL Tool ===")
	fmt.Println()

	// Get current working directory (should be workspace root when run via go run)
	wd, err := os.Getwd()
	if err != nil {
		fmt.Printf("ERROR: Failed to get working directory: %v\n", err)
		os.Exit(1)
	}

	// Define paths relative to workspace root
	boltPath := filepath.Join(wd, "pkg/tests/copy/shared/main_test.db")
	duckPath := filepath.Join(wd, "pkg/tests/copy/shared/main_test-duck.db")

	// Check if BoltDB file exists
	if _, err := os.Stat(boltPath); os.IsNotExist(err) {
		fmt.Printf("ERROR: BoltDB file not found: %s\n", boltPath)
		fmt.Println("Please run the copy test first to generate main_test.db")
		os.Exit(1)
	}

	fmt.Printf("Source BoltDB: %s\n", boltPath)
	fmt.Printf("Destination DuckDB: %s\n", duckPath)
	fmt.Println()

	// Check if DuckDB already exists
	if _, err := os.Stat(duckPath); err == nil {
		fmt.Printf("WARNING: DuckDB file already exists: %s\n", duckPath)
		fmt.Println("   It will be overwritten.")
		fmt.Println()
	}

	// Run ETL
	fmt.Println("Starting ETL migration...")
	err = etl.RunBoltToDuck(etl.BoltToDuckConfig{
		BoltPath:    boltPath,
		DuckDBPath:  duckPath,
		Overwrite:   true,
		RequireOpen: false, // Standalone mode - auto-open BoltDB
	})

	if err != nil {
		fmt.Printf("\nETL FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("ETL completed successfully!")
	fmt.Printf("DuckDB file ready for inspection: %s\n", duckPath)
	fmt.Println()
	fmt.Println("You can now open this file in DBeaver or another DuckDB client.")
}
