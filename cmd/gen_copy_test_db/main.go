// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// gen_copy_test_db creates a DuckDB at pkg/tests/copy/shared/main_test.db
// with schema and root nodes only (no Spectra). Run from repository root.
// Run a test traversal to populate it, then use that DB for copy-phase tests.
package main

import (
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func main() {
	fmt.Println("Generating copy test DB (DuckDB) at pkg/tests/copy/shared/main_test.db ...")
	fmt.Println("Schema + root nodes only (no Spectra). Run traversal to populate, then use for copy tests.")
	fmt.Println()

	path := "pkg/tests/copy/shared/main_test.db"
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Remove existing: %v\n", err)
		os.Exit(1)
	}

	database, err := db.Open(db.Options{Path: path})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	now := time.Now().Format(time.RFC3339)
	srcRoot := types.Folder{
		ServiceID:    "test-src-root",
		ParentId:     "",
		DisplayName:  "Source Root",
		LocationPath: "/",
		LastUpdated:  now,
		ParentPath:   "",
		Type:         types.NodeTypeFolder,
	}
	dstRoot := types.Folder{
		ServiceID:    "test-dst-root",
		ParentId:     "",
		DisplayName:  "Destination Root",
		LocationPath: "/",
		LastUpdated:  now,
		ParentPath:   "",
		Type:         types.NodeTypeFolder,
	}

	if _, err := migration.SeedRootTasks(srcRoot, dstRoot, database); err != nil {
		fmt.Fprintf(os.Stderr, "Seed roots: %v\n", err)
		os.Exit(1)
	}
	if err := db.BootstrapRootStats(database); err != nil {
		fmt.Fprintf(os.Stderr, "Bootstrap stats: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Done. pkg/tests/copy/shared/main_test.db has schema and root nodes.")
}
