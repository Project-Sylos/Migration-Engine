// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/queue"
	"github.com/Project-Sylos/Spectra/sdk"
)

// setupTest initializes database, creates tables, and seeds root tasks
func setupTest() (*db.DB, *sdk.SpectraFS, error) {
	// 1. Create database
	fmt.Println("Creating database...")

	// if the database file exists, remove it
	if _, err := os.Stat("internal/tests/migration_test.db"); err == nil {
		os.Remove("internal/tests/migration_test.db")
	}

	database, err := db.NewDB("internal/tests/migration_test.db")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create database: %w", err)
	}

	// 2. Register tables
	fmt.Println("Registering tables...")
	tables := []db.TableDef{
		db.SrcNodesTable{},
		db.DstNodesTable{},
		db.LogsTable{},
	}
	for _, tbl := range tables {
		if err := database.RegisterTable(tbl); err != nil {
			database.Close()
			return nil, nil, fmt.Errorf("failed to register table %s: %w", tbl.Name(), err)
		}
	}
	fmt.Println("✓ Tables registered")

	// 3. Initialize Spectra
	fmt.Println("Initializing Spectra filesystem...")

	// if the spectra db file exists, remove it
	if _, err := os.Stat("./spectra.db"); err == nil {
		os.Remove("./spectra.db")
	}

	spectraFS, err := sdk.New("internal/configs/spectra.json")
	if err != nil {
		database.Close()
		return nil, nil, fmt.Errorf("failed to create Spectra: %w", err)
	}
	fmt.Println("✓ Spectra initialized")

	// 4. Seed root tasks directly into database
	fmt.Println("Seeding root tasks...")
	if err := seedRootTasks(database, spectraFS); err != nil {
		database.Close()
		return nil, nil, fmt.Errorf("failed to seed roots: %w", err)
	}

	// 5. Verify seeds were inserted
	var srcCount, dstCount int
	rows, _ := database.Query("SELECT COUNT(*) FROM src_nodes WHERE depth_level = 0")
	if rows != nil && rows.Next() {
		rows.Scan(&srcCount)
		rows.Close()
	}
	rows, _ = database.Query("SELECT COUNT(*) FROM dst_nodes WHERE depth_level = 0")
	if rows != nil && rows.Next() {
		rows.Scan(&dstCount)
		rows.Close()
	}

	if srcCount == 0 || dstCount == 0 {
		database.Close()
		return nil, nil, fmt.Errorf("root tasks not inserted (src=%d, dst=%d)", srcCount, dstCount)
	}

	fmt.Printf("✓ Root tasks seeded and verified (src: %d, dst: %d)\n", srcCount, dstCount)

	return database, spectraFS, nil
}

// seedRootTasks inserts the initial root folders into the database
func seedRootTasks(database *db.DB, spectraFS *sdk.SpectraFS) error {
	// Get root nodes from Spectra using request structs
	srcRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fmt.Errorf("failed to get src root from Spectra: %w", err)
	}

	dstRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fmt.Errorf("failed to get dst root from Spectra: %w", err)
	}

	// Create folder structs
	srcFolder := fsservices.Folder{
		Id:           srcRoot.ID,
		ParentId:     "",
		DisplayName:  srcRoot.Name,
		LocationPath: "/",
		LastUpdated:  srcRoot.LastUpdated.Format(time.RFC3339),
		Type:         fsservices.NodeTypeFolder,
	}

	dstFolder := fsservices.Folder{
		Id:           dstRoot.ID,
		ParentId:     "",
		DisplayName:  dstRoot.Name,
		LocationPath: "/",
		LastUpdated:  dstRoot.LastUpdated.Format(time.RFC3339),
		Type:         fsservices.NodeTypeFolder,
	}

	// Insert into database
	return queue.SeedRootTasks(database, srcFolder, dstFolder)
}
