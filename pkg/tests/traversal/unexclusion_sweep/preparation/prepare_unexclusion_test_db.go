// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// This is a helper script to prepare the base DB for unexclusion sweep test.
// It marks the first root folder as excluded, runs an exclusion sweep,
// and saves the result as the base DB for the unexclusion test.

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/traversal/shared"
	"github.com/Project-Sylos/Sylos-FS/pkg/fs"
)

func main() {
	fmt.Println("=== Preparing Unexclusion Test Base DB ===")
	fmt.Println()

	if err := prepareBaseDB(); err != nil {
		fmt.Printf("\n❌ FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ Base DB prepared successfully!")
	fmt.Println("You can now use this DB as unexclusion_sweep_test_base.db")
}

func prepareBaseDB() error {
	fmt.Println("📋 Step 1: Load base DB (from exclusion test)")
	fmt.Println("=============================================")

	// Start with the exclusion test base DB (which has no excluded nodes)
	sourceDBPath := "pkg/tests/unexclusion_sweep/preparation/excluded.db"
	sourceYAMLPath := "pkg/tests/unexclusion_sweep/preparation/excluded.yaml"
	sourceSpectraPath := "pkg/tests/unexclusion_sweep/preparation/excluded_spectra.db"

	targetDBPath := "pkg/tests/traversal/shared/main_test.db"
	targetYAMLPath := "pkg/tests/traversal/shared/main_test.yaml"
	targetSpectraPath := "pkg/tests/traversal/shared/spectra_test.db"

	// Check if source exists
	if _, err := os.Stat(sourceDBPath); os.IsNotExist(err) {
		return fmt.Errorf("source DB not found: %s - please ensure exclusion test base files exist", sourceDBPath)
	}

	// Copy source files to target location
	filesToCopy := map[string]string{
		sourceDBPath:      targetDBPath,
		sourceYAMLPath:    targetYAMLPath,
		sourceSpectraPath: targetSpectraPath,
	}

	for src, dst := range filesToCopy {
		if _, err := os.Stat(src); os.IsNotExist(err) {
			// YAML and Spectra are optional, but DB is required
			if src == sourceDBPath {
				return fmt.Errorf("source file not found: %s", src)
			}
			continue
		}

		sourceData, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("failed to read source file %s: %w", src, err)
		}

		if err := os.WriteFile(dst, sourceData, 0644); err != nil {
			return fmt.Errorf("failed to write target file %s: %w", dst, err)
		}

		fmt.Printf("Copied: %s -> %s\n", src, dst)
	}

	// Open the target DB
	boltDB, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           targetDBPath,
		RemoveExisting: false,
	})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer boltDB.Close()

	// Load Spectra configuration (test-specific config pointing to shared directory's spectra.db)
	spectraFS, err := shared.SetupSpectraFS("pkg/tests/traversal/shared/spectra.json", false)
	if err != nil {
		return fmt.Errorf("failed to setup Spectra: %w", err)
	}

	srcRoot, dstRoot, err := shared.LoadSpectraRoots(spectraFS)
	if err != nil {
		return fmt.Errorf("failed to load Spectra roots: %w", err)
	}

	srcAdapter, err := fs.NewSpectraFS(spectraFS, srcRoot.ServiceID, "primary")
	if err != nil {
		return fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewSpectraFS(spectraFS, dstRoot.ServiceID, "s1")
	if err != nil {
		return fmt.Errorf("failed to create dst adapter: %w", err)
	}

	fmt.Println("\n📋 Step 2: Mark First Root Folder as Excluded")
	fmt.Println("==============================================")

	// Get root path
	rootPath := "/"

	// Get first top-level child folder
	firstChild, err := shared.PickRandomTopLevelChild(boltDB, "SRC", rootPath)
	if err != nil {
		return fmt.Errorf("failed to get first top-level child: %w", err)
	}

	fmt.Printf("Selected first root folder: %s (depth: %d)\n", firstChild.Path, firstChild.Depth)

	// Mark as excluded
	if err := shared.MarkNodeAsExcluded(boltDB, "SRC", firstChild.Path); err != nil {
		return fmt.Errorf("failed to mark node as excluded: %w", err)
	}

	// Add to exclusion-holding bucket
	if err := addToExclusionHolding(boltDB, "SRC", firstChild.Path, firstChild.Depth); err != nil {
		return fmt.Errorf("failed to add to exclusion-holding bucket: %w", err)
	}

	fmt.Println("Marked as excluded and added to exclusion-holding bucket")

	fmt.Println("\n📋 Step 3: Run Exclusion Sweep")
	fmt.Println("===============================")

	// Run exclusion sweep
	sweepConfig := migration.SweepConfig{
		BoltDB:          boltDB,
		SrcAdapter:      srcAdapter,
		DstAdapter:      dstAdapter,
		WorkerCount:     10,
		MaxRetries:      3,
		LogAddress:      "127.0.0.1:8085",
		LogLevel:        "info",
		SkipListener:    true,
		StartupDelay:    2 * time.Second,
		ProgressTick:    2 * time.Second,
		ShutdownContext: context.Background(),
	}

	stats, err := migration.RunExclusionSweep(sweepConfig)
	if err != nil {
		return fmt.Errorf("exclusion sweep failed: %w", err)
	}

	fmt.Printf("Exclusion sweep completed in %v\n", stats.Duration)
	fmt.Printf("  SRC: Round=%d Pending=%d InProgress=%d TotalTracked=%d\n",
		stats.Src.Round, stats.Src.Pending, stats.Src.InProgress, stats.Src.TotalTracked)

	// Count excluded nodes
	excludedCount, err := shared.CountExcludedNodes(boltDB, "SRC")
	if err != nil {
		return fmt.Errorf("failed to count excluded nodes: %w", err)
	}

	fmt.Printf("\n✅ Base DB prepared with %d excluded nodes\n", excludedCount)
	fmt.Printf("✅ Saved to: %s\n", targetDBPath)

	return nil
}

// addToExclusionHolding adds a node to the exclusion-holding bucket.
func addToExclusionHolding(boltDB *db.DB, queueType string, nodePath string, depth int) error {
	// Find node by path to get ULID
	var nodeID string
	err := boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		cursor := nodesBucket.Cursor()
		for nodeIDBytes, nodeData := cursor.First(); nodeIDBytes != nil; nodeIDBytes, nodeData = cursor.Next() {
			nodeState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue
			}
			if nodeState.Path == nodePath {
				nodeID = nodeState.ID
				return nil
			}
		}
		return fmt.Errorf("node not found: %s", nodePath)
	})
	if err != nil {
		return err
	}

	depthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(depthBytes, uint64(depth))

	return boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := db.GetOrCreateHoldingBucket(tx, queueType, "exclude")
		if err != nil {
			return fmt.Errorf("failed to get exclusion-holding bucket: %w", err)
		}

		return bucket.Put([]byte(nodeID), depthBytes)
	})
}
