// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/shared"
	"github.com/Project-Sylos/Spectra/sdk"
	"github.com/Project-Sylos/Sylos-FS/pkg/fs"
)

func main() {
	fmt.Println("=== Retry Sweep Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")

	// Load pre-configured test database (should be copied by PowerShell script)
	// Path is relative to where the script is run from (project root)
	dbPath := "pkg/tests/retry_sweep/retry_sweep_test.db"
	boltDB, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: false, // Use existing pre-configured DB
	})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer boltDB.Close()

	// Load Spectra configuration (test-specific config pointing to test directory's spectra.db)
	spectraFS, err := shared.SetupSpectraFS("pkg/tests/retry_sweep/spectra.json", false)
	if err != nil {
		return fmt.Errorf("failed to setup Spectra: %w", err)
	}

	srcRoot, dstRoot, err := shared.LoadSpectraRoots(spectraFS)
	if err != nil {
		return fmt.Errorf("failed to load Spectra roots: %w", err)
	}

	srcAdapter, err := fs.NewSpectraFS(spectraFS, srcRoot.Id, "primary")
	if err != nil {
		return fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fs.NewSpectraFS(spectraFS, dstRoot.Id, "s1")
	if err != nil {
		return fmt.Errorf("failed to create dst adapter: %w", err)
	}

	// Count nodes in Spectra DB before test
	spectraNodeCount, err := countSpectraNodes(spectraFS)
	if err != nil {
		return fmt.Errorf("failed to count Spectra nodes: %w", err)
	}
	fmt.Printf("Spectra DB node count: %d\n", spectraNodeCount)

	// Count nodes in BoltDB BEFORE any mutations (baseline check)
	boltNodeCountInitial, err := boltDB.CountNodes("SRC")
	if err != nil {
		return fmt.Errorf("failed to count initial BoltDB SRC nodes: %w", err)
	}
	fmt.Printf("BoltDB SRC node count (initial): %d\n", boltNodeCountInitial)

	// Count DST nodes too for comparison
	boltNodeCountDST, err := boltDB.CountNodes("DST")
	if err != nil {
		return fmt.Errorf("failed to count initial BoltDB DST nodes: %w", err)
	}
	fmt.Printf("BoltDB DST node count (initial): %d\n", boltNodeCountDST)

	// Check if counts match
	if boltNodeCountInitial != spectraNodeCount {
		fmt.Printf("⚠️  WARNING: Initial SRC count (%d) does not match Spectra count (%d) - difference: %d\n",
			boltNodeCountInitial, spectraNodeCount, boltNodeCountInitial-spectraNodeCount)
	}

	// Get root path (should be "/")
	rootPath := "/"

	fmt.Println("\n📋 Phase 2: Select and Prepare Test Node")
	fmt.Println("==========================================")

	// Pick a random top-level child
	selectedChild, err := shared.PickRandomTopLevelChild(boltDB, "SRC", rootPath)
	if err != nil {
		return fmt.Errorf("failed to pick random child: %w", err)
	}

	fmt.Printf("Selected node: %s (depth: %d, type: %s)\n", selectedChild.Path, selectedChild.Depth, selectedChild.Type)

	// Count subtree before deletion
	subtreeStats, err := shared.CountSubtree(boltDB, "SRC", selectedChild.Path)
	if err != nil {
		return fmt.Errorf("failed to count subtree: %w", err)
	}
	fmt.Printf("Subtree stats: %d nodes (%d folders, %d files), max depth: %d\n",
		subtreeStats.TotalNodes, subtreeStats.TotalFolders, subtreeStats.TotalFiles, subtreeStats.MaxDepth)

	// Mark the selected node as failed
	fmt.Printf("Marking node as failed...\n")
	if err := shared.MarkNodeAsFailed(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to mark node as failed: %w", err)
	}

	// Delete all children of the selected node from BoltDB
	fmt.Printf("Deleting subtree from BoltDB (keeping Spectra DB intact)...\n")
	if err := shared.DeleteSubtree(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to delete subtree: %w", err)
	}

	// Count nodes in BoltDB after deletion
	boltNodeCountBefore, err := boltDB.CountNodes("SRC")
	if err != nil {
		return fmt.Errorf("failed to count BoltDB nodes: %w", err)
	}
	fmt.Printf("BoltDB node count after deletion: %d\n", boltNodeCountBefore)

	fmt.Println("\n🚀 Phase 3: Run Retry Sweep")
	fmt.Println("============================")

	// Run retry sweep
	sweepConfig := migration.SweepConfig{
		BoltDB:          boltDB,
		SrcAdapter:      srcAdapter,
		DstAdapter:      dstAdapter,
		WorkerCount:     10,
		MaxRetries:      3,
		LogAddress:      "127.0.0.1:8082",
		LogLevel:        "info",
		SkipListener:    true,
		StartupDelay:    3 * time.Second,
		ProgressTick:    2 * time.Second,
		MaxKnownDepth:   -1, // Auto-detect
		ShutdownContext: context.Background(),
	}

	stats, err := migration.RunRetrySweep(sweepConfig)
	if err != nil {
		return fmt.Errorf("retry sweep failed: %w", err)
	}

	fmt.Printf("Retry sweep completed in %v\n", stats.Duration)
	fmt.Printf("  SRC: Round=%d Pending=%d InProgress=%d TotalTracked=%d\n",
		stats.Src.Round, stats.Src.Pending, stats.Src.InProgress, stats.Src.TotalTracked)
	fmt.Printf("  DST: Round=%d Pending=%d InProgress=%d TotalTracked=%d\n",
		stats.Dst.Round, stats.Dst.Pending, stats.Dst.InProgress, stats.Dst.TotalTracked)

	fmt.Println("\n✓ Phase 4: Verification")
	fmt.Println("========================")

	// Count nodes in BoltDB after retry sweep
	boltNodeCountAfter, err := boltDB.CountNodes("SRC")
	if err != nil {
		return fmt.Errorf("failed to count BoltDB nodes after sweep: %w", err)
	}
	fmt.Printf("BoltDB node count after retry sweep: %d\n", boltNodeCountAfter)

	// Verify that we found all nodes again
	expectedCount := spectraNodeCount
	if boltNodeCountAfter != expectedCount {
		return fmt.Errorf("node count mismatch: expected %d (from Spectra), got %d (in BoltDB)",
			expectedCount, boltNodeCountAfter)
	}

	fmt.Printf("✅ Node count matches Spectra DB: %d nodes\n", expectedCount)

	// Verify no pending nodes remain
	pendingCount, err := shared.CountPendingNodes(boltDB, "SRC")
	if err != nil {
		return fmt.Errorf("failed to count pending nodes: %w", err)
	}

	if pendingCount > 0 {
		fmt.Printf("⚠️  Warning: %d pending nodes remain (this may be expected if some nodes failed)\n", pendingCount)
	} else {
		fmt.Println("✅ No pending nodes remaining")
	}

	return nil
}

// countSpectraNodes counts all nodes in the Spectra database.
// This is a simple recursive traversal.
func countSpectraNodes(spectraFS *sdk.SpectraFS) (int, error) {
	count := 0
	visited := make(map[string]bool)

	var dfs func(nodeID string) error
	dfs = func(nodeID string) error {
		if visited[nodeID] {
			return nil
		}
		visited[nodeID] = true
		count++

		// Get node
		node, err := spectraFS.GetNode(&sdk.GetNodeRequest{ID: nodeID})
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", nodeID, err)
		}

		// If folder, recurse into children
		if node.Type == "folder" {
			children, err := spectraFS.ListChildren(&sdk.ListChildrenRequest{ParentID: nodeID})
			if err != nil {
				return fmt.Errorf("failed to list children of %s: %w", nodeID, err)
			}

			// Process folders
			for _, folder := range children.Folders {
				if err := dfs(folder.ID); err != nil {
					return err
				}
			}

			// Process files
			for _, file := range children.Files {
				if err := dfs(file.ID); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Start from root
	if err := dfs("root"); err != nil {
		return 0, err
	}

	return count, nil
}
