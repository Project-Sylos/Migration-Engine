// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
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
	dbPath := "pkg/tests/shared/main_test.db"
	boltDB, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: false, // Use existing pre-configured DB
	})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer boltDB.Close()

	// Load Spectra configuration (test-specific config pointing to shared directory's spectra.db)
	spectraFS, err := shared.SetupSpectraFS("pkg/tests/shared/spectra.json", false)
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

	fmt.Printf("Selected SRC node: %s (depth: %d, type: %s)\n", selectedChild.Path, selectedChild.Depth, selectedChild.Type)

	// Find corresponding DST node using join-lookup table
	dstNodeID, err := db.GetDstIDFromSrcID(boltDB, selectedChild.ID)
	if err != nil {
		return fmt.Errorf("failed to get DST node ID from SRC node: %w", err)
	}
	if dstNodeID == "" {
		return fmt.Errorf("no corresponding DST node found for SRC node %s", selectedChild.ID)
	}

	// Get DST node state to get its path
	dstNodeState, err := db.GetNodeState(boltDB, "DST", dstNodeID)
	if err != nil {
		return fmt.Errorf("failed to get DST node state: %w", err)
	}
	if dstNodeState == nil {
		return fmt.Errorf("DST node not found: %s", dstNodeID)
	}

	fmt.Printf("Found corresponding DST node: %s (depth: %d, type: %s)\n", dstNodeState.Path, dstNodeState.Depth, dstNodeState.Type)

	// Count SRC subtree before deletion
	srcSubtreeStats, err := shared.CountSubtree(boltDB, "SRC", selectedChild.Path)
	if err != nil {
		return fmt.Errorf("failed to count SRC subtree: %w", err)
	}
	fmt.Printf("SRC subtree stats: %d nodes (%d folders, %d files), max depth: %d\n",
		srcSubtreeStats.TotalNodes, srcSubtreeStats.TotalFolders, srcSubtreeStats.TotalFiles, srcSubtreeStats.MaxDepth)

	// Count DST subtree before deletion
	dstSubtreeStats, err := shared.CountSubtree(boltDB, "DST", dstNodeState.Path)
	if err != nil {
		return fmt.Errorf("failed to count DST subtree: %w", err)
	}
	fmt.Printf("DST subtree stats: %d nodes (%d folders, %d files), max depth: %d\n",
		dstSubtreeStats.TotalNodes, dstSubtreeStats.TotalFolders, dstSubtreeStats.TotalFiles, dstSubtreeStats.MaxDepth)

	// Mark both nodes as pending
	fmt.Printf("Marking SRC node as pending...\n")
	if err := shared.MarkNodeAsPending(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to mark SRC node as pending: %w", err)
	}

	fmt.Printf("Marking DST node as pending...\n")
	if err := shared.MarkNodeAsPending(boltDB, "DST", dstNodeState.Path); err != nil {
		return fmt.Errorf("failed to mark DST node as pending: %w", err)
	}

	// Delete all children of the selected SRC node from BoltDB
	fmt.Printf("Deleting SRC subtree from BoltDB (keeping Spectra DB intact)...\n")
	if err := shared.DeleteSubtree(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to delete SRC subtree: %w", err)
	}

	// Delete all children of the corresponding DST node from BoltDB
	fmt.Printf("Deleting DST subtree from BoltDB (keeping Spectra DB intact)...\n")
	if err := shared.DeleteSubtree(boltDB, "DST", dstNodeState.Path); err != nil {
		return fmt.Errorf("failed to delete DST subtree: %w", err)
	}

	// Count nodes in BoltDB after deletion
	boltNodeCountSRCBefore, err := boltDB.CountNodes("SRC")
	if err != nil {
		return fmt.Errorf("failed to count BoltDB SRC nodes: %w", err)
	}
	fmt.Printf("BoltDB SRC node count after deletion: %d\n", boltNodeCountSRCBefore)

	boltNodeCountDSTBefore, err := boltDB.CountNodes("DST")
	if err != nil {
		return fmt.Errorf("failed to count BoltDB DST nodes: %w", err)
	}
	fmt.Printf("BoltDB DST node count after deletion: %d\n", boltNodeCountDSTBefore)

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
	boltNodeCountSRCAfter, err := boltDB.CountNodes("SRC")
	if err != nil {
		return fmt.Errorf("failed to count BoltDB SRC nodes after sweep: %w", err)
	}
	fmt.Printf("BoltDB SRC node count after retry sweep: %d\n", boltNodeCountSRCAfter)

	boltNodeCountDSTAfter, err := boltDB.CountNodes("DST")
	if err != nil {
		return fmt.Errorf("failed to count BoltDB DST nodes after sweep: %w", err)
	}
	fmt.Printf("BoltDB DST node count after retry sweep: %d\n", boltNodeCountDSTAfter)

	// Verify that we found all SRC nodes again
	expectedSRCCount := spectraNodeCount
	if boltNodeCountSRCAfter != expectedSRCCount {
		return fmt.Errorf("SRC node count mismatch: expected %d (from Spectra), got %d (in BoltDB)",
			expectedSRCCount, boltNodeCountSRCAfter)
	}

	fmt.Printf("✅ SRC node count matches Spectra DB: %d nodes\n", expectedSRCCount)

	// Verify that DST was also restored to original count
	expectedDSTCount := boltNodeCountDST // Original DST count before deletion
	if boltNodeCountDSTAfter != expectedDSTCount {
		return fmt.Errorf("DST node count mismatch: expected %d (original count), got %d (after retry sweep)",
			expectedDSTCount, boltNodeCountDSTAfter)
	}

	fmt.Printf("✅ DST node count matches original: %d nodes\n", expectedDSTCount)

	// Verify no pending nodes remain in SRC
	srcPendingCount, err := shared.CountPendingNodes(boltDB, "SRC")
	if err != nil {
		return fmt.Errorf("failed to count pending SRC nodes: %w", err)
	}

	if srcPendingCount > 0 {
		fmt.Printf("⚠️  Warning: %d pending SRC nodes remain (this may be expected if some nodes failed)\n", srcPendingCount)
	} else {
		fmt.Println("✅ No pending SRC nodes remaining")
	}

	// Verify no pending nodes remain in DST
	dstPendingCount, err := shared.CountPendingNodes(boltDB, "DST")
	if err != nil {
		return fmt.Errorf("failed to count pending DST nodes: %w", err)
	}

	if dstPendingCount > 0 {
		fmt.Printf("⚠️  Warning: %d pending DST nodes remain (this may be expected if some nodes failed)\n", dstPendingCount)
	} else {
		fmt.Println("✅ No pending DST nodes remaining")
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
