// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

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
	fmt.Println("=== Unexclusion Sweep Test Runner ===")
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
	// This DB should already have some excluded nodes from a previous exclusion sweep
	dbPath := "pkg/tests/traversal/shared/main_test.db"
	boltDB, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: false, // Use existing pre-configured DB
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

	// Cache total node count from stats bucket (O(1) lookup)
	nodesBucketPath := []string{"Traversal-Data", "SRC", "nodes"}
	totalNodesBefore, err := boltDB.GetBucketCount(nodesBucketPath)
	if err != nil {
		// Fallback to slow count if stats unavailable
		totalNodesBeforeInt, err2 := boltDB.CountNodes("SRC")
		if err2 != nil {
			return fmt.Errorf("failed to count total nodes: %w (stats error: %v)", err2, err)
		}
		totalNodesBefore = int64(totalNodesBeforeInt)
	}

	excludedNodesBefore, err := shared.CountExcludedNodes(boltDB, "SRC")
	if err != nil {
		return fmt.Errorf("failed to count excluded nodes: %w", err)
	}
	fmt.Printf("Total nodes (from stats): %d\n", totalNodesBefore)
	fmt.Printf("Excluded nodes before unexclusion: %d\n", excludedNodesBefore)

	if excludedNodesBefore == 0 {
		return fmt.Errorf("no excluded nodes found in database - test requires a pre-configured DB with excluded nodes")
	}

	fmt.Println("\n📋 Phase 2: Select and Mark Node as Unexcluded")
	fmt.Println("===============================================")

	// Get root path (should be "/")
	rootPath := "/"

	// Pick the first excluded top-level child (deterministic for testing)
	selectedChild, err := shared.PickFirstExcludedTopLevelChild(boltDB, "SRC", rootPath)
	if err != nil {
		return fmt.Errorf("failed to pick excluded child: %w", err)
	}

	fmt.Printf("Selected node: %s (depth: %d, type: %s)\n", selectedChild.Path, selectedChild.Depth, selectedChild.Type)

	// Count subtree that will be unexcluded
	subtreeStats, err := shared.CountSubtree(boltDB, "SRC", selectedChild.Path)
	if err != nil {
		return fmt.Errorf("failed to count subtree: %w", err)
	}
	fmt.Printf("Subtree to unexclude: %d nodes (%d folders, %d files), max depth: %d\n",
		subtreeStats.TotalNodes, subtreeStats.TotalFolders, subtreeStats.TotalFiles, subtreeStats.MaxDepth)

	// Mark the selected node as unexcluded
	fmt.Printf("Marking node as unexcluded...\n")
	if err := shared.MarkNodeAsUnexcluded(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to mark node as unexcluded: %w", err)
	}

	// Add the selected node to unexclusion-holding bucket (simulating what the API would do)
	if err := addToUnexclusionHolding(boltDB, "SRC", selectedChild.Path, selectedChild.Depth); err != nil {
		return fmt.Errorf("failed to add to unexclusion-holding bucket: %w", err)
	}

	// Get children and add them to unexclusion-holding bucket
	// They will keep their inherited_excluded status until the sweep processes them
	childIDs, err := db.GetChildrenIDsByParentID(boltDB, "SRC", selectedChild.ID)
	if err != nil {
		return fmt.Errorf("failed to get children: %w", err)
	}

	if len(childIDs) > 0 {
		fmt.Printf("Adding %d children to unexclusion-holding bucket...\n", len(childIDs))
		for _, childID := range childIDs {
			childState, err := db.GetNodeState(boltDB, "SRC", childID)
			if err != nil {
				continue
			}
			if err := addToUnexclusionHolding(boltDB, "SRC", childState.Path, childState.Depth); err != nil {
				return fmt.Errorf("failed to add child %s to unexclusion-holding bucket: %w", childState.Path, err)
			}
		}
	}

	fmt.Println("\n🚀 Phase 3: Run Unexclusion Sweep")
	fmt.Println("===================================")

	// Run unexclusion sweep
	sweepConfig := migration.SweepConfig{
		BoltDB:          boltDB,
		SrcAdapter:      srcAdapter,
		DstAdapter:      dstAdapter,
		WorkerCount:     10,
		MaxRetries:      3,
		LogAddress:      "127.0.0.1:8084",
		LogLevel:        "debug",
		SkipListener:    true,
		StartupDelay:    3 * time.Second,
		ProgressTick:    2 * time.Second,
		ShutdownContext: context.Background(),
	}

	stats, err := migration.RunExclusionSweep(sweepConfig)
	if err != nil {
		return fmt.Errorf("unexclusion sweep failed: %w", err)
	}

	fmt.Printf("Unexclusion sweep completed in %v\n", stats.Duration)
	fmt.Printf("  SRC: Round=%d Pending=%d InProgress=%d TotalTracked=%d\n",
		stats.Src.Round, stats.Src.Pending, stats.Src.InProgress, stats.Src.TotalTracked)
	fmt.Printf("  DST: Round=%d Pending=%d InProgress=%d TotalTracked=%d\n",
		stats.Dst.Round, stats.Dst.Pending, stats.Dst.InProgress, stats.Dst.TotalTracked)

	fmt.Println("\n✓ Phase 4: Verification")
	fmt.Println("========================")

	// Count excluded nodes after unexclusion sweep
	excludedNodesAfter, err := shared.CountExcludedNodes(boltDB, "SRC")
	if err != nil {
		return fmt.Errorf("failed to count excluded nodes after sweep: %w", err)
	}
	fmt.Printf("Excluded nodes after unexclusion sweep: %d\n", excludedNodesAfter)

	// Expected excluded count = original excluded - subtree size (all nodes in subtree should be unexcluded)
	expectedExcluded := excludedNodesBefore - subtreeStats.TotalNodes
	if excludedNodesAfter != expectedExcluded {
		return fmt.Errorf("excluded count mismatch: expected %d (original %d - subtree %d), got %d",
			expectedExcluded, excludedNodesBefore, subtreeStats.TotalNodes, excludedNodesAfter)
	}

	// Verify that the selected node and all its descendants are no longer excluded
	excludedInSubtree, err := shared.CountExcludedInSubtree(boltDB, "SRC", selectedChild.Path)
	if err != nil {
		return fmt.Errorf("failed to count excluded nodes in subtree: %w", err)
	}

	if excludedInSubtree != 0 {
		return fmt.Errorf("subtree unexclusion mismatch: expected 0 excluded nodes in subtree, got %d",
			excludedInSubtree)
	}

	fmt.Printf("✅ Excluded count matches expected: %d (original %d - unexcluded subtree %d)\n",
		expectedExcluded, excludedNodesBefore, subtreeStats.TotalNodes)
	fmt.Printf("✅ All %d nodes in unexcluded subtree are no longer excluded\n", subtreeStats.TotalNodes)

	return nil
}

// addToUnexclusionHolding adds a node to the unexclusion-holding bucket.
func addToUnexclusionHolding(boltDB *db.DB, queueType string, nodePath string, depth int) error {
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
		bucket, err := db.GetOrCreateHoldingBucket(tx, queueType, "unexclude")
		if err != nil {
			return fmt.Errorf("failed to get unexclusion-holding bucket: %w", err)
		}

		return bucket.Put([]byte(nodeID), depthBytes)
	})
}
