// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/shared"
	"github.com/Project-Sylos/Sylos-FS/pkg/fs"
)

func main() {
	fmt.Println("=== Exclusion Sweep Test Runner ===")
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
	fmt.Printf("Excluded nodes before exclusion: %d\n", excludedNodesBefore)

	fmt.Println("\n📋 Phase 2: Select and Mark Node as Excluded")
	fmt.Println("==============================================")

	// Get root path (should be "/")
	rootPath := "/"

	// Pick a random top-level child
	selectedChild, err := shared.PickRandomTopLevelChild(boltDB, "SRC", rootPath)
	if err != nil {
		return fmt.Errorf("failed to pick random child: %w", err)
	}

	fmt.Printf("Selected node: %s (depth: %d, type: %s)\n", selectedChild.Path, selectedChild.Depth, selectedChild.Type)

	// Count subtree that will be excluded
	subtreeStats, err := shared.CountSubtree(boltDB, "SRC", selectedChild.Path)
	if err != nil {
		return fmt.Errorf("failed to count subtree: %w", err)
	}
	fmt.Printf("Subtree to exclude: %d nodes (%d folders, %d files), max depth: %d\n",
		subtreeStats.TotalNodes, subtreeStats.TotalFolders, subtreeStats.TotalFiles, subtreeStats.MaxDepth)

	// Mark the selected node as excluded
	fmt.Printf("Marking node as excluded...\n")
	if err := shared.MarkNodeAsExcluded(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to mark node as excluded: %w", err)
	}

	// Add the node's CHILDREN to exclusion-holding bucket (simulating what the API would do)
	// The node itself is already explicitly excluded, so workers should process its children
	if err := addChildrenToExclusionHolding(boltDB, "SRC", selectedChild.Path); err != nil {
		return fmt.Errorf("failed to add children to exclusion-holding bucket: %w", err)
	}

	fmt.Println("\n🚀 Phase 3: Run Exclusion Sweep")
	fmt.Println("=================================")

	// Run exclusion sweep
	sweepConfig := migration.SweepConfig{
		BoltDB:          boltDB,
		SrcAdapter:      srcAdapter,
		DstAdapter:      dstAdapter,
		WorkerCount:     10,
		MaxRetries:      3,
		LogAddress:      "127.0.0.1:8083",
		LogLevel:        "debug",
		SkipListener:    false,
		StartupDelay:    3 * time.Second,
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
	fmt.Printf("  DST: Round=%d Pending=%d InProgress=%d TotalTracked=%d\n",
		stats.Dst.Round, stats.Dst.Pending, stats.Dst.InProgress, stats.Dst.TotalTracked)

	fmt.Println("\n✓ Phase 4: Verification")
	fmt.Println("========================")

	// Count excluded nodes after exclusion sweep
	excludedNodesAfter, err := shared.CountExcludedNodes(boltDB, "SRC")
	if err != nil {
		return fmt.Errorf("failed to count excluded nodes after sweep: %w", err)
	}
	fmt.Printf("Excluded nodes after exclusion sweep: %d\n", excludedNodesAfter)

	// Expected excluded count = original excluded + subtree size (all nodes in subtree should be excluded)
	expectedExcluded := excludedNodesBefore + subtreeStats.TotalNodes
	if excludedNodesAfter != expectedExcluded {
		return fmt.Errorf("excluded count mismatch: expected %d (original %d + subtree %d), got %d",
			expectedExcluded, excludedNodesBefore, subtreeStats.TotalNodes, excludedNodesAfter)
	}

	// Verify that the selected node and all its descendants are excluded (explicitly or inherited)
	excludedInSubtree, err := shared.CountExcludedInSubtree(boltDB, "SRC", selectedChild.Path)
	if err != nil {
		return fmt.Errorf("failed to count excluded nodes in subtree: %w", err)
	}

	if excludedInSubtree != subtreeStats.TotalNodes {
		return fmt.Errorf("subtree exclusion mismatch: expected %d excluded nodes in subtree, got %d",
			subtreeStats.TotalNodes, excludedInSubtree)
	}

	fmt.Printf("✅ Excluded count matches expected: %d (original %d + excluded subtree %d)\n",
		expectedExcluded, excludedNodesBefore, subtreeStats.TotalNodes)
	fmt.Printf("✅ All %d nodes in excluded subtree are excluded (explicitly or inherited)\n", excludedInSubtree)

	return nil
}

// addChildrenToExclusionHolding adds a node's children to the exclusion-holding bucket.
// This simulates what the API would do: when a node is marked as explicitly excluded,
// its children should be added to the exclusion-holding bucket for workers to process.
func addChildrenToExclusionHolding(boltDB *db.DB, queueType string, nodePath string) error {
	// Find node by path to get ULID and children
	var nodeID string
	var childIDs []string
	err := boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		// Find the node by path
		cursor := nodesBucket.Cursor()
		for nodeIDBytes, nodeData := cursor.First(); nodeIDBytes != nil; nodeIDBytes, nodeData = cursor.Next() {
			nodeState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue
			}
			if nodeState.Path == nodePath {
				nodeID = nodeState.ID
				break
			}
		}
		if nodeID == "" {
			return fmt.Errorf("node not found: %s", nodePath)
		}

		// Get children from children bucket
		childrenBucket := db.GetChildrenBucket(tx, queueType)
		if childrenBucket != nil {
			childrenData := childrenBucket.Get([]byte(nodeID))
			if childrenData != nil {
				if err := json.Unmarshal(childrenData, &childIDs); err != nil {
					return fmt.Errorf("failed to unmarshal children: %w", err)
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if len(childIDs) == 0 {
		// No children to add - this is fine, the node itself is already explicitly excluded
		return nil
	}

	// Get child depths and add to exclusion-holding bucket
	type childEntry struct {
		nodeID string
		depth  int
	}
	var entries []childEntry
	var missingChildren []string

	err = boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		for _, childID := range childIDs {
			nodeData := nodesBucket.Get([]byte(childID))
			if nodeData == nil {
				missingChildren = append(missingChildren, childID)
				continue // Child doesn't exist in nodes bucket
			}

			childState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid nodes
			}

			entries = append(entries, childEntry{
				nodeID: childID,
				depth:  childState.Depth,
			})
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Report data integrity issue if any children are missing
	if len(missingChildren) > 0 {
		fmt.Printf("⚠️  WARNING: Children bucket has %d stale references (nodes don't exist):\n", len(missingChildren))
		for i, childID := range missingChildren {
			if i < 5 { // Only print first 5
				fmt.Printf("  - %s\n", childID)
			}
		}
		if len(missingChildren) > 5 {
			fmt.Printf("  ... and %d more\n", len(missingChildren)-5)
		}
		return fmt.Errorf("data integrity issue: children bucket has stale references - database may be corrupted")
	}

	fmt.Printf("Found %d existing children (out of %d in children bucket)\n", len(entries), len(childIDs))

	// Add all children to exclusion-holding bucket
	return boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := db.GetOrCreateExclusionHoldingBucket(tx, queueType)
		if err != nil {
			return fmt.Errorf("failed to get exclusion-holding bucket: %w", err)
		}

		for _, entry := range entries {
			depthBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(depthBytes, uint64(entry.depth))
			if err := bucket.Put([]byte(entry.nodeID), depthBytes); err != nil {
				return fmt.Errorf("failed to add child to exclusion-holding bucket: %w", err)
			}
		}

		return nil
	})
}
