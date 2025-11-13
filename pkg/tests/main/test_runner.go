// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Spectra/sdk"
)

func main() {
	fmt.Println("=== Spectra Migration Test Runner ===")
	fmt.Println()

	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	// 1. Setup phase
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	database, spectraFS, err := setupTest()
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	defer database.Close()
	fmt.Println()

	// 2. Migration phase
	fmt.Println("🚀 Phase 2: Migration")
	fmt.Println("=====================")
	if err := runMigration(database, spectraFS); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	fmt.Println()

	// 3. Verification phase
	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")
	if err := verifyMigration(database); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	return nil
}

func runMigration(database *db.DB, spectraFS *sdk.SpectraFS) error {
	// Start logging service
	fmt.Println("Starting logging service...")
	if err := logservice.StartListener("127.0.0.1:8081"); err != nil {
		fmt.Printf("Warning: failed to start listener: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	// Initialize global logger
	if err := logservice.InitGlobalLogger(database, "127.0.0.1:8081", "trace"); err != nil {
		return fmt.Errorf("failed to initialize global logger: %w", err)
	}
	fmt.Println("✓ Log service started")
	fmt.Println()

	// Create filesystem adapters
	fmt.Println("Creating filesystem adapters...")
	// Source uses "primary" world, destination uses "s1" world
	// Both use the same root ID "root" but filter by different worlds
	srcAdapter, err := fsservices.NewSpectraFS(spectraFS, "root", "primary")
	if err != nil {
		return fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fsservices.NewSpectraFS(spectraFS, "root", "s1")
	if err != nil {
		return fmt.Errorf("failed to create dst adapter: %w", err)
	}

	srcContext := fsservices.NewServiceContext("Spectra-Src", srcAdapter)
	fmt.Println("✓ Adapters ready")
	fmt.Println()

	// Create shared coordinator for queue coordination
	fmt.Println("Creating coordinator...")
	maxLead := 4 // Coordinator maxLead determines how far ahead src can get
	coordinator := queue.NewQueueCoordinator(maxLead)
	fmt.Println("✓ Coordinator initialized")

	numWorkers := 10

	// Create and initialize queues - workers start automatically
	fmt.Println("Creating source queue...")
	srcQueue := queue.NewQueue("src", 3, numWorkers, coordinator)
	srcQueue.Initialize(database, "src_nodes", srcAdapter, nil, nil)
	fmt.Println("✓ Source queue initialized with ", numWorkers, " workers")

	fmt.Println("Creating destination queue...")
	dstQueue := queue.NewQueue("dst", 3, numWorkers, coordinator)
	// dst needs src context to query src_nodes for expected children
	dstQueue.Initialize(database, "dst_nodes", dstAdapter, srcContext, srcQueue)
	fmt.Println("✓ Destination queue initialized with ", numWorkers, " workers")

	// Seed SRC root task initially (database seeding already done in setup.go)
	fmt.Println("Seeding SRC root task into queue...")
	if err := seedSrcRootTask(spectraFS, srcQueue); err != nil {
		return fmt.Errorf("failed to seed SRC root task: %w", err)
	}
	fmt.Println("✓ SRC root task seeded")

	// Wait for SRC to complete round 0, then seed DST root with expected children
	fmt.Println("Waiting for SRC round 0 to complete...")
	for {
		if srcQueue.IsExhausted() {
			return fmt.Errorf("SRC queue exhausted before completing round 0")
		}
		srcStats := srcQueue.Stats()
		if srcStats.Round > 0 {
			// SRC has advanced to round 1, meaning round 0 is complete
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Get SRC root task from successful queue to extract expected children
	fmt.Println("Seeding DST root task with expected children from SRC...")
	if err := seedDstRootTask(spectraFS, srcQueue, dstQueue); err != nil {
		return fmt.Errorf("failed to seed DST root task: %w", err)
	}
	fmt.Println("✓ DST root task seeded")
	fmt.Println()

	// Wait for migration to complete (queues manage themselves)
	fmt.Println("Migration in progress (check log window for details)...")
	fmt.Println("Waiting for completion...")

	for {
		// Check if migration is complete
		if srcQueue.IsExhausted() && dstQueue.IsExhausted() {
			fmt.Println("\nMigration complete!")
			return nil
		}

		// Print progress
		srcStats := srcQueue.Stats()
		dstStats := dstQueue.Stats()
		fmt.Printf("\r  Src: Round %d (P:%d IP:%d W:%d) | Dst: Round %d (P:%d IP:%d W:%d)   ",
			srcStats.Round, srcStats.Pending, srcStats.InProgress, srcStats.Workers,
			dstStats.Round, dstStats.Pending, dstStats.InProgress, dstStats.Workers)

		time.Sleep(500 * time.Millisecond)
	}
}

// seedSrcRootTask injects SRC root task directly into queue round 0.
// Note: Database seeding already happened in setup.go, so we only inject into queues.
func seedSrcRootTask(spectraFS *sdk.SpectraFS, srcQueue *queue.Queue) error {
	// Get root node from Spectra
	srcRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fmt.Errorf("failed to get src root from Spectra: %w", err)
	}

	// Create folder struct
	srcFolder := fsservices.Folder{
		Id:           srcRoot.ID,
		ParentId:     "",
		DisplayName:  srcRoot.Name,
		LocationPath: "/",
		ParentPath:   "",
		LastUpdated:  srcRoot.LastUpdated.Format(time.RFC3339),
		DepthLevel:   0,
		Type:         fsservices.NodeTypeFolder,
	}

	// Inject task directly into queue round 0
	srcTask := &queue.TaskBase{
		Type:   queue.TaskTypeSrcTraversal,
		Folder: srcFolder,
		Round:  0,
	}

	srcQueue.Add(srcTask)
	return nil
}

// seedDstRootTask injects DST root task into queue round 0 with expected children from SRC's completed root task.
// Note: Database seeding already happened in setup.go, so we only inject into queues.
func seedDstRootTask(spectraFS *sdk.SpectraFS, srcQueue *queue.Queue, dstQueue *queue.Queue) error {
	// Get root node from Spectra
	dstRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fmt.Errorf("failed to get dst root from Spectra: %w", err)
	}

	// Create folder struct
	dstFolder := fsservices.Folder{
		Id:           dstRoot.ID,
		ParentId:     "",
		DisplayName:  dstRoot.Name,
		LocationPath: "/",
		ParentPath:   "",
		LastUpdated:  dstRoot.LastUpdated.Format(time.RFC3339),
		DepthLevel:   0,
		Type:         fsservices.NodeTypeFolder,
	}

	// Get SRC root task from successful queue (round 0)
	srcTask := srcQueue.GetSuccessfulTask(0, "/")
	if srcTask == nil {
		return fmt.Errorf("SRC root task not found in successful queue - cannot seed DST with expected children")
	}

	// Extract expected children from SRC task's discovered children
	var expectedFolders []fsservices.Folder
	var expectedFiles []fsservices.File
	for _, srcChild := range srcTask.DiscoveredChildren {
		if srcChild.IsFile {
			expectedFiles = append(expectedFiles, srcChild.File)
		} else {
			expectedFolders = append(expectedFolders, srcChild.Folder)
		}
	}

	// Inject task directly into queue round 0 with expected children
	dstTask := &queue.TaskBase{
		Type:            queue.TaskTypeDstTraversal,
		Folder:          dstFolder,
		ExpectedFolders: expectedFolders,
		ExpectedFiles:   expectedFiles,
		Round:           0,
	}

	dstQueue.Add(dstTask)
	return nil
}
