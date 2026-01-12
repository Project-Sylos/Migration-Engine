// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
)

// PrintCopyVerification prints the copy phase statistics in a formatted way.
func PrintCopyVerification(stats queue.QueueStats) {
	fmt.Printf("Copy phase statistics:\n")
	fmt.Printf("  Round: %d\n", stats.Round)
	fmt.Printf("  Pending: %d\n", stats.Pending)
	fmt.Printf("  In-Progress: %d\n", stats.InProgress)
	fmt.Printf("  Total Tracked: %d\n", stats.TotalTracked)
	fmt.Printf("  Workers: %d\n", stats.Workers)
	fmt.Println()
}

// VerifyCopyCompletion verifies that all copy status buckets are in expected state.
// Uses stats bucket for O(1) lookups instead of O(N) scans.
// Checks that no pending copy tasks remain and reports successful/failed counts.
func VerifyCopyCompletion(boltDB *db.DB) error {
	fmt.Println("Verifying copy completion...")

	// Get all levels
	levels, err := boltDB.GetAllLevels("SRC")
	if err != nil {
		return fmt.Errorf("failed to get levels: %w", err)
	}

	totalPending := int64(0)
	totalSuccessful := int64(0)
	totalFailed := int64(0)
	totalSkipped := int64(0)
	totalInProgress := int64(0)

	// Count buckets directly (like inspect tool) to ensure accuracy
	// CountCopyStatusBucket tries stats first, then falls back to actual counting
	// This ensures we get accurate counts even if stats are stale
	for _, level := range levels {
		// Skip round 0 (root is not copied)
		if level == 0 {
			continue
		}

		// Count both folder and file buckets for each status
		nodeTypes := []string{db.NodeTypeFolder, db.NodeTypeFile}
		copyStatuses := []string{db.CopyStatusPending, db.CopyStatusSuccessful, db.CopyStatusFailed, db.CopyStatusSkipped, db.CopyStatusInProgress}

		for _, nodeType := range nodeTypes {
			for _, status := range copyStatuses {
				count, err := boltDB.CountCopyStatusBucket(level, nodeType, status)
				if err == nil {
					switch status {
					case db.CopyStatusPending:
						totalPending += int64(count)
					case db.CopyStatusSuccessful:
						totalSuccessful += int64(count)
					case db.CopyStatusFailed:
						totalFailed += int64(count)
					case db.CopyStatusSkipped:
						totalSkipped += int64(count)
					case db.CopyStatusInProgress:
						totalInProgress += int64(count)
					}
				}
			}
		}
	}

	fmt.Printf("Copy status summary:\n")
	fmt.Printf("  Pending: %d\n", totalPending)
	fmt.Printf("  Successful: %d\n", totalSuccessful)
	fmt.Printf("  Failed: %d\n", totalFailed)
	fmt.Printf("  Skipped: %d\n", totalSkipped)
	fmt.Printf("  In-Progress: %d\n", totalInProgress)
	fmt.Println()

	// Verify no pending or in-progress tasks remain
	if totalPending > 0 {
		return fmt.Errorf("copy verification failed: %d pending tasks remain", totalPending)
	}

	if totalInProgress > 0 {
		return fmt.Errorf("copy verification failed: %d in-progress tasks remain", totalInProgress)
	}

	// Verify that at least some work was done
	// If nothing was copied, skipped, or failed, then no work was performed
	totalWorkDone := totalSuccessful + totalFailed + totalSkipped
	if totalWorkDone == 0 {
		return fmt.Errorf("copy verification failed: no items were processed (0 successful, 0 failed, 0 skipped) - queue may not have found any tasks to process")
	}

	fmt.Println("✓ All copy tasks completed!")
	fmt.Printf("✓ Successfully copied: %d items\n", totalSuccessful)
	if totalFailed > 0 {
		fmt.Printf("⚠ Warning: %d items failed to copy\n", totalFailed)
	}
	if totalSkipped > 0 {
		fmt.Printf("ℹ Info: %d items skipped (already exist)\n", totalSkipped)
	}

	return nil
}
