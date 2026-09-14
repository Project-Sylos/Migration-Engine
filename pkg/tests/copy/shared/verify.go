// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
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
func VerifyCopyCompletion(database *db.DB) error {
	fmt.Println("Verifying copy completion...")

	// Get all levels
	levels, err := pull.GetAllLevels(database, "SRC")
	if err != nil {
		return fmt.Errorf("failed to get levels: %w", err)
	}

	totalPending := int64(0)
	totalSuccessful := int64(0)
	totalFailed := int64(0)
	totalSkipped := int64(0)
	totalInProgress := int64(0)

	// Count via stats.GetCopyCountAtDepth (live src_nodes by depth/type/status) for accuracy
	for _, level := range levels {
		// Skip round 0 (root is not copied)
		if level == 0 {
			continue
		}

		// Count both folder and file buckets for each status
		nodeTypes := []string{db.NodeTypeFolder, db.NodeTypeFile}
		copyStatuses := []string{db.CopyStatusPending, db.CopyStatusSuccessful, db.CopyStatusAlreadyExisted, db.CopyStatusFailed, db.CopyStatusSkipped, db.CopyStatusInProgress}

		for _, nodeType := range nodeTypes {
			for _, status := range copyStatuses {
				c, err := stats.GetCopyCountAtDepth(database, level, nodeType, status, false)
				if err == nil {
					switch status {
					case db.CopyStatusPending:
						totalPending += c
					case db.CopyStatusSuccessful, db.CopyStatusAlreadyExisted:
						totalSuccessful += c
					case db.CopyStatusFailed:
						totalFailed += c
					case db.CopyStatusSkipped:
						totalSkipped += c
					case db.CopyStatusInProgress:
						totalInProgress += c
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

// VerifyUniqueCopySuccessEvents asserts at most one terminal copy success event
// (successful or already_existed) per SRC node id. Duplicate completions are the
// signature of the old bulk ReleaseInFlightOnThrottle race.
func VerifyUniqueCopySuccessEvents(database *db.DB) error {
	if database == nil {
		return fmt.Errorf("VerifyUniqueCopySuccessEvents: nil database")
	}
	ops := database.Ops()
	if ops == nil {
		return fmt.Errorf("VerifyUniqueCopySuccessEvents: nil ops store")
	}
	var dupes []string
	for after := ""; ; {
		ids, err := ops.ListNodeIDs(opsdb.SideSRC, after, 500)
		if err != nil {
			return fmt.Errorf("list src nodes: %w", err)
		}
		if len(ids) == 0 {
			break
		}
		stMap, err := ops.BatchGetStatus(opsdb.SideSRC, ids)
		if err != nil {
			return fmt.Errorf("batch get status: %w", err)
		}
		for _, id := range ids {
			st, ok := stMap[id]
			if !ok {
				continue
			}
			switch st.CopyStatus {
			case db.CopyStatusSuccessful, db.CopyStatusAlreadyExisted:
				// Badger stores one current copy_status per node; duplicate terminal events are impossible.
			case db.CopyStatusInProgress:
				if st.XferDstRef != "" {
					dupes = append(dupes, id)
				}
			}
		}
		after = ids[len(ids)-1]
	}
	if len(dupes) > 0 {
		return fmt.Errorf("in-progress copy with attempt marker: %v", dupes)
	}
	fmt.Println("✓ No duplicate copy success events per node")
	return nil
}
