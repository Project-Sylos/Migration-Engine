// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	bolt "go.etcd.io/bbolt"
)

// InspectCopyBuckets opens the database and prints the first 10 items from each copy status bucket
// for each level. If a bucket doesn't exist, it prints that information.
func InspectCopyBuckets(dbPath string) error {
	// Open database using the DB wrapper
	dbInstance, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer dbInstance.Close()

	fmt.Printf("Inspecting copy status buckets in: %s\n", dbPath)
	fmt.Println("==================================================================================")

	// Get all levels from SRC
	levels, err := dbInstance.GetAllLevels("SRC")
	if err != nil {
		return fmt.Errorf("failed to get levels: %w", err)
	}

	if len(levels) == 0 {
		fmt.Println("No levels found in SRC")
		return nil
	}

	fmt.Printf("Found %d levels: %v\n\n", len(levels), levels)

	// Copy statuses to check
	copyStatuses := []string{
		db.CopyStatusPending,
		db.CopyStatusInProgress,
		db.CopyStatusSuccessful,
		db.CopyStatusSkipped,
		db.CopyStatusFailed,
	}

	// Node types to check
	nodeTypes := []string{db.NodeTypeFolder, db.NodeTypeFile}

	// Iterate through each level
	for _, level := range levels {
		fmt.Printf("Level %d:\n", level)
		fmt.Println("----------------------------------------------------------------------------------")

		// Check each copy status bucket for both folders and files
		for _, status := range copyStatuses {
			fmt.Printf("  [%s]:\n", status)

			for _, nodeType := range nodeTypes {
				fmt.Printf("    [%s]: ", nodeType)

				err := dbInstance.View(func(tx *bolt.Tx) error {
					// Verify we're using the correct bucket path
					bucketPath := db.GetCopyStatusBucketPath(level, nodeType, status)
					fmt.Printf("(path: %v) ", bucketPath)

					statusBucket := db.GetCopyStatusBucket(tx, level, nodeType, status)
					if statusBucket == nil {
						fmt.Printf("bucket does not exist\n")
						return nil
					}

					// Count items and print first 10
					cursor := statusBucket.Cursor()
					count := 0
					first10 := make([][]byte, 0, 10)

					for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
						count++
						if len(first10) < 10 {
							keyCopy := make([]byte, len(k))
							copy(keyCopy, k)
							first10 = append(first10, keyCopy)
						}
					}

					fmt.Printf("total=%d", count)
					if count > 0 {
						fmt.Printf(", first 10 ULIDs:")
						for i, ulid := range first10 {
							fmt.Printf("\n      %d. %s", i+1, string(ulid))
						}
					}
					fmt.Println()

					return nil
				})

				if err != nil {
					fmt.Printf("ERROR: %v\n", err)
				}
			}
		}

		fmt.Println()
	}

	return nil
}

// RunInspectCopyBuckets is a main function that can be called from a test or standalone
func RunInspectCopyBuckets() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <db_path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s pkg/tests/copy/shared/main_test.db\n", os.Args[0])
		os.Exit(1)
	}

	dbPath := os.Args[1]
	if err := InspectCopyBuckets(dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
