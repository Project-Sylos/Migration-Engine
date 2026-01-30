// +build ignore

// Diagnostic script to check for duplicate paths in BoltDB (source data).
// Run with: go run diagnose_bolt.go <boltdb_path>
// This verifies if duplicates exist BEFORE ETL runs.
// Uses sampling for large databases to avoid O(n) full scan.

package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	bolt "go.etcd.io/bbolt"
)

const (
	// Sample size - check this many nodes per queue
	sampleSize = 100000
	// Skip interval for sampling (check every Nth node)
	// Adjust based on expected DB size
	skipInterval = 100
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run diagnose_bolt.go <boltdb_path>")
		os.Exit(1)
	}

	dbPath := os.Args[1]
	fmt.Printf("Opening BoltDB: %s\n\n", dbPath)

	// Open BoltDB in read-only mode
	boltDB, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		fmt.Printf("Failed to open BoltDB: %v\n", err)
		os.Exit(1)
	}
	defer boltDB.Close()

	rand.Seed(time.Now().UnixNano())

	foundDuplicates := false
	sameIDDuplicates := false
	diffIDDuplicates := false

	// Check both SRC and DST queues
	queueTypes := []string{"SRC", "DST"}

	for _, queueType := range queueTypes {
		fmt.Printf("=== Checking %s nodes for duplicate paths (sampled) ===\n", queueType)

		// Build map of path -> []ID to find duplicates (sampled)
		pathToIDs := make(map[string][]string)
		nodeCount := 0
		sampledCount := 0
		startTime := time.Now()

		err := boltDB.View(func(tx *bolt.Tx) error {
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket == nil {
				fmt.Printf("  No nodes bucket found for %s\n", queueType)
				return nil
			}

			cursor := nodesBucket.Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				nodeCount++

				// Sample: only process every Nth node, up to sampleSize
				if nodeCount%skipInterval != 0 {
					continue
				}
				if sampledCount >= sampleSize {
					continue // Keep counting total but stop sampling
				}

				nodeID := string(k)
				sampledCount++

				// Deserialize node to get path
				ns, err := db.DeserializeNodeState(v)
				if err != nil {
					continue
				}

				path := ns.Path
				if path == "" {
					continue
				}

				pathToIDs[path] = append(pathToIDs[path], nodeID)

				// Progress indicator every 10k samples
				if sampledCount%10000 == 0 {
					fmt.Printf("  ... sampled %d nodes (scanned %d)...\n", sampledCount, nodeCount)
				}
			}

			return nil
		})

		elapsed := time.Since(startTime)

		if err != nil {
			fmt.Printf("  Error reading %s nodes: %v\n", queueType, err)
			continue
		}

		fmt.Printf("  Total nodes scanned: %d\n", nodeCount)
		fmt.Printf("  Nodes sampled: %d (every %dth node)\n", sampledCount, skipInterval)
		fmt.Printf("  Unique paths in sample: %d\n", len(pathToIDs))
		fmt.Printf("  Scan time: %v\n", elapsed)

		// Find duplicates (paths with more than one ID)
		duplicateCount := 0
		for path, ids := range pathToIDs {
			if len(ids) > 1 {
				duplicateCount++
				foundDuplicates = true

				if duplicateCount <= 10 {
					fmt.Printf("\n  DUPLICATE: path='%s' appears %d times in sample\n", path, len(ids))

					// Check if all IDs are the same or different
					uniqueIDs := make(map[string]int)
					for _, id := range ids {
						uniqueIDs[id]++
					}

					if len(uniqueIDs) == 1 {
						// All rows have the SAME ID - shouldn't happen in BoltDB (keys are unique)
						for id, count := range uniqueIDs {
							fmt.Printf("    SAME ID repeated %d times: %s\n", count, id)
							fmt.Printf("    DIAGNOSIS: IMPOSSIBLE - BoltDB keys are unique!\n")
							sameIDDuplicates = true
						}
					} else {
						// Rows have DIFFERENT IDs - multiple nodes share same path
						fmt.Printf("    DIFFERENT IDs (%d unique):\n", len(uniqueIDs))
						idCount := 0
						for id := range uniqueIDs {
							if idCount < 5 {
								fmt.Printf("      ID=%s\n", id)
							}
							idCount++
						}
						if idCount > 5 {
							fmt.Printf("      ... and %d more IDs\n", idCount-5)
						}
						fmt.Printf("    DIAGNOSIS: SOURCE DATA BUG - multiple nodes share same path\n")
						diffIDDuplicates = true
					}
				}
			}
		}

		if duplicateCount == 0 {
			fmt.Printf("  OK: No duplicate paths found in sample\n")
		} else {
			fmt.Printf("\n  Total paths with duplicates in sample: %d\n", duplicateCount)
		}
		fmt.Println()
	}

	// Summary
	fmt.Println("=== FINAL DIAGNOSIS ===")
	if !foundDuplicates {
		fmt.Println("RESULT: No duplicates found in sampled BoltDB data")
		fmt.Println("Note: This checked a sample of ~100k nodes per queue.")
		fmt.Println("If DuckDB has duplicates, the bug may be in the ETL process,")
		fmt.Println("or duplicates exist outside the sampled range.")
	} else if diffIDDuplicates {
		fmt.Println("RESULT: SOURCE DATA BUG CONFIRMED")
		fmt.Println("  Multiple different nodes share the same path in BoltDB.")
		fmt.Println("  The bug is UPSTREAM of ETL (traversal or data generation).")
		os.Exit(1)
	} else if sameIDDuplicates {
		fmt.Println("RESULT: IMPOSSIBLE STATE DETECTED")
		fmt.Println("  Same ID appearing multiple times - BoltDB corruption?")
		os.Exit(1)
	}
}
