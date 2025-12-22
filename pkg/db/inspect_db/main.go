// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// inspect_db performs O(n) bucket scans to count nodes and status buckets.
// This ensures we're counting actual data, not cached stats.
// For fast O(1) stats-based inspection, use inspect_db_stats instead.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/tests/shared"
	"github.com/Project-Sylos/Spectra/sdk"
	bolt "go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <path-to-bolt.db> [spectra-config-path]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s pkg/tests/bolt.db\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s pkg/tests/bolt.db pkg/configs/spectra.json\n", os.Args[0])
		os.Exit(1)
	}

	dbPath := os.Args[1]

	// Make path absolute if it's relative
	if !filepath.IsAbs(dbPath) {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting working directory: %v\n", err)
			os.Exit(1)
		}
		dbPath = filepath.Join(wd, dbPath)
	}

	// Check if file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: Database file does not exist: %s\n", dbPath)
		os.Exit(1)
	}

	// Open database (read-only mode handled by BoltDB automatically)
	boltDB, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer boltDB.Close()

	// Generate report
	report, err := inspectDatabase(boltDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error inspecting database: %v\n", err)
		os.Exit(1)
	}

	// Print formatted report
	printReport(report, dbPath)

	// If Spectra config path provided, compare counts
	if len(os.Args) >= 3 {
		spectraConfigPath := os.Args[2]
		if err := compareWithSpectra(boltDB, spectraConfigPath); err != nil {
			fmt.Fprintf(os.Stderr, "\n⚠️  Warning: Failed to compare with Spectra: %v\n", err)
			// Don't exit with error, just warn
		}
	}
}

type LevelStatus struct {
	Level      int
	Pending    int
	Successful int
	Failed     int
	NotOnSrc   int
}

type QueueReport struct {
	QueueType       string
	TotalNodes      int
	Levels          []LevelStatus
	TotalPending    int
	TotalSuccessful int
	TotalFailed     int
	TotalNotOnSrc   int
	MinPendingLevel *int
}

type DatabaseReport struct {
	DatabasePath string
	Src          QueueReport
	Dst          QueueReport
}

func inspectDatabase(boltDB *db.DB) (*DatabaseReport, error) {
	report := &DatabaseReport{
		Src: QueueReport{QueueType: "SRC"},
		Dst: QueueReport{QueueType: "DST"},
	}

	// Inspect SRC
	srcReport, err := inspectQueue(boltDB, "SRC")
	if err != nil {
		return nil, fmt.Errorf("failed to inspect SRC: %w", err)
	}
	report.Src = *srcReport

	// Inspect DST
	dstReport, err := inspectQueue(boltDB, "DST")
	if err != nil {
		return nil, fmt.Errorf("failed to inspect DST: %w", err)
	}
	report.Dst = *dstReport

	return report, nil
}

func inspectQueue(boltDB *db.DB, queueType string) (*QueueReport, error) {
	report := &QueueReport{
		QueueType: queueType,
	}

	// Count total nodes by scanning (O(n)) - not using stats bucket
	// This ensures we're counting actual nodes, not cached stats
	totalNodes, err := countNodesByScan(boltDB, queueType)
	if err != nil {
		return nil, fmt.Errorf("failed to count nodes: %w", err)
	}
	report.TotalNodes = totalNodes

	// Get all levels
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return nil, fmt.Errorf("failed to get levels: %w", err)
	}

	// Sort levels
	sort.Ints(levels)

	// Inspect each level
	levelMap := make(map[int]*LevelStatus)
	for _, level := range levels {
		levelStatus := &LevelStatus{Level: level}

		// Count pending by scanning bucket (O(n)) - not using stats bucket
		pending, err := countStatusBucketByScan(boltDB, queueType, level, db.StatusPending)
		if err != nil {
			// Bucket might not exist, that's okay
			pending = 0
		}
		levelStatus.Pending = pending

		// Count successful by scanning bucket (O(n))
		successful, err := countStatusBucketByScan(boltDB, queueType, level, db.StatusSuccessful)
		if err != nil {
			successful = 0
		}
		levelStatus.Successful = successful

		// Count failed by scanning bucket (O(n))
		failed, err := countStatusBucketByScan(boltDB, queueType, level, db.StatusFailed)
		if err != nil {
			failed = 0
		}
		levelStatus.Failed = failed

		// Count not_on_src (DST only) by scanning bucket (O(n))
		if queueType == "DST" {
			notOnSrc, err := countStatusBucketByScan(boltDB, queueType, level, db.StatusNotOnSrc)
			if err != nil {
				notOnSrc = 0
			}
			levelStatus.NotOnSrc = notOnSrc
		}

		levelMap[level] = levelStatus
		report.TotalPending += pending
		report.TotalSuccessful += successful
		report.TotalFailed += failed
		if queueType == "DST" {
			report.TotalNotOnSrc += levelStatus.NotOnSrc
		}

		// Track minimum pending level
		if pending > 0 {
			if report.MinPendingLevel == nil || level < *report.MinPendingLevel {
				level := level
				report.MinPendingLevel = &level
			}
		}
	}

	// Convert map to sorted slice
	report.Levels = make([]LevelStatus, 0, len(levelMap))
	for _, level := range levels {
		report.Levels = append(report.Levels, *levelMap[level])
	}

	return report, nil
}

func printReport(report *DatabaseReport, dbPath string) {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("BoltDB Inspection Report (Bucket Scan - O(n)): %s\n", dbPath)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()

	// Print SRC report
	printQueueReport(&report.Src)

	fmt.Println()
	fmt.Println(strings.Repeat("-", 80))
	fmt.Println()

	// Print DST report
	printQueueReport(&report.Dst)

	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))

	// Summary
	fmt.Println("\nSUMMARY:")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("SRC Total Nodes:     %d\n", report.Src.TotalNodes)
	fmt.Printf("SRC Pending:         %d\n", report.Src.TotalPending)
	fmt.Printf("SRC Successful:      %d\n", report.Src.TotalSuccessful)
	fmt.Printf("SRC Failed:          %d\n", report.Src.TotalFailed)
	if report.Src.MinPendingLevel != nil {
		fmt.Printf("SRC Min Pending Level: %d\n", *report.Src.MinPendingLevel)
	} else {
		fmt.Printf("SRC Min Pending Level: N/A (no pending)\n")
	}

	fmt.Println()
	fmt.Printf("DST Total Nodes:     %d\n", report.Dst.TotalNodes)
	fmt.Printf("DST Pending:         %d\n", report.Dst.TotalPending)
	fmt.Printf("DST Successful:      %d\n", report.Dst.TotalSuccessful)
	fmt.Printf("DST Failed:          %d\n", report.Dst.TotalFailed)
	fmt.Printf("DST Not On SRC:      %d\n", report.Dst.TotalNotOnSrc)
	if report.Dst.MinPendingLevel != nil {
		fmt.Printf("DST Min Pending Level: %d\n", *report.Dst.MinPendingLevel)
	} else {
		fmt.Printf("DST Min Pending Level: N/A (no pending)\n")
	}

	fmt.Println()
	fmt.Println(strings.Repeat("-", 80))

	// Completion status
	fmt.Println("\nCOMPLETION STATUS:")
	fmt.Println(strings.Repeat("-", 80))

	// Only consider pending items for completion status, ignore failures
	srcComplete := report.Src.TotalPending == 0
	dstComplete := report.Dst.TotalPending == 0

	if srcComplete {
		fmt.Printf("✓ SRC: COMPLETE (no pending)\n")
	} else {
		fmt.Printf("✗ SRC: INCOMPLETE\n")
		if report.Src.TotalPending > 0 {
			fmt.Printf("  - %d pending items remaining\n", report.Src.TotalPending)
		}
	}

	if dstComplete {
		fmt.Printf("✓ DST: COMPLETE (no pending)\n")
	} else {
		fmt.Printf("✗ DST: INCOMPLETE\n")
		if report.Dst.TotalPending > 0 {
			fmt.Printf("  - %d pending items remaining\n", report.Dst.TotalPending)
		}
		if report.Dst.TotalNotOnSrc > 0 {
			fmt.Printf("  - %d items not on source\n", report.Dst.TotalNotOnSrc)
		}
	}

	if srcComplete && dstComplete {
		fmt.Println()
		fmt.Printf("✓✓ MIGRATION APPEARS TO BE COMPLETE ✓✓\n")
	} else {
		fmt.Println()
		fmt.Printf("⚠ MIGRATION MAY STILL BE IN PROGRESS OR HANGING ⚠\n")
	}

	fmt.Println(strings.Repeat("=", 80))
}

// compareWithSpectra compares the SRC node count in BoltDB with the Spectra node count.
func compareWithSpectra(boltDB *db.DB, spectraConfigPath string) error {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("SPECTRA COMPARISON")
	fmt.Println(strings.Repeat("=", 80))

	// Setup Spectra
	spectraFS, err := shared.SetupSpectraFS(spectraConfigPath, false)
	if err != nil {
		return fmt.Errorf("failed to setup Spectra: %w", err)
	}

	// Count Spectra nodes
	spectraCount, err := countSpectraNodes(spectraFS)
	if err != nil {
		return fmt.Errorf("failed to count Spectra nodes: %w", err)
	}

	// Count SRC nodes
	srcCount, err := boltDB.CountNodes("SRC")
	if err != nil {
		return fmt.Errorf("failed to count SRC nodes: %w", err)
	}

	// Print comparison
	fmt.Printf("Spectra DB node count: %d\n", spectraCount)
	fmt.Printf("BoltDB SRC node count: %d\n", srcCount)

	if srcCount == spectraCount {
		fmt.Printf("✅ Counts match perfectly!\n")
	} else {
		diff := srcCount - spectraCount
		fmt.Printf("⚠️  MISMATCH: Difference of %d nodes\n", diff)
		if diff > 0 {
			fmt.Printf("   BoltDB has %d more nodes than Spectra\n", diff)
		} else {
			fmt.Printf("   BoltDB has %d fewer nodes than Spectra\n", -diff)
		}
	}

	fmt.Println(strings.Repeat("=", 80))
	return nil
}

// countSpectraNodes counts all nodes in the Spectra database using DFS traversal.
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

func printQueueReport(qr *QueueReport) {
	fmt.Printf("%s QUEUE REPORT\n", qr.QueueType)
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("Total Nodes: %d\n", qr.TotalNodes)
	fmt.Printf("Total Pending: %d | Successful: %d | Failed: %d", qr.TotalPending, qr.TotalSuccessful, qr.TotalFailed)
	if qr.QueueType == "DST" {
		fmt.Printf(" | Not On SRC: %d", qr.TotalNotOnSrc)
	}
	fmt.Println()

	if qr.MinPendingLevel != nil {
		fmt.Printf("Minimum Pending Level: %d\n", *qr.MinPendingLevel)
	} else {
		fmt.Printf("Minimum Pending Level: N/A (no pending items)\n")
	}

	if len(qr.Levels) == 0 {
		fmt.Println("\nNo levels found in database.")
		return
	}

	fmt.Println()
	fmt.Println("Level-by-Level Breakdown:")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-8s %10s %12s %10s", "Level", "Pending", "Successful", "Failed")
	if qr.QueueType == "DST" {
		fmt.Printf(" %12s", "NotOnSrc")
	}
	fmt.Printf(" %10s\n", "Total")
	fmt.Println(strings.Repeat("-", 80))

	for _, level := range qr.Levels {
		total := level.Pending + level.Successful + level.Failed
		if qr.QueueType == "DST" {
			total += level.NotOnSrc
		}

		fmt.Printf("%-8d %10d %12d %10d", level.Level, level.Pending, level.Successful, level.Failed)
		if qr.QueueType == "DST" {
			fmt.Printf(" %12d", level.NotOnSrc)
		}
		fmt.Printf(" %10d\n", total)
	}
}

// countNodesByScan counts nodes by scanning the nodes bucket (O(n)).
// This is used by inspect_db to ensure we're counting actual nodes, not cached stats.
func countNodesByScan(boltDB *db.DB, queueType string) (int, error) {
	count := 0
	err := boltDB.GetDB().View(func(tx *bolt.Tx) error {
		bucket := db.GetNodesBucket(tx, queueType)
		if bucket == nil {
			return nil // Bucket doesn't exist, count is 0
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count++
		}

		return nil
	})
	return count, err
}

// countStatusBucketByScan counts nodes in a status bucket by scanning (O(n)).
// This is used by inspect_db to ensure we're counting actual nodes, not cached stats.
func countStatusBucketByScan(boltDB *db.DB, queueType string, level int, status string) (int, error) {
	count := 0
	err := boltDB.GetDB().View(func(tx *bolt.Tx) error {
		bucket := db.GetStatusBucket(tx, queueType, level, status)
		if bucket == nil {
			return nil // Bucket doesn't exist, count is 0
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count++
		}

		return nil
	})
	return count, err
}
