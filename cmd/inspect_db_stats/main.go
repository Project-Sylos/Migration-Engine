// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <path-to-bolt.db>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s pkg/tests/bolt.db\n", os.Args[0])
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

	// Generate report using stats bucket
	report, err := inspectDatabase(boltDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error inspecting database: %v\n", err)
		os.Exit(1)
	}

	// Print formatted report
	printReport(report, dbPath)
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

	// Count total nodes using stats bucket
	nodesPath := db.GetNodesBucketPath(queueType)
	totalNodes, err := boltDB.GetBucketCount(nodesPath)
	if err != nil {
		// Stats might not exist, fall back to counting
		totalNodesInt, countErr := boltDB.CountNodes(queueType)
		if countErr != nil {
			return nil, fmt.Errorf("failed to count nodes: %w", countErr)
		}
		totalNodes = int64(totalNodesInt)
	}
	report.TotalNodes = int(totalNodes)

	// Get all levels
	levels, err := boltDB.GetAllLevels(queueType)
	if err != nil {
		return nil, fmt.Errorf("failed to get levels: %w", err)
	}

	// Sort levels
	sort.Ints(levels)

	// Inspect each level using stats bucket
	levelMap := make(map[int]*LevelStatus)
	for _, level := range levels {
		levelStatus := &LevelStatus{Level: level}

		// Count pending using stats bucket
		pendingPath := db.GetStatusBucketPath(queueType, level, db.StatusPending)
		pending, err := boltDB.GetBucketCount(pendingPath)
		if err != nil {
			// Stats might not exist, fall back to counting
			pendingInt, countErr := boltDB.CountStatusBucket(queueType, level, db.StatusPending)
			if countErr != nil {
				pending = 0
			} else {
				pending = int64(pendingInt)
			}
		}
		levelStatus.Pending = int(pending)

		// Count successful using stats bucket
		successfulPath := db.GetStatusBucketPath(queueType, level, db.StatusSuccessful)
		successful, err := boltDB.GetBucketCount(successfulPath)
		if err != nil {
			successfulInt, countErr := boltDB.CountStatusBucket(queueType, level, db.StatusSuccessful)
			if countErr != nil {
				successful = 0
			} else {
				successful = int64(successfulInt)
			}
		}
		levelStatus.Successful = int(successful)

		// Count failed using stats bucket
		failedPath := db.GetStatusBucketPath(queueType, level, db.StatusFailed)
		failed, err := boltDB.GetBucketCount(failedPath)
		if err != nil {
			failedInt, countErr := boltDB.CountStatusBucket(queueType, level, db.StatusFailed)
			if countErr != nil {
				failed = 0
			} else {
				failed = int64(failedInt)
			}
		}
		levelStatus.Failed = int(failed)

		// Count not_on_src (DST only) using stats bucket
		if queueType == "DST" {
			notOnSrcPath := db.GetStatusBucketPath(queueType, level, db.StatusNotOnSrc)
			notOnSrc, err := boltDB.GetBucketCount(notOnSrcPath)
			if err != nil {
				notOnSrcInt, countErr := boltDB.CountStatusBucket(queueType, level, db.StatusNotOnSrc)
				if countErr != nil {
					notOnSrc = 0
				} else {
					notOnSrc = int64(notOnSrcInt)
				}
			}
			levelStatus.NotOnSrc = int(notOnSrc)
		}

		levelMap[level] = levelStatus
		report.TotalPending += levelStatus.Pending
		report.TotalSuccessful += levelStatus.Successful
		report.TotalFailed += levelStatus.Failed
		if queueType == "DST" {
			report.TotalNotOnSrc += levelStatus.NotOnSrc
		}

		// Track minimum pending level
		if levelStatus.Pending > 0 {
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
	fmt.Printf("BoltDB Inspection Report (Stats-Based): %s\n", dbPath)
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

	srcComplete := report.Src.TotalPending == 0 && report.Src.TotalFailed == 0
	dstComplete := report.Dst.TotalPending == 0 && report.Dst.TotalFailed == 0

	if srcComplete {
		fmt.Printf("✓ SRC: COMPLETE (no pending, no failures)\n")
	} else {
		fmt.Printf("✗ SRC: INCOMPLETE\n")
		if report.Src.TotalPending > 0 {
			fmt.Printf("  - %d pending items remaining\n", report.Src.TotalPending)
		}
		if report.Src.TotalFailed > 0 {
			fmt.Printf("  - %d failed items\n", report.Src.TotalFailed)
		}
	}

	if dstComplete {
		fmt.Printf("✓ DST: COMPLETE (no pending, no failures)\n")
	} else {
		fmt.Printf("✗ DST: INCOMPLETE\n")
		if report.Dst.TotalPending > 0 {
			fmt.Printf("  - %d pending items remaining\n", report.Dst.TotalPending)
		}
		if report.Dst.TotalFailed > 0 {
			fmt.Printf("  - %d failed items\n", report.Dst.TotalFailed)
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
