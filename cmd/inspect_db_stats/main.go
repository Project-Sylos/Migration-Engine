// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// inspect_db_stats performs O(1) stats bucket lookups for fast inspection.
// This uses cached statistics and may not reflect the actual bucket contents if stats are stale.
// For accurate O(n) bucket scans, use inspect_db instead.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Project-Sylos/Sylos-DB/pkg/store"
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

	// Open store
	storeInstance, err := store.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer storeInstance.Close()

	// Generate report using Store API with stats mode (O(1) - uses cached stats)
	report, err := storeInstance.InspectDatabase(store.InspectionModeStats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error inspecting database: %v\n", err)
		os.Exit(1)
	}

	// Print formatted report
	printReport(report, dbPath)
}

// Type aliases for Store API types to keep print functions unchanged
type LevelStatus = store.LevelStatus
type QueueReport = store.QueueReport
type DatabaseReport = store.DatabaseReport

func printReport(report *store.DatabaseReport, dbPath string) {
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

	srcComplete := report.Src.TotalPending == 0
	dstComplete := report.Dst.TotalPending == 0

	if srcComplete {
		fmt.Printf("✓ SRC: COMPLETE (no pending items)\n")
	} else {
		fmt.Printf("✗ SRC: INCOMPLETE\n")
		if report.Src.TotalPending > 0 {
			fmt.Printf("  - %d pending items remaining\n", report.Src.TotalPending)
		}
	}

	if dstComplete {
		fmt.Printf("✓ DST: COMPLETE (no pending items)\n")
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

func printQueueReport(qr *store.QueueReport) {
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
