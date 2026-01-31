// +build ignore

// Diagnostic script to check for duplicate paths in DuckDB tables.
// Run with: go run diagnose.go <duckdb_path>

package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run diagnose.go <duckdb_path>")
		os.Exit(1)
	}

	dbPath := os.Args[1]
	fmt.Printf("Opening DuckDB: %s\n\n", dbPath)

	db, err := sql.Open("duckdb", dbPath+"?access_mode=read_only")
	if err != nil {
		fmt.Printf("Failed to open DuckDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Check for duplicate paths in primary UI tables (path is PK; duplicates indicate ETL bug)
	coreTables := []string{"src_nodes_ui", "dst_nodes_ui"}

	foundDuplicates := false
	sameIDDuplicates := false
	diffIDDuplicates := false

	for _, table := range coreTables {
		fmt.Printf("=== Checking %s for duplicate paths ===\n", table)

		// First, find paths that appear more than once
		query := fmt.Sprintf(`
			SELECT path, COUNT(*) as cnt 
			FROM %s 
			GROUP BY path 
			HAVING COUNT(*) > 1 
			ORDER BY cnt DESC
			LIMIT 10
		`, table)

		rows, err := db.Query(query)
		if err != nil {
			fmt.Printf("  Error querying %s: %v\n", table, err)
			continue
		}

		var dupPaths []string
		for rows.Next() {
			var path string
			var cnt int
			if err := rows.Scan(&path, &cnt); err != nil {
				fmt.Printf("  Error scanning: %v\n", err)
				continue
			}
			fmt.Printf("  DUPLICATE: path='%s' appears %d times\n", path, cnt)
			dupPaths = append(dupPaths, path)
			foundDuplicates = true
		}
		rows.Close()

		if len(dupPaths) == 0 {
			fmt.Printf("  OK: No duplicate paths found\n")
			fmt.Println()
			continue
		}

		// Now check if duplicates have SAME or DIFFERENT IDs
		fmt.Printf("\n  --- Checking if duplicates have same or different IDs ---\n")
		for _, dupPath := range dupPaths[:min(3, len(dupPaths))] {
			// Get all IDs for this path
			idQuery := fmt.Sprintf("SELECT id FROM %s WHERE path = ? ORDER BY id", table)
			idRows, err := db.Query(idQuery, dupPath)
			if err != nil {
				fmt.Printf("  Error querying IDs for path '%s': %v\n", dupPath, err)
				continue
			}

			var ids []string
			for idRows.Next() {
				var id string
				idRows.Scan(&id)
				ids = append(ids, id)
			}
			idRows.Close()

			// Check if all IDs are the same or different
			uniqueIDs := make(map[string]int)
			for _, id := range ids {
				uniqueIDs[id]++
			}

			if len(uniqueIDs) == 1 {
				// All rows have the SAME ID - ETL is writing same node multiple times
				for id, count := range uniqueIDs {
					fmt.Printf("  PATH: '%s'\n", dupPath)
					fmt.Printf("    SAME ID repeated %d times: %s\n", count, id)
					fmt.Printf("    DIAGNOSIS: ETL BUG - same node written multiple times\n")
					sameIDDuplicates = true
				}
			} else {
				// Rows have DIFFERENT IDs - source data has multiple nodes with same path
				fmt.Printf("  PATH: '%s'\n", dupPath)
				fmt.Printf("    DIFFERENT IDs (%d unique):\n", len(uniqueIDs))
				for id, count := range uniqueIDs {
					fmt.Printf("      ID=%s (appears %d times)\n", id, count)
				}
				fmt.Printf("    DIAGNOSIS: SOURCE DATA BUG - multiple nodes share same path\n")
				diffIDDuplicates = true
			}
		}
		fmt.Println()
	}

	// Get total row counts
	fmt.Println("=== Row counts ===")
	allTables := []string{
		"src_nodes_core", "src_nodes_status", "src_nodes_children",
		"dst_nodes_core", "dst_nodes_status", "dst_nodes_children",
	}
	for _, table := range allTables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			fmt.Printf("  %s: error - %v\n", table, err)
		} else {
			fmt.Printf("  %s: %d rows\n", table, count)
		}
	}
	fmt.Println()

	// Look up specific failing nodes if they exist
	failingIDs := []string{
		"01KG5M6XM9298ZB7C9GY37H4GY",
		"01KG5KEVBHG2E6VM2SY3ZCFXY8",
		"01KG5KF0HAQRFEKTZMH4QWD0Q3",
	}

	fmt.Println("=== Looking up specific failing DST nodes ===")
	for _, id := range failingIDs {
		fmt.Printf("\nNode ID: %s\n", id)

		// Get path from primary UI table
		var path string
		err := db.QueryRow("SELECT path FROM dst_nodes_ui WHERE id = ?", id).Scan(&path)
		if err != nil {
			fmt.Printf("  Not found in dst_nodes_ui\n")
			continue
		}
		fmt.Printf("  Path: %s\n", path)

		// Status is in same table (dst_nodes_ui)
		rows, err := db.Query("SELECT traversal_status, copy_status FROM dst_nodes_ui WHERE path = ?", path)
		if err != nil {
			fmt.Printf("  Error querying status: %v\n", err)
		} else {
			statusCount := 0
			for rows.Next() {
				var ts, cs sql.NullString
				rows.Scan(&ts, &cs)
				fmt.Printf("  Status row %d: traversal_status='%s', copy_status='%s'\n", statusCount+1, ts.String, cs.String)
				statusCount++
			}
			rows.Close()
			if statusCount > 1 {
				fmt.Printf("  WARNING: %d status rows for this path (expected 1)\n", statusCount)
			}
		}

		// Check children table for this path
		rows, err = db.Query("SELECT child_ids FROM dst_nodes_children WHERE path = ?", path)
		if err != nil {
			fmt.Printf("  Error querying children: %v\n", err)
		} else {
			childCount := 0
			for rows.Next() {
				var childIDs sql.NullString
				rows.Scan(&childIDs)
				if childIDs.Valid && len(childIDs.String) > 100 {
					fmt.Printf("  Children row %d: child_ids='%s...' (truncated, len=%d)\n", childCount+1, childIDs.String[:100], len(childIDs.String))
				} else {
					fmt.Printf("  Children row %d: child_ids='%s'\n", childCount+1, childIDs.String)
				}
				childCount++
			}
			rows.Close()
			if childCount > 1 {
				fmt.Printf("  WARNING: %d children rows for this path (expected 1)\n", childCount)
			}
		}
	}

	fmt.Println()
	fmt.Println("=== FINAL DIAGNOSIS ===")
	if !foundDuplicates {
		fmt.Println("RESULT: No duplicates found in any table")
		fmt.Println("The issue may be elsewhere (verification query, BoltDB source data, etc.)")
	} else if sameIDDuplicates && !diffIDDuplicates {
		fmt.Println("RESULT: ETL BUG CONFIRMED")
		fmt.Println("  Same node IDs are being written multiple times to DuckDB.")
		fmt.Println("  Check: streaming logic, buffer flush, or appender writes.")
		os.Exit(1)
	} else if diffIDDuplicates && !sameIDDuplicates {
		fmt.Println("RESULT: SOURCE DATA BUG CONFIRMED")
		fmt.Println("  Multiple different nodes share the same path in BoltDB.")
		fmt.Println("  Check: traversal logic, data generation, or parent-child relationships.")
		os.Exit(1)
	} else {
		fmt.Println("RESULT: MIXED - Both ETL and source data issues detected")
		fmt.Println("  Some paths have same ID repeated (ETL bug)")
		fmt.Println("  Some paths have different IDs (source data bug)")
		os.Exit(1)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
