// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/db/etl"
	_ "github.com/marcboeker/go-duckdb"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultDuckDBPath = "pkg/tests/etl/duck_to_bolt/main-duck.db"
	defaultBoltDBPath = "pkg/tests/etl/duck_to_bolt/main-bolt.db"
	sampleSize        = 100 // Tier 2 sample size
)

func main() {
	fmt.Println("=== ETL Test Runner (Duck to Bolt) ===")
	fmt.Println()

	duckDBPath := defaultDuckDBPath
	boltDBPath := defaultBoltDBPath

	// Allow override via environment variables
	if envDuck := os.Getenv("DUCK_DB_PATH"); envDuck != "" {
		duckDBPath = envDuck
	}
	if envBolt := os.Getenv("BOLT_DB_PATH"); envBolt != "" {
		boltDBPath = envBolt
	}

	if err := runTest(duckDBPath, boltDBPath); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest(duckDBPath, boltDBPath string) error {
	// Phase 1: Setup
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("=================")
	fmt.Printf("  DuckDB: %s\n", duckDBPath)
	fmt.Printf("  BoltDB: %s\n", boltDBPath)

	// Check if DuckDB file exists
	if _, err := os.Stat(duckDBPath); os.IsNotExist(err) {
		return fmt.Errorf("DuckDB file does not exist: %s", duckDBPath)
	}

	// Ensure BoltDB directory exists
	boltDBDir := filepath.Dir(boltDBPath)
	if boltDBDir != "." && boltDBDir != "" {
		if err := os.MkdirAll(boltDBDir, 0755); err != nil {
			return fmt.Errorf("failed to create BoltDB directory: %w", err)
		}
	}

	// Remove BoltDB file if overwrite is requested
	if _, err := os.Stat(boltDBPath); err == nil {
		fmt.Println("  Removing existing BoltDB file...")
		if err := os.Remove(boltDBPath); err != nil {
			return fmt.Errorf("failed to remove existing BoltDB file: %w", err)
		}
	}
	fmt.Println()

	// Phase 2: Run ETL
	fmt.Println("🚀 Phase 2: ETL Migration")
	fmt.Println("==========================")
	cfg := etl.DuckToBoltConfig{
		DuckDBPath: duckDBPath,
		BoltPath:   boltDBPath,
		Overwrite:  true,
	}
	if err := etl.RunDuckToBolt(cfg); err != nil {
		return fmt.Errorf("ETL migration failed: %w", err)
	}
	fmt.Println()

	// Phase 3: Verification
	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")

	// Open DuckDB for verification (source) - reopen after ETL is done
	duckDB, err := sql.Open("duckdb", duckDBPath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB for verification: %w", err)
	}
	defer duckDB.Close()

	// Open BoltDB for verification (result)
	boltOpts := db.DefaultOptions()
	boltOpts.Path = boltDBPath
	boltDB, err := db.Open(boltOpts)
	if err != nil {
		return fmt.Errorf("failed to open BoltDB for verification: %w", err)
	}
	defer boltDB.Close()

	// Tier 1: Quick structural checks
	tier1Result, err := verifyETLQuick(duckDB, boltDB)
	if err != nil {
		return fmt.Errorf("tier 1 verification failed: %w", err)
	}

	// Tier 2: Sampled spot checks
	tier2Result, err := verifyETLSampled(duckDB, boltDB, sampleSize)
	if err != nil {
		return fmt.Errorf("tier 2 verification failed: %w", err)
	}

	// Print results
	fmt.Println("\n=== Verification Results ===")
	fmt.Println()

	if !tier1Result.Tier1Passed {
		fmt.Println("❌ Tier 1 (Structural Checks): FAILED")
		for _, errMsg := range tier1Result.Errors {
			fmt.Printf("   ✗ %s\n", errMsg)
		}
	} else {
		fmt.Println("✅ Tier 1 (Structural Checks): PASSED")
	}

	if len(tier1Result.Warnings) > 0 {
		for _, warn := range tier1Result.Warnings {
			fmt.Printf("   ⚠ %s\n", warn)
		}
	}

	fmt.Println()

	if !tier2Result.Tier2Passed {
		fmt.Println("❌ Tier 2 (Sampled Checks): FAILED")
		errorCount := 0
		for _, errMsg := range tier2Result.Errors {
			if errorCount < 10 { // Limit output
				fmt.Printf("   ✗ %s\n", errMsg)
			}
			errorCount++
		}
		if errorCount > 10 {
			fmt.Printf("   ... and more errors\n")
		}
	} else {
		fmt.Println("✅ Tier 2 (Sampled Checks): PASSED")
	}

	if len(tier2Result.Warnings) > 0 {
		for _, warn := range tier2Result.Warnings {
			fmt.Printf("   ⚠ %s\n", warn)
		}
	}

	// Determine overall result
	if !tier1Result.Tier1Passed || !tier2Result.Tier2Passed {
		return fmt.Errorf("verification failed")
	}

	return nil
}

// VerificationResult holds the results of ETL verification
type VerificationResult struct {
	Tier1Passed bool
	Tier2Passed bool
	Errors      []string
	Warnings    []string
}

// verifyETLQuick performs Tier 1 verification: stat-to-stat comparison
// Compares DuckDB stats table against BoltDB stats bucket
func verifyETLQuick(duckDB *sql.DB, boltDB *db.DB) (*VerificationResult, error) {
	result := &VerificationResult{
		Tier1Passed: true,
		Errors:      []string{},
		Warnings:    []string{},
	}

	fmt.Println("\n✓ Tier 1: Stats Comparison")
	fmt.Println("============================")

	// Query all stats from DuckDB
	rows, err := duckDB.Query("SELECT bucket_path, count FROM stats ORDER BY bucket_path")
	if err != nil {
		return nil, fmt.Errorf("failed to query DuckDB stats: %w", err)
	}
	defer rows.Close()

	mismatchCount := 0
	matchCount := 0

	for rows.Next() {
		var bucketPathStr string
		var duckCount int64

		if err := rows.Scan(&bucketPathStr, &duckCount); err != nil {
			return nil, fmt.Errorf("failed to scan stats row: %w", err)
		}

		// Convert bucket path string to []string array (split on "/")
		bucketPath := strings.Split(bucketPathStr, "/")

		// Get corresponding count from BoltDB stats bucket
		boltCount, err := boltDB.GetBucketCount(bucketPath)
		if err != nil {
			result.Tier1Passed = false
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to get BoltDB count for %s: %v", bucketPathStr, err))
			mismatchCount++
			continue
		}

		// Compare counts
		if duckCount != boltCount {
			result.Tier1Passed = false
			result.Errors = append(result.Errors, fmt.Sprintf("Stats mismatch for %s: DuckDB=%d, BoltDB=%d", bucketPathStr, duckCount, boltCount))
			mismatchCount++
		} else {
			matchCount++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating stats rows: %w", err)
	}

	if result.Tier1Passed {
		fmt.Printf("    ✓ All %d stats entries match\n", matchCount)
	} else {
		fmt.Printf("    ✗ %d mismatches found (out of %d total stats entries)\n", mismatchCount, matchCount+mismatchCount)
	}

	return result, nil
}

// verifyETLSampled performs Tier 2 verification: probabilistic spot checks
// Compares DuckDB (source) against BoltDB (result)
func verifyETLSampled(duckDB *sql.DB, boltDB *db.DB, sampleSize int) (*VerificationResult, error) {
	result := &VerificationResult{
		Tier2Passed: true,
		Errors:      []string{},
		Warnings:    []string{},
	}

	fmt.Println("\n✓ Tier 2: Probabilistic Spot Checks")
	fmt.Println("=====================================")

	// Sample nodes from DuckDB (source) and verify they exist in BoltDB (result)
	fmt.Printf("  Sampling %d random SRC nodes from DuckDB...\n", sampleSize)
	srcNodeIDs, err := getRandomNodeIDsFromDuck(duckDB, "src_nodes", sampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to sample SRC nodes from DuckDB: %w", err)
	}

	if len(srcNodeIDs) > 0 {
		missingCount := 0
		for _, nodeID := range srcNodeIDs {
			exists, err := nodeExistsInBolt(boltDB, "SRC", nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to check SRC node in BoltDB: %w", err)
			}
			if !exists {
				result.Tier2Passed = false
				result.Errors = append(result.Errors, fmt.Sprintf("SRC node %s: not found in BoltDB", nodeID))
				missingCount++
			}
		}

		if missingCount == 0 {
			fmt.Printf("    ✓ All %d sampled SRC nodes found in BoltDB\n", len(srcNodeIDs))
		} else {
			fmt.Printf("    ✗ %d of %d sampled SRC nodes missing in BoltDB\n", missingCount, len(srcNodeIDs))
		}
	}

	// Sample nodes from DuckDB (source) and verify they exist in BoltDB (result)
	fmt.Printf("  Sampling %d random DST nodes from DuckDB...\n", sampleSize)
	dstNodeIDs, err := getRandomNodeIDsFromDuck(duckDB, "dst_nodes", sampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to sample DST nodes from DuckDB: %w", err)
	}

	if len(dstNodeIDs) > 0 {
		missingCount := 0
		for _, nodeID := range dstNodeIDs {
			exists, err := nodeExistsInBolt(boltDB, "DST", nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to check DST node in BoltDB: %w", err)
			}
			if !exists {
				result.Tier2Passed = false
				result.Errors = append(result.Errors, fmt.Sprintf("DST node %s: not found in BoltDB", nodeID))
				missingCount++
			}
		}

		if missingCount == 0 {
			fmt.Printf("    ✓ All %d sampled DST nodes found in BoltDB\n", len(dstNodeIDs))
		} else {
			fmt.Printf("    ✗ %d of %d sampled DST nodes missing in BoltDB\n", missingCount, len(dstNodeIDs))
		}
	}

	return result, nil
}

// Helper functions

// getRandomNodeIDsFromDuck samples random node IDs from DuckDB table
func getRandomNodeIDsFromDuck(duckDB *sql.DB, tableName string, sampleSize int) ([]string, error) {
	// Get all IDs
	rows, err := duckDB.Query(fmt.Sprintf("SELECT id FROM %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to query DuckDB node IDs: %w", err)
	}
	defer rows.Close()

	var allIDs []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("failed to scan node ID: %w", err)
		}
		allIDs = append(allIDs, nodeID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if len(allIDs) == 0 {
		return []string{}, nil
	}

	// Sample randomly
	if len(allIDs) <= sampleSize {
		return allIDs, nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(allIDs), func(i, j int) {
		allIDs[i], allIDs[j] = allIDs[j], allIDs[i]
	})

	return allIDs[:sampleSize], nil
}

// nodeExistsInBolt checks if a node exists in BoltDB
func nodeExistsInBolt(boltDB *db.DB, queueType, nodeID string) (bool, error) {
	exists := false
	err := boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return nil
		}
		exists = nodesBucket.Get([]byte(nodeID)) != nil
		return nil
	})
	return exists, err
}
