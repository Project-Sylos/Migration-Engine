// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/etl"
	_ "github.com/marcboeker/go-duckdb"
	bolt "go.etcd.io/bbolt"
)

const (
	defaultBoltDBPath = "/home/lmaup/Code/Codeberg/Sylos/Migration-Engine/pkg/tests/etl/bolt_to_duck/main_test.db"
	defaultDuckDBPath = "/home/lmaup/Code/Codeberg/Sylos/Migration-Engine/pkg/tests/etl/bolt_to_duck/main_test-duck.db"
	sampleSize        = 100 // Tier 2 sample size
)

func main() {
	fmt.Println("=== ETL Test Runner ===")
	fmt.Println()

	boltDBPath := defaultBoltDBPath
	duckDBPath := defaultDuckDBPath

	// Allow override via environment variables
	if envBolt := os.Getenv("BOLT_DB_PATH"); envBolt != "" {
		boltDBPath = envBolt
	}
	if envDuck := os.Getenv("DUCK_DB_PATH"); envDuck != "" {
		duckDBPath = envDuck
	}

	if err := runTest(boltDBPath, duckDBPath); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED!")
}

func runTest(boltDBPath, duckDBPath string) error {
	// Phase 1: Setup
	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("=================")
	fmt.Printf("  BoltDB: %s\n", boltDBPath)
	fmt.Printf("  DuckDB: %s\n", duckDBPath)

	// Check if BoltDB file exists
	if _, err := os.Stat(boltDBPath); os.IsNotExist(err) {
		return fmt.Errorf("BoltDB file does not exist: %s", boltDBPath)
	}

	// Open BoltDB
	boltOpts := db.DefaultOptions()
	boltOpts.Path = boltDBPath
	boltDB, err := db.Open(boltOpts)
	if err != nil {
		return fmt.Errorf("failed to open BoltDB: %w", err)
	}
	defer boltDB.Close()

	// Ensure DuckDB directory exists
	duckDBDir := filepath.Dir(duckDBPath)
	if duckDBDir != "." && duckDBDir != "" {
		if err := os.MkdirAll(duckDBDir, 0755); err != nil {
			return fmt.Errorf("failed to create DuckDB directory: %w", err)
		}
	}

	// Remove DuckDB file if overwrite is requested
	if _, err := os.Stat(duckDBPath); err == nil {
		fmt.Println("  Removing existing DuckDB file...")
		if err := os.Remove(duckDBPath); err != nil {
			return fmt.Errorf("failed to remove existing DuckDB file: %w", err)
		}
	}
	fmt.Println()

	// Phase 2: Run ETL
	fmt.Println("🚀 Phase 2: ETL Migration")
	fmt.Println("==========================")
	cfg := etl.BoltToDuckConfig{
		BoltDB:     boltDB,
		DuckDBPath: duckDBPath,
		Overwrite:  true,
	}
	if err := etl.RunBoltToDuck(cfg); err != nil {
		return fmt.Errorf("ETL migration failed: %w", err)
	}
	fmt.Println()

	// Phase 3: Verification
	fmt.Println("✓ Phase 3: Verification")
	fmt.Println("========================")

	// Open DuckDB for verification
	duckDB, err := sql.Open("duckdb", duckDBPath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB for verification: %w", err)
	}
	defer duckDB.Close()

	// Tier 1: Quick structural checks
	tier1Result, err := verifyETLQuick(boltDB, duckDB)
	if err != nil {
		return fmt.Errorf("tier 1 verification failed: %w", err)
	}

	// Tier 2: Sampled spot checks
	tier2Result, err := verifyETLSampled(boltDB, duckDB, sampleSize)
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

// verifyETLQuick performs Tier 1 verification: cheap structural sanity checks
func verifyETLQuick(boltDB *db.DB, duckDB *sql.DB) (*VerificationResult, error) {
	result := &VerificationResult{
		Tier1Passed: true,
		Errors:      []string{},
		Warnings:    []string{},
	}

	fmt.Println("\n✓ Tier 1: Structural Sanity Checks")
	fmt.Println("====================================")

	// 1. Node count parity
	fmt.Println("  Checking node counts...")
	srcBoltCount, dstBoltCount, err := countBoltNodes(boltDB)
	if err != nil {
		return nil, fmt.Errorf("failed to count BoltDB nodes: %w", err)
	}

	var srcDuckCount, dstDuckCount int
	if err := duckDB.QueryRow("SELECT COUNT(*) FROM src_nodes_ui").Scan(&srcDuckCount); err != nil {
		return nil, fmt.Errorf("failed to count DuckDB src_nodes_ui: %w", err)
	}
	if err := duckDB.QueryRow("SELECT COUNT(*) FROM dst_nodes_ui").Scan(&dstDuckCount); err != nil {
		return nil, fmt.Errorf("failed to count DuckDB dst_nodes_ui: %w", err)
	}

	if srcBoltCount != srcDuckCount {
		result.Tier1Passed = false
		result.Errors = append(result.Errors, fmt.Sprintf("SRC node count mismatch: BoltDB=%d, DuckDB=%d", srcBoltCount, srcDuckCount))
	} else {
		fmt.Printf("    ✓ SRC nodes: %d (match)\n", srcBoltCount)
	}

	if dstBoltCount != dstDuckCount {
		result.Tier1Passed = false
		result.Errors = append(result.Errors, fmt.Sprintf("DST node count mismatch: BoltDB=%d, DuckDB=%d", dstBoltCount, dstDuckCount))
	} else {
		fmt.Printf("    ✓ DST nodes: %d (match)\n", dstBoltCount)
	}

	// 2. Status distribution parity
	fmt.Println("  Checking status distribution...")
	if err := verifyStatusDistribution(boltDB, duckDB, result); err != nil {
		return nil, fmt.Errorf("failed to verify status distribution: %w", err)
	}

	// 3. Join coverage sanity
	fmt.Println("  Checking join coverage...")
	if err := verifyJoinCoverage(boltDB, duckDB, result); err != nil {
		return nil, fmt.Errorf("failed to verify join coverage: %w", err)
	}

	// 4. Logs count
	fmt.Println("  Checking log counts...")
	boltLogCount, err := countBoltLogs(boltDB)
	if err != nil {
		return nil, fmt.Errorf("failed to count BoltDB logs: %w", err)
	}

	var duckLogCount int
	if err := duckDB.QueryRow("SELECT COUNT(*) FROM logs").Scan(&duckLogCount); err != nil {
		return nil, fmt.Errorf("failed to count DuckDB logs: %w", err)
	}

	if boltLogCount != duckLogCount {
		result.Tier1Passed = false
		result.Errors = append(result.Errors, fmt.Sprintf("Log count mismatch: BoltDB=%d, DuckDB=%d", boltLogCount, duckLogCount))
	} else {
		fmt.Printf("    ✓ Logs: %d (match)\n", boltLogCount)
	}

	return result, nil
}

// verifyETLSampled performs Tier 2 verification: probabilistic spot checks
func verifyETLSampled(boltDB *db.DB, duckDB *sql.DB, sampleSize int) (*VerificationResult, error) {
	result := &VerificationResult{
		Tier2Passed: true,
		Errors:      []string{},
		Warnings:    []string{},
	}

	fmt.Println("\n✓ Tier 2: Probabilistic Spot Checks")
	fmt.Println("=====================================")

	// Sample nodes from SRC bucket and verify they exist in DuckDB (nodes_* tables)
	fmt.Printf("  Sampling %d random SRC nodes...\n", sampleSize)
	srcNodeIDs, err := getRandomNodeIDsFromBucket(boltDB, "SRC", sampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to sample SRC nodes: %w", err)
	}

	if len(srcNodeIDs) > 0 {
		srcNodes, err := getDuckNodesBatch(duckDB, "SRC", srcNodeIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to batch fetch DuckDB SRC nodes: %w", err)
		}

		missingCount := 0
		compareErrors := 0
		for _, nodeID := range srcNodeIDs {
			duckNode, exists := srcNodes[nodeID]
			if !exists {
				result.Tier2Passed = false
				result.Errors = append(result.Errors, fmt.Sprintf("SRC node %s: not found in DuckDB src_nodes_ui", nodeID))
				missingCount++
				continue
			}
			boltNode, err := getBoltNode(boltDB, "SRC", nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to get Bolt SRC node %s: %w", nodeID, err)
			}
			if err := compareNodes(boltNode, duckNode, nodeID, result); err != nil {
				result.Tier2Passed = false
				compareErrors++
			}
		}

		if missingCount == 0 && compareErrors == 0 {
			fmt.Printf("    ✓ All %d sampled SRC nodes found in DuckDB and match BoltDB\n", len(srcNodeIDs))
		} else if missingCount > 0 {
			fmt.Printf("    ✗ %d of %d sampled SRC nodes missing in DuckDB\n", missingCount, len(srcNodeIDs))
		} else if compareErrors > 0 {
			fmt.Printf("    ✗ %d of %d sampled SRC nodes had field mismatches\n", compareErrors, len(srcNodeIDs))
		}
	}

	// Sample nodes from DST bucket and verify they exist in DuckDB (nodes_* tables)
	fmt.Printf("  Sampling %d random DST nodes...\n", sampleSize)
	dstNodeIDs, err := getRandomNodeIDsFromBucket(boltDB, "DST", sampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to sample DST nodes: %w", err)
	}

	if len(dstNodeIDs) > 0 {
		dstNodes, err := getDuckNodesBatch(duckDB, "DST", dstNodeIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to batch fetch DuckDB DST nodes: %w", err)
		}

		missingCount := 0
		compareErrors := 0
		for _, nodeID := range dstNodeIDs {
			duckNode, exists := dstNodes[nodeID]
			if !exists {
				result.Tier2Passed = false
				result.Errors = append(result.Errors, fmt.Sprintf("DST node %s: not found in DuckDB dst_nodes_ui", nodeID))
				missingCount++
				continue
			}
			boltNode, err := getBoltNode(boltDB, "DST", nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to get Bolt DST node %s: %w", nodeID, err)
			}
			if err := compareNodes(boltNode, duckNode, nodeID, result); err != nil {
				result.Tier2Passed = false
				compareErrors++
			}
		}

		if missingCount == 0 && compareErrors == 0 {
			fmt.Printf("    ✓ All %d sampled DST nodes found in DuckDB and match BoltDB\n", len(dstNodeIDs))
		} else if missingCount > 0 {
			fmt.Printf("    ✗ %d of %d sampled DST nodes missing in DuckDB\n", missingCount, len(dstNodeIDs))
		} else if compareErrors > 0 {
			fmt.Printf("    ✗ %d of %d sampled DST nodes had field mismatches\n", compareErrors, len(dstNodeIDs))
		}
	}

	return result, nil
}

// Helper functions

func countBoltNodes(boltDB *db.DB) (srcCount, dstCount int, err error) {
	srcCount, err = boltDB.CountNodes("SRC")
	if err != nil {
		return 0, 0, err
	}
	dstCount, err = boltDB.CountNodes("DST")
	if err != nil {
		return 0, 0, err
	}
	return srcCount, dstCount, nil
}

func countBoltLogs(boltDB *db.DB) (int, error) {
	count := 0
	err := boltDB.View(func(tx *bolt.Tx) error {
		logsBucket := db.GetLogsBucket(tx)
		if logsBucket == nil {
			return nil
		}

		levels := []string{"trace", "debug", "info", "warning", "error", "critical"}
		for _, level := range levels {
			levelBucket := db.GetLogLevelBucket(tx, level)
			if levelBucket == nil {
				continue
			}
			cursor := levelBucket.Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				count++
			}
		}
		return nil
	})
	return count, err
}

func verifyStatusDistribution(boltDB *db.DB, duckDB *sql.DB, result *VerificationResult) error {
	// Get status distribution from BoltDB by scanning nodes
	boltStatusCounts := make(map[string]map[string]int) // queueType -> status -> count
	for _, queueType := range []string{"SRC", "DST"} {
		boltStatusCounts[queueType] = make(map[string]int)
		err := boltDB.View(func(tx *bolt.Tx) error {
			levels, _ := db.GetAllLevelsFromTx(tx, queueType)
			if levels == nil {
				return nil
			}
			for _, level := range levels {
				nodesBucket := db.GetNodesBucket(tx, queueType, level)
				if nodesBucket == nil {
					continue
				}
				cursor := nodesBucket.Cursor()
				for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
					var ns db.NodeState
					if err := json.Unmarshal(v, &ns); err != nil {
						continue
					}
					status := transformTraversalStatus(&ns)
					boltStatusCounts[queueType][status]++
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Get status distribution from DuckDB (src_nodes_ui / dst_nodes_ui)
	duckStatusCounts := make(map[string]map[string]int)
	for _, queueType := range []string{"SRC", "DST"} {
		duckStatusCounts[queueType] = make(map[string]int)
		tableName := "src_nodes_ui"
		if queueType == "DST" {
			tableName = "dst_nodes_ui"
		}
		rows, err := duckDB.Query(fmt.Sprintf("SELECT traversal_status, COUNT(*) FROM %s GROUP BY traversal_status", tableName))
		if err != nil {
			return fmt.Errorf("failed to query DuckDB status distribution: %w", err)
		}
		for rows.Next() {
			var status string
			var count int
			if err := rows.Scan(&status, &count); err != nil {
				rows.Close()
				return err
			}
			duckStatusCounts[queueType][status] = count
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}

	// Compare distributions
	for _, queueType := range []string{"SRC", "DST"} {
		allStatuses := make(map[string]bool)
		for status := range boltStatusCounts[queueType] {
			allStatuses[status] = true
		}
		for status := range duckStatusCounts[queueType] {
			allStatuses[status] = true
		}

		for status := range allStatuses {
			boltCount := boltStatusCounts[queueType][status]
			duckCount := duckStatusCounts[queueType][status]
			if boltCount != duckCount {
				result.Tier1Passed = false
				result.Errors = append(result.Errors, fmt.Sprintf("nodes_status %s status '%s' count mismatch: BoltDB=%d, DuckDB=%d", queueType, status, boltCount, duckCount))
			}
		}
	}

	if result.Tier1Passed {
		fmt.Println("    ✓ Status distributions match")
	}

	return nil
}

func verifyJoinCoverage(boltDB *db.DB, duckDB *sql.DB, result *VerificationResult) error {
	// Count joins in BoltDB
	srcToDstCount := 0
	dstToSrcCount := 0
	err := boltDB.View(func(tx *bolt.Tx) error {
		levels, _ := db.GetAllLevelsFromTx(tx, "SRC")
		if levels == nil {
			levels = []int{}
		}
		for _, level := range levels {
			srcToDstBucket := db.GetSrcToDstBucket(tx, level)
			if srcToDstBucket != nil {
				cursor := srcToDstBucket.Cursor()
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					srcToDstCount++
				}
			}
		}
		dstLevels, _ := db.GetAllLevelsFromTx(tx, "DST")
		if dstLevels == nil {
			dstLevels = []int{}
		}
		for _, level := range dstLevels {
			dstToSrcBucket := db.GetDstToSrcBucket(tx, level)
			if dstToSrcBucket != nil {
				cursor := dstToSrcBucket.Cursor()
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					dstToSrcCount++
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Count joins in DuckDB (src_nodes_ui.join_id / dst_nodes_ui.join_id)
	var srcWithDstCount int
	if err := duckDB.QueryRow("SELECT COUNT(*) FROM src_nodes_ui WHERE join_id IS NOT NULL").Scan(&srcWithDstCount); err != nil {
		return fmt.Errorf("failed to count src_nodes_ui with join_id: %w", err)
	}
	var dstWithSrcCount int
	if err := duckDB.QueryRow("SELECT COUNT(*) FROM dst_nodes_ui WHERE join_id IS NOT NULL").Scan(&dstWithSrcCount); err != nil {
		return fmt.Errorf("failed to count dst_nodes_ui with join_id: %w", err)
	}

	if srcToDstCount != srcWithDstCount {
		result.Tier1Passed = false
		result.Errors = append(result.Errors, fmt.Sprintf("SRC→DST join count mismatch: BoltDB=%d, DuckDB=%d", srcToDstCount, srcWithDstCount))
	} else {
		fmt.Printf("    ✓ SRC→DST joins: %d (match)\n", srcToDstCount)
	}

	if dstToSrcCount != dstWithSrcCount {
		result.Tier1Passed = false
		result.Errors = append(result.Errors, fmt.Sprintf("DST→SRC join count mismatch: BoltDB=%d, DuckDB=%d", dstToSrcCount, dstWithSrcCount))
	} else {
		fmt.Printf("    ✓ DST→SRC joins: %d (match)\n", dstToSrcCount)
	}

	return nil
}

// getRandomNodeIDsFromBucket samples random node IDs from a specific queue type (level-sharded)
func getRandomNodeIDsFromBucket(boltDB *db.DB, queueType string, sampleSize int) ([]string, error) {
	var allIDs []string
	err := boltDB.View(func(tx *bolt.Tx) error {
		levels, _ := db.GetAllLevelsFromTx(tx, queueType)
		if levels == nil {
			return nil
		}
		for _, level := range levels {
			nodesBucket := db.GetNodesBucket(tx, queueType, level)
			if nodesBucket == nil {
				continue
			}
			cursor := nodesBucket.Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				allIDs = append(allIDs, string(k))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
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

type nodeData struct {
	ID              string
	Path            string
	Depth           int
	TraversalStatus string
	CopyStatus      string
	DstID           string
	SrcID           string
	ChildIDs        string
}

func getBoltNode(boltDB *db.DB, queueType, nodeID string) (*nodeData, error) {
	ns, err := db.GetNodeState(boltDB, queueType, nodeID)
	if err != nil || ns == nil {
		return nil, fmt.Errorf("node not found: %s", nodeID)
	}
	level := ns.Depth

	// Get children (at same level)
	childIDs := []string{}
	boltDB.View(func(tx *bolt.Tx) error {
		childrenBucket := db.GetChildrenBucket(tx, queueType, level)
		if childrenBucket != nil {
			childData := childrenBucket.Get([]byte(nodeID))
			if childData != nil {
				json.Unmarshal(childData, &childIDs)
			}
		}
		return nil
	})
	childIDsJSON, _ := json.Marshal(childIDs)

	// Get join ID (per level)
	var joinID string
	boltDB.View(func(tx *bolt.Tx) error {
		if queueType == "SRC" {
			srcToDstBucket := db.GetSrcToDstBucket(tx, level)
			if srcToDstBucket != nil {
				joinData := srcToDstBucket.Get([]byte(nodeID))
				if joinData != nil {
					joinID = string(joinData)
				}
			}
		} else {
			dstToSrcBucket := db.GetDstToSrcBucket(tx, level)
			if dstToSrcBucket != nil {
				joinData := dstToSrcBucket.Get([]byte(nodeID))
				if joinData != nil {
					joinID = string(joinData)
				}
			}
		}
		return nil
	})

	node := &nodeData{
		ID:              nodeID,
		Path:            ns.Path,
		Depth:           ns.Depth,
		TraversalStatus: transformTraversalStatus(ns),
		CopyStatus:      ns.CopyStatus,
		ChildIDs:        string(childIDsJSON),
	}

	// Set join ID based on queue type
	if queueType == "SRC" {
		node.DstID = joinID
	} else {
		node.SrcID = joinID
	}

	return node, nil
}

// getDuckNodesBatch fetches multiple nodes by joining the five narrow tables.
// Uses separate SRC/DST table sets based on queueType ("SRC" or "DST").
func getDuckNodesBatch(duckDB *sql.DB, queueType string, nodeIDs []string) (map[string]*nodeData, error) {
	if len(nodeIDs) == 0 {
		return make(map[string]*nodeData), nil
	}

	// Compute table prefix from queue type
	prefix := "src"
	if queueType == "DST" {
		prefix = "dst"
	}

	// Primary UI table + children (path is unique)
	query := fmt.Sprintf(`
		SELECT u.id, u.path, u.depth, u.traversal_status, u.copy_status, u.join_id, ch.child_ids
		FROM %s_nodes_ui u
		LEFT JOIN %s_nodes_children ch ON u.path = ch.path
		WHERE u.id IN (`, prefix, prefix)
	args := make([]interface{}, 0, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args = append(args, nodeID)
	}
	query += ")"

	rows, err := duckDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query DuckDB: %w", err)
	}
	defer rows.Close()

	nodes := make(map[string]*nodeData)
	for rows.Next() {
		var node nodeData
		var childIDs sql.NullString
		var joinID sql.NullString

		err := rows.Scan(
			&node.ID,
			&node.Path,
			&node.Depth,
			&node.TraversalStatus,
			&node.CopyStatus,
			&joinID,
			&childIDs,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if joinID.Valid {
			if queueType == "SRC" {
				node.DstID = joinID.String
			} else {
				node.SrcID = joinID.String
			}
		}
		if childIDs.Valid {
			node.ChildIDs = childIDs.String
		}
		nodes[node.ID] = &node
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	return nodes, nil
}

func compareNodes(boltNode, duckNode *nodeData, nodeID string, result *VerificationResult) error {
	errors := []string{}

	if boltNode.Path != duckNode.Path {
		errors = append(errors, fmt.Sprintf("path: BoltDB='%s' vs DuckDB='%s'", boltNode.Path, duckNode.Path))
	}
	if boltNode.Depth != duckNode.Depth {
		errors = append(errors, fmt.Sprintf("depth: BoltDB=%d vs DuckDB=%d", boltNode.Depth, duckNode.Depth))
	}
	if boltNode.TraversalStatus != duckNode.TraversalStatus {
		errors = append(errors, fmt.Sprintf("traversal_status: BoltDB='%s' vs DuckDB='%s'", boltNode.TraversalStatus, duckNode.TraversalStatus))
	}
	if boltNode.CopyStatus != duckNode.CopyStatus {
		errors = append(errors, fmt.Sprintf("copy_status: BoltDB='%s' vs DuckDB='%s'", boltNode.CopyStatus, duckNode.CopyStatus))
	}

	// Compare join IDs
	if boltNode.DstID != "" && boltNode.DstID != duckNode.DstID {
		errors = append(errors, fmt.Sprintf("dst_id: BoltDB='%s' vs DuckDB='%s'", boltNode.DstID, duckNode.DstID))
	}
	if boltNode.SrcID != "" && boltNode.SrcID != duckNode.SrcID {
		errors = append(errors, fmt.Sprintf("src_id: BoltDB='%s' vs DuckDB='%s'", boltNode.SrcID, duckNode.SrcID))
	}

	// Compare child_ids length (just count, not exact match)
	var boltChildren []string
	json.Unmarshal([]byte(boltNode.ChildIDs), &boltChildren)
	var duckChildren []string
	json.Unmarshal([]byte(duckNode.ChildIDs), &duckChildren)
	if len(boltChildren) != len(duckChildren) {
		errors = append(errors, fmt.Sprintf("child_ids count: BoltDB=%d vs DuckDB=%d", len(boltChildren), len(duckChildren)))
	}

	if len(errors) > 0 {
		result.Errors = append(result.Errors, fmt.Sprintf("Node %s: %v", nodeID, errors))
		return fmt.Errorf("node comparison failed")
	}

	return nil
}

func transformTraversalStatus(ns *db.NodeState) string {
	if ns.ExplicitExcluded {
		return "excluded_explicit"
	}
	if ns.InheritedExcluded {
		return "excluded_inherited"
	}
	if ns.TraversalStatus != "" {
		return ns.TraversalStatus
	}
	if ns.Status != "" {
		return ns.Status
	}
	return "pending"
}
