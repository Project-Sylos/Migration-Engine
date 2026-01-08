// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package etl

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	_ "github.com/marcboeker/go-duckdb"
	bolt "go.etcd.io/bbolt"
)

const (
	// Default number of workers per queue type for DuckDB reads
	defaultNumWorkersDuckToBolt = 8
	// Batch size for BoltDB inserts (rows per transaction)
	defaultBatchSizeDuckToBolt = 50000
	// Stream batch size for distributing work to workers
	streamBatchSizeDuckToBolt = 10000
	// Performance reporting interval
	statsReportIntervalDuckToBolt = 3 * time.Second
)

// DuckNodeRow represents a node row read from DuckDB
type DuckNodeRow struct {
	ID              string
	ServiceID       string
	ParentID        sql.NullString
	ParentServiceID sql.NullString
	ParentPath      sql.NullString
	Name            string
	Path            string
	PathHash        string
	ChildIDs        sql.NullString // JSON array as string
	Type            string
	Size            sql.NullInt64
	MTime           string
	Depth           int32
	TraversalStatus string
	CopyStatus      sql.NullString // SRC only
	DstID           sql.NullString // For SRC nodes (nullable)
	SrcID           sql.NullString // For DST nodes (nullable)
}

// parseTraversalStatus converts DuckDB traversal_status values back to NodeState flags
// Returns: (normalized traversal status, explicitExcluded, inheritedExcluded)
func parseTraversalStatus(duckStatus string) (traversalStatus string, explicitExcluded bool, inheritedExcluded bool) {
	switch duckStatus {
	case "excluded_explicit":
		return db.StatusExcluded, true, false
	case "excluded_inherited":
		return db.StatusExcluded, false, true
	default:
		// Regular status: pending, successful, failed, not_on_src
		return duckStatus, false, false
	}
}

// duckNodeRowToNodeState converts a DuckDB row to NodeState struct
func duckNodeRowToNodeState(row DuckNodeRow, queueType string) (*db.NodeState, error) {
	// Parse traversal status
	traversalStatus, explicitExcluded, inheritedExcluded := parseTraversalStatus(row.TraversalStatus)

	// Build NodeState
	ns := &db.NodeState{
		ID:                row.ID,
		ServiceID:         row.ServiceID,
		Name:              row.Name,
		Path:              row.Path,
		Type:              row.Type,
		MTime:             row.MTime,
		Depth:             int(row.Depth),
		TraversalStatus:   traversalStatus,
		ExplicitExcluded:  explicitExcluded,
		InheritedExcluded: inheritedExcluded,
	}

	// Handle nullable fields
	if row.ParentID.Valid {
		ns.ParentID = row.ParentID.String
	}
	if row.ParentServiceID.Valid {
		ns.ParentServiceID = row.ParentServiceID.String
	}
	if row.ParentPath.Valid {
		ns.ParentPath = row.ParentPath.String
	}

	// Handle size (nullable)
	if row.Size.Valid {
		ns.Size = row.Size.Int64
	}

	// Handle copy status (SRC only, nullable)
	if row.CopyStatus.Valid {
		ns.CopyStatus = row.CopyStatus.String
	}

	return ns, nil
}

// MigrateDuckToBolt migrates data from DuckDB to BoltDB
func MigrateDuckToBolt(boltDB *db.DB, duckDBPath string) error {
	fmt.Println("[ETL] Starting DuckDB to BoltDB migration...")
	startTime := time.Now()

	duckDB, err := OpenDuckDB(duckDBPath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer duckDB.Close()

	// Migrate nodes (SRC and DST)
	fmt.Println("[ETL] Migrating SRC nodes...")
	if err := migrateNodesFromDuck(duckDB, boltDB, "SRC", defaultNumWorkersDuckToBolt); err != nil {
		return fmt.Errorf("failed to migrate SRC nodes: %w", err)
	}

	fmt.Println("[ETL] Migrating DST nodes...")
	if err := migrateNodesFromDuck(duckDB, boltDB, "DST", defaultNumWorkersDuckToBolt); err != nil {
		return fmt.Errorf("failed to migrate DST nodes: %w", err)
	}

	// Migrate stats
	fmt.Println("[ETL] Migrating stats...")
	if err := migrateStatsFromDuck(duckDB, boltDB); err != nil {
		return fmt.Errorf("failed to migrate stats: %w", err)
	}

	// Migrate queue stats
	fmt.Println("[ETL] Migrating queue stats...")
	if err := migrateQueueStatsFromDuck(duckDB, boltDB); err != nil {
		return fmt.Errorf("failed to migrate queue stats: %w", err)
	}

	// Migrate logs
	fmt.Println("[ETL] Migrating logs...")
	if err := migrateLogsFromDuck(duckDB, boltDB, defaultNumWorkersDuckToBolt); err != nil {
		return fmt.Errorf("failed to migrate logs: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[ETL] Migration completed in %v\n", elapsed.Round(time.Second))

	return nil
}

// migrateNodesFromDuck migrates nodes from DuckDB to BoltDB using streaming worker pools
func migrateNodesFromDuck(duckDB *DuckDB, boltDB *db.DB, queueType string, numWorkers int) error {
	// Determine table name
	tableName := "src_nodes"
	if queueType == "DST" {
		tableName = "dst_nodes"
	}

	// Create buffer for this queue type (reuse NodeRow from bolt_to_duck.go)
	buffer := NewNodeBuffer(tableName)

	// Create stats tracker
	stats := newETLStats(queueType)

	// Channel to stream batches of node IDs to workers
	idBatches := make(chan []string, numWorkers*2)

	var wg sync.WaitGroup
	var workerErr error
	var workerErrMu sync.Mutex

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range idBatches {
				if err := migrateNodesWorkerFromDuck(duckDB.db, batch, queueType, tableName, buffer, stats); err != nil {
					workerErrMu.Lock()
					if workerErr == nil {
						workerErr = err
					}
					workerErrMu.Unlock()
				}
			}
		}()
	}

	// Start writer goroutine
	writerDone := make(chan error, 1)
	workersDone := make(chan struct{})
	go func() {
		writerDone <- migrateNodesWriterFromDuck(buffer, boltDB, queueType, stats, workersDone)
	}()

	// Stream node IDs in batches from DuckDB
	query := fmt.Sprintf("SELECT id FROM %s", tableName)
	rows, err := duckDB.db.Query(query)
	if err != nil {
		close(idBatches)
		wg.Wait()
		close(workersDone)
		<-writerDone
		return fmt.Errorf("failed to query node IDs: %w", err)
	}

	var batch []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			rows.Close()
			close(idBatches)
			wg.Wait()
			close(workersDone)
			<-writerDone
			return fmt.Errorf("failed to scan node ID: %w", err)
		}

		batch = append(batch, nodeID)

		if len(batch) >= streamBatchSizeDuckToBolt {
			idBatches <- batch
			batch = make([]string, 0, streamBatchSizeDuckToBolt)
		}
	}
	rows.Close()

	// Send remaining batch
	if len(batch) > 0 {
		idBatches <- batch
	}

	close(idBatches)

	// Wait for workers to finish
	wg.Wait()
	close(workersDone)

	// Wait for writer to finish
	if err := <-writerDone; err != nil {
		return fmt.Errorf("writer error: %w", err)
	}

	if workerErr != nil {
		return fmt.Errorf("worker error: %w", workerErr)
	}

	stats.report()
	totalFlushed := stats.GetTotalFlushed()
	fmt.Printf("[ETL %s] Completed: %d nodes migrated\n", queueType, totalFlushed)

	return nil
}

// migrateNodesWorkerFromDuck processes a batch of node IDs and adds rows to buffer
func migrateNodesWorkerFromDuck(duckDB *sql.DB, nodeIDs []string, queueType string, tableName string, buffer *nodeBuffer, stats *ETLStats) error {
	processed := int64(0)
	defer func() {
		stats.AddProcessed(processed)
	}()

	if len(nodeIDs) == 0 {
		return nil
	}

	// Build query with IN clause - select appropriate join column based on table
	// src_nodes has dst_id, dst_nodes has src_id
	var joinColumn string
	if tableName == "src_nodes" {
		joinColumn = "dst_id"
	} else {
		joinColumn = "src_id"
	}

	query := fmt.Sprintf(`
		SELECT id, service_id, parent_id, parent_service_id, parent_path, name, path, path_hash,
		       child_ids, type, size, mtime, depth, traversal_status, copy_status, %s
		FROM %s
		WHERE id IN (`, joinColumn, tableName)

	args := make([]interface{}, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args[i] = nodeID
	}
	query += ")"

	rows, err := duckDB.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query nodes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row DuckNodeRow
		var joinID sql.NullString

		err := rows.Scan(
			&row.ID,
			&row.ServiceID,
			&row.ParentID,
			&row.ParentServiceID,
			&row.ParentPath,
			&row.Name,
			&row.Path,
			&row.PathHash,
			&row.ChildIDs,
			&row.Type,
			&row.Size,
			&row.MTime,
			&row.Depth,
			&row.TraversalStatus,
			&row.CopyStatus,
			&joinID,
		)
		if err != nil {
			continue // Skip invalid rows
		}

		// Set the join ID in the appropriate field based on queue type
		if joinID.Valid {
			joinIDStr := joinID.String
			if queueType == "SRC" {
				row.DstID = sql.NullString{String: joinIDStr, Valid: true}
			} else {
				row.SrcID = sql.NullString{String: joinIDStr, Valid: true}
			}
		}

		// Store original traversal_status (we'll parse it again in the writer to get exclusion flags)
		// Convert to NodeRow for buffer (reuse NodeRow struct from bolt_to_duck.go)
		nodeRow := NodeRow{
			ID:              row.ID,
			ServiceID:       row.ServiceID,
			ParentID:        "",
			ParentServiceID: "",
			ParentPath:      "",
			Name:            row.Name,
			Path:            row.Path,
			PathHash:        row.PathHash,
			ChildIDs:        "",
			Type:            row.Type,
			Size:            nil,
			MTime:           row.MTime,
			Depth:           int(row.Depth),
			TraversalStatus: row.TraversalStatus, // Store original status, will parse in writer
			CopyStatus:      "",
			DstID:           nil,
			SrcID:           nil,
		}

		// Handle nullable fields
		if row.ParentID.Valid {
			nodeRow.ParentID = row.ParentID.String
		}
		if row.ParentServiceID.Valid {
			nodeRow.ParentServiceID = row.ParentServiceID.String
		}
		if row.ParentPath.Valid {
			nodeRow.ParentPath = row.ParentPath.String
		}
		if row.ChildIDs.Valid {
			nodeRow.ChildIDs = row.ChildIDs.String
		}
		if row.Size.Valid {
			sizeVal := row.Size.Int64
			nodeRow.Size = &sizeVal
		}
		if row.CopyStatus.Valid {
			nodeRow.CopyStatus = row.CopyStatus.String
		}
		if row.DstID.Valid {
			dstIDVal := row.DstID.String
			nodeRow.DstID = &dstIDVal
		}
		if row.SrcID.Valid {
			srcIDVal := row.SrcID.String
			nodeRow.SrcID = &srcIDVal
		}

		// Store NodeState in a way the writer can access it
		// We'll need to modify the buffer or pass NodeState separately
		// For now, let's create a struct that holds both
		buffer.Add(nodeRow)
		processed++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}

// nodeRowWithState holds both NodeRow and NodeState for the writer
type nodeRowWithState struct {
	Row   NodeRow
	State *db.NodeState
}

// migrateNodesWriterFromDuck periodically flushes the buffer when threshold is reached
func migrateNodesWriterFromDuck(buffer *nodeBuffer, boltDB *db.DB, queueType string, stats *ETLStats, workersDone <-chan struct{}) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-workersDone:
			// Workers are done, flush any remaining data before exiting
			for {
				if buffer.IsFlushing() {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				batch := buffer.GetAndClear()
				if batch == nil || len(batch) == 0 {
					return nil
				}
				buffer.SetFlushing(true)

				// Flush synchronously
				if err := flushNodeBatchToBolt(boltDB, batch, queueType, stats); err != nil {
					buffer.SetFlushing(false)
					return err
				}

				buffer.SetFlushing(false)
			}
		case <-ticker.C:
			// Snapshot + unlock + flush synchronously
			batch := buffer.GetAndClearIfReady()
			if batch == nil {
				// Report stats periodically
				stats.report()
				continue
			}

			// Flush synchronously
			if err := flushNodeBatchToBolt(boltDB, batch, queueType, stats); err != nil {
				buffer.SetFlushing(false)
				return err
			}

			buffer.SetFlushing(false)

			// Report stats periodically
			stats.report()
		}
	}
}

// flushNodeBatchToBolt flushes a batch of node rows to BoltDB
// Chunks the batch to avoid long-running transactions
// Writes all buckets in one transaction per chunk: nodes, children, join-lookup, path-to-ulid, status buckets, copy status buckets
func flushNodeBatchToBolt(boltDB *db.DB, batch []NodeRow, queueType string, stats *ETLStats) error {
	if len(batch) == 0 {
		return nil
	}

	const chunkSize = 10_000

	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[i:end]

		if err := boltDB.Update(func(tx *bolt.Tx) error {
			// Get buckets (should already exist after DB initialization)
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found for %s", queueType)
			}

			childrenBucket := db.GetChildrenBucket(tx, queueType)
			if childrenBucket == nil {
				return fmt.Errorf("children bucket not found for %s", queueType)
			}

			// Get join-lookup buckets
			var srcToDstBucket, dstToSrcBucket *bolt.Bucket
			var err error
			if queueType == "SRC" {
				srcToDstBucket, err = db.GetOrCreateSrcToDstBucket(tx)
				if err != nil {
					return fmt.Errorf("failed to get src-to-dst bucket: %w", err)
				}
			} else {
				dstToSrcBucket, err = db.GetOrCreateDstToSrcBucket(tx)
				if err != nil {
					return fmt.Errorf("failed to get dst-to-src bucket: %w", err)
				}
			}

			// Get path-to-ulid bucket
			pathBucket, err := db.EnsurePathToULIDBucket(tx, queueType)
			if err != nil {
				return fmt.Errorf("failed to get path-to-ulid bucket: %w", err)
			}

			for _, row := range chunk {
				// Parse traversal status to get exclusion flags and normalized status
				normalizedStatus, explicitExcluded, inheritedExcluded := parseTraversalStatus(row.TraversalStatus)

				// Build NodeState
				ns := &db.NodeState{
					ID:                row.ID,
					ServiceID:         row.ServiceID,
					ParentID:          row.ParentID,
					ParentServiceID:   row.ParentServiceID,
					ParentPath:        row.ParentPath,
					Name:              row.Name,
					Path:              row.Path,
					Type:              row.Type,
					MTime:             row.MTime,
					Depth:             row.Depth,
					TraversalStatus:   normalizedStatus,
					CopyStatus:        row.CopyStatus,
					ExplicitExcluded:  explicitExcluded,
					InheritedExcluded: inheritedExcluded,
				}

				// Handle size
				if row.Size != nil {
					ns.Size = *row.Size
				}

				nodeID := []byte(row.ID)

				// 1. Insert into nodes bucket
				nodeData, err := ns.Serialize()
				if err != nil {
					return fmt.Errorf("failed to serialize node: %w", err)
				}
				if err := nodesBucket.Put(nodeID, nodeData); err != nil {
					return fmt.Errorf("failed to insert node: %w", err)
				}

				// 2. Add to traversal status bucket
				statusBucket, err := db.GetOrCreateTraversalStatusBucket(tx, queueType, row.Depth, normalizedStatus)
				if err != nil {
					return fmt.Errorf("failed to get traversal status bucket: %w", err)
				}
				if err := statusBucket.Put(nodeID, []byte{}); err != nil {
					return fmt.Errorf("failed to add to traversal status bucket: %w", err)
				}

				// 3. Update traversal status-lookup index
				if err := db.UpdateTraversalStatusLookup(tx, queueType, row.Depth, nodeID, normalizedStatus); err != nil {
					return fmt.Errorf("failed to update traversal status-lookup: %w", err)
				}

				// 4. Write children bucket (from child_ids JSON)
				if row.ChildIDs != "" {
					var childIDs []string
					if err := json.Unmarshal([]byte(row.ChildIDs), &childIDs); err == nil && len(childIDs) > 0 {
						childrenData, err := db.SerializeStringSlice(childIDs)
						if err != nil {
							return fmt.Errorf("failed to serialize children: %w", err)
						}
						if err := childrenBucket.Put(nodeID, childrenData); err != nil {
							return fmt.Errorf("failed to write children bucket: %w", err)
						}
					}
				}

				// 5. Write join-lookup buckets
				if queueType == "SRC" && row.DstID != nil && *row.DstID != "" {
					if err := srcToDstBucket.Put(nodeID, []byte(*row.DstID)); err != nil {
						return fmt.Errorf("failed to write src-to-dst mapping: %w", err)
					}
				} else if queueType == "DST" && row.SrcID != nil && *row.SrcID != "" {
					if err := dstToSrcBucket.Put(nodeID, []byte(*row.SrcID)); err != nil {
						return fmt.Errorf("failed to write dst-to-src mapping: %w", err)
					}
				}

				// 6. Write path-to-ulid mapping
				if row.PathHash != "" {
					if err := pathBucket.Put([]byte(row.PathHash), nodeID); err != nil {
						return fmt.Errorf("failed to write path-to-ulid mapping: %w", err)
					}
				}

				// 7. Write copy status buckets (SRC only)
				if queueType == "SRC" && row.CopyStatus != "" {
					copyStatusBucket, err := db.GetOrCreateCopyStatusBucket(tx, row.Depth, row.CopyStatus)
					if err != nil {
						return fmt.Errorf("failed to get copy status bucket: %w", err)
					}
					if err := copyStatusBucket.Put(nodeID, []byte{}); err != nil {
						return fmt.Errorf("failed to add to copy status bucket: %w", err)
					}

					// Update copy status-lookup index
					if err := db.UpdateCopyStatusLookup(tx, row.Depth, nodeID, row.CopyStatus); err != nil {
						return fmt.Errorf("failed to update copy status-lookup: %w", err)
					}
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("failed to flush chunk: %w", err)
		}

		// Update stats after each chunk
		stats.AddFlushed(int64(len(chunk)))
	}

	return nil
}

// rebuildAuxiliaryStructures rebuilds children buckets, join-lookup tables, and path-to-ULID mappings
func rebuildAuxiliaryStructures(duckDB *sql.DB, boltDB *db.DB, queueType string, tableName string) error {
	// Query all nodes with their child_ids, path_hash, and join IDs
	var joinColumn string
	if tableName == "src_nodes" {
		joinColumn = "dst_id"
	} else {
		joinColumn = "src_id"
	}

	query := fmt.Sprintf(`
		SELECT id, parent_id, child_ids, path_hash, path, %s as join_id
		FROM %s`, joinColumn, tableName)

	rows, err := duckDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query nodes for auxiliary structures: %w", err)
	}
	defer rows.Close()

	type nodeAuxData struct {
		ID       string
		ParentID sql.NullString
		ChildIDs sql.NullString
		PathHash string
		Path     string
		JoinID   sql.NullString
	}

	var nodes []nodeAuxData
	for rows.Next() {
		var node nodeAuxData
		if err := rows.Scan(&node.ID, &node.ParentID, &node.ChildIDs, &node.PathHash, &node.Path, &node.JoinID); err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	rows.Close()

	// Rebuild in batches within transactions
	const batchSize = 10000
	for i := 0; i < len(nodes); i += batchSize {
		end := i + batchSize
		if end > len(nodes) {
			end = len(nodes)
		}
		batch := nodes[i:end]

		if err := boltDB.Update(func(tx *bolt.Tx) error {
			// Rebuild children buckets
			childrenBucket, err := getOrCreateChildrenBucket(tx, queueType)
			if err != nil {
				return fmt.Errorf("failed to get children bucket: %w", err)
			}

			// Group children by parent (from child_ids JSON)
			parentChildren := make(map[string][]string)
			for _, node := range batch {
				if node.ChildIDs.Valid && node.ChildIDs.String != "" {
					var childIDs []string
					if err := json.Unmarshal([]byte(node.ChildIDs.String), &childIDs); err == nil {
						// The node.ID is the parent, childIDs are its children
						parentChildren[node.ID] = childIDs
					}
				}
			}

			// Write children buckets
			for parentID, childIDs := range parentChildren {
				// Serialize and write
				childrenData, err := db.SerializeStringSlice(childIDs)
				if err != nil {
					return fmt.Errorf("failed to serialize children: %w", err)
				}
				if err := childrenBucket.Put([]byte(parentID), childrenData); err != nil {
					return fmt.Errorf("failed to write children bucket: %w", err)
				}
			}

			// Rebuild path-to-ULID mappings
			pathBucket, err := db.EnsurePathToULIDBucket(tx, queueType)
			if err != nil {
				return fmt.Errorf("failed to get path-to-ulid bucket: %w", err)
			}

			for _, node := range batch {
				if node.PathHash != "" {
					if err := pathBucket.Put([]byte(node.PathHash), []byte(node.ID)); err != nil {
						return fmt.Errorf("failed to write path-to-ulid mapping: %w", err)
					}
				}
			}

			// Rebuild join-lookup tables
			for _, node := range batch {
				if node.JoinID.Valid && node.JoinID.String != "" {
					if queueType == "SRC" {
						// SRC -> DST mapping
						srcToDstBucket, err := db.GetOrCreateSrcToDstBucket(tx)
						if err != nil {
							return fmt.Errorf("failed to get src-to-dst bucket: %w", err)
						}
						if err := srcToDstBucket.Put([]byte(node.ID), []byte(node.JoinID.String)); err != nil {
							return fmt.Errorf("failed to write src-to-dst mapping: %w", err)
						}
					} else {
						// DST -> SRC mapping
						dstToSrcBucket, err := db.GetOrCreateDstToSrcBucket(tx)
						if err != nil {
							return fmt.Errorf("failed to get dst-to-src bucket: %w", err)
						}
						if err := dstToSrcBucket.Put([]byte(node.ID), []byte(node.JoinID.String)); err != nil {
							return fmt.Errorf("failed to write dst-to-src mapping: %w", err)
						}
					}
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("failed to rebuild auxiliary structures: %w", err)
		}
	}

	return nil
}

// getOrCreateChildrenBucket gets or creates the children bucket
func getOrCreateChildrenBucket(tx *bolt.Tx, queueType string) (*bolt.Bucket, error) {
	traversalBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalBucket == nil {
		return nil, fmt.Errorf("Traversal-Data bucket not found")
	}
	topBucket := traversalBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return nil, fmt.Errorf("queue bucket %s not found", queueType)
	}
	return topBucket.CreateBucketIfNotExists([]byte("children"))
}

// rebuildCopyStatusBuckets rebuilds copy status buckets for SRC nodes
func rebuildCopyStatusBuckets(duckDB *sql.DB, boltDB *db.DB) error {
	// Query SRC nodes with their copy_status and depth
	query := `SELECT id, depth, copy_status FROM src_nodes WHERE copy_status IS NOT NULL`

	rows, err := duckDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query copy status: %w", err)
	}
	defer rows.Close()

	type copyStatusData struct {
		ID         string
		Depth      int32
		CopyStatus string
	}

	var nodes []copyStatusData
	for rows.Next() {
		var node copyStatusData
		if err := rows.Scan(&node.ID, &node.Depth, &node.CopyStatus); err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	rows.Close()

	// Group by depth and copy_status for batch processing
	statusMap := make(map[int]map[string][]string) // depth -> copy_status -> []nodeIDs
	for _, node := range nodes {
		depth := int(node.Depth)
		if statusMap[depth] == nil {
			statusMap[depth] = make(map[string][]string)
		}
		statusMap[depth][node.CopyStatus] = append(statusMap[depth][node.CopyStatus], node.ID)
	}

	// Rebuild copy status buckets
	return boltDB.Update(func(tx *bolt.Tx) error {
		for depth, statusGroups := range statusMap {
			for copyStatus, nodeIDs := range statusGroups {
				// Get or create copy status bucket
				copyStatusBucket, err := db.GetOrCreateCopyStatusBucket(tx, depth, copyStatus)
				if err != nil {
					return fmt.Errorf("failed to get copy status bucket: %w", err)
				}

				// Add nodes to copy status bucket
				for _, nodeID := range nodeIDs {
					if err := copyStatusBucket.Put([]byte(nodeID), []byte{}); err != nil {
						return fmt.Errorf("failed to add node to copy status bucket: %w", err)
					}
				}

				// Update copy status-lookup index
				copyLookupBucket, err := db.GetOrCreateCopyStatusLookupBucket(tx, depth)
				if err != nil {
					return fmt.Errorf("failed to get copy status-lookup bucket: %w", err)
				}

				for _, nodeID := range nodeIDs {
					if err := copyLookupBucket.Put([]byte(nodeID), []byte(copyStatus)); err != nil {
						return fmt.Errorf("failed to update copy status-lookup: %w", err)
					}
				}
			}
		}
		return nil
	})
}

// migrateStatsFromDuck migrates stats from DuckDB to BoltDB
func migrateStatsFromDuck(duckDB *DuckDB, boltDB *db.DB) error {
	query := `SELECT bucket_path, count FROM stats`
	rows, err := duckDB.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query stats: %w", err)
	}
	defer rows.Close()

	return boltDB.Update(func(tx *bolt.Tx) error {
		statsBucket, err := getStatsBucket(tx)
		if err != nil {
			return fmt.Errorf("failed to get stats bucket: %w", err)
		}

		for rows.Next() {
			var bucketPath string
			var count int64

			if err := rows.Scan(&bucketPath, &count); err != nil {
				continue
			}

			// Convert count to 8-byte big-endian format
			valueBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(valueBytes, uint64(count))

			if err := statsBucket.Put([]byte(bucketPath), valueBytes); err != nil {
				return fmt.Errorf("failed to write stats entry: %w", err)
			}
		}

		return rows.Err()
	})
}

// getStatsBucket is a helper to get the stats bucket
// Stats are stored directly in Traversal-Data/STATS bucket (not in a totals sub-bucket)
func getStatsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	traversalBucket, err := tx.CreateBucketIfNotExists([]byte("Traversal-Data"))
	if err != nil {
		return nil, fmt.Errorf("failed to get Traversal-Data bucket: %w", err)
	}
	bucket, err := traversalBucket.CreateBucketIfNotExists([]byte("STATS"))
	if err != nil {
		return nil, fmt.Errorf("failed to get stats bucket: %w", err)
	}
	// Stats are stored directly in the STATS bucket, not in a totals sub-bucket
	return bucket, nil
}

// migrateQueueStatsFromDuck migrates queue stats from DuckDB to BoltDB
func migrateQueueStatsFromDuck(duckDB *DuckDB, boltDB *db.DB) error {
	query := `SELECT queue_key, metrics_json FROM queue_stats`
	rows, err := duckDB.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query queue_stats: %w", err)
	}
	defer rows.Close()

	return boltDB.Update(func(tx *bolt.Tx) error {
		queueStatsBucket, err := db.GetOrCreateQueueStatsBucket(tx)
		if err != nil {
			return fmt.Errorf("failed to get queue_stats bucket: %w", err)
		}

		for rows.Next() {
			var queueKey string
			var metricsJSON string

			if err := rows.Scan(&queueKey, &metricsJSON); err != nil {
				continue
			}

			if err := queueStatsBucket.Put([]byte(queueKey), []byte(metricsJSON)); err != nil {
				return fmt.Errorf("failed to write queue_stats entry: %w", err)
			}
		}

		return rows.Err()
	})
}

// migrateLogsFromDuck migrates logs from DuckDB to BoltDB
func migrateLogsFromDuck(duckDB *DuckDB, boltDB *db.DB, numWorkers int) error {
	// Create buffer for logs
	buffer := NewLogBuffer()

	// Create stats tracker
	stats := newETLStats("logs")

	// Channel to stream batches of log keys to workers
	type logKeyBatch struct {
		keys   []string
		rowNum int // Starting row number for this batch
	}
	keyBatches := make(chan logKeyBatch, numWorkers*2)

	var wg sync.WaitGroup
	var workerErr error
	var workerErrMu sync.Mutex

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range keyBatches {
				if err := migrateLogsWorkerFromDuck(duckDB.db, batch.keys, batch.rowNum, buffer, stats); err != nil {
					workerErrMu.Lock()
					if workerErr == nil {
						workerErr = err
					}
					workerErrMu.Unlock()
				}
			}
		}()
	}

	// Start writer goroutine
	writerDone := make(chan error, 1)
	workersDone := make(chan struct{})
	go func() {
		writerDone <- migrateLogsWriterFromDuck(buffer, boltDB, stats, workersDone)
	}()

	// Stream log IDs in batches from DuckDB (ordered by timestamp or row_num if available)
	query := `SELECT id FROM logs ORDER BY timestamp`
	rows, err := duckDB.db.Query(query)
	if err != nil {
		close(keyBatches)
		wg.Wait()
		close(workersDone)
		<-writerDone
		return fmt.Errorf("failed to query log IDs: %w", err)
	}

	var batch []string
	rowNum := 0
	for rows.Next() {
		var logID string
		if err := rows.Scan(&logID); err != nil {
			rows.Close()
			close(keyBatches)
			wg.Wait()
			close(workersDone)
			<-writerDone
			return fmt.Errorf("failed to scan log ID: %w", err)
		}

		batch = append(batch, logID)

		if len(batch) >= streamBatchSizeDuckToBolt {
			keyBatches <- logKeyBatch{keys: batch, rowNum: rowNum}
			rowNum += len(batch)
			batch = make([]string, 0, streamBatchSizeDuckToBolt)
		}
	}
	rows.Close()

	// Send remaining batch
	if len(batch) > 0 {
		keyBatches <- logKeyBatch{keys: batch, rowNum: rowNum}
	}

	close(keyBatches)

	// Wait for workers to finish
	wg.Wait()
	close(workersDone)

	// Wait for writer to finish
	if err := <-writerDone; err != nil {
		return fmt.Errorf("writer error: %w", err)
	}

	if workerErr != nil {
		return fmt.Errorf("worker error: %w", workerErr)
	}

	stats.report()
	totalFlushed := stats.GetTotalFlushed()
	fmt.Printf("[ETL logs] Completed: %d logs migrated\n", totalFlushed)

	return nil
}

// migrateLogsWorkerFromDuck processes a batch of log IDs and adds entries to buffer
func migrateLogsWorkerFromDuck(duckDB *sql.DB, logIDs []string, startRowNum int, buffer *logBuffer, stats *ETLStats) error {
	processed := int64(0)
	defer func() {
		stats.AddProcessed(processed)
	}()

	if len(logIDs) == 0 {
		return nil
	}

	// Build query with IN clause
	query := `SELECT id, timestamp, level, entity, entity_id, message, queue FROM logs WHERE id IN (`
	args := make([]interface{}, len(logIDs))
	for i, logID := range logIDs {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args[i] = logID
	}
	query += ") ORDER BY timestamp"

	rows, err := duckDB.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	rowNum := startRowNum
	for rows.Next() {
		var entry db.LogEntry
		err := rows.Scan(
			&entry.ID,
			&entry.Timestamp,
			&entry.Level,
			&entry.Entity,
			&entry.EntityID,
			&entry.Message,
			&entry.Queue,
		)
		if err != nil {
			continue // Skip invalid rows
		}

		buffer.Add(LogRow{
			Entry:  &entry,
			RowNum: rowNum,
		})
		rowNum++
		processed++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}

// migrateLogsWriterFromDuck periodically flushes the buffer when threshold is reached
func migrateLogsWriterFromDuck(buffer *logBuffer, boltDB *db.DB, stats *ETLStats, workersDone <-chan struct{}) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-workersDone:
			// Workers are done, flush any remaining data before exiting
			for {
				if buffer.IsFlushing() {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				batch := buffer.GetAndClearSorted()
				if batch == nil || len(batch) == 0 {
					return nil
				}
				buffer.SetFlushing(true)

				// Flush synchronously
				if err := flushLogBatchToBolt(boltDB, batch, stats); err != nil {
					buffer.SetFlushing(false)
					return err
				}

				buffer.SetFlushing(false)
			}
		case <-ticker.C:
			// Snapshot + unlock + flush synchronously
			batch := buffer.GetAndClearSortedIfReady()
			if batch == nil {
				// Report stats periodically
				stats.report()
				continue
			}

			// Flush synchronously
			if err := flushLogBatchToBolt(boltDB, batch, stats); err != nil {
				buffer.SetFlushing(false)
				return err
			}

			buffer.SetFlushing(false)

			// Report stats periodically
			stats.report()
		}
	}
}

// flushLogBatchToBolt flushes a batch of log entries to BoltDB
func flushLogBatchToBolt(boltDB *db.DB, batch []LogRow, stats *ETLStats) error {
	if len(batch) == 0 {
		return nil
	}

	return boltDB.Update(func(tx *bolt.Tx) error {
		for _, logRow := range batch {
			entry := logRow.Entry
			if entry == nil {
				continue
			}

			// Get or create log level bucket
			levelBucket, err := db.GetOrCreateLogLevelBucket(tx, entry.Level)
			if err != nil {
				return fmt.Errorf("failed to get log level bucket: %w", err)
			}

			// Serialize log entry
			data, err := db.SerializeLogEntry(*entry)
			if err != nil {
				return fmt.Errorf("failed to serialize log entry: %w", err)
			}

			// Write to bucket
			if err := levelBucket.Put([]byte(entry.ID), data); err != nil {
				return fmt.Errorf("failed to write log entry: %w", err)
			}
		}

		stats.AddFlushed(int64(len(batch)))

		return nil
	})
}
