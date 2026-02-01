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

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "github.com/marcboeker/go-duckdb"
	bolt "go.etcd.io/bbolt"
)

const (
	// Default number of workers per queue type for DuckDB reads
	defaultNumWorkersDuckToBolt = 8
	// Stream batch size for distributing work to workers
	streamBatchSizeDuckToBolt = 10000
)

// DuckNodeRow represents a node row read from DuckDB (primary UI table; path_hash removed, computed from path when writing back to Bolt).
type DuckNodeRow struct {
	ID              string
	ServiceID       string
	ParentID        sql.NullString
	ParentServiceID sql.NullString
	ParentPath      sql.NullString
	Name            string
	Path            string
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


	elapsed := time.Since(startTime)
	fmt.Printf("[ETL] Migration completed in %v\n", elapsed.Round(time.Second))

	return nil
}

// migrateNodesFromDuck migrates nodes from DuckDB to BoltDB using streaming worker pools.
// Reads from primary UI table {prefix}_nodes_ui and children table {prefix}_nodes_children.
func migrateNodesFromDuck(duckDB *DuckDB, boltDB *db.DB, queueType string, numWorkers int) error {
	// Compute table prefix from queue type
	prefix := "src"
	if queueType == "DST" {
		prefix = "dst"
	}

	buffer := NewNodeBuffer(prefix + "_nodes_ui")
	stats := newETLStats(queueType)

	idBatches := make(chan []string, numWorkers*2)

	var wg sync.WaitGroup
	var workerErr error
	var workerErrMu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range idBatches {
				if err := migrateNodesWorkerFromDuck(duckDB.db, batch, queueType, prefix, buffer, stats); err != nil {
					workerErrMu.Lock()
					if workerErr == nil {
						workerErr = err
					}
					workerErrMu.Unlock()
				}
			}
		}()
	}

	writerDone := make(chan error, 1)
	workersDone := make(chan struct{})
	go func() {
		writerDone <- migrateNodesWriterFromDuck(buffer, boltDB, queueType, stats, workersDone)
	}()

	// Stream node IDs from {prefix}_nodes_ui
	query := fmt.Sprintf("SELECT id FROM %s_nodes_ui", prefix)
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

// migrateNodesWorkerFromDuck processes a batch of node IDs by reading from primary UI table and joining children.
func migrateNodesWorkerFromDuck(duckDB *sql.DB, nodeIDs []string, queueType string, prefix string, buffer *nodeBuffer, stats *ETLStats) error {
	processed := int64(0)
	defer func() {
		stats.AddProcessed(processed)
	}()

	if len(nodeIDs) == 0 {
		return nil
	}

	// SELECT from {prefix}_nodes_ui u LEFT JOIN {prefix}_nodes_children ch ON u.path = ch.path
	query := fmt.Sprintf(`
		SELECT u.id, u.service_id, u.parent_id, u.parent_service_id, u.parent_path, u.name, u.path,
		       ch.child_ids, u.type, u.size, u.mtime, u.depth, u.traversal_status, u.copy_status, u.join_id
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
		return fmt.Errorf("failed to query nodes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row DuckNodeRow
		var joinID sql.NullString
		var childIDs sql.NullString
		var size sql.NullInt64
		var copyStatus sql.NullString

		err := rows.Scan(
			&row.ID,
			&row.ServiceID,
			&row.ParentID,
			&row.ParentServiceID,
			&row.ParentPath,
			&row.Name,
			&row.Path,
			&childIDs,
			&row.Type,
			&size,
			&row.MTime,
			&row.Depth,
			&row.TraversalStatus,
			&copyStatus,
			&joinID,
		)
		if err != nil {
			continue
		}
		row.ChildIDs = childIDs
		row.Size = size
		row.CopyStatus = copyStatus
		if joinID.Valid {
			if queueType == "SRC" {
				row.DstID = sql.NullString{String: joinID.String, Valid: true}
			} else {
				row.SrcID = sql.NullString{String: joinID.String, Valid: true}
			}
		}

		nodeRow := NodeRow{
			ID:              row.ID,
			ServiceID:       row.ServiceID,
			ParentID:        "",
			ParentServiceID: "",
			ParentPath:      "",
			Name:            row.Name,
			Path:            row.Path,
			ChildIDs:        "",
			Type:            row.Type,
			Size:            nil,
			MTime:           row.MTime,
			Depth:           int(row.Depth),
			TraversalStatus: row.TraversalStatus,
			CopyStatus:      "",
			DstID:           nil,
			SrcID:           nil,
		}
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
			v := row.DstID.String
			nodeRow.DstID = &v
		}
		if row.SrcID.Valid {
			v := row.SrcID.String
			nodeRow.SrcID = &v
		}
		buffer.Add(nodeRow)
		processed++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}
	return nil
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
				if len(batch) == 0 {
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
// Writes all buckets in one transaction per chunk: nodes, children, join-lookup, status buckets, copy status buckets
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
			// Group rows by level (row.Depth) — plan: write into levels/<level>/ nodes, children, join
			depthToRows := make(map[int][]NodeRow)
			for _, row := range chunk {
				depthToRows[row.Depth] = append(depthToRows[row.Depth], row)
			}

			for level, rowsAtLevel := range depthToRows {
				if err := db.EnsureLevelBucket(tx, queueType, level); err != nil {
					return fmt.Errorf("ensure level %d: %w", level, err)
				}

				nodesBucket := db.GetNodesBucket(tx, queueType, level)
				if nodesBucket == nil {
					return fmt.Errorf("nodes bucket not found for %s level %d", queueType, level)
				}

				childrenBucket := db.GetChildrenBucket(tx, queueType, level)
				if childrenBucket == nil {
					return fmt.Errorf("children bucket not found for %s level %d", queueType, level)
				}

				var srcToDstBucket, dstToSrcBucket *bolt.Bucket
				var err error
				if queueType == "SRC" {
					srcToDstBucket, err = db.GetOrCreateSrcToDstBucket(tx, level)
					if err != nil {
						return fmt.Errorf("failed to get src-to-dst bucket: %w", err)
					}
				} else {
					dstToSrcBucket, err = db.GetOrCreateDstToSrcBucket(tx, level)
					if err != nil {
						return fmt.Errorf("failed to get dst-to-src bucket: %w", err)
					}
				}

				for _, row := range rowsAtLevel {
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

				// 6. Write copy status buckets (SRC only)
				if queueType == "SRC" && row.CopyStatus != "" {
					// Determine node type from row.Type
					nodeType := db.NodeTypeFile
					if row.Type == "folder" {
						nodeType = db.NodeTypeFolder
					}

					copyStatusBucket, err := db.GetOrCreateCopyStatusBucket(tx, row.Depth, nodeType, row.CopyStatus)
					if err != nil {
						return fmt.Errorf("failed to get copy status bucket: %w", err)
					}

					// Check if node already exists in bucket (to avoid double-counting stats)
					alreadyExists := copyStatusBucket.Get(nodeID) != nil
					if err := copyStatusBucket.Put(nodeID, []byte{}); err != nil {
						return fmt.Errorf("failed to add to copy status bucket: %w", err)
					}

					// Update stats only if this is a new entry
					if !alreadyExists {
						bucketPath := db.GetCopyStatusBucketPath(row.Depth, nodeType, row.CopyStatus)
						if err := db.UpdateBucketStatsInTx(tx, bucketPath, 1); err != nil {
							return fmt.Errorf("failed to update copy status stats: %w", err)
						}
					}

					// Update copy status-lookup index (no node type needed in lookup)
					if err := db.UpdateCopyStatusLookup(tx, row.Depth, nodeID, row.CopyStatus); err != nil {
						return fmt.Errorf("failed to update copy status-lookup: %w", err)
					}
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
