// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package etl

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/marcboeker/go-duckdb"
	bolt "go.etcd.io/bbolt"
)

const (
	// Default number of workers per queue type
	defaultNumWorkers = 8
	// Batch size for DuckDB inserts (rows per transaction)
	defaultBatchSize = 100000
	// Stream batch size for distributing work to workers
	streamBatchSize = 10000
	// Performance reporting interval
	statsReportInterval = 3 * time.Second
)

// DuckDB wraps DuckDB connection with mutex protection
type DuckDB struct {
	db     *sql.DB
	conn   driver.Conn
	mu     sync.Mutex
	dbPath string
}

// OpenDuckDB opens or creates a DuckDB database
func OpenDuckDB(dbPath string) (*DuckDB, error) {
	// Create connector first - this will be used for both sql.DB and Appender API
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}

	// Use the connector to open sql.DB
	sqlDB := sql.OpenDB(connector)

	// Get a connection from sql.DB for the Appender API
	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		sqlDB.Close()
		connector.Close()
		return nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}

	return &DuckDB{
		db:     sqlDB,
		conn:   conn,
		dbPath: dbPath,
	}, nil
}

// Close closes the DuckDB connection
func (d *DuckDB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var errs []error
	if d.conn != nil {
		if err := d.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		d.conn = nil
	}
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			errs = append(errs, err)
		}
		d.db = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing DuckDB: %v", errs)
	}
	return nil
}

// NodeRow represents a single node row for migration
type NodeRow struct {
	ID              string
	ServiceID       string
	ParentID        string
	ParentServiceID string
	ParentPath      string
	Name            string
	Path            string
	PathHash        string
	ChildIDs        string // JSON array as string
	Type            string
	Size            *int64
	MTime           string
	Depth           int
	TraversalStatus string
	CopyStatus      string
	DstID           *string // For SRC nodes (nullable)
	SrcID           *string // For DST nodes (nullable)
}

// LogRow represents a log entry with row number for ordering
type LogRow struct {
	Entry  *db.LogEntry
	RowNum int
}

// ETLStats tracks migration performance
type ETLStats struct {
	mu             sync.RWMutex
	QueueType      string
	TotalProcessed int64
	TotalFlushed   int64
	StartTime      time.Time
	LastReportTime time.Time
	LastFlushTime  time.Time
}

// newETLStats creates a new stats tracker
func newETLStats(queueType string) *ETLStats {
	now := time.Now()
	return &ETLStats{
		QueueType:      queueType,
		StartTime:      now,
		LastReportTime: now,
		LastFlushTime:  now,
	}
}

// Getters and setters for ETLStats (all thread-safe)

// GetQueueType returns the queue type
func (s *ETLStats) GetQueueType() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.QueueType
}

// GetTotalProcessed returns the total processed count
func (s *ETLStats) GetTotalProcessed() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalProcessed
}

// SetTotalProcessed sets the total processed count
func (s *ETLStats) SetTotalProcessed(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalProcessed = count
}

// AddProcessed increments processed count
func (s *ETLStats) AddProcessed(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalProcessed += count
}

// GetTotalFlushed returns the total flushed count
func (s *ETLStats) GetTotalFlushed() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.TotalFlushed
}

// SetTotalFlushed sets the total flushed count
func (s *ETLStats) SetTotalFlushed(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalFlushed = count
}

// AddFlushed increments flushed count
func (s *ETLStats) AddFlushed(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalFlushed += count
	s.LastFlushTime = time.Now()
}

// GetStartTime returns the start time
func (s *ETLStats) GetStartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.StartTime
}

// GetLastReportTime returns the last report time
func (s *ETLStats) GetLastReportTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastReportTime
}

// SetLastReportTime sets the last report time
func (s *ETLStats) SetLastReportTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastReportTime = t
}

// GetLastFlushTime returns the last flush time
func (s *ETLStats) GetLastFlushTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastFlushTime
}

// report prints performance stats
func (s *ETLStats) report() {
	now := time.Now()

	// Check if we should report (quick check without holding lock)
	lastReportTime := s.GetLastReportTime()
	timeSinceLastReport := now.Sub(lastReportTime)
	if timeSinceLastReport < statsReportInterval {
		return
	}

	// Get all stats we need for reporting
	startTime := s.GetStartTime()
	elapsed := now.Sub(startTime)
	totalProcessed := s.GetTotalProcessed()
	totalFlushed := s.GetTotalFlushed()
	queueType := s.GetQueueType()

	// Calculate rates and print (no locks held during I/O)
	processedRate := float64(totalProcessed) / elapsed.Seconds()
	flushedRate := float64(totalFlushed) / elapsed.Seconds()

	fmt.Printf("[ETL %s] Processed: %d (%.0f/sec) | Flushed: %d (%.0f/sec) | Elapsed: %v\n",
		queueType, totalProcessed, processedRate, totalFlushed, flushedRate, elapsed.Round(time.Second))

	// Update LastReportTime
	s.SetLastReportTime(now)
}

// MigrateBoltToDuck migrates data from BoltDB to DuckDB
func MigrateBoltToDuck(boltDB *db.DB, duckDBPath string, overwrite bool) error {
	fmt.Println("[ETL] Starting BoltDB to DuckDB migration...")
	startTime := time.Now()

	duckDB, err := OpenDuckDB(duckDBPath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer duckDB.Close()

	// Drop existing tables if overwrite is true
	if overwrite {
		fmt.Println("[ETL] Dropping existing tables...")
		if err := dropTables(duckDB.db); err != nil {
			return fmt.Errorf("failed to drop existing tables: %w", err)
		}
	}

	// Create tables without indexes
	fmt.Println("[ETL] Creating tables...")
	if err := createTables(duckDB.db); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// Migrate nodes (SRC and DST)
	fmt.Println("[ETL] Migrating SRC nodes...")
	if err := migrateNodes(boltDB, duckDB, "SRC", defaultNumWorkers); err != nil {
		return fmt.Errorf("failed to migrate SRC nodes: %w", err)
	}

	fmt.Println("[ETL] Migrating DST nodes...")
	if err := migrateNodes(boltDB, duckDB, "DST", defaultNumWorkers); err != nil {
		return fmt.Errorf("failed to migrate DST nodes: %w", err)
	}

	// Migrate stats
	fmt.Println("[ETL] Migrating stats...")
	if err := migrateStats(boltDB, duckDB); err != nil {
		return fmt.Errorf("failed to migrate stats: %w", err)
	}

	// Migrate queue stats
	fmt.Println("[ETL] Migrating queue stats...")
	if err := migrateQueueStats(boltDB, duckDB); err != nil {
		return fmt.Errorf("failed to migrate queue stats: %w", err)
	}

	// Migrate logs
	fmt.Println("[ETL] Migrating logs...")
	if err := migrateLogs(boltDB, duckDB, defaultNumWorkers); err != nil {
		return fmt.Errorf("failed to migrate logs: %w", err)
	}

	// Create indexes after all data is loaded
	fmt.Println("[ETL] Creating indexes...")
	if err := createIndexes(duckDB.db); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[ETL] Migration completed in %v\n", elapsed.Round(time.Second))

	return nil
}

// dropTables drops all tables if they exist
func dropTables(db *sql.DB) error {
	tables := []string{"src_nodes", "dst_nodes", "stats", "queue_stats", "logs"}
	for _, table := range tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}
	return nil
}

// createTables creates all table schemas without indexes
func createTables(db *sql.DB) error {
	// Create src_nodes table
	srcNodesDDL := `
		CREATE TABLE src_nodes (
			id VARCHAR PRIMARY KEY,
			service_id VARCHAR,
			parent_id VARCHAR,
			parent_service_id VARCHAR,
			parent_path VARCHAR,
			name VARCHAR,
			path VARCHAR,
			path_hash VARCHAR,
			child_ids VARCHAR,
			type VARCHAR,
			size BIGINT,
			mtime VARCHAR,
			depth INTEGER,
			traversal_status VARCHAR,
			copy_status VARCHAR,
			dst_id VARCHAR
		)
	`
	if _, err := db.Exec(srcNodesDDL); err != nil {
		return fmt.Errorf("failed to create src_nodes table: %w", err)
	}

	// Create dst_nodes table
	dstNodesDDL := `
		CREATE TABLE dst_nodes (
			id VARCHAR PRIMARY KEY,
			service_id VARCHAR,
			parent_id VARCHAR,
			parent_service_id VARCHAR,
			parent_path VARCHAR,
			name VARCHAR,
			path VARCHAR,
			path_hash VARCHAR,
			child_ids VARCHAR,
			type VARCHAR,
			size BIGINT,
			mtime VARCHAR,
			depth INTEGER,
			traversal_status VARCHAR,
			copy_status VARCHAR,
			src_id VARCHAR
		)
	`
	if _, err := db.Exec(dstNodesDDL); err != nil {
		return fmt.Errorf("failed to create dst_nodes table: %w", err)
	}

	// Create stats table
	statsDDL := `
		CREATE TABLE stats (
			bucket_path VARCHAR PRIMARY KEY,
			count BIGINT
		)
	`
	if _, err := db.Exec(statsDDL); err != nil {
		return fmt.Errorf("failed to create stats table: %w", err)
	}

	// Create queue_stats table
	queueStatsDDL := `
		CREATE TABLE queue_stats (
			queue_key VARCHAR PRIMARY KEY,
			metrics_json VARCHAR
		)
	`
	if _, err := db.Exec(queueStatsDDL); err != nil {
		return fmt.Errorf("failed to create queue_stats table: %w", err)
	}

	// Create logs table
	logsDDL := `
		CREATE TABLE logs (
			id VARCHAR PRIMARY KEY,
			timestamp VARCHAR,
			level VARCHAR,
			entity VARCHAR,
			entity_id VARCHAR,
			message VARCHAR,
			queue VARCHAR
		)
	`
	if _, err := db.Exec(logsDDL); err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}

	return nil
}

// transformTraversalStatus transforms NodeState status fields into traversal_status string
func transformTraversalStatus(ns *db.NodeState) string {
	// Check exclusion flags first
	if ns.ExplicitExcluded {
		return "excluded_explicit"
	}
	if ns.InheritedExcluded {
		return "excluded_inherited"
	}
	// Use traversal_status if set
	if ns.TraversalStatus != "" {
		return ns.TraversalStatus
	}
	// Fall back to legacy status field
	if ns.Status != "" {
		return ns.Status
	}
	return "pending"
}

// nodeBuffer holds rows for a specific queue type with mutex protection

// migrateNodes migrates nodes from BoltDB to DuckDB using streaming worker pools
func migrateNodes(boltDB *db.DB, duckDB *DuckDB, queueType string, numWorkers int) error {
	// Determine table name
	tableName := "src_nodes"
	if queueType == "DST" {
		tableName = "dst_nodes"
	}

	// Create buffer for this queue type
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
				if err := migrateNodesWorker(boltDB, batch, queueType, buffer, stats); err != nil {
					workerErrMu.Lock()
					if workerErr == nil {
						workerErr = err
					}
					workerErrMu.Unlock()
				}
			}
		}()
	}

	// Start writer with reusable appender
	duckDB.mu.Lock()
	appender, err := duckdb.NewAppenderFromConn(duckDB.conn, "", tableName)
	if err != nil {
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create appender: %w", err)
	}

	writerDone := make(chan error, 1)
	workersDone := make(chan struct{})
	go func() {
		defer appender.Close()
		duckDB.mu.Unlock()
		writerDone <- migrateNodesWriter(buffer, appender, tableName, stats, workersDone)
	}()

	// Stream node IDs in batches from BoltDB
	streamErr := boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return nil // No nodes to migrate
		}

		var batch []string
		cursor := nodesBucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			batch = append(batch, string(k))

			if len(batch) >= streamBatchSize {
				idBatches <- batch
				batch = make([]string, 0, streamBatchSize)
			}
		}

		// Send remaining batch
		if len(batch) > 0 {
			idBatches <- batch
		}

		return nil
	})

	close(idBatches)

	if streamErr != nil {
		wg.Wait()
		<-writerDone
		return fmt.Errorf("failed to stream node IDs: %w", streamErr)
	}

	// Wait for workers to finish
	wg.Wait()
	close(workersDone)

	// Wait for writer to finish (writer will flush remaining data before exiting)
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

// migrateNodesWorker processes a batch of node IDs and adds rows to buffer
func migrateNodesWorker(boltDB *db.DB, nodeIDs []string, queueType string, buffer *nodeBuffer, stats *ETLStats) error {
	processed := int64(0)
	defer func() {
		stats.AddProcessed(processed)
	}()

	return boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return nil
		}

		childrenBucket := db.GetChildrenBucket(tx, queueType)

		// Get join lookup bucket
		var joinBucket *bolt.Bucket
		if queueType == "SRC" {
			joinBucket = db.GetSrcToDstBucket(tx)
		} else {
			joinBucket = db.GetDstToSrcBucket(tx)
		}

		for _, nodeID := range nodeIDs {
			// Get node data
			nodeData := nodesBucket.Get([]byte(nodeID))
			if nodeData == nil {
				continue
			}

			ns, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid nodes
			}

			// Get children
			var childIDs string
			if childrenBucket != nil {
				childData := childrenBucket.Get([]byte(nodeID))
				if childData != nil {
					var children []string
					if err := db.DeserializeStringSlice(childData, &children); err == nil {
						// Re-serialize as JSON string
						if jsonData, err := json.Marshal(children); err == nil {
							childIDs = string(jsonData)
						}
					}
				}
			}

			// Get join lookup from the join bucket
			// If the nodeID doesn't exist in the join bucket, joinID remains nil
			// This means the corresponding node (DST for SRC nodes, SRC for DST nodes) doesn't exist
			// and the field will be NULL in DuckDB
			var joinID *string
			if joinBucket != nil {
				joinData := joinBucket.Get([]byte(nodeID))
				if joinData != nil {
					// Join exists - set the pointer to the ID string
					joinIDStr := string(joinData)
					joinID = &joinIDStr
				}
				// If joinData is nil, joinID stays nil (NULL in DuckDB)
			}

			// Compute path hash
			/* TODO: This is already in the buckets but not a way to get it from the ULID or path itself, so we'll need to rework the bolt 
			buckets to include this. Maybe in the node entry itself perhaps.
			This is wasteful to compute it here (again). For now this will work.
			For my optimization nerds, make that change and update it here to 
			pull from wherever you are storing it in the buckets instead. 
			It's cheap to calculate during traversal, but not so cheap here.
			*/
			pathHash := db.HashPath(ns.Path)

			// Transform status
			traversalStatus := transformTraversalStatus(ns)

			// Create row
			row := NodeRow{
				ID:              ns.ID,
				ServiceID:       ns.ServiceID,
				ParentID:        ns.ParentID,
				ParentServiceID: ns.ParentServiceID,
				ParentPath:      ns.ParentPath,
				Name:            ns.Name,
				Path:            ns.Path,
				PathHash:        pathHash,
				ChildIDs:        childIDs,
				Type:            ns.Type,
				MTime:           ns.MTime,
				Depth:           ns.Depth,
				TraversalStatus: traversalStatus,
				CopyStatus:      ns.CopyStatus,
			}

			// Set size: null for folders, actual size (including 0) for files
			if ns.Type == "folder" {
				row.Size = nil
			} else {
				// For files, include size even if 0
				row.Size = &ns.Size
			}

			// Set join ID based on queue type (nil if no join exists)
			if queueType == "SRC" {
				row.DstID = joinID
			} else {
				row.SrcID = joinID
			}

			buffer.Add(row)
			processed++
		}

		return nil
	})
}

// migrateNodesWriter periodically flushes the buffer when threshold is reached
func migrateNodesWriter(buffer *nodeBuffer, appender *duckdb.Appender, tableName string, stats *ETLStats, workersDone <-chan struct{}) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var flushErr error
	var flushErrMu sync.Mutex

	for {
		select {
		case <-workersDone:
			// Workers are done, flush any remaining data before exiting
			// Wait for any in-flight flush to complete
			for {
				if buffer.IsFlushing() {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				batch := buffer.GetAndClear()
				if batch == nil {
					flushErrMu.Lock()
					err := flushErr
					flushErrMu.Unlock()
					return err
				}
				buffer.SetFlushing(true)

				// Flush synchronously on final flush
				if err := flushNodeBatch(appender, batch, tableName, stats); err != nil {
					buffer.SetFlushing(false)
					return err
				}

				buffer.SetFlushing(false)
			}
		case <-ticker.C:
			// Snapshot + unlock + flush synchronously (appender is not thread-safe)
			batch := buffer.GetAndClearIfReady()
			if batch == nil {
				// Report stats periodically
				stats.report()
				continue
			}

			// Flush synchronously (appender must be used from single goroutine)
			if err := flushNodeBatch(appender, batch, tableName, stats); err != nil {
				buffer.SetFlushing(false)
				return err
			}

			buffer.SetFlushing(false)

			// Report stats periodically
			stats.report()
		}
	}
}

// flushNodeBatch flushes a batch of node rows using the appender
// Chunks the batch to avoid blocking for too long
func flushNodeBatch(appender *duckdb.Appender, batch []NodeRow, tableName string, stats *ETLStats) error {
	const chunkSize = 10_000

	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}

		for _, row := range batch[i:end] {
			var err error

			// Convert *int64 to interface{} (nil or int64) for DuckDB
			var sizeValue interface{}
			if row.Size != nil {
				sizeValue = *row.Size
			} else {
				sizeValue = nil
			}

			// Convert int to int32 for DuckDB INTEGER type
			depthValue := int32(row.Depth)

			// Convert *string to interface{} (nil or string) for nullable join IDs
			var dstIDValue interface{}
			if row.DstID != nil {
				dstIDValue = *row.DstID
			} else {
				dstIDValue = nil
			}

			var srcIDValue interface{}
			if row.SrcID != nil {
				srcIDValue = *row.SrcID
			} else {
				srcIDValue = nil
			}

			if tableName == "src_nodes" {
				err = appender.AppendRow(
					row.ID,
					row.ServiceID,
					row.ParentID,
					row.ParentServiceID,
					row.ParentPath,
					row.Name,
					row.Path,
					row.PathHash,
					row.ChildIDs,
					row.Type,
					sizeValue,
					row.MTime,
					depthValue,
					row.TraversalStatus,
					row.CopyStatus,
					dstIDValue,
				)
			} else {
				err = appender.AppendRow(
					row.ID,
					row.ServiceID,
					row.ParentID,
					row.ParentServiceID,
					row.ParentPath,
					row.Name,
					row.Path,
					row.PathHash,
					row.ChildIDs,
					row.Type,
					sizeValue,
					row.MTime,
					depthValue,
					row.TraversalStatus,
					row.CopyStatus,
					srcIDValue,
				)
			}
			if err != nil {
				return fmt.Errorf("failed to append row: %w", err)
			}
		}

		// Update stats after each chunk
		stats.AddFlushed(int64(end - i))
	}

	return nil
}

// migrateStats migrates bucket statistics
func migrateStats(boltDB *db.DB, duckDB *DuckDB) error {
	duckDB.mu.Lock()
	defer duckDB.mu.Unlock()

	appender, err := duckdb.NewAppenderFromConn(duckDB.conn, "", "stats")
	if err != nil {
		return fmt.Errorf("failed to create stats appender: %w", err)
	}
	defer appender.Close()

	return boltDB.View(func(tx *bolt.Tx) error {
		statsBucket := tx.Bucket([]byte("Traversal-Data"))
		if statsBucket == nil {
			return nil
		}

		statsSubBucket := statsBucket.Bucket([]byte("STATS"))
		if statsSubBucket == nil {
			return nil
		}

		totalsBucket := statsSubBucket.Bucket([]byte("totals"))
		if totalsBucket == nil {
			return nil
		}

		cursor := totalsBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if len(v) != 8 {
				continue
			}

			// Decode big-endian int64
			count := int64(binary.BigEndian.Uint64(v))

			if err := appender.AppendRow(string(k), count); err != nil {
				return fmt.Errorf("failed to append stats row: %w", err)
			}
		}

		return nil
	})
}

// migrateQueueStats migrates queue statistics
func migrateQueueStats(boltDB *db.DB, duckDB *DuckDB) error {
	duckDB.mu.Lock()
	defer duckDB.mu.Unlock()

	appender, err := duckdb.NewAppenderFromConn(duckDB.conn, "", "queue_stats")
	if err != nil {
		return fmt.Errorf("failed to create queue_stats appender: %w", err)
	}
	defer appender.Close()

	return boltDB.View(func(tx *bolt.Tx) error {
		queueStatsBucket := db.GetQueueStatsBucket(tx)
		if queueStatsBucket == nil {
			return nil
		}

		cursor := queueStatsBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if err := appender.AppendRow(string(k), string(v)); err != nil {
				return fmt.Errorf("failed to append queue_stats row: %w", err)
			}
		}

		return nil
	})
}

// migrateLogs migrates log entries using streaming worker pools
func migrateLogs(boltDB *db.DB, duckDB *DuckDB, numWorkers int) error {
	// Create buffer for logs
	buffer := NewLogBuffer()

	// Create stats tracker
	stats := newETLStats("logs")

	// Channel to stream batches of log keys to workers
	type logKeyBatch struct {
		keys   []logKey
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
				if err := migrateLogsWorker(boltDB, batch.keys, batch.rowNum, buffer, stats); err != nil {
					workerErrMu.Lock()
					if workerErr == nil {
						workerErr = err
					}
					workerErrMu.Unlock()
				}
			}
		}()
	}

	// Start writer with reusable appender
	duckDB.mu.Lock()
	appender, err := duckdb.NewAppenderFromConn(duckDB.conn, "", "logs")
	if err != nil {
		duckDB.mu.Unlock()
		close(keyBatches)
		wg.Wait()
		return fmt.Errorf("failed to create logs appender: %w", err)
	}

	writerDone := make(chan error, 1)
	workersDone := make(chan struct{})
	go func() {
		defer appender.Close()
		duckDB.mu.Unlock()
		writerDone <- migrateLogsWriter(buffer, appender, stats, workersDone)
	}()

	// Stream log entries in batches from BoltDB (chronological order)
	streamErr := boltDB.View(func(tx *bolt.Tx) error {
		logsBucket := db.GetLogsBucket(tx)
		if logsBucket == nil {
			return nil
		}

		levels := []string{"trace", "debug", "info", "warning", "error", "critical"}
		var batch []logKey
		rowNum := 0

		// Iterate through levels in order, then through entries within each level
		for _, level := range levels {
			levelBucket := db.GetLogLevelBucket(tx, level)
			if levelBucket == nil {
				continue
			}

			cursor := levelBucket.Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				batch = append(batch, logKey{level: level, id: string(k)})

				if len(batch) >= streamBatchSize {
					keyBatches <- logKeyBatch{keys: batch, rowNum: rowNum}
					rowNum += len(batch)
					batch = make([]logKey, 0, streamBatchSize)
				}
			}
		}

		// Send remaining batch
		if len(batch) > 0 {
			keyBatches <- logKeyBatch{keys: batch, rowNum: rowNum}
		}

		return nil
	})

	close(keyBatches)

	if streamErr != nil {
		wg.Wait()
		<-writerDone
		return fmt.Errorf("failed to stream log keys: %w", streamErr)
	}

	// Wait for workers to finish
	wg.Wait()
	close(workersDone)

	// Wait for writer to finish (writer will flush remaining data before exiting)
	if err := <-writerDone; err != nil {
		return fmt.Errorf("writer error: %w", err)
	}

	if workerErr != nil {
		return fmt.Errorf("worker error: %w", workerErr)
	}

	stats.report()
	totalFlushed := stats.GetTotalFlushed()
	fmt.Printf("[ETL logs] Completed: %d log entries migrated\n", totalFlushed)

	return nil
}

// logKey represents a log entry key (level + ID)
type logKey struct {
	level string
	id    string
}

// migrateLogsWorker processes a batch of log keys and adds entries to buffer
func migrateLogsWorker(boltDB *db.DB, logKeys []logKey, startRowNum int, buffer *logBuffer, stats *ETLStats) error {
	processed := int64(0)
	defer func() {
		stats.AddProcessed(processed)
	}()

	return boltDB.View(func(tx *bolt.Tx) error {
		for i, key := range logKeys {
			levelBucket := db.GetLogLevelBucket(tx, key.level)
			if levelBucket == nil {
				continue
			}

			logData := levelBucket.Get([]byte(key.id))
			if logData == nil {
				continue
			}

			entry, err := db.DeserializeLogEntry(logData)
			if err != nil {
				continue
			}

			buffer.Add(LogRow{
				Entry:  entry,
				RowNum: startRowNum + i,
			})
			processed++
		}

		return nil
	})
}

// migrateLogsWriter periodically flushes the buffer when threshold is reached
func migrateLogsWriter(buffer *logBuffer, appender *duckdb.Appender, stats *ETLStats, workersDone <-chan struct{}) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var flushErr error
	var flushErrMu sync.Mutex

	for {
		select {
		case <-workersDone:
			// Workers are done, flush any remaining data before exiting
			// Wait for any in-flight flush to complete
			for {
				if buffer.IsFlushing() {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				batch := buffer.GetAndClearSorted()
				if batch == nil {
					flushErrMu.Lock()
					err := flushErr
					flushErrMu.Unlock()
					return err
				}
				buffer.SetFlushing(true)

				// Flush synchronously on final flush
				if err := flushLogBatch(appender, batch, stats); err != nil {
					buffer.SetFlushing(false)
					return err
				}

				buffer.SetFlushing(false)
			}
		case <-ticker.C:
			// Snapshot + unlock + flush synchronously (appender is not thread-safe)
			batch := buffer.GetAndClearSortedIfReady()
			if batch == nil {
				// Report stats periodically
				stats.report()
				continue
			}

			// Flush synchronously (appender must be used from single goroutine)
			if err := flushLogBatch(appender, batch, stats); err != nil {
				buffer.SetFlushing(false)
				return err
			}

			buffer.SetFlushing(false)

			// Report stats periodically
			stats.report()
		}
	}
}

// flushLogBatch flushes a batch of log entries using the appender
// Chunks the batch to avoid blocking for too long
func flushLogBatch(appender *duckdb.Appender, batch []LogRow, stats *ETLStats) error {
	const chunkSize = 10_000

	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}

		for _, logRow := range batch[i:end] {
			entry := logRow.Entry
			if err := appender.AppendRow(
				entry.ID,
				entry.Timestamp,
				entry.Level,
				entry.Entity,
				entry.EntityID,
				entry.Message,
				entry.Queue,
			); err != nil {
				return fmt.Errorf("failed to append log row: %w", err)
			}
		}

		// Update stats after each chunk
		stats.AddFlushed(int64(end - i))
	}

	return nil
}

// createIndexes creates all indexes after ETL completes
func createIndexes(db *sql.DB) error {
	// Indexes for src_nodes
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_path_hash ON src_nodes(path_hash)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_parent_id ON src_nodes(parent_id)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_depth ON src_nodes(depth)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_traversal_status ON src_nodes(traversal_status)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_path ON src_nodes(path)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_dst_id ON src_nodes(dst_id)",

		// Indexes for dst_nodes
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_path_hash ON dst_nodes(path_hash)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_parent_id ON dst_nodes(parent_id)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_depth ON dst_nodes(depth)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_traversal_status ON dst_nodes(traversal_status)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_path ON dst_nodes(path)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_src_id ON dst_nodes(src_id)",

		// Indexes for logs
		"CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)",
		"CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_logs_entity ON logs(entity)",
		"CREATE INDEX IF NOT EXISTS idx_logs_queue ON logs(queue)",
	}

	for _, idxSQL := range indexes {
		if _, err := db.Exec(idxSQL); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}
