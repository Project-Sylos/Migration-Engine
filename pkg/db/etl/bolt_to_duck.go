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
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"github.com/marcboeker/go-duckdb"
	bolt "go.etcd.io/bbolt"
)

const (
	// Default number of workers per queue type
	defaultNumWorkers = 8
	// Batch size for DuckDB inserts (rows per transaction)
	defaultBatchSize = 50000
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

// DuckDB configuration constants
const (
	// Default memory limit for DuckDB. Ingestion is bounded by appender.Flush();
	// index creation is the peak memory phase and needs headroom (e.g. 20M-row index build).
	defaultMemoryLimit = "4GB"
	// Number of threads for DuckDB (low to avoid contention during ETL)
	defaultThreads = 2
)

// OpenDuckDB opens or creates a DuckDB database with memory and thread limits configured.
// This prevents DuckDB from consuming excessive memory during large ETL operations.
func OpenDuckDB(dbPath string) (*DuckDB, error) {
	// Use system temp directory for DuckDB spill files
	tempDir := filepath.Join(os.TempDir(), "duckdb_temp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory for DuckDB: %w", err)
	}

	// Create connector with initialization function to configure memory limits
	// This runs SET commands on each new connection before it's used
	connector, err := duckdb.NewConnector(dbPath, func(execer driver.ExecerContext) error {
		ctx := context.Background()
		initQueries := []string{
			fmt.Sprintf("SET memory_limit = '%s'", defaultMemoryLimit),
			fmt.Sprintf("SET threads TO %d", defaultThreads),
			fmt.Sprintf("SET temp_directory = '%s'", tempDir),
		}
		for _, query := range initQueries {
			if _, err := execer.ExecContext(ctx, query, nil); err != nil {
				return fmt.Errorf("failed to execute init query %q: %w", query, err)
			}
		}
		return nil
	})
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

	fmt.Printf("[ETL] DuckDB opened with memory_limit=%s, threads=%d, temp_directory=%s\n",
		defaultMemoryLimit, defaultThreads, tempDir)

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
	
	// Checkpoint before closing to ensure WAL is flushed and indexes are consistent
	if d.db != nil {
		if _, err := d.db.Exec("CHECKPOINT"); err != nil {
			// Log but don't fail - checkpoint errors are not critical during close
			fmt.Printf("[ETL] Warning: failed to checkpoint before close: %v\n", err)
		}
	}
	
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

	// Use a dedicated connection for index creation so we can raise threads for this phase only
	ctx := context.Background()
	conn, err := duckDB.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection for index creation: %w", err)
	}
	defer conn.Close()

	// CHECKPOINT before index creation to flush state and free memory for index builds
	fmt.Println("[ETL] Checkpointing before index creation...")
	if _, err := conn.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("failed to checkpoint before index creation: %w", err)
	}

	// Raise memory limit and threads for index creation (increase memory 2GB -> 4GB)
	indexThreads := runtime.NumCPU()
	if indexThreads < 2 {
		indexThreads = 2
	}
	if indexThreads > 16 {
		indexThreads = 16
	}
	indexMemoryLimit := "4GB"

	fmt.Printf("[ETL] Setting memory_limit to %s and threads to %d for index creation...\n", indexMemoryLimit, indexThreads)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit = '%s'", indexMemoryLimit)); err != nil {
		return fmt.Errorf("failed to set memory_limit for index creation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET threads TO %d", indexThreads)); err != nil {
		return fmt.Errorf("failed to set threads for index creation: %w", err)
	}

	// Create indexes after all data is loaded
	fmt.Println("[ETL] Creating indexes...")
	if err := createIndexes(ctx, conn); err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	// Restore threads and memory limit to default for any subsequent use of this connection
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET threads TO %d", defaultThreads)); err != nil {
		return fmt.Errorf("failed to restore threads after index creation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit = '%s'", defaultMemoryLimit)); err != nil {
		return fmt.Errorf("failed to restore memory_limit after index creation: %w", err)
	}

	// ANALYZE tables to update statistics and stabilize indexes
	fmt.Println("[ETL] Analyzing tables...")
	nodeTables := []string{
		"src_nodes_ui", "src_nodes_children",
		"dst_nodes_ui", "dst_nodes_children",
	}
	for _, table := range nodeTables {
		if _, err := duckDB.db.Exec(fmt.Sprintf("ANALYZE %s", table)); err != nil {
			return fmt.Errorf("failed to analyze %s: %w", table, err)
		}
	}
	if _, err := duckDB.db.Exec("ANALYZE stats"); err != nil {
		return fmt.Errorf("failed to analyze stats: %w", err)
	}
	if _, err := duckDB.db.Exec("ANALYZE queue_stats"); err != nil {
		return fmt.Errorf("failed to analyze queue_stats: %w", err)
	}
	if _, err := duckDB.db.Exec("ANALYZE logs"); err != nil {
		return fmt.Errorf("failed to analyze logs: %w", err)
	}

	// CHECKPOINT to flush WAL and ensure index consistency
	// This resolves UPDATE constraint violations from bulk Appender loading
	fmt.Println("[ETL] Checkpointing database...")
	if _, err := duckDB.db.Exec("CHECKPOINT"); err != nil {
		return fmt.Errorf("failed to checkpoint database: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("[ETL] Migration completed in %v\n", elapsed.Round(time.Second))

	return nil
}

// dropTables drops all tables if they exist.
// Node data: primary UI table + children table per queue; stats, queue_stats, logs.
func dropTables(db *sql.DB) error {
	tables := []string{
		// SRC tables
		"src_nodes_children", "src_nodes_ui",
		// DST tables
		"dst_nodes_children", "dst_nodes_ui",
		// Other tables
		"stats", "queue_stats", "logs",
	}
	for _, table := range tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}
	return nil
}

// createPrimaryNodeTableForQueue creates the single primary UI table for a queue (src or dst).
// path is the primary key; join_id holds counterpart node id (dst_id for src, src_id for dst).
func createPrimaryNodeTableForQueue(db *sql.DB, prefix string) error {
	// Note: PRIMARY KEY (path) used; if DuckDB ART bug #3249 hits, use path VARCHAR NOT NULL + CREATE UNIQUE INDEX.
	uiDDL := fmt.Sprintf(`
		CREATE TABLE %s_nodes_ui (
			path VARCHAR PRIMARY KEY,
			id VARCHAR,
			parent_path VARCHAR,
			parent_id VARCHAR,
			service_id VARCHAR,
			parent_service_id VARCHAR,
			type VARCHAR,
			name VARCHAR,
			depth INTEGER,
			traversal_status VARCHAR,
			copy_status VARCHAR,
			size BIGINT,
			mtime VARCHAR,
			join_id VARCHAR
		)
	`, prefix)
	if _, err := db.Exec(uiDDL); err != nil {
		return fmt.Errorf("failed to create %s_nodes_ui table: %w", prefix, err)
	}
	return nil
}

// createChildrenTableForQueue creates the children table for a queue (path -> child_ids).
func createChildrenTableForQueue(db *sql.DB, prefix string) error {
	childrenDDL := fmt.Sprintf(`
		CREATE TABLE %s_nodes_children (
			path VARCHAR NOT NULL,
			child_ids VARCHAR
		)
	`, prefix)
	if _, err := db.Exec(childrenDDL); err != nil {
		return fmt.Errorf("failed to create %s_nodes_children table: %w", prefix, err)
	}
	return nil
}

// createTables creates all table schemas without indexes.
// Node data: one primary UI table + one children table per queue.
func createTables(db *sql.DB) error {
	// Create SRC node tables (primary UI + children)
	if err := createPrimaryNodeTableForQueue(db, "src"); err != nil {
		return err
	}
	if err := createChildrenTableForQueue(db, "src"); err != nil {
		return err
	}
	// Create DST node tables (primary UI + children)
	if err := createPrimaryNodeTableForQueue(db, "dst"); err != nil {
		return err
	}
	if err := createChildrenTableForQueue(db, "dst"); err != nil {
		return err
	}

	// Create stats table
	// Note: bucket_path uses NOT NULL only (no PRIMARY KEY or UNIQUE constraints) to avoid DuckDB ART index bug #3249
	statsDDL := `
		CREATE TABLE stats (
			bucket_path VARCHAR NOT NULL,
			count BIGINT
		)
	`
	if _, err := db.Exec(statsDDL); err != nil {
		return fmt.Errorf("failed to create stats table: %w", err)
	}

	// Create queue_stats table
	// Note: queue_key uses NOT NULL only (no PRIMARY KEY or UNIQUE constraints) to avoid DuckDB ART index bug #3249
	queueStatsDDL := `
		CREATE TABLE queue_stats (
			queue_key VARCHAR NOT NULL,
			metrics_json VARCHAR
		)
	`
	if _, err := db.Exec(queueStatsDDL); err != nil {
		return fmt.Errorf("failed to create queue_stats table: %w", err)
	}

	// Create logs table
	// Note: id uses NOT NULL only (no PRIMARY KEY or UNIQUE constraints) to avoid DuckDB ART index bug #3249
	logsDDL := `
		CREATE TABLE logs (
			id VARCHAR NOT NULL,
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
	// Check exclusion flags firstStructural Sanity
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

// nodeAppenders holds one appender for the primary UI table and one for children per queue.
type nodeAppenders struct {
	ui       *duckdb.Appender
	children *duckdb.Appender
}

// migrateNodes migrates nodes from BoltDB to DuckDB using streaming worker pools.
// Writes each node into primary UI table {prefix}_nodes_ui and children table {prefix}_nodes_children.
func migrateNodes(boltDB *db.DB, duckDB *DuckDB, queueType string, numWorkers int) error {
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

	// Create one appender for primary UI table and one for children (single-threaded use)
	duckDB.mu.Lock()
	appenders := &nodeAppenders{}
	var err error
	if appenders.ui, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_ui"); err != nil {
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create %s_nodes_ui appender: %w", prefix, err)
	}
	if appenders.children, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_children"); err != nil {
		appenders.ui.Close()
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create %s_nodes_children appender: %w", prefix, err)
	}

	writerDone := make(chan error, 1)
	workersDone := make(chan struct{})
	go func() {
		duckDB.mu.Unlock()
		err := migrateNodesWriter(buffer, appenders, stats, workersDone)
		// Close all appenders before signaling completion
		if e := appenders.children.Close(); e != nil && err == nil {
			err = fmt.Errorf("failed to close %s_nodes_children appender: %w", prefix, e)
		}
		if e := appenders.ui.Close(); e != nil && err == nil {
			err = fmt.Errorf("failed to close %s_nodes_ui appender: %w", prefix, e)
		}
		writerDone <- err
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

// migrateNodesWriter periodically flushes the buffer when threshold is reached.
// Writes each batch to primary UI table and children table via appenders.
func migrateNodesWriter(buffer *nodeBuffer, appenders *nodeAppenders, stats *ETLStats, workersDone <-chan struct{}) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var flushErr error
	var flushErrMu sync.Mutex

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
				if batch == nil {
					flushErrMu.Lock()
					err := flushErr
					flushErrMu.Unlock()
					return err
				}
				buffer.SetFlushing(true)
				if err := flushNodeBatch(appenders, batch, stats); err != nil {
					buffer.SetFlushing(false)
					return err
				}
				buffer.SetFlushing(false)
			}
		case <-ticker.C:
			batch := buffer.GetAndClearIfReady()
			if batch == nil {
				stats.report()
				continue
			}
			if err := flushNodeBatch(appenders, batch, stats); err != nil {
				buffer.SetFlushing(false)
				return err
			}
			buffer.SetFlushing(false)
			stats.report()
		}
	}
}

// flushNodeBatch flushes a batch of node rows into the primary UI table and children table.
// Chunks the batch to avoid blocking; flushes each appender after each chunk.
func flushNodeBatch(appenders *nodeAppenders, batch []NodeRow, stats *ETLStats) error {
	const chunkSize = 10_000

	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[i:end]

		for _, row := range chunk {
			var joinID interface{}
			if row.DstID != nil {
				joinID = *row.DstID
			} else if row.SrcID != nil {
				joinID = *row.SrcID
			} else {
				joinID = nil
			}
			var sizeVal interface{}
			if row.Size != nil {
				sizeVal = *row.Size
			} else {
				sizeVal = nil
			}

			// {prefix}_nodes_ui: path, id, parent_path, parent_id, service_id, parent_service_id, type, name, depth, traversal_status, copy_status, size, mtime, join_id
			if err := appenders.ui.AppendRow(
				row.Path,
				row.ID,
				row.ParentPath,
				row.ParentID,
				row.ServiceID,
				row.ParentServiceID,
				row.Type,
				row.Name,
				int32(row.Depth),
				row.TraversalStatus,
				row.CopyStatus,
				sizeVal,
				row.MTime,
				joinID,
			); err != nil {
				return fmt.Errorf("failed to append nodes_ui row: %w", err)
			}

			// {prefix}_nodes_children: path, child_ids
			if err := appenders.children.AppendRow(row.Path, row.ChildIDs); err != nil {
				return fmt.Errorf("failed to append nodes_children row: %w", err)
			}
		}

		for _, ap := range []*duckdb.Appender{appenders.ui, appenders.children} {
			if err := ap.Flush(); err != nil {
				return fmt.Errorf("failed to flush appender: %w", err)
			}
		}
		stats.AddFlushed(int64(len(chunk)))
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

	var rowCount int
	err = boltDB.View(func(tx *bolt.Tx) error {
		traversalBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalBucket == nil {
			return nil
		}

		statsBucket := traversalBucket.Bucket([]byte("STATS"))
		if statsBucket == nil {
			return nil
		}

		// Stats are stored directly in STATS bucket with keys like "SRC/nodes", "DST/nodes", etc.
		// Skip the "queue-stats" sub-bucket (it's for queue statistics, not bucket counts)
		cursor := statsBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			// Skip sub-buckets (like "queue-stats")
			if v == nil {
				// This is a sub-bucket, not a stat entry - skip it
				continue
			}

			// Check if this is a valid stats entry (8-byte big-endian int64)
			if len(v) != 8 {
				continue
			}

			// Decode big-endian int64
			count := int64(binary.BigEndian.Uint64(v))

			// Append to DuckDB (bucket_path, count)
			if err := appender.AppendRow(string(k), count); err != nil {
				return fmt.Errorf("failed to append stats row: %w", err)
			}
			rowCount++
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Explicitly flush the appender before closing
	if err := appender.Flush(); err != nil {
		return fmt.Errorf("failed to flush stats appender: %w", err)
	}

	if rowCount > 0 {
		fmt.Printf("[ETL stats] Migrated %d stats entries\n", rowCount)
	} else {
		fmt.Printf("[ETL stats] No stats entries found (stats bucket may be empty)\n")
	}

	return nil
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

// createIndexes creates indexes on primary UI tables only (path is already PRIMARY KEY).
// Indexes: parent_path, traversal_status, copy_status per queue. Secondary tables unindexed.
func createIndexes(ctx context.Context, conn *sql.Conn) error {
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_ui_parent_path ON src_nodes_ui(parent_path)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_ui_traversal_status ON src_nodes_ui(traversal_status)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_ui_copy_status ON src_nodes_ui(copy_status)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_ui_parent_path ON dst_nodes_ui(parent_path)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_ui_traversal_status ON dst_nodes_ui(traversal_status)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_ui_copy_status ON dst_nodes_ui(copy_status)",
	}

	for i, idxSQL := range indexes {
		if _, err := conn.ExecContext(ctx, idxSQL); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
		// CHECKPOINT after each index to release memory before building the next
		// (index creation is memory-heavy; this avoids OOM on large tables)
		if _, err := conn.ExecContext(ctx, "CHECKPOINT"); err != nil {
			return fmt.Errorf("failed to checkpoint after index %d: %w", i+1, err)
		}
	}

	fmt.Println("[ETL] Indexes created successfully")
	return nil
}
