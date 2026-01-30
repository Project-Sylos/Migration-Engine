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
		"src_nodes_core", "src_nodes_status", "src_nodes_metrics", "src_nodes_text", "src_nodes_children",
		"dst_nodes_core", "dst_nodes_status", "dst_nodes_metrics", "dst_nodes_text", "dst_nodes_children",
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
// Node data lives in separate SRC and DST table sets (5 tables each).
func dropTables(db *sql.DB) error {
	tables := []string{
		// SRC tables (drop in reverse dependency order)
		"src_nodes_children", "src_nodes_text", "src_nodes_metrics", "src_nodes_status", "src_nodes_core",
		// DST tables (drop in reverse dependency order)
		"dst_nodes_children", "dst_nodes_text", "dst_nodes_metrics", "dst_nodes_status", "dst_nodes_core",
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

// createNodeTablesForQueue creates the 5 narrow node tables for a given queue prefix (src or dst).
// Path is the unique key within each table set.
func createNodeTablesForQueue(db *sql.DB, prefix string) error {
	// {prefix}_nodes_core: identity, joins, tree structure. Path is unique within this queue.
	// join_id holds dst_id (for src tables) or src_id (for dst tables).
	coreDDL := fmt.Sprintf(`
		CREATE TABLE %s_nodes_core (
			path VARCHAR NOT NULL,
			id VARCHAR NOT NULL,
			parent_path VARCHAR,
			parent_id VARCHAR,
			service_id VARCHAR,
			parent_service_id VARCHAR,
			type VARCHAR,
			depth INTEGER,
			path_hash VARCHAR,
			join_id VARCHAR
		)
	`, prefix)
	if _, err := db.Exec(coreDDL); err != nil {
		return fmt.Errorf("failed to create %s_nodes_core table: %w", prefix, err)
	}

	// {prefix}_nodes_status: traversal and copy status only.
	statusDDL := fmt.Sprintf(`
		CREATE TABLE %s_nodes_status (
			path VARCHAR NOT NULL,
			traversal_status VARCHAR,
			copy_status VARCHAR
		)
	`, prefix)
	if _, err := db.Exec(statusDDL); err != nil {
		return fmt.Errorf("failed to create %s_nodes_status table: %w", prefix, err)
	}

	// {prefix}_nodes_metrics: numeric and sortable fields.
	metricsDDL := fmt.Sprintf(`
		CREATE TABLE %s_nodes_metrics (
			path VARCHAR NOT NULL,
			size BIGINT,
			mtime VARCHAR
		)
	`, prefix)
	if _, err := db.Exec(metricsDDL); err != nil {
		return fmt.Errorf("failed to create %s_nodes_metrics table: %w", prefix, err)
	}

	// {prefix}_nodes_text: searchable text. path_text duplicates path for LIKE/search use.
	textDDL := fmt.Sprintf(`
		CREATE TABLE %s_nodes_text (
			path VARCHAR NOT NULL,
			name VARCHAR,
			path_text VARCHAR
		)
	`, prefix)
	if _, err := db.Exec(textDDL); err != nil {
		return fmt.Errorf("failed to create %s_nodes_text table: %w", prefix, err)
	}

	// {prefix}_nodes_children: children relationships.
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
// Node data is split into separate SRC and DST table sets (5 tables each).
func createTables(db *sql.DB) error {
	// Create SRC node tables
	if err := createNodeTablesForQueue(db, "src"); err != nil {
		return err
	}

	// Create DST node tables
	if err := createNodeTablesForQueue(db, "dst"); err != nil {
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

// nodeAppenders holds one appender per narrow node table for single-threaded write.
type nodeAppenders struct {
	core     *duckdb.Appender
	status   *duckdb.Appender
	metrics  *duckdb.Appender
	text     *duckdb.Appender
	children *duckdb.Appender
}

// migrateNodes migrates nodes from BoltDB to DuckDB using streaming worker pools.
// Writes each node into five narrow tables: {prefix}_nodes_core, {prefix}_nodes_status, etc.
func migrateNodes(boltDB *db.DB, duckDB *DuckDB, queueType string, numWorkers int) error {
	// Compute table prefix from queue type
	prefix := "src"
	if queueType == "DST" {
		prefix = "dst"
	}

	buffer := NewNodeBuffer(prefix + "_nodes_core")
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

	// Create one appender per narrow table (single-threaded use)
	duckDB.mu.Lock()
	appenders := &nodeAppenders{}
	var err error
	if appenders.core, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_core"); err != nil {
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create %s_nodes_core appender: %w", prefix, err)
	}
	if appenders.status, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_status"); err != nil {
		appenders.core.Close()
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create %s_nodes_status appender: %w", prefix, err)
	}
	if appenders.metrics, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_metrics"); err != nil {
		appenders.status.Close()
		appenders.core.Close()
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create %s_nodes_metrics appender: %w", prefix, err)
	}
	if appenders.text, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_text"); err != nil {
		appenders.metrics.Close()
		appenders.status.Close()
		appenders.core.Close()
		duckDB.mu.Unlock()
		close(idBatches)
		wg.Wait()
		return fmt.Errorf("failed to create %s_nodes_text appender: %w", prefix, err)
	}
	if appenders.children, err = duckdb.NewAppenderFromConn(duckDB.conn, "", prefix+"_nodes_children"); err != nil {
		appenders.text.Close()
		appenders.metrics.Close()
		appenders.status.Close()
		appenders.core.Close()
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
		if e := appenders.text.Close(); e != nil && err == nil {
			err = fmt.Errorf("failed to close %s_nodes_text appender: %w", prefix, e)
		}
		if e := appenders.metrics.Close(); e != nil && err == nil {
			err = fmt.Errorf("failed to close %s_nodes_metrics appender: %w", prefix, e)
		}
		if e := appenders.status.Close(); e != nil && err == nil {
			err = fmt.Errorf("failed to close %s_nodes_status appender: %w", prefix, e)
		}
		if e := appenders.core.Close(); e != nil && err == nil {
			err = fmt.Errorf("failed to close %s_nodes_core appender: %w", prefix, e)
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

// migrateNodesWriter periodically flushes the buffer when threshold is reached.
// Writes each batch to all five narrow node tables via appenders.
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

// flushNodeBatch flushes a batch of node rows into the five narrow node tables.
// Chunks the batch to avoid blocking; flushes each appender after each chunk.
// No queue column - table names already determine SRC vs DST.
func flushNodeBatch(appenders *nodeAppenders, batch []NodeRow, stats *ETLStats) error {
	const chunkSize = 10_000

	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[i:end]

		for _, row := range chunk {
			// join_id: use whichever is set (DstID for SRC tables, SrcID for DST tables)
			var joinID interface{}
			if row.DstID != nil {
				joinID = *row.DstID
			} else if row.SrcID != nil {
				joinID = *row.SrcID
			} else {
				joinID = nil
			}

			// {prefix}_nodes_core: path, id, parent_path, parent_id, service_id, parent_service_id, type, depth, path_hash, join_id
			if err := appenders.core.AppendRow(
				row.Path,
				row.ID,
				row.ParentPath,
				row.ParentID,
				row.ServiceID,
				row.ParentServiceID,
				row.Type,
				int32(row.Depth),
				row.PathHash,
				joinID,
			); err != nil {
				return fmt.Errorf("failed to append nodes_core row: %w", err)
			}

			// {prefix}_nodes_status: path, traversal_status, copy_status
			if err := appenders.status.AppendRow(row.Path, row.TraversalStatus, row.CopyStatus); err != nil {
				return fmt.Errorf("failed to append nodes_status row: %w", err)
			}

			// {prefix}_nodes_metrics: path, size, mtime
			var sizeVal interface{}
			if row.Size != nil {
				sizeVal = *row.Size
			} else {
				sizeVal = nil
			}
			if err := appenders.metrics.AppendRow(row.Path, sizeVal, row.MTime); err != nil {
				return fmt.Errorf("failed to append nodes_metrics row: %w", err)
			}

			// {prefix}_nodes_text: path, name, path_text (path duplicate for LIKE/search)
			if err := appenders.text.AppendRow(row.Path, row.Name, row.Path); err != nil {
				return fmt.Errorf("failed to append nodes_text row: %w", err)
			}

			// {prefix}_nodes_children: path, child_ids
			if err := appenders.children.AppendRow(row.Path, row.ChildIDs); err != nil {
				return fmt.Errorf("failed to append nodes_children row: %w", err)
			}
		}

		// Flush all appenders after each chunk to release DuckDB's internal memory buffers
		for _, ap := range []*duckdb.Appender{appenders.core, appenders.status, appenders.metrics, appenders.text, appenders.children} {
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
		duckDB.mu.Unlock()
		err := migrateLogsWriter(buffer, appender, stats, workersDone)
		
		// Close appender BEFORE signaling completion to avoid race with index creation
		if closeErr := appender.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close logs appender: %w", closeErr)
		}
		
		writerDone <- err
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

		// Flush appender after each chunk to release DuckDB's internal memory buffers
		// Without this, memory grows unbounded as rows accumulate in column vectors
		if err := appender.Flush(); err != nil {
			return fmt.Errorf("failed to flush appender: %w", err)
		}

		// Update stats after each chunk
		stats.AddFlushed(int64(end - i))
	}

	return nil
}

// createIndexes creates all indexes after ETL completes.
// Indexes are created in order: core, status, metrics, text, children for each queue.
// Caller must pass the same connection used for SET threads (so index creation uses that thread count).
func createIndexes(ctx context.Context, conn *sql.Conn) error {
	indexes := []string{

		// SRC anchor table only - satellite tables (status, metrics, text, children) are unindexed.
		// DuckDB handles hash joins efficiently without indexes on join keys.
		// Secondary indexes deferred to Part 2 after query shape analysis.
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_core_path ON src_nodes_core(path)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_core_id ON src_nodes_core(id)",
		"CREATE INDEX IF NOT EXISTS idx_src_nodes_core_parent_path ON src_nodes_core(parent_path)",

		// DST anchor table only
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_core_path ON dst_nodes_core(path)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_core_id ON dst_nodes_core(id)",
		"CREATE INDEX IF NOT EXISTS idx_dst_nodes_core_parent_path ON dst_nodes_core(parent_path)",
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
