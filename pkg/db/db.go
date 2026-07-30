// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Options configures DB open behavior.
type Options struct {
	Path          string             // Path to DuckDB file (e.g. "migration.duckdb")
	EncryptionKey []byte             // nil = plaintext open; non-nil = DuckDB native encryption via ATTACH
	SealBuffer    *SealBufferOptions // Optional overrides for seal buffer; nil uses defaults. Seal buffer is always created.
}

// DefaultOptions returns default options.
func DefaultOptions() Options {
	return Options{Path: ":memory:"}
}

// DB is the DuckDB-backed database handle. Single physical connection for all DB operations (schema, bulk append at seal, pulls, checkpoint).
type DB struct {
	path         string
	conn         *sql.DB        // single connection for all operations
	writeMu      sync.Mutex     // one global mutex for all DB writes
	checkpointMu sync.Mutex     // serializes CHECKPOINT; only one connection runs it since it's a global DB op
	sealBuffer   SealController // always set by Open; seal jobs + discovery (nodes/events), flushes async and on demand
}

// Open opens a DuckDB database at the given path and creates schema if missing.
func Open(opts Options) (*DB, error) {
	path := opts.Path
	if path == "" {
		path = ":memory:"
	}
	conn, resolvedPath, err := openConnection(opts)
	if err != nil {
		return nil, err
	}
	// Two conns so the phase can hold one (persistent appenders) while SealLevelDepth0 or other RunWrite callers can use the other.
	conn.SetMaxOpenConns(2)
	// Limit DuckDB memory and threads to avoid OOM during stress testing
	if _, err := conn.Exec("PRAGMA memory_limit='4GB'"); err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	if _, err := conn.Exec("PRAGMA threads=4"); err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	if err := initSchemaConn(conn); err != nil {
		err := conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	db := &DB{path: resolvedPath, conn: conn}
	if path != ":memory:" {
		if _, err := conn.Exec("CHECKPOINT"); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	if sealAttach == nil {
		_ = conn.Close()
		return nil, ErrNoSealController
	}
	sbOpts := SealBufferOptions{}
	if opts.SealBuffer != nil {
		sbOpts = *opts.SealBuffer
	}
	db.AttachSeal(sealAttach(db, sbOpts))
	return db, nil
}

func schemaDDLs() []string {
	return []string{
		nodeTableDDL(TableSrcNodes),
		nodeTableDDL(TableDstNodes),
		srcStatusEventsTableDDL(),
		dstStatusEventsTableDDL(),
		gplIssuesTableDDL(),
		idMapTableDDL(),
		srcCurrentTableDDL(),
		dstCurrentTableDDL(),
		statsTableDDL(),
		srcStatsTableDDL(),
		dstStatsTableDDL(),
		copyWorkRoundStatsTableDDL(),
		deleteWorkRoundStatsTableDDL(),
		logsTableDDL(),
		queueStatsTableDDL(),
		taskErrorsTableDDL(),
		migrationsTableDDL(),
		fsCredentialBindingTableDDL(),
		oauthCredentialsTableDDL(),
	}
}

func initSchemaConn(conn *sql.DB) error {
	for _, ddl := range schemaDDLs() {
		if _, err := conn.Exec(ddl); err != nil {
			return err
		}
	}
	if err := ensureSrcTransferCheckpointColumns(conn); err != nil {
		return err
	}
	if _, err := conn.Exec(srcNodesGPLStateAlter()); err != nil {
		return fmt.Errorf("ensure gpl_state column: %w", err)
	}
	if _, err := conn.Exec(gplIssuesDstActionAlter()); err != nil {
		return fmt.Errorf("ensure gpl_issues.dst_action column: %w", err)
	}
	for _, ddl := range statusEventsGPLStatusAlters() {
		if _, err := conn.Exec(ddl); err != nil {
			return fmt.Errorf("ensure gpl_status column: %w", err)
		}
	}
	for _, ddl := range nodeTableNameAlters() {
		if _, err := conn.Exec(ddl); err != nil {
			return fmt.Errorf("ensure name column: %w", err)
		}
	}
	return nil
}

func ensureSrcTransferCheckpointColumns(conn *sql.DB) error {
	for _, ddl := range srcNodesTransferCheckpointAlters() {
		if _, err := conn.Exec(ddl); err != nil {
			return fmt.Errorf("ensure transfer checkpoint columns: %w", err)
		}
	}
	return nil
}

// Close closes the database connection. Stops the seal buffer first (flushing any pending seal and discovery jobs), then CHECKPOINT on disk (graceful shutdown durability).
func (db *DB) Close() error {
	if db.sealBuffer != nil {
		db.sealBuffer.Stop()
	}
	var errs []error
	if db.path != ":memory:" {
		if err := db.Checkpoint(); err != nil {
			errs = append(errs, fmt.Errorf("checkpoint on close: %w", err))
		}
	}
	if err := db.conn.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Path returns the database file path (or ":memory:").
func (db *DB) Path() string {
	return db.path
}

// Checkpoint runs CHECKPOINT on the main conn, guarded by checkpointMu.
// Uses a single connection; running CHECKPOINT on multiple connections causes "Could not remove file X.wal: No such file or directory".
// On success, resets the seal buffer periodic checkpoint schedule (rows + timer) so external checkpoints do not immediately retrigger a periodic one.
func (db *DB) Checkpoint() error {
	if db.path == ":memory:" {
		return nil
	}
	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()
	_, err := db.conn.Exec("CHECKPOINT")
	if err != nil {
		return err
	}
	if db.sealBuffer != nil {
		db.sealBuffer.OnCheckpointOK()
	}
	return nil
}

// CheckpointWithRetry runs CHECKPOINT up to maxAttempts times with exponential backoff between failures.
// DuckDB may reject CHECKPOINT while another write transaction is open; waiting often allows graceful suspend to complete.
// Honors ctx cancellation between attempts. If maxAttempts <= 0, uses 5.
func (db *DB) CheckpointWithRetry(ctx context.Context, maxAttempts int) error {
	if db.path == ":memory:" {
		return nil
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	backoff := 50 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("checkpoint retry: %w", err)
		}
		fmt.Println("checkpoint retry", attempt)
		lastErr = db.Checkpoint()
		if lastErr == nil {
			fmt.Println("checkpoint retry success", attempt)
			return nil
		}
		if attempt == maxAttempts {
			fmt.Println("checkpoint retry failed", attempt)
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("checkpoint retry: %w", ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return fmt.Errorf("checkpoint after %d attempts: %w", maxAttempts, lastErr)
}

// GetDB returns the underlying *sql.DB for read-only queries (main conn).
func (db *DB) GetDB() (*sql.DB, error) {
	return db.conn, nil
}

// AddNodeDeletion deletes a node immediately (retry DST cleanup).
func (db *DB) AddNodeDeletion(table, nodeID string) error {
	return db.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error { return w.DeleteNode(table, nodeID) })
	})
}

// AddNodeDeletions deletes multiple nodes.
func (db *DB) AddNodeDeletions(deletions []NodeDeletion) error {
	if len(deletions) == 0 {
		return nil
	}
	return db.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			for _, d := range deletions {
				if err := w.DeleteNode(d.Table, d.NodeID); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// NodeDeletion represents a node delete (retry DST cleanup).
type NodeDeletion struct {
	Table  string
	NodeID string
}

// SealLevel persists a sealed level from memory cache to the DB (bulk append + stats snapshot). copyP/copyS/copyF are used for SRC copy stats when >= 0.
// Payload is enqueued to the seal buffer, which flushes asynchronously. CHECKPOINT policy: periodic (rows + interval, see SealBufferOptions), phase end (migration runners), soft suspend, seeding, and Close.
func (db *DB) SealLevel(table string, depth int, nodes []*NodeState, pending, successful, failed, completed int64, copyP, copyS, copyF int64) error {
	if table != "SRC" && table != "DST" {
		return nil
	}
	return db.sealBuffer.Add(table, depth, nodes, pending, successful, failed, completed, copyP, copyS, copyF)
}

// AddSealNodes writes a subset of nodes and their status events to the seal buffer (no level stats).
// Use when removing nodes from cache after DST task completion or SRC early completion; only remove from cache if this returns nil.
func (db *DB) AddSealNodes(table string, depth int, nodes []*NodeState) error {
	if table != "SRC" && table != "DST" || len(nodes) == 0 {
		return nil
	}
	return db.sealBuffer.Add(table, depth, nodes, 0, 0, 0, 0, 0, 0, 0)
}

// SealLevelDepth0 updates existing root row(s) at depth 0 and emits status events.
// Root rows are seeded up-front, so depth 0 uses update semantics instead of appender insert.
func (db *DB) SealLevelDepth0(table string, nodes []*NodeState) error {
	if table != "SRC" && table != "DST" {
		return nil
	}
	return db.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SealDepth0(table, nodes)
		})
	})
}

// Flush drains pending seal jobs to the DB. Call before backpressure wait so we don't block on the flush interval.
func (db *DB) Flush() error {
	return db.sealBuffer.Flush()
}

// WaitUntilFlushedThrough blocks until the seal buffer has written at least the given depth (for backpressure: don't run more than one round ahead of flushed state).
func (db *DB) WaitUntilFlushedThrough(depth int) {
	db.sealBuffer.WaitUntilFlushedThrough(depth)
}

// SealIOWaitActive reports whether the seal buffer is flushing, waiting on flush depth, or producers are blocked on hard-cap backpressure. Queue stall and progress watchdogs use this to avoid false stalls during long seal I/O.
func (db *DB) SealIOWaitActive() bool {
	return db != nil && db.sealBuffer != nil && db.sealBuffer.IOWaitActive()
}

// SealBufferTelemetry returns seal buffer gauges and read-and-reset event counters.
func (db *DB) SealBufferTelemetry() SealBufferTelemetry {
	if db == nil || db.sealBuffer == nil {
		return SealBufferTelemetry{}
	}
	return db.sealBuffer.TelemetrySnapshot()
}

// UpdateSealBufferOptions hot-updates seal buffer tuning knobs.
func (db *DB) UpdateSealBufferOptions(opts SealBufferOptions) {
	if db != nil && db.sealBuffer != nil {
		db.sealBuffer.UpdateOptions(opts)
	}
}

// AppendDiscoveredNodes adds discovered nodes (and their initial status events) to the seal buffer discovery queue. Call from traversal completion; flush is async until Flush.
func (db *DB) AppendDiscoveredNodes(ops []InsertOperation) {
	if db.sealBuffer != nil && len(ops) > 0 {
		db.sealBuffer.AddDiscoveryNodes(ops)
	}
}

// AppendStatusEvent adds a status event (e.g. completed/failed) to the seal buffer discovery queue. Call from CompleteTraversalTask / FailTraversalTask. fromRetry should be true when the completion is from retry mode so we decrement PendingRetry (not Pending) when the path zeros.
func (db *DB) AppendStatusEvent(table string, e StatusEvent, fromRetry bool) {
	if db.sealBuffer != nil {
		db.sealBuffer.AddDiscoveryStatusEvent(table, e, fromRetry)
	}
}

// AppendTaskError adds a task error to the seal buffer for async flush. Call from workers on failure (replaces immediate RecordTaskError).
func (db *DB) AppendTaskError(queueType, phase, nodeID, message string, attempts int, path string) {
	if db.sealBuffer != nil {
		db.sealBuffer.AddTaskError(TaskErrorRecord{
			QueueType: queueType,
			Phase:     phase,
			NodeID:    nodeID,
			Message:   message,
			Attempts:  attempts,
			Path:      path,
		})
	}
}

func (db *DB) withSealBuffer(fn func(SealController)) {
	if db.sealBuffer != nil {
		fn(db.sealBuffer)
	}
}

// AppendFailedSubtree enqueues an SRC folder path for subtree failure propagation.
// At the next flush, all pending descendants will be marked as copy_status='failed'.
func (db *DB) AppendFailedSubtree(parentPath string) {
	db.withSealBuffer(func(sb SealController) {
		sb.AddFailedSubtreePath(parentPath)
	})
}

// AppendGPLIssue enqueues a sparse GPL review row for async seal flush.
func (db *DB) AppendGPLIssue(e GPLIssue) {
	db.withSealBuffer(func(sb SealController) {
		sb.AddGPLIssue(e)
	})
}

// AppendIDMapEvent enqueues an id_map row for async seal flush.
func (db *DB) AppendIDMapEvent(e IDMapEvent) {
	db.withSealBuffer(func(sb SealController) {
		sb.AddIDMapEvent(e)
	})
}

// WriteSession is the handle passed to RunWrite. Caller must not retain conn after the callback returns.
type WriteSession struct {
	conn *sql.Conn
}

// Conn returns the single DB connection for raw use (e.g. DuckDB appender). Valid only during the RunWrite callback.
func (s *WriteSession) Conn() *sql.Conn {
	return s.conn
}

// WithTx runs fn inside a transaction on this session's connection.
func (s *WriteSession) WithTx(fn func(w *Writer) error) error {
	ctx := context.Background()
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	w := &Writer{tx: tx}
	if err := fn(w); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// RunWrite holds writeMu and runs fn with a WriteSession (single connection). Use Conn() for raw conn (e.g. appender) or WithTx for transactional writes. Caller must not retain the session or conn after fn returns.
func (db *DB) RunWrite(ctx context.Context, fn func(s *WriteSession) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(&WriteSession{conn: conn})
}

// BeginTraversalPhase starts the traversal phase: drops secondary indexes, acquires a persistent connection and creates 4 appenders for the seal buffer. Flush will use one tx per flush until EndTraversalPhase.
func (db *DB) BeginTraversalPhase(ctx context.Context) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := DropBulkPhaseNodeIndexes(db); err != nil {
		return err
	}
	if err := DropBulkPhaseStatusEventIndexes(db); err != nil {
		return err
	}
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return err
	}
	if err := db.sealBuffer.StartPhase(conn); err != nil {
		conn.Close()
		return err
	}
	return nil
}

// EnsureBulkPhaseSecondaryIndexes recreates the minimal secondary indexes after a bulk
// traversal/copy phase (same set dropped in BeginTraversalPhase):
//   - nodes: parent_id, depth (SRC + DST) — 4 indexes
//   - status events: (id, event_time) only (SRC + DST) — 2 indexes
//
// Uses conservative PRAGMA settings during creation to reduce OOM risk on large tables.
//
// When indexes already exist, duckdb_indexes is consulted so present names are skipped.
// Call after retry sweep if the DB may have been left without indexes after an interrupted run.
func (db *DB) EnsureBulkPhaseSecondaryIndexes() error {
	runtime.GC()
	if _, err := db.conn.Exec("PRAGMA threads=1"); err != nil {
		return err
	}
	if _, err := db.conn.Exec("PRAGMA memory_limit='2GB'"); err != nil {
		return err
	}
	defer func() {
		_, _ = db.conn.Exec("PRAGMA threads=4")
		_, _ = db.conn.Exec("PRAGMA memory_limit='4GB'")
	}()
	if err := EnsureBulkPhaseNodeIndexesIfMissing(db); err != nil {
		return err
	}
	return EnsureBulkPhaseStatusEventIndexesIfMissing(db)
}

// EndTraversalPhase flushes remaining seal jobs, closes phase appenders, then rebuilds indexes.
// CHECKPOINT is not run here: callers run it once after both traversal/retry/copy queues finish (see migration run/copy/sweeps), plus periodic seal policy, suspend, and Close.
func (db *DB) EndTraversalPhase() error {
	if err := db.sealBuffer.StopPhase(); err != nil {
		return err
	}
	return db.EnsureBulkPhaseSecondaryIndexes()
}
