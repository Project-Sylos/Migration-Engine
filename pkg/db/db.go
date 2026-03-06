// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
)

const pathHashMigrationBatchSize = 5000

// Options configures DB open behavior.
type Options struct {
	Path       string             // Path to DuckDB file (e.g. "migration.duckdb")
	SealBuffer *SealBufferOptions // Optional overrides for seal buffer; nil uses defaults. Seal buffer is always created.
}

// DefaultOptions returns default options.
func DefaultOptions() Options {
	return Options{Path: ":memory:"}
}

// DB is the DuckDB-backed database handle. Single physical connection for all DB operations (schema, bulk append at seal, pulls, checkpoint).
type DB struct {
	path       string
	conn       *sql.DB    // single connection for all operations
	writeMu    sync.Mutex // one global mutex for all DB writes
	checkpointMu sync.Mutex // serializes CHECKPOINT; only one connection runs it since it's a global DB op
	sealBuffer *SealBuffer // always set; seal jobs + discovery (nodes/events), flushes async and on demand
}

// Open opens a DuckDB database at the given path and creates schema if missing.
func Open(opts Options) (*DB, error) {
	path := opts.Path
	if path == "" {
		path = ":memory:"
	}
	conn, err := sql.Open("duckdb", path)
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
	db := &DB{path: path, conn: conn}
	if err := migratePathHashColumns(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if path != ":memory:" {
		if _, err := conn.Exec("CHECKPOINT"); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	sbOpts := SealBufferOptions{}
	if opts.SealBuffer != nil {
		sbOpts = *opts.SealBuffer
	}
	db.sealBuffer = NewSealBuffer(db, sbOpts)
	return db, nil
}

func schemaDDLs() []string {
	return []string{
		nodeTableDDL(tableSrcNodes),
		nodeTableDDL(tableDstNodes),
		srcStatusEventsTableDDL(),
		dstStatusEventsTableDDL(),
		statsTableDDL(),
		srcStatsTableDDL(),
		dstStatsTableDDL(),
		logsTableDDL(),
		queueStatsTableDDL(),
		taskErrorsTableDDL(),
		migrationsTableDDL(),
	}
}

func initSchemaConn(conn *sql.DB) error {
	for _, ddl := range schemaDDLs() {
		if _, err := conn.Exec(ddl); err != nil {
			return err
		}
	}
	return nil
}

// migratePathHashColumns adds path_hash and parent_path_hash to existing node tables, backfills them, and swaps indexes.
func migratePathHashColumns(conn *sql.DB) error {
	ctx := context.Background()
	var exists int64
	err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'src_nodes' AND column_name = 'path_hash'").Scan(&exists)
	if err != nil {
		return fmt.Errorf("path_hash migration check: %w", err)
	}
	if exists > 0 {
		return nil
	}
	for _, table := range []string{tableSrcNodes, tableDstNodes} {
		if _, err := conn.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN path_hash VARCHAR"); err != nil {
			return fmt.Errorf("path_hash migration add path_hash %s: %w", table, err)
		}
		if _, err := conn.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN parent_path_hash VARCHAR"); err != nil {
			return fmt.Errorf("path_hash migration add parent_path_hash %s: %w", table, err)
		}
	}
	for _, table := range []string{tableSrcNodes, tableDstNodes} {
		if err := backfillPathHash(conn, table); err != nil {
			return err
		}
	}
	for _, name := range []string{
		tableSrcNodes + "_path_idx", tableSrcNodes + "_parent_path_idx",
		tableDstNodes + "_path_idx", tableDstNodes + "_parent_path_idx",
	} {
		_, _ = conn.ExecContext(ctx, "DROP INDEX IF EXISTS "+name)
	}
	for _, table := range []string{tableSrcNodes, tableDstNodes} {
		for _, idx := range []struct{ name, col string }{
			{table + "_path_hash_idx", "path_hash"},
			{table + "_parent_path_hash_idx", "parent_path_hash"},
		} {
			q := "CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + table + " (" + idx.col + ")"
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("path_hash migration create index %s: %w", idx.name, err)
			}
		}
	}
	return nil
}

func backfillPathHash(conn *sql.DB, table string) error {
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, "SELECT id, path, parent_path FROM "+table)
	if err != nil {
		return fmt.Errorf("path_hash backfill select %s: %w", table, err)
	}
	defer rows.Close()
	var id, path, parentPath string
	var batch []struct{ id, pathHash, parentPathHash string }
	for rows.Next() {
		if err := rows.Scan(&id, &path, &parentPath); err != nil {
			return fmt.Errorf("path_hash backfill scan %s: %w", table, err)
		}
		batch = append(batch, struct{ id, pathHash, parentPathHash string }{id, PathHash(path), PathHash(parentPath)})
		if len(batch) >= pathHashMigrationBatchSize {
			if err := execPathHashBatch(conn, table, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("path_hash backfill rows %s: %w", table, err)
	}
	if len(batch) > 0 {
		if err := execPathHashBatch(conn, table, batch); err != nil {
			return err
		}
	}
	return nil
}

func execPathHashBatch(conn *sql.DB, table string, batch []struct{ id, pathHash, parentPathHash string }) error {
	ctx := context.Background()
	for _, r := range batch {
		_, err := conn.ExecContext(ctx, "UPDATE "+table+" SET path_hash = $1, parent_path_hash = $2 WHERE id = $3", r.pathHash, r.parentPathHash, r.id)
		if err != nil {
			return fmt.Errorf("path_hash backfill update %s: %w", table, err)
		}
	}
	return nil
}

// Close closes the database connection. Stops the seal buffer first (flushing any pending seal and discovery jobs).
func (db *DB) Close() error {
	if db.sealBuffer != nil {
		db.sealBuffer.Stop()
	}
	err := db.conn.Close()
	return err
}

// Path returns the database file path (or ":memory:").
func (db *DB) Path() string {
	return db.path
}

// Checkpoint runs CHECKPOINT on the main conn, guarded by checkpointMu. Call at root seeding and round advancement only.
// Uses a single connection; running CHECKPOINT on multiple connections causes "Could not remove file X.wal: No such file or directory".
func (db *DB) Checkpoint() error {
	if db.path == ":memory:" {
		return nil
	}
	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()
	_, err := db.conn.Exec("CHECKPOINT")
	return err
}

// GetDB returns the underlying *sql.DB for read-only queries (main conn).
func (db *DB) GetDB() (*sql.DB, error) {
	return db.conn, nil
}

// GetDBForPulls returns the main connection for pull queries. queueType is "SRC" or "DST" (both use same conn).
func (db *DB) GetDBForPulls(queueType string) (*sql.DB, error) {
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
// Payload is enqueued to the seal buffer, which writes and checkpoints asynchronously.
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

// SealLevelDepth0 updates existing root row(s) at depth 0 and writes stats.
// Root rows are seeded up-front, so depth 0 uses update semantics instead of appender insert.
func (db *DB) SealLevelDepth0(table string, nodes []*NodeState, pending, successful, failed, completed int64, copyP, copyS, copyF int64) error {
	if table != "SRC" && table != "DST" {
		return nil
	}
	return db.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.SealDepth0(table, nodes, pending, successful, failed, completed, copyP, copyS, copyF)
		})
	})
}

// FlushSealBuffer drains pending seal jobs to the DB. Call before backpressure wait so we don't block on the flush interval.
func (db *DB) FlushSealBuffer() error {
	return db.sealBuffer.Flush()
}

// WaitUntilSealFlushedThrough blocks until the seal buffer has written at least the given depth (for backpressure: don't run more than one round ahead of flushed state).
func (db *DB) WaitUntilSealFlushedThrough(depth int) {
	db.sealBuffer.WaitUntilFlushedThrough(depth)
}

// AppendDiscoveredNodes adds discovered nodes (and their initial status events) to the seal buffer discovery queue. Call from traversal completion; flush is async until FlushAppenderBuffer.
func (db *DB) AppendDiscoveredNodes(ops []InsertOperation) {
	if db.sealBuffer != nil && len(ops) > 0 {
		db.sealBuffer.AddDiscoveryNodes(ops)
	}
}

// AppendStatusEvent adds a status event (e.g. completed/failed) to the seal buffer discovery queue. Call from CompleteTraversalTask / FailTraversalTask.
func (db *DB) AppendStatusEvent(table string, e StatusEvent) {
	if db.sealBuffer != nil {
		db.sealBuffer.AddDiscoveryStatusEvent(table, e)
	}
}

// FlushAppenderBuffer flushes the seal buffer (including discovery queue) and returns when all pending nodes and status events are persisted. Call before round advance.
func (db *DB) FlushAppenderBuffer() error {
	if db.sealBuffer == nil {
		return nil
	}
	return db.sealBuffer.Flush()
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
	if err := DropNodeTableIndexes(db, tableSrcNodes); err != nil {
		return err
	}
	if err := DropNodeTableIndexes(db, tableDstNodes); err != nil {
		return err
	}
	if err := DropStatusEventTableIndexes(db, tableSrcStatusEvents); err != nil {
		return err
	}
	if err := DropStatusEventTableIndexes(db, tableDstStatusEvents); err != nil {
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

// EndTraversalPhase flushes remaining seal jobs, closes phase appenders, checkpoints to flush WAL and free memory, then rebuilds indexes.
func (db *DB) EndTraversalPhase() error {
	if err := db.sealBuffer.StopPhase(); err != nil {
		return err
	}
	if err := db.Checkpoint(); err != nil {
		return err
	}
	runtime.GC()
	if _, err := db.conn.Exec("PRAGMA threads=1"); err != nil {
		return err
	}
	if _, err := db.conn.Exec("PRAGMA memory_limit='2GB'"); err != nil {
		return err
	}
	// reset defaults after indexes are created. :)
	// Duck DB index creation on large tables is super memory hungry so we are essentially trying to limit this to prevent OOM crashes.
	defer func() {
		_, _ = db.conn.Exec("PRAGMA threads=4")
		_, _ = db.conn.Exec("PRAGMA memory_limit='4GB'")
	}()
	for _, table := range []string{tableSrcNodes, tableDstNodes} {
		if err := EnsureNodeTableIndexes(db, table); err != nil {
			return err
		}
	}
	for _, table := range []string{tableSrcStatusEvents, tableDstStatusEvents} {
		if err := EnsureStatusEventTableIndexes(db, table); err != nil {
			return err
		}
	}
	return db.Checkpoint()
}

// BeginCopyPhase starts the copy phase (same as traversal: drop indexes, persistent appenders).
func (db *DB) BeginCopyPhase(ctx context.Context) error {
	return db.BeginTraversalPhase(ctx)
}

// EndCopyPhase ends the copy phase (flush, close appenders, rebuild indexes, checkpoint).
func (db *DB) EndCopyPhase() error {
	return db.EndTraversalPhase()
}
