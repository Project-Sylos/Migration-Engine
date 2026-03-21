// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
)

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
	path         string
	conn         *sql.DB     // single connection for all operations
	writeMu      sync.Mutex  // one global mutex for all DB writes
	checkpointMu sync.Mutex  // serializes CHECKPOINT; only one connection runs it since it's a global DB op
	sealBuffer   *SealBuffer // always set; flushes sealed levels and checkpoints WAL
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
	conn.SetMaxOpenConns(1)
	// Limit DuckDB memory and threads to avoid OOM during stress testing
	if _, err := conn.Exec("PRAGMA memory_limit='8GB'"); err != nil {
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
	if path != ":memory:" {
		if _, err := conn.Exec("CHECKPOINT"); err != nil {
			err := conn.Close()
			if err != nil {
				return nil, err
			}
			return nil, err
		}
	}

	db := &DB{path: path, conn: conn}
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

// Close closes the database connection. Stops the seal buffer first (flushing any pending jobs).
func (db *DB) Close() error {
	if db.sealBuffer != nil {
		db.sealBuffer.Stop()
	}
	return db.conn.Close()
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
	return db.RunUpdateWriterTx(func(w *Writer) error {
		return w.DeleteNode(table, nodeID)
	})
}

// AddNodeDeletions deletes multiple nodes.
func (db *DB) AddNodeDeletions(deletions []NodeDeletion) error {
	if len(deletions) == 0 {
		return nil
	}
	return db.RunUpdateWriterTx(func(w *Writer) error {
		for _, d := range deletions {
			if err := w.DeleteNode(d.Table, d.NodeID); err != nil {
				return err
			}
		}
		return nil
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
	db.sealBuffer.Add(table, depth, nodes, pending, successful, failed, completed, copyP, copyS, copyF)
	return nil
}

// SealLevelDepth0 updates existing root row(s) at depth 0 and writes stats.
// Root rows are seeded up-front, so depth 0 uses update semantics instead of appender insert.
func (db *DB) SealLevelDepth0(table string, nodes []*NodeState, pending, successful, failed, completed int64, copyP, copyS, copyF int64) error {
	if table != "SRC" && table != "DST" {
		return nil
	}
	return db.RunUpdateWriterTx(func(w *Writer) error {
		return w.SealDepth0(table, nodes, pending, successful, failed, completed, copyP, copyS, copyF)
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

// RunWithConn runs fn with the single DB connection while holding writeMu. Used by the seal buffer for appender + stats. Caller must not retain conn after fn returns.
func (db *DB) RunWithConn(ctx context.Context, fn func(conn *sql.Conn) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)
}

// RunUpdateWriterTx runs fn inside a transaction on the main connection.
func (db *DB) RunUpdateWriterTx(fn func(w *Writer) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	ctx := context.Background()
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	w := &Writer{tx: tx}
	if err := fn(w); err != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
		return err
	}
	return tx.Commit()
}

// RunAppenderWriterTx runs fn inside a transaction on the main connection.
func (db *DB) RunAppenderWriterTx(queueType string, fn func(w *Writer) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	ctx := context.Background()
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	w := &Writer{tx: tx}
	if err := fn(w); err != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
		return err
	}
	return tx.Commit()
}
