// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
)

// Options configures DB open behavior.
type Options struct {
	Path string // Path to DuckDB file (e.g. "migration.duckdb")
}

// DefaultOptions returns default options.
func DefaultOptions() Options {
	return Options{Path: ":memory:"}
}

// DB is the DuckDB-backed database handle. Two connections (appender + direct updater); one active write Tx at a time via writeMu.
type DB struct {
	path         string
	conn         *sql.DB    // used for read-only (GetDB); schema is created on this connection before others are opened
	appenderConn *sql.DB    // dedicated connection for appender writes (staging, nodes, logs)
	updateConn   *sql.DB    // dedicated connection for merge, stats, deletes
	writeMu      sync.Mutex // one global mutex for all DB writes (appender flush, log flush, seal)
}

// Open opens a DuckDB database at the given path and creates schema if missing.
// Opens one connection first, runs schema DDL on it, then opens appender and update connections
// so they see the schema (avoids other connections touching the DB before it exists).
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
	if err := initSchemaConn(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if path != ":memory:" {
		if _, err := conn.Exec("CHECKPOINT"); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	appenderConn, err := sql.Open("duckdb", path)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	appenderConn.SetMaxOpenConns(1)
	updateConn, err := sql.Open("duckdb", path)
	if err != nil {
		_ = conn.Close()
		_ = appenderConn.Close()
		return nil, err
	}
	updateConn.SetMaxOpenConns(1)
	return &DB{path: path, conn: conn, appenderConn: appenderConn, updateConn: updateConn}, nil
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

// Close closes all database connections.
func (db *DB) Close() error {
	if db.appenderConn != nil {
		_ = db.appenderConn.Close()
	}
	if db.updateConn != nil {
		_ = db.updateConn.Close()
	}
	return db.conn.Close()
}

// Path returns the database file path (or ":memory:").
func (db *DB) Path() string {
	return db.path
}

// GetDB returns the underlying *sql.DB for read-only queries (main conn).
// Writes must go through RunUpdateWriterTx or RunAppenderTx.
func (db *DB) GetDB() (*sql.DB, error) {
	return db.conn, nil
}

// GetDBForPulls returns the update connection for pull queries (ListNodesByDepthKeyset, ListDstBatchWithSrcChildren, ListNodesCopyKeyset).
// Using the same connection as writes ensures pulls see committed roots and node inserts without cross-connection visibility issues.
func (db *DB) GetDBForPulls() (*sql.DB, error) {
	return db.updateConn, nil
}

// RunUpdateWriterTx runs fn inside a single transaction on the direct-updater connection. Writer holds only *sql.Tx.
// writeMu is held for the duration so only one write (appender or updater) runs at a time.
func (db *DB) RunUpdateWriterTx(fn func(w *Writer) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	ctx := context.Background()
	tx, err := db.updateConn.BeginTx(ctx, nil)
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

// RunAppenderTx runs fn with an AppenderWriter inside a single transaction on the appender connection.
// Ensures src_staging and dst_staging exist, then creates appenders, runs fn(aw), flushes appenders, and commits.
// writeMu is held for the duration. Use for high-volume staging and node inserts; call aw.Flush() is done by RunAppenderTx after fn returns.
func (db *DB) RunAppenderTx(fn func(aw *AppenderWriter) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	ctx := context.Background()
	conn, err := db.appenderConn.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, srcStagingDDL()); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, dstStagingDDL()); err != nil {
		_ = tx.Rollback()
		return err
	}
	var fnErr error
	err = conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return errors.New("raw connection is not driver.Conn")
		}
		aw, err := newAppenderWriter(dc)
		if err != nil {
			return err
		}
		defer aw.Close()
		fnErr = fn(aw)
		if fnErr != nil {
			return fnErr
		}
		return aw.Flush()
	})
	if err != nil {
		_ = tx.Rollback()
		if fnErr != nil {
			return fnErr
		}
		return err
	}
	return tx.Commit()
}

