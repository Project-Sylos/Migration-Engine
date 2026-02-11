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

// DB is the DuckDB-backed database handle. Single physical connection for all DB operations (schema, appender writes, pulls, merge, checkpoint).
type DB struct {
	path        string
	conn        *sql.DB    // single connection for all operations
	writeMu     sync.Mutex // one global mutex for all DB writes
	checkpointMu sync.Mutex // serializes CHECKPOINT; only one connection runs it since it's a global DB op

	// Table-scoped buffers (DB-owned, not queue-owned)
	srcStagingBuffer *stagingBuffer
	dstStagingBuffer *stagingBuffer
	srcNodesBuffer   *nodesBuffer
	dstNodesBuffer   *nodesBuffer

	// Flush callbacks per queue for leased-key removal
	onFlushSRC func(nodeIDs []string)
	onFlushDST func(nodeIDs []string)
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

	d := &DB{
		path: path,
		conn: conn,
	}
	d.srcStagingBuffer = newStagingBuffer(d, "SRC")
	d.dstStagingBuffer = newStagingBuffer(d, "DST")
	d.srcNodesBuffer = newNodesBuffer(d, "SRC")
	d.dstNodesBuffer = newNodesBuffer(d, "DST")
	return d, nil
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

// Close closes all database connections and stops buffers.
func (db *DB) Close() error {
	if db.srcStagingBuffer != nil {
		db.srcStagingBuffer.Stop()
	}
	if db.dstStagingBuffer != nil {
		db.dstStagingBuffer.Stop()
	}
	if db.srcNodesBuffer != nil {
		db.srcNodesBuffer.Stop()
	}
	if db.dstNodesBuffer != nil {
		db.dstNodesBuffer.Stop()
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

// SetOnFlush sets the callback for leased-key removal when buffers flush. queueType is "SRC" or "DST".
func (db *DB) SetOnFlush(queueType string, fn func(nodeIDs []string)) {
	if queueType == "DST" {
		db.onFlushDST = fn
		db.dstStagingBuffer.SetOnFlush(fn)
		db.dstNodesBuffer.SetOnFlush(fn)
	} else {
		db.onFlushSRC = fn
		db.srcStagingBuffer.SetOnFlush(fn)
		db.srcNodesBuffer.SetOnFlush(fn)
	}
}

// AddToStaging buffers a status update for the staging table. table is "SRC" or "DST".
// For traversal: pass nodeID, newTraversal, "".
// For copy (SRC only): use AddCopyToStaging.
func (db *DB) AddToStaging(table, nodeID, newTraversal string) {
	if table == "DST" {
		db.dstStagingBuffer.addTraversal(nodeID, newTraversal)
	} else {
		db.srcStagingBuffer.addTraversal(nodeID, newTraversal)
	}
}

// AddCopyToStaging buffers a copy status update for src_staging (SRC table only).
func (db *DB) AddCopyToStaging(nodeID, newCopyStatus string) {
	db.srcStagingBuffer.addCopy(nodeID, newCopyStatus)
}

// AddNode buffers a node insert for src_nodes or dst_nodes.
func (db *DB) AddNode(table string, n *NodeState, status string) {
	if table == "DST" {
		db.dstNodesBuffer.Add(n, status)
	} else {
		db.srcNodesBuffer.Add(n, status)
	}
}

// AddNodes buffers node inserts. ops are InsertOperation values; table is inferred from QueueType.
func (db *DB) AddNodes(ops []InsertOperation) {
	var srcNodes []*NodeState
	var dstNodes []*NodeState
	var srcStatus, dstStatus string
	for _, op := range ops {
		if op.State == nil {
			continue
		}
		st := op.Status
		if st == "" {
			st = op.State.TraversalStatus
		}
		if op.QueueType == "DST" {
			dstNodes = append(dstNodes, op.State)
			dstStatus = st
		} else {
			srcNodes = append(srcNodes, op.State)
			srcStatus = st
		}
	}
	if len(srcNodes) > 0 {
		db.srcNodesBuffer.AddBatch(srcNodes, srcStatus)
	}
	if len(dstNodes) > 0 {
		db.dstNodesBuffer.AddBatch(dstNodes, dstStatus)
	}
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

// FlushTablesForQueue flushes the buffers for the tables that queue writes to.
// SRC: src_staging + src_nodes. DST: dst_staging + dst_nodes + src_staging.
func (db *DB) FlushTablesForQueue(queueType string) {
	if queueType == "DST" {
		db.srcStagingBuffer.Flush() // DST writes copy updates to src_staging
		db.dstStagingBuffer.Flush()
		db.dstNodesBuffer.Flush()
	} else {
		db.srcStagingBuffer.Flush()
		db.srcNodesBuffer.Flush()
	}
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
		_ = tx.Rollback()
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
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// runStagingFlush writes staging rows to the DB. queueType determines which staging appenders to create (SRC vs DST).
func (db *DB) runStagingFlush(queueType string, srcRows map[string]srcStagingRow, dstRows map[string]string) error {
	if len(srcRows) == 0 && len(dstRows) == 0 {
		return nil
	}
	return db.runAppenderTxConn(db.conn, queueType, false, func(aw appenderFlusher) error {
		for nodeID, r := range srcRows {
			if err := aw.appendSrcStaging(nodeID, r.traversal, r.copy); err != nil {
				return err
			}
		}
		for nodeID, newTraversal := range dstRows {
			if err := aw.appendDstStaging(nodeID, newTraversal); err != nil {
				return err
			}
		}
		return nil
	})
}

// runNodesFlush writes node rows via native DuckDB Appender (same path as staging). Bypasses SQL execution overhead.
func (db *DB) runNodesFlush(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	return db.runAppenderTxConn(db.conn, table, true, func(aw appenderFlusher) error {
		for _, n := range nodes {
			if err := aw.appendNode(table, n); err != nil {
				return err
			}
		}
		return nil
	})
}

// appenderFlusher abstracts the per-queue appender for staging and node flushes.
type appenderFlusher interface {
	appendSrcStaging(nodeID, traversal, copy string) error
	appendDstStaging(nodeID, newTraversal string) error
	appendNode(table string, n *NodeState) error
}

// runAppenderTxConn runs fn with an appender on the given connection. stagingOnly=true means only staging appenders are created.
func (db *DB) runAppenderTxConn(conn *sql.DB, queueType string, nodesIncluded bool, fn func(aw appenderFlusher) error) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	ctx := context.Background()
	c, err := conn.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	tx, err := c.BeginTx(ctx, nil)
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
	err = c.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return errors.New("raw connection is not driver.Conn")
		}
		aw, err := newQueueAppenderWriter(dc, queueType, nodesIncluded)
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
