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

// statsDeltaAccum holds per-(depth, key) deltas to apply to stats at seal. Updated when the queue adds to the buffer.
type statsDeltaAccum struct {
	mu   sync.Mutex
	src  map[statsKey]int64
	dst  map[statsKey]int64
}

func newStatsDeltaAccum() *statsDeltaAccum {
	return &statsDeltaAccum{
		src: make(map[statsKey]int64),
		dst: make(map[statsKey]int64),
	}
}

// DB is the DuckDB-backed database handle. Single physical connection for all DB operations (schema, appender writes, pulls, merge, checkpoint).
type DB struct {
	path         string
	conn         *sql.DB    // single connection for all operations
	writeMu      sync.Mutex // one global mutex for all DB writes
	checkpointMu sync.Mutex // serializes CHECKPOINT; only one connection runs it since it's a global DB op

	// Table-scoped buffers (DB-owned, not queue-owned)
	srcStagingBuffer *writeBuffer
	dstStagingBuffer *writeBuffer
	srcNodesBuffer   *writeBuffer
	dstNodesBuffer   *writeBuffer

	// Stats deltas accumulated at staging flush; applied at seal (O(1) per key). No recompute from node table at seal.
	statsDeltas *statsDeltaAccum

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

	d := &DB{
		path:        path,
		conn:        conn,
		statsDeltas: newStatsDeltaAccum(),
	}
	d.srcStagingBuffer = newWriteBuffer(d, "SRC", bufferKindStaging)
	d.dstStagingBuffer = newWriteBuffer(d, "DST", bufferKindStaging)
	d.srcNodesBuffer = newWriteBuffer(d, "SRC", bufferKindNodes)
	d.dstNodesBuffer = newWriteBuffer(d, "DST", bufferKindNodes)
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

// AddToStaging buffers a traversal status update. depth and oldTraversal/newTraversal are used to update stats counters in memory (no DB read).
func (db *DB) AddToStaging(table, nodeID string, depth int, oldTraversal, newTraversal string) {
	if table == "DST" {
		db.dstStagingBuffer.addTraversal(nodeID, depth, oldTraversal, newTraversal)
	} else {
		db.srcStagingBuffer.addTraversal(nodeID, depth, oldTraversal, newTraversal)
	}
}

// AddCopyToStaging buffers a copy status update for src_staging. depth and old/new are used to update stats counters in memory.
func (db *DB) AddCopyToStaging(nodeID string, depth int, oldCopy, newCopy string) {
	db.srcStagingBuffer.addCopy(nodeID, depth, oldCopy, newCopy)
}

// AddNode buffers a node insert for src_nodes or dst_nodes.
func (db *DB) AddNode(table string, n *NodeState, status string) {
	if table == "DST" {
		db.dstNodesBuffer.addNode(n, status)
	} else {
		db.srcNodesBuffer.addNode(n, status)
	}
}

// AddNodes buffers node inserts. ops are InsertOperation values; table is inferred from QueueType.
func (db *DB) AddNodes(ops []InsertOperation) {
	var srcNodes []*NodeState
	var dstNodes []*NodeState
	for _, op := range ops {
		if op.State == nil {
			continue
		}
		if op.QueueType == "DST" {
			dstNodes = append(dstNodes, op.State)
		} else {
			srcNodes = append(srcNodes, op.State)
		}
	}
	if len(srcNodes) > 0 {
		db.srcNodesBuffer.addNodeBatch(srcNodes)
	}
	if len(dstNodes) > 0 {
		db.dstNodesBuffer.addNodeBatch(dstNodes)
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
// Uses ForceFlush to persist partial batches before pull/round advance.
// SRC: src_staging + src_nodes. DST: dst_staging + dst_nodes + src_staging.
func (db *DB) FlushTablesForQueue(queueType string) {
	if queueType == "DST" {
		db.srcStagingBuffer.ForceFlush() // DST writes copy updates to src_staging
		db.dstStagingBuffer.ForceFlush()
		db.dstNodesBuffer.ForceFlush()
	} else {
		db.srcStagingBuffer.ForceFlush()
		db.srcNodesBuffer.ForceFlush()
	}
}

// GetStagingRowCount returns the total number of rows in src_staging + dst_staging.
func (db *DB) GetStagingRowCount() (int64, error) {
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx,
		`SELECT (SELECT COUNT(*) FROM src_staging) + (SELECT COUNT(*) FROM dst_staging)`).Scan(&n)
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// MaybeMergeStagingEarly merges staging into live and clears it when row count exceeds threshold.
// Use for pathological wide rounds (e.g. Spectra stress) to prevent unbounded staging growth.
// Pass table="" to skip writing completed count (we are mid-round).
// Returns true if a merge was performed.
func (db *DB) MaybeMergeStagingEarly(round int, threshold int64) (bool, error) {
	count, err := db.GetStagingRowCount()
	if err != nil || count <= threshold {
		return false, err
	}
	db.FlushTablesForQueue("SRC")
	db.FlushTablesForQueue("DST")
	err = db.RunUpdateWriterTx(func(w *Writer) error {
		return w.ApplyStatusStagingAndDrop(round, "", 0)
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// addTraversalDelta records one traversal status transition at the given depth (counters updated at add time, no DB read).
func (a *statsDeltaAccum) addTraversalDelta(table string, depth int, oldStatus, newStatus string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if oldStatus != "" {
		k := statsKey{depth, StatsKeyTraversalStatus(oldStatus)}
		if table == "SRC" {
			a.src[k]--
		} else {
			a.dst[k]--
		}
	}
	if newStatus != "" {
		k := statsKey{depth, StatsKeyTraversalStatus(newStatus)}
		if table == "SRC" {
			a.src[k]++
		} else {
			a.dst[k]++
		}
	}
}

// addCopyDelta records one copy status transition at the given depth (SRC only).
func (a *statsDeltaAccum) addCopyDelta(depth int, oldStatus, newStatus string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if oldStatus != "" {
		a.src[statsKey{depth, StatsKeyCopyStatus(oldStatus)}]--
	}
	if newStatus != "" {
		a.src[statsKey{depth, StatsKeyCopyStatus(newStatus)}]++
	}
}

// GetAndClearStatsDeltasForDepth returns and removes accumulated stats deltas for the given depth.
// Call while holding writeMu (e.g. inside RunUpdateWriterTx before ApplyStatusStagingAndDrop).
func (db *DB) GetAndClearStatsDeltasForDepth(depth int) (src, dst map[statsKey]int64) {
	db.statsDeltas.mu.Lock()
	defer db.statsDeltas.mu.Unlock()
	src = make(map[statsKey]int64)
	dst = make(map[statsKey]int64)
	for k, v := range db.statsDeltas.src {
		if k.depth == depth && v != 0 {
			src[k] = v
			delete(db.statsDeltas.src, k)
		}
	}
	for k, v := range db.statsDeltas.dst {
		if k.depth == depth && v != 0 {
			dst[k] = v
			delete(db.statsDeltas.dst, k)
		}
	}
	return src, dst
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
	w := &Writer{tx: tx, db: db}
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

// runStagingFlush writes staging rows to the DB. Stats deltas are already accumulated in memory when the queue adds to the buffer (no read here).
func (db *DB) runStagingFlush(queueType string, srcRows map[string]srcStagingRow, dstRows map[string]string) error {
	if len(srcRows) == 0 && len(dstRows) == 0 {
		return nil
	}
	return db.runAppenderTxConn(db.conn, queueType, false, nil, func(aw appenderFlusher) error {
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
// Preallocates a single []driver.Value per flush and reuses it for every row to avoid 50k-100k allocations.
func (db *DB) runNodesFlush(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	rowArgs := make([]driver.Value, 14)
	return db.runAppenderTxConn(db.conn, table, true, nil, func(aw appenderFlusher) error {
		for _, n := range nodes {
			if err := aw.appendNode(table, n, rowArgs); err != nil {
				return err
			}
		}
		return nil
	})
}

// appenderFlusher abstracts the per-queue appender for staging and node flushes.
// appendNode receives a preallocated rowArgs slice (length 14) to avoid per-row allocations.
type appenderFlusher interface {
	appendSrcStaging(nodeID, traversal, copy string) error
	appendDstStaging(nodeID, newTraversal string) error
	appendNode(table string, n *NodeState, rowArgs []driver.Value) error
}

// runAppenderTxConn runs fn with an appender on the given connection. nodesIncluded=true means node appenders are created. If pre != nil, it runs after DDL inside the same transaction (e.g. to accumulate stats deltas from live before writing staging).
func (db *DB) runAppenderTxConn(conn *sql.DB, queueType string, nodesIncluded bool, pre func(*sql.Tx) error, fn func(aw appenderFlusher) error) error {
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
	if pre != nil {
		if err := pre(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
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
		err := tx.Rollback()
		if err != nil {
			return err
		}
		if fnErr != nil {
			return fnErr
		}
		return err
	}
	return tx.Commit()
}
