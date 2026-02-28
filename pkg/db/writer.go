// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
)

// Writer is the write handle for DuckDB. Used inside RunUpdateWriterTx.
type Writer struct {
	tx *sql.Tx
}

// NodeStateAppendRowArgs returns the column values for one NodeState in table order (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors) for use with duckdb.Appender.AppendRow.
func NodeStateAppendRowArgs(n *NodeState) []interface{} {
	trav := n.TraversalStatus
	if trav == "" {
		trav = n.Status
	}
	return []interface{}{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, n.Path, n.ParentPath,
		n.Type, n.Size, n.MTime, int32(n.Depth), trav, n.CopyStatus, n.Excluded, n.Errors,
	}
}

// AppenderInsert inserts nodes into src_nodes or dst_nodes (batch INSERT). Used when not using DuckDB Appender path; for Appender path use RunAppenderTx.
func (w *Writer) AppenderInsert(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, n := range nodes {
		traversalStatus := n.TraversalStatus
		if traversalStatus == "" {
			traversalStatus = n.Status
		}
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, n.Path, n.ParentPath, n.Type, n.Size, n.MTime, n.Depth, traversalStatus, n.CopyStatus, n.Excluded, n.Errors,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteLevelStatsSnapshot writes per-depth stats for a sealed level (traversal counts + completed). If copyPending >= 0 and table is SRC, also writes copy/* keys.
func (w *Writer) WriteLevelStatsSnapshot(table string, depth int, pending, successful, failed, completed int64, copyPending, copySuccessful, copyFailed int64) error {
	ctx := context.Background()
	statsTbl := tableSrcStats
	if table == "DST" {
		statsTbl = tableDstStats
	}
	_, err := w.tx.ExecContext(ctx, `DELETE FROM `+statsTbl+` WHERE depth = $1`, depth)
	if err != nil {
		return err
	}
	for _, pair := range []struct {
		key   string
		count int64
	}{
		{StatsKeyTraversalStatus(StatusPending), pending},
		{StatsKeyTraversalStatus(StatusSuccessful), successful},
		{StatsKeyTraversalStatus(StatusFailed), failed},
		{StatsKeyCompleted, completed},
	} {
		if err := w.SetStatsCountForDepth(table, depth, pair.key, pair.count); err != nil {
			return err
		}
	}
	if table == "SRC" && copyPending >= 0 {
		for _, pair := range []struct {
			key   string
			count int64
		}{
			{StatsKeyCopyStatus(CopyStatusPending), copyPending},
			{StatsKeyCopyStatus(CopyStatusSuccessful), copySuccessful},
			{StatsKeyCopyStatus(CopyStatusFailed), copyFailed},
		} {
			if err := w.SetStatsCountForDepth(table, depth, pair.key, pair.count); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetStatsCountForDepth sets (depth, key, count) in src_stats or dst_stats. Must be called inside RunUpdateWriterTx.
func (w *Writer) SetStatsCountForDepth(table string, depth int, key string, count int64) error {
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO `+tbl+` (depth, key, count) VALUES ($1, $2, $3)
		 ON CONFLICT (depth, key) DO UPDATE SET count = excluded.count`,
		depth, key, count,
	)
	return err
}

// RecomputeStatsForDepth deletes stats for the given table and depth, then writes
// fresh counts from the live nodes table at that depth (by traversal_status, and
// for SRC by copy_status). Call after direct writes that affect node counts at
// that depth (e.g. SetNodeTraversalStatus, DeleteSubtree).
func (w *Writer) RecomputeStatsForDepth(table string, depth int) error {
	ctx := context.Background()
	t := tableName(table)
	statsTbl := tableSrcStats
	if table == "DST" {
		statsTbl = tableDstStats
	}
	_, err := w.tx.ExecContext(ctx, `DELETE FROM `+statsTbl+` WHERE depth = $1`, depth)
	if err != nil {
		return err
	}

	// Rebuild traversal stats in one set-based statement.
	_, err = w.tx.ExecContext(ctx,
		`INSERT INTO `+statsTbl+` (depth, key, count)
		 SELECT $1 AS depth, 'traversal/' || traversal_status AS key, COUNT(*)::BIGINT AS count
		 FROM `+t+`
		 WHERE depth = $1 AND COALESCE(traversal_status, '') <> ''
		 GROUP BY traversal_status`,
		depth,
	)
	if err != nil {
		return err
	}

	if table == "SRC" {
		// Rebuild copy stats in one set-based statement.
		_, err = w.tx.ExecContext(ctx,
			`INSERT INTO `+statsTbl+` (depth, key, count)
			 SELECT $1 AS depth, 'copy/' || copy_status AS key, COUNT(*)::BIGINT AS count
			 FROM `+t+`
			 WHERE depth = $1 AND COALESCE(copy_status, '') <> ''
			 GROUP BY copy_status`,
			depth,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteNode deletes the node from the given table (for retry DST cleanup).
func (w *Writer) DeleteNode(table, nodeID string) error {
	t := tableName(table)
	_, err := w.tx.ExecContext(context.Background(), `DELETE FROM `+t+` WHERE id = $1`, nodeID)
	return err
}

// SealDepth0 updates existing depth-0 nodes (seeded roots) and writes the depth-0 stats snapshot.
// Unlike SealLevel insert flow, this does not append rows; it updates existing root rows.
func (w *Writer) SealDepth0(table string, nodes []*NodeState, pending, successful, failed, completed int64, copyPending, copySuccessful, copyFailed int64) error {
	ctx := context.Background()
	t := tableName(table)
	for _, n := range nodes {
		if n == nil || n.ID == "" {
			continue
		}
		traversalStatus := n.TraversalStatus
		if traversalStatus == "" {
			traversalStatus = n.Status
		}
		_, err := w.tx.ExecContext(ctx, `UPDATE `+t+` SET traversal_status = $1, copy_status = $2 WHERE id = $3`, traversalStatus, n.CopyStatus, n.ID)
		if err != nil {
			return err
		}
	}
	return w.WriteLevelStatsSnapshot(table, 0, pending, successful, failed, completed, copyPending, copySuccessful, copyFailed)
}

// SetNodeTraversalStatus updates a node's traversal_status on the live table and recomputes stats for that depth. For test setup only; normal flow uses staging. Table is "SRC" or "DST".
func (w *Writer) SetNodeTraversalStatus(table, nodeID, status string) error {
	ctx := context.Background()
	t := tableName(table)
	var depth int
	var oldStatus string
	err := w.tx.QueryRowContext(ctx, `SELECT depth, COALESCE(traversal_status, '') FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth, &oldStatus)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx, `UPDATE `+t+` SET traversal_status = $1 WHERE id = $2`, status, nodeID)
	if err != nil {
		return err
	}
	return w.RecomputeStatsForDepth(table, depth)
}

// SetNodeCopyStatus updates a SRC node's copy_status on the live table and recomputes stats for that depth.
func (w *Writer) SetNodeCopyStatus(table, nodeID, status string) error {
	if table != "SRC" {
		return nil
	}
	ctx := context.Background()
	t := tableName(table)
	var depth int
	err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx, `UPDATE `+t+` SET copy_status = $1 WHERE id = $2`, status, nodeID)
	if err != nil {
		return err
	}
	return w.RecomputeStatsForDepth(table, depth)
}

// SetNodeExcluded updates a node's excluded flag on the live table. No stats update (schema has no excluded key in stats).
func (w *Writer) SetNodeExcluded(table, nodeID string, excluded bool) error {
	t := tableName(table)
	_, err := w.tx.ExecContext(context.Background(), `UPDATE `+t+` SET excluded = $1 WHERE id = $2`, excluded, nodeID)
	return err
}

// DeleteSubtree deletes all nodes in the subtree at rootPath (path = rootPath OR path LIKE rootPath || '/%'; for rootPath "/" uses path LIKE '/%'), then recomputes stats for each affected depth. Table is "SRC" or "DST".
func (w *Writer) DeleteSubtree(table, rootPath string) error {
	ctx := context.Background()
	t := tableName(table)
	var rows *sql.Rows
	var err error
	if rootPath == "/" {
		rows, err = w.tx.QueryContext(ctx, `SELECT DISTINCT depth FROM `+t+` WHERE path LIKE '/%'`)
	} else {
		prefix := rootPath + "/%"
		rows, err = w.tx.QueryContext(ctx, `SELECT DISTINCT depth FROM `+t+` WHERE path = $1 OR path LIKE $2`, rootPath, prefix)
	}
	if err != nil {
		return err
	}
	var depths []int
	for rows.Next() {
		var d int
		if err := rows.Scan(&d); err != nil {
			rows.Close()
			return err
		}
		depths = append(depths, d)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if rootPath == "/" {
		_, err = w.tx.ExecContext(ctx, `DELETE FROM `+t+` WHERE path LIKE '/%'`)
	} else {
		prefix := rootPath + "/%"
		_, err = w.tx.ExecContext(ctx, `DELETE FROM `+t+` WHERE path = $1 OR path LIKE $2`, rootPath, prefix)
	}
	if err != nil {
		return err
	}
	for _, depth := range depths {
		if err := w.RecomputeStatsForDepth(table, depth); err != nil {
			return err
		}
	}
	return nil
}

// InsertLog inserts a row into logs. id must be unique (e.g. from GenerateLogID).
func (w *Writer) InsertLog(id int64, level, message, component, entity, entityID, queue string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO logs (id, level, message, component, entity, entity_id, queue) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		id, level, message, component, entity, entityID, queue,
	)
	return err
}

// RecordTaskError inserts a row into task_errors.
func (w *Writer) RecordTaskError(queueType, phase, nodeID, message string, attempts int, path string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO task_errors (queue_type, phase, node_id, message, attempts, path) VALUES ($1, $2, $3, $4, $5, $6)`,
		queueType, phase, nodeID, message, attempts, path,
	)
	return err
}

// WriteQueueStats upserts queue metrics JSON into queue_stats.
func (w *Writer) WriteQueueStats(queueKey, metricsJSON string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO queue_stats (queue_key, metrics_json) VALUES ($1, $2)
		 ON CONFLICT (queue_key) DO UPDATE SET metrics_json = excluded.metrics_json`,
		queueKey, metricsJSON,
	)
	return err
}
