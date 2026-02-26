// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
)

// Writer is the write handle for DuckDB. Used inside RunUpdateWriterTx. Holds only *sql.Tx so all operations are in one transaction.
type Writer struct {
	tx *sql.Tx
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

// ensureStaging creates src_staging and dst_staging if they do not exist (on the Tx).
func (w *Writer) ensureStaging() error {
	_, err := w.tx.Exec(srcStagingDDL())
	if err != nil {
		return err
	}
	_, err = w.tx.Exec(dstStagingDDL())
	if err != nil {
		return err
	}
	return nil
}

// AppendStatusStaging appends a traversal status update to src_staging (table SRC) or dst_staging (table DST). Uses ON CONFLICT to merge with existing row for the same node.
func (w *Writer) AppendStatusStaging(tableName, nodeID, _, newTraversalStatus string) error {
	if err := w.ensureStaging(); err != nil {
		return err
	}
	ctx := context.Background()
	if tableName == "DST" {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO dst_staging (node_id, new_traversal_status) VALUES ($1, $2)
			 ON CONFLICT (node_id) DO UPDATE SET new_traversal_status = excluded.new_traversal_status`,
			nodeID, newTraversalStatus,
		)
		return err
	}
	_, err := w.tx.ExecContext(ctx,
		`INSERT INTO src_staging (node_id, new_traversal_status) VALUES ($1, $2)
		 ON CONFLICT (node_id) DO UPDATE SET new_traversal_status = excluded.new_traversal_status`,
		nodeID, newTraversalStatus,
	)
	return err
}

// AppendCopyStaging appends a copy status update to src_staging. Uses ON CONFLICT to merge with existing row for the same node.
func (w *Writer) AppendCopyStaging(nodeID, _, newCopyStatus string) error {
	if err := w.ensureStaging(); err != nil {
		return err
	}
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO src_staging (node_id, new_copy_status) VALUES ($1, $2)
		 ON CONFLICT (node_id) DO UPDATE SET new_copy_status = excluded.new_copy_status`,
		nodeID, newCopyStatus,
	)
	return err
}

// SetStatsCountForDepth sets (depth, key, count) in src_stats or dst_stats. Must be called inside RunUpdateWriterTx. Used at seal after merging staging into live.
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

// RecomputeStatsForDepth deletes stats for the given table and depth, then counts from the live nodes table at that depth (by traversal_status, and for SRC by copy_status) and writes each key/count to the stats table. Call after any direct write that affects node counts at that depth (e.g. SetNodeTraversalStatus, DeleteSubtree).
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
	rows, err := w.tx.QueryContext(ctx,
		`SELECT COALESCE(traversal_status, '') AS status, COUNT(*)::BIGINT FROM `+t+` WHERE depth = $1 GROUP BY traversal_status`, depth)
	if err != nil {
		return err
	}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return err
		}
		if status == "" {
			continue
		}
		if err := w.SetStatsCountForDepth(table, depth, StatsKeyTraversalStatus(status), count); err != nil {
			rows.Close()
			return err
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if table == "SRC" {
		rows, err = w.tx.QueryContext(ctx,
			`SELECT COALESCE(copy_status, '') AS status, COUNT(*)::BIGINT FROM `+t+` WHERE depth = $1 GROUP BY copy_status`, depth)
		if err != nil {
			return err
		}
		for rows.Next() {
			var status string
			var count int64
			if err := rows.Scan(&status, &count); err != nil {
				rows.Close()
				return err
			}
			if status == "" {
				continue
			}
			if err := w.SetStatsCountForDepth("SRC", depth, StatsKeyCopyStatus(status), count); err != nil {
				rows.Close()
				return err
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// ApplyStatusStagingAndDrop merges src_staging and dst_staging into live nodes, recomputes stats for the given depth, writes completed count for the given table (if table != ""), then clears staging tables. Call at level seal with the depth being sealed; table is "SRC" or "DST", completed is that queue's completed count for this round.
func (w *Writer) ApplyStatusStagingAndDrop(depth int, table string, completed int64) error {
	ctx := context.Background()

	// 1) Merge staging into live
	_, err := w.tx.ExecContext(ctx,
		`UPDATE src_nodes SET
			traversal_status = COALESCE(s.new_traversal_status, src_nodes.traversal_status),
			copy_status = COALESCE(s.new_copy_status, src_nodes.copy_status)
		 FROM src_staging s WHERE src_nodes.id = s.node_id`)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx,
		`UPDATE dst_nodes SET traversal_status = d.new_traversal_status
		 FROM dst_staging d WHERE dst_nodes.id = d.node_id`)
	if err != nil {
		return err
	}

	// 2) Count from live at this depth and write to stats (replace all stats for this depth)
	if err := w.RecomputeStatsForDepth("SRC", depth); err != nil {
		return err
	}
	if err := w.RecomputeStatsForDepth("DST", depth); err != nil {
		return err
	}

	// 3) Write completed count for this depth/table (if provided)
	if table != "" {
		if err := w.SetStatsCountForDepth(table, depth, StatsKeyCompleted, completed); err != nil {
			return err
		}
	}

	// 4) Clear staging tables for next level (DELETE is quick; tables stay in place)
	_, err = w.tx.ExecContext(ctx, `DELETE FROM src_staging`)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx, `DELETE FROM dst_staging`)
	if err != nil {
		return err
	}
	return nil
}

// DeleteNode deletes the node from the given table (for retry DST cleanup).
func (w *Writer) DeleteNode(table, nodeID string) error {
	t := tableName(table)
	_, err := w.tx.ExecContext(context.Background(), `DELETE FROM `+t+` WHERE id = $1`, nodeID)
	return err
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
