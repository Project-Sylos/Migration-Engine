// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
)

// statsKey is (depth, key) for incremental stats updates at seal.
type statsKey struct {
	depth int
	key   string
}

// Writer is the write handle for DuckDB. Used inside RunUpdateWriterTx. Holds tx and optional db for seal-time stats deltas.
type Writer struct {
	tx *sql.Tx
	db *DB // optional; when set, seal uses pre-accumulated stats deltas instead of computing from staging+live
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

// computeSrcStatsDeltas returns (depth, key) -> delta for the sealed depth by joining src_staging with src_nodes at that depth. Empty statuses are skipped (no key).
func (w *Writer) computeSrcStatsDeltas(sealedDepth int) (map[statsKey]int64, error) {
	ctx := context.Background()
	rows, err := w.tx.QueryContext(ctx,
		`SELECT n.depth,
			COALESCE(n.traversal_status, '') AS old_trav, COALESCE(n.copy_status, '') AS old_copy,
			COALESCE(s.new_traversal_status, '') AS new_trav, COALESCE(s.new_copy_status, '') AS new_copy
		 FROM src_nodes n INNER JOIN src_staging s ON n.id = s.node_id WHERE n.depth = $1`,
		sealedDepth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deltas := make(map[statsKey]int64)
	for rows.Next() {
		var d int
		var oldTrav, oldCopy, newTrav, newCopy string
		if err := rows.Scan(&d, &oldTrav, &oldCopy, &newTrav, &newCopy); err != nil {
			return nil, err
		}
		if oldTrav != "" {
			deltas[statsKey{d, StatsKeyTraversalStatus(oldTrav)}]--
		}
		if newTrav != "" {
			deltas[statsKey{d, StatsKeyTraversalStatus(newTrav)}]++
		}
		if oldCopy != "" {
			deltas[statsKey{d, StatsKeyCopyStatus(oldCopy)}]--
		}
		if newCopy != "" {
			deltas[statsKey{d, StatsKeyCopyStatus(newCopy)}]++
		}
	}
	return deltas, rows.Err()
}

// computeDstStatsDeltas returns (depth, key) -> delta for the sealed depth by joining dst_staging with dst_nodes at that depth.
func (w *Writer) computeDstStatsDeltas(sealedDepth int) (map[statsKey]int64, error) {
	ctx := context.Background()
	rows, err := w.tx.QueryContext(ctx,
		`SELECT n.depth,
			COALESCE(n.traversal_status, '') AS old_trav,
			COALESCE(d.new_traversal_status, '') AS new_trav
		 FROM dst_nodes n INNER JOIN dst_staging d ON n.id = d.node_id WHERE n.depth = $1`,
		sealedDepth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deltas := make(map[statsKey]int64)
	for rows.Next() {
		var d int
		var oldTrav, newTrav string
		if err := rows.Scan(&d, &oldTrav, &newTrav); err != nil {
			return nil, err
		}
		if oldTrav != "" {
			deltas[statsKey{d, StatsKeyTraversalStatus(oldTrav)}]--
		}
		if newTrav != "" {
			deltas[statsKey{d, StatsKeyTraversalStatus(newTrav)}]++
		}
	}
	return deltas, rows.Err()
}

// applyStatsDeltas applies deltas to src_stats or dst_stats (INSERT/ON CONFLICT increment).
func (w *Writer) applyStatsDeltas(table string, deltas map[statsKey]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	ctx := context.Background()
	for k, delta := range deltas {
		if delta == 0 {
			continue
		}
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tbl+` (depth, key, count) VALUES ($1, $2, $3)
			 ON CONFLICT (depth, key) DO UPDATE SET count = `+tbl+`.count + excluded.count`,
			k.depth, k.key, delta,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyStatusStagingAndDrop merges src_staging and dst_staging into live nodes,
// applies pre-accumulated stats deltas for the sealed depth (O(1) per key), writes
// completed count, then clears staging. When w.db is set, deltas come from the
// accumulator (filled at staging flush); otherwise we fall back to computing from
// staging+live JOIN (e.g. MaybeMergeStagingEarly or tests).
func (w *Writer) ApplyStatusStagingAndDrop(depth int, table string, completed int64) error {
	ctx := context.Background()

	var srcDeltas, dstDeltas map[statsKey]int64
	if w.db != nil {
		srcDeltas, dstDeltas = w.db.GetAndClearStatsDeltasForDepth(depth)
	}
	if srcDeltas == nil {
		srcDeltas = make(map[statsKey]int64)
	}
	if dstDeltas == nil {
		dstDeltas = make(map[statsKey]int64)
	}
	if w.db == nil {
		var err error
		if srcDeltas, err = w.computeSrcStatsDeltas(depth); err != nil {
			return err
		}
		if dstDeltas, err = w.computeDstStatsDeltas(depth); err != nil {
			return err
		}
	}

	// 1) Merge staging into live (depth filter lets planner use depth index)
	_, err := w.tx.ExecContext(ctx,
		`UPDATE src_nodes SET
			traversal_status = COALESCE(s.new_traversal_status, src_nodes.traversal_status),
			copy_status = COALESCE(s.new_copy_status, src_nodes.copy_status)
		 FROM src_staging s WHERE src_nodes.id = s.node_id AND src_nodes.depth = $1`,
		depth)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx,
		`UPDATE dst_nodes SET traversal_status = d.new_traversal_status
		 FROM dst_staging d WHERE dst_nodes.id = d.node_id AND dst_nodes.depth = $1`,
		depth)
	if err != nil {
		return err
	}

	// 2) Apply stats deltas (pre-accumulated at flush or just computed)
	switch table {
	case "SRC":
		if err := w.applyStatsDeltas("SRC", srcDeltas); err != nil {
			return err
		}
	case "DST":
		if err := w.applyStatsDeltas("DST", dstDeltas); err != nil {
			return err
		}
	default:
		if err := w.applyStatsDeltas("SRC", srcDeltas); err != nil {
			return err
		}
		if err := w.applyStatsDeltas("DST", dstDeltas); err != nil {
			return err
		}
	}

	// 3) Write completed count for this depth/table (if provided)
	if table != "" {
		if err := w.SetStatsCountForDepth(table, depth, StatsKeyCompleted, completed); err != nil {
			return err
		}
	}

	// 4) Drop and recreate staging tables (avoids accumulating deleted rows across rounds)
	_, err = w.tx.ExecContext(ctx, `DROP TABLE IF EXISTS src_staging`)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx, srcStagingDDL())
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx, `DROP TABLE IF EXISTS dst_staging`)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx, dstStagingDDL())
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
