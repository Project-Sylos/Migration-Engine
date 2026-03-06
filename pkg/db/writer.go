// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Writer is the write handle for DuckDB. Used inside RunWrite via WriteSession.WithTx.
type Writer struct {
	tx *sql.Tx
}

// NodeStateAppendRowArgs returns the column values for one NodeState (metadata only) in table order for use with duckdb.Appender.AppendRow.
func NodeStateAppendRowArgs(n *NodeState) []any {
	return []any{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, n.Path, n.ParentPath,
		PathHash(n.Path), PathHash(n.ParentPath),
		n.Type, n.Size, n.MTime, int32(n.Depth),
	}
}

// AppenderInsert inserts node metadata into src_nodes or dst_nodes (batch INSERT). No status columns.
func (w *Writer) AppenderInsert(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, n := range nodes {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, path_hash, parent_path_hash, type, size, mtime, depth)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, n.Path, n.ParentPath, PathHash(n.Path), PathHash(n.ParentPath), n.Type, n.Size, n.MTime, n.Depth,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// SrcStatusEventAppendRowArgs returns column values for one row in src_status_events (id, traversal_status, copy_status, event_time, depth) for appender.
func SrcStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.CopyStatus, e.EventTime, int32(e.Depth)}
}

// DstStatusEventAppendRowArgs returns column values for one row in dst_status_events (id, traversal_status, event_time, depth) for appender.
func DstStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.EventTime, int32(e.Depth)}
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

// WriteLevelStatsSnapshotsBatch writes stats for multiple (table, depth) snapshots in two batched DELETEs and two batched INSERTs. Call from seal flush to reduce round-trips.
func (w *Writer) WriteLevelStatsSnapshotsBatch(rows []sealStatsSnapshot) error {
	if len(rows) == 0 {
		return nil
	}
	ctx := context.Background()
	srcDepths := make([]int, 0, len(rows))
	dstDepths := make([]int, 0, len(rows))
	var srcTuples []struct {
		depth int
		key   string
		count int64
	}
	var dstTuples []struct {
		depth int
		key   string
		count int64
	}
	for _, r := range rows {
		depth := r.depth
		if r.table == "SRC" {
			srcDepths = append(srcDepths, depth)
			srcTuples = append(srcTuples,
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyTraversalStatus(StatusPending), r.pending},
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyTraversalStatus(StatusSuccessful), r.success},
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyTraversalStatus(StatusFailed), r.failed},
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyCompleted, r.completed},
			)
			if r.copyP >= 0 {
				srcTuples = append(srcTuples,
					struct {
						depth int
						key   string
						count int64
					}{depth, StatsKeyCopyStatus(CopyStatusPending), r.copyP},
					struct {
						depth int
						key   string
						count int64
					}{depth, StatsKeyCopyStatus(CopyStatusSuccessful), r.copyS},
					struct {
						depth int
						key   string
						count int64
					}{depth, StatsKeyCopyStatus(CopyStatusFailed), r.copyF},
				)
			}
		} else {
			dstDepths = append(dstDepths, depth)
			dstTuples = append(dstTuples,
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyTraversalStatus(StatusPending), r.pending},
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyTraversalStatus(StatusSuccessful), r.success},
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyTraversalStatus(StatusFailed), r.failed},
				struct {
					depth int
					key   string
					count int64
				}{depth, StatsKeyCompleted, r.completed},
			)
		}
	}
	if len(srcDepths) > 0 {
		if _, err := w.tx.ExecContext(ctx, buildDeleteIn(tableSrcStats, srcDepths)); err != nil {
			return err
		}
		if len(srcTuples) > 0 {
			if err := w.bulkInsertStats(ctx, tableSrcStats, srcTuples); err != nil {
				return err
			}
		}
	}
	if len(dstDepths) > 0 {
		if _, err := w.tx.ExecContext(ctx, buildDeleteIn(tableDstStats, dstDepths)); err != nil {
			return err
		}
		if len(dstTuples) > 0 {
			if err := w.bulkInsertStats(ctx, tableDstStats, dstTuples); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildDeleteIn(table string, depths []int) string {
	if len(depths) == 0 {
		return "SELECT 1"
	}
	b := "DELETE FROM " + table + " WHERE depth IN ("
	for i := range depths {
		if i > 0 {
			b += ","
		}
		b += strconv.Itoa(depths[i])
	}
	return b + ")"
}

func (w *Writer) bulkInsertStats(ctx context.Context, table string, tuples []struct {
	depth int
	key   string
	count int64
}) error {
	if len(tuples) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(tuples)*3)
	placeholders := make([]string, 0, len(tuples))
	for i, t := range tuples {
		args = append(args, t.depth, t.key, t.count)
		n := i*3 + 1
		placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d)", n, n+1, n+2))
	}
	q := "INSERT INTO " + table + " (depth, key, count) VALUES " + strings.Join(placeholders, ",") + " ON CONFLICT (depth, key) DO UPDATE SET count = excluded.count"
	_, err := w.tx.ExecContext(ctx, q, args...)
	return err
}

// SetStatsCountForDepth sets (depth, key, count) in src_stats or dst_stats. Must be called inside RunWrite (WithTx).
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

// UpdateStatsCountByDelta applies a delta to the (depth, key) count. Used for single-node status changes (traversal/copy hot path); avoids full recompute.
func (w *Writer) UpdateStatsCountByDelta(table string, depth int, key string, delta int64) error {
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO `+tbl+` (depth, key, count) VALUES ($1, $2, $3)
		 ON CONFLICT (depth, key) DO UPDATE SET count = count + excluded.count`,
		depth, key, delta,
	)
	return err
}

// RecomputeStatsForDepth deletes stats for the given table and depth, then writes fresh counts from nodes joined with current status (event-derived).
// For bulk operations only (DeleteSubtree, exclusion propagation, diffs/search). Single-node updates use UpdateStatsCountByDelta instead.
func (w *Writer) RecomputeStatsForDepth(table string, depth int) error {
	ctx := context.Background()
	t := tableName(table)
	statsTbl := tableSrcStats
	cte := cteSrcCurrentStatus
	if table == "DST" {
		statsTbl = tableDstStats
		cte = cteDstCurrentStatus
	}
	_, err := w.tx.ExecContext(ctx, `DELETE FROM `+statsTbl+` WHERE depth = $1`, depth)
	if err != nil {
		return err
	}
	_, err = w.tx.ExecContext(ctx,
		`INSERT INTO `+statsTbl+` (depth, key, count)
		 SELECT $1 AS depth, 'traversal/' || COALESCE(e.traversal_status,'') AS key, COUNT(*)::BIGINT AS count
		 FROM `+t+` n LEFT JOIN `+cte+` e ON n.id = e.id
		 WHERE n.depth = $1 AND COALESCE(e.traversal_status,'') <> ''
		 GROUP BY e.traversal_status`,
		depth,
	)
	if err != nil {
		return err
	}
	if table == "SRC" {
		_, err = w.tx.ExecContext(ctx,
			`INSERT INTO `+statsTbl+` (depth, key, count)
			 SELECT $1 AS depth, 'copy/' || COALESCE(e.copy_status,'') AS key, COUNT(*)::BIGINT AS count
			 FROM `+t+` n LEFT JOIN `+cte+` e ON n.id = e.id
			 WHERE n.depth = $1 AND COALESCE(e.copy_status,'') <> ''
			 GROUP BY e.copy_status`,
			depth,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertExclusionEventsForSubtree appends one status event per node in the subtree (path = rootPath OR path LIKE rootPath/'%') with traversal_status = 'exclusion_inherited'. Table is "SRC" or "DST". Call inside RunWrite.
func (w *Writer) InsertExclusionEventsForSubtree(table, rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	if table == "DST" {
		if rootPath == "/" {
			_, err := w.tx.ExecContext(ctx, `INSERT INTO dst_status_events (id, traversal_status, event_time, depth) SELECT id, 'exclusion_inherited', $1, depth FROM dst_nodes WHERE path LIKE '/%'`, eventTime)
			return err
		}
		prefix := rootPath + "/%"
		_, err := w.tx.ExecContext(ctx, `INSERT INTO dst_status_events (id, traversal_status, event_time, depth) SELECT id, 'exclusion_inherited', $1, depth FROM dst_nodes WHERE path = $2 OR path LIKE $3`, eventTime, rootPath, prefix)
		return err
	}
	if rootPath == "/" {
		_, err := w.tx.ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth) SELECT n.id, 'exclusion_inherited', COALESCE((SELECT arg_max(e.copy_status, e.event_time) FROM src_status_events e WHERE e.id = n.id), ''), $1, n.depth FROM src_nodes n WHERE n.path LIKE '/%'`, eventTime)
		return err
	}
	prefix := rootPath + "/%"
	_, err := w.tx.ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth) SELECT n.id, 'exclusion_inherited', COALESCE((SELECT arg_max(e.copy_status, e.event_time) FROM src_status_events e WHERE e.id = n.id), ''), $1, n.depth FROM src_nodes n WHERE n.path = $2 OR n.path LIKE $3`, eventTime, rootPath, prefix)
	return err
}

// InsertStatusEvent appends one row to src_status_events or dst_status_events. Table is "SRC" or "DST".
func (w *Writer) InsertStatusEvent(table string, e *StatusEvent) error {
	ctx := context.Background()
	if table == "DST" {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableDstStatusEvents+` (id, traversal_status, event_time, depth) VALUES ($1, $2, $3, $4)`,
			e.ID, e.TraversalStatus, e.EventTime, e.Depth,
		)
		return err
	}
	_, err := w.tx.ExecContext(ctx,
		`INSERT INTO `+tableSrcStatusEvents+` (id, traversal_status, copy_status, event_time, depth) VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.TraversalStatus, e.CopyStatus, e.EventTime, e.Depth,
	)
	return err
}

// DeleteNode deletes the node from the given table (for retry DST cleanup).
func (w *Writer) DeleteNode(table, nodeID string) error {
	t := tableName(table)
	_, err := w.tx.ExecContext(context.Background(), `DELETE FROM `+t+` WHERE id = $1`, nodeID)
	return err
}

// SealDepth0 writes the depth-0 stats snapshot and emits status events for each depth-0 node so the events table reflects current state (e.g. root marked successful after round 0 completes).
func (w *Writer) SealDepth0(table string, nodes []*NodeState, pending, successful, failed, completed int64, copyPending, copySuccessful, copyFailed int64) error {
	if err := w.WriteLevelStatsSnapshot(table, 0, pending, successful, failed, completed, copyPending, copySuccessful, copyFailed); err != nil {
		return err
	}
	eventTime := time.Now().UnixNano()
	for _, nd := range nodes {
		trav := nd.TraversalStatus
		if trav == "" {
			trav = nd.Status
		}
		ev := &StatusEvent{ID: nd.ID, TraversalStatus: trav, EventTime: eventTime, Depth: 0}
		if table == "SRC" {
			ev.CopyStatus = nd.CopyStatus
		}
		if err := w.InsertStatusEvent(table, ev); err != nil {
			return err
		}
	}
	return nil
}

// SetNodeTraversalStatus emits a traversal_status event and applies stat deltas (decrement old, increment new). Single-node path: no full recompute.
func (w *Writer) SetNodeTraversalStatus(table, nodeID, status string) error {
	ctx := context.Background()
	t := tableName(table)
	var depth int
	err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth)
	if err != nil {
		return err
	}
	var oldStatus string
	if table == "SRC" {
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&oldStatus)
	} else {
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM dst_status_events WHERE id = $1`, nodeID).Scan(&oldStatus)
	}
	ev := &StatusEvent{ID: nodeID, TraversalStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	if table == "SRC" {
		var copyStatus string
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&copyStatus)
		ev.CopyStatus = copyStatus
	}
	if err := w.InsertStatusEvent(table, ev); err != nil {
		return err
	}
	if oldStatus != "" {
		if err := w.UpdateStatsCountByDelta(table, depth, StatsKeyTraversalStatus(oldStatus), -1); err != nil {
			return err
		}
	}
	if status != "" {
		if err := w.UpdateStatsCountByDelta(table, depth, StatsKeyTraversalStatus(status), 1); err != nil {
			return err
		}
	}
	return nil
}

// SetNodeCopyStatus emits a copy_status event and applies stat deltas for SRC. Single-node path: no full recompute.
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
	var oldCopy string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&oldCopy)
	var trav string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&trav)
	ev := &StatusEvent{ID: nodeID, TraversalStatus: trav, CopyStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	if err := w.InsertStatusEvent("SRC", ev); err != nil {
		return err
	}
	if oldCopy != "" && oldCopy != CopyStatusInProgress {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKeyCopyStatus(oldCopy), -1); err != nil {
			return err
		}
	}
	if status != "" && status != CopyStatusInProgress {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKeyCopyStatus(status), 1); err != nil {
			return err
		}
	}
	return nil
}

// SetNodeExcluded emits a traversal_status event and applies stat deltas (decrement old, increment new). Single-node path: no full recompute.
func (w *Writer) SetNodeExcluded(table, nodeID string, excluded bool) error {
	ctx := context.Background()
	t := tableName(table)
	var depth int
	err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth)
	if err != nil {
		return err
	}
	var oldStatus string
	if table == "SRC" {
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&oldStatus)
	} else {
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM dst_status_events WHERE id = $1`, nodeID).Scan(&oldStatus)
	}
	status := StatusPending
	if excluded {
		status = StatusExcluded
	}
	ev := &StatusEvent{ID: nodeID, TraversalStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	if table == "SRC" {
		var copyStatus string
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&copyStatus)
		ev.CopyStatus = copyStatus
	}
	if err := w.InsertStatusEvent(table, ev); err != nil {
		return err
	}
	if oldStatus != "" && oldStatus != status {
		if err := w.UpdateStatsCountByDelta(table, depth, StatsKeyTraversalStatus(oldStatus), -1); err != nil {
			return err
		}
	}
	if status != "" {
		if err := w.UpdateStatsCountByDelta(table, depth, StatsKeyTraversalStatus(status), 1); err != nil {
			return err
		}
	}
	return nil
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
func (w *Writer) InsertLog(id string, level, message, component, entity, entityID, queue string) error {
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
