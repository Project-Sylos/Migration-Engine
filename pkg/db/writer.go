// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Writer is the write handle for DuckDB. Used inside RunWrite via WriteSession.WithTx.
type Writer struct {
	tx *sql.Tx
}

// NodeStateAppendRowArgs returns the column values for one NodeState (metadata only) in table order for use with duckdb.Appender.AppendRow.
func NodeStateAppendRowArgs(n *NodeState) []any {
	path, parentPath, pathHash, parentPathHash := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
	return []any{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath,
		pathHash, parentPathHash,
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
		path, parentPath, pathHash, parentPathHash := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, path_hash, parent_path_hash, type, size, mtime, depth)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, pathHash, parentPathHash, n.Type, n.Size, n.MTime, n.Depth,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpsertNodes inserts node metadata into src_nodes or dst_nodes. On conflict (id) does nothing so
// duplicate inserts (e.g. retry re-discovery before DST children are deleted) are safe.
func (w *Writer) UpsertNodes(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, n := range nodes {
		path, parentPath, pathHash, parentPathHash := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, path_hash, parent_path_hash, type, size, mtime, depth)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			 ON CONFLICT (id) DO NOTHING`,
			n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, pathHash, parentPathHash, n.Type, n.Size, n.MTime, n.Depth,
		)
		if err != nil {
			return fmt.Errorf("insert node %s into %s: %w", n.ID, table, err)
		}
	}
	return nil
}

// SrcStatusEventAppendRowArgs returns column values for one row in src_status_events for appender.
func SrcStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.CopyStatus, e.EventTime, int32(e.Depth), e.ErrorLogID}
}

// DstStatusEventAppendRowArgs returns column values for one row in dst_status_events for appender.
func DstStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.EventTime, int32(e.Depth), e.ErrorLogID}
}

// BatchInsertSrcStatusEvents inserts status events into src_status_events inside the current transaction. Used by seal flush so events are atomic with nodes/stats.
func (w *Writer) BatchInsertSrcStatusEvents(events []StatusEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableSrcStatusEvents+` (id, traversal_status, copy_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.TraversalStatus, e.CopyStatus, e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		if err != nil {
			return fmt.Errorf("insert src_status_event %s: %w", e.ID, err)
		}
	}
	return nil
}

// BatchInsertDstStatusEvents inserts status events into dst_status_events inside the current transaction. Used by seal flush so events are atomic with nodes/stats.
func (w *Writer) BatchInsertDstStatusEvents(events []StatusEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableDstStatusEvents+` (id, traversal_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5)`,
			e.ID, e.TraversalStatus, e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		if err != nil {
			return fmt.Errorf("insert dst_status_event %s: %w", e.ID, err)
		}
	}
	return nil
}

// WriteLevelStatsSnapshot is a no-op; per-depth stats are no longer persisted (callers use live nodes+events).
func (w *Writer) WriteLevelStatsSnapshot(table string, depth int, pending, successful, failed, completed int64, copyPending, copySuccessful, copyFailed int64) error {
	return nil
}

func (w *Writer) UpsertStatsCounts(table string, tuples []struct {
	depth int
	key   string
	count int64
}) error {
	for _, t := range tuples {
		if err := w.SetStatsCountForDepth(table, t.depth, t.key, t.count); err != nil {
			return err
		}
	}
	return nil
}

// SetStatsCountForDepth is a no-op; per-depth stats are no longer persisted (callers use live nodes+events).
func (w *Writer) SetStatsCountForDepth(table string, depth int, key string, count int64) error {
	return nil
}

// UpdateStatsCountByDelta is a no-op; per-depth stats are no longer persisted.
func (w *Writer) UpdateStatsCountByDelta(table string, depth int, key string, delta int64) error {
	return nil
}

// StatsDelta is one (table, depth, key) delta for batch application.
type StatsDelta struct {
	Table string
	Depth int
	Key   string
	Delta int64
}

// ApplyStatsDeltas applies multiple stats deltas in one transaction. Used by seal buffer after discovery/copy flushes.
func (w *Writer) ApplyStatsDeltas(deltas []StatsDelta) error {
	for _, d := range deltas {
		if d.Delta == 0 {
			continue
		}
		if err := w.UpdateStatsCountByDelta(d.Table, d.Depth, d.Key, d.Delta); err != nil {
			return err
		}
	}
	return nil
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
	rows, err := w.tx.QueryContext(ctx,
		`SELECT COALESCE(e.traversal_status,'') AS status, COUNT(*)::BIGINT AS count
		 FROM `+t+` n LEFT JOIN `+cte+` e ON n.id = e.id
		 WHERE n.depth = $1 AND COALESCE(e.traversal_status,'') <> ''
		 GROUP BY 1`,
		depth,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return err
		}
		if err := w.SetStatsCountForDepth(table, depth, StatsKeyTraversalStatus(status), count); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if table == "SRC" {
		rows, err = w.tx.QueryContext(ctx,
			`SELECT COALESCE(e.copy_status,'') AS status, COUNT(*)::BIGINT AS count
			 FROM `+t+` n LEFT JOIN `+cte+` e ON n.id = e.id
			 WHERE n.depth = $1 AND COALESCE(e.copy_status,'') <> ''
			 GROUP BY 1`,
			depth,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			var count int64
			if err := rows.Scan(&status, &count); err != nil {
				return err
			}
			if err := w.SetStatsCountForDepth(table, depth, StatsKeyCopyStatus(status), count); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}

// InsertExclusionEventsForSubtree appends one status event per node in the SRC subtree: copy_status = excluded_explicit (root) or excluded_inherited (descendants); traversal_status preserved. Call inside RunWrite.
func (w *Writer) InsertExclusionEventsForSubtree(table, rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	if rootPath == "/" {
		_, err := w.tx.ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT n.id, COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM src_status_events e WHERE e.id = n.id), ''), CASE WHEN n.path = '/' THEN 'excluded_explicit' ELSE 'excluded_inherited' END, $1, n.depth FROM src_nodes n WHERE n.path LIKE '/%'`, eventTime)
		if err != nil {
			return err
		}
		return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, "/")
	}
	prefix := rootPath + "/%"
	_, err := w.tx.ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT n.id, COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM src_status_events e WHERE e.id = n.id), ''), CASE WHEN n.path = $2 THEN 'excluded_explicit' ELSE 'excluded_inherited' END, $1, n.depth FROM src_nodes n WHERE n.path = $2 OR n.path LIKE $3`, eventTime, rootPath, prefix)
	if err != nil {
		return err
	}
	return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, rootPath)
}

// recomputeStatsForSubtreeDepths runs RecomputeStatsForDepth for each depth present in the subtree at rootPath.
func (w *Writer) recomputeStatsForSubtreeDepths(ctx context.Context, table, rootPath string) error {
	var rows *sql.Rows
	var err error
	if rootPath == "/" {
		rows, err = w.tx.QueryContext(ctx, `SELECT DISTINCT depth FROM `+table+` WHERE path LIKE '/%'`)
	} else {
		prefix := rootPath + "/%"
		rows, err = w.tx.QueryContext(ctx, `SELECT DISTINCT depth FROM `+table+` WHERE path = $1 OR path LIKE $2`, rootPath, prefix)
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
	t := "SRC"
	if table == tableDstNodes {
		t = "DST"
	}
	for _, depth := range depths {
		if err := w.RecomputeStatsForDepth(t, depth); err != nil {
			return err
		}
	}
	return nil
}

// InsertUnexcludeEventsForSubtree appends one status event per node in the SRC subtree: copy_status = 'pending', traversal_status preserved. Call inside RunWrite.
func (w *Writer) InsertUnexcludeEventsForSubtree(table, rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	if rootPath == "/" {
		_, err := w.tx.ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT n.id, COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM src_status_events e WHERE e.id = n.id), ''), 'pending', $1, n.depth FROM src_nodes n WHERE n.path LIKE '/%'`, eventTime)
		if err != nil {
			return err
		}
		return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, "/")
	}
	prefix := rootPath + "/%"
	_, err := w.tx.ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT n.id, COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM src_status_events e WHERE e.id = n.id), ''), 'pending', $1, n.depth FROM src_nodes n WHERE n.path = $2 OR n.path LIKE $3`, eventTime, rootPath, prefix)
	if err != nil {
		return err
	}
	return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, rootPath)
}

// PropagateSubtreeFailure inserts copy_status='failed' events for all SRC descendants of parentPath
// whose current copy_status is 'pending'. Returns the number of affected nodes. Call inside a transaction.
func (w *Writer) PropagateSubtreeFailure(parentPath string) (int64, error) {
	parentPath = NormalizeSubtreeRootPathForPropagation(parentPath)
	if parentPath == "" || parentPath == "/" {
		return 0, nil
	}
	// Strict descendants only: use starts_with so '_' and '%' in path segments are not LIKE wildcards.
	prefix := parentPath + "/"
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	res, err := w.tx.ExecContext(ctx, `INSERT INTO `+tableSrcStatusEvents+` (id, traversal_status, copy_status, event_time, depth)
SELECT n.id,
       COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM `+tableSrcStatusEvents+` e WHERE e.id = n.id), ''),
       'failed',
       $1,
       n.depth
FROM `+tableSrcNodes+` n
LEFT JOIN `+cteSrcCurrentStatus+` cur ON n.id = cur.id
WHERE starts_with(n.path, $2)
  AND COALESCE(cur.copy_status, '') = 'pending'`, eventTime, prefix)
	if err != nil {
		return 0, fmt.Errorf("propagate subtree failure for %s: %w", parentPath, err)
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		deltas := []ReviewStatsDelta{
			{Key: ReviewKeyCopyPending, Delta: -affected},
			{Key: ReviewKeyCopyFailed, Delta: affected},
		}
		if err := w.ApplyReviewStatsDeltas(deltas); err != nil {
			return affected, err
		}
	}
	return affected, nil
}

// InsertDstChildrenTraversalStatusEvents appends one traversal_status event for each DST node whose parent_path equals parentPath. Used when marking/unmarking SRC node for retry (DST-only children get pending or not_on_src). Call inside RunWrite.
func (w *Writer) InsertDstChildrenTraversalStatusEvents(parentPath, status string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	parentHash := PathHash(parentPath)
	_, err := w.tx.ExecContext(ctx, `INSERT INTO dst_status_events (id, traversal_status, event_time, depth) SELECT n.id, $1, $2, n.depth FROM dst_nodes n WHERE n.parent_path_hash = $3`, status, eventTime, parentHash)
	if err != nil {
		return err
	}
	var depths []int
	rows, err := w.tx.QueryContext(ctx, `SELECT DISTINCT depth FROM dst_nodes WHERE parent_path_hash = $1`, parentHash)
	if err != nil {
		return err
	}
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
	for _, depth := range depths {
		if err := w.RecomputeStatsForDepth("DST", depth); err != nil {
			return err
		}
	}
	return nil
}

// InsertStatusEvent appends one row to src_status_events or dst_status_events. Table is "SRC" or "DST".
func (w *Writer) InsertStatusEvent(table string, e *StatusEvent) error {
	ctx := context.Background()
	if table == "DST" {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableDstStatusEvents+` (id, traversal_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5)`,
			e.ID, e.TraversalStatus, e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		return err
	}
	_, err := w.tx.ExecContext(ctx,
		`INSERT INTO `+tableSrcStatusEvents+` (id, traversal_status, copy_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6)`,
		e.ID, e.TraversalStatus, e.CopyStatus, e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
	)
	return err
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// DeleteNode deletes the node from the given table (for retry DST cleanup).
func (w *Writer) DeleteNode(table, nodeID string) error {
	t := tableName(table)
	_, err := w.tx.ExecContext(context.Background(), `DELETE FROM `+t+` WHERE id = $1`, nodeID)
	return err
}

// CountExcludedInSubtree returns (excluded, notExcluded) counts for nodes in the SRC subtree. Excluded = copy_status in (excluded_explicit, excluded_inherited). Call inside a transaction.
func (w *Writer) CountExcludedInSubtree(table, rootPath string) (excluded, notExcluded int64, err error) {
	ctx := context.Background()
	q := `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM ` + tableSrcStatusEvents + ` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') IN ('excluded_explicit','excluded_inherited'))::BIGINT,
       COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited'))::BIGINT
FROM ` + tableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id WHERE n.path = $1 OR n.path LIKE $2`
	if rootPath == "/" {
		err = w.tx.QueryRowContext(ctx, `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM `+tableSrcStatusEvents+` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') IN ('excluded_explicit','excluded_inherited'))::BIGINT,
       COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited'))::BIGINT
FROM `+tableSrcNodes+` n LEFT JOIN latest e ON n.id = e.id WHERE n.path LIKE '/%'`).Scan(&excluded, &notExcluded)
	} else {
		err = w.tx.QueryRowContext(ctx, q, rootPath, rootPath+"/%").Scan(&excluded, &notExcluded)
	}
	return excluded, notExcluded, err
}

// CopyStatusBucketsSubtreeNotExcluded holds counts of current SRC copy_status among nodes in the subtree that are not yet excluded (exclude propagation runs on this set).
type CopyStatusBucketsSubtreeNotExcluded struct {
	Pending    int64
	Failed     int64
	Successful int64
	Skipped    int64
	InProgress int64
}

// CountCopyStatusBucketsSubtreeNotExcluded counts SRC nodes under rootPath whose latest copy_status is not excluded, by bucket. Matches Writer.SetNodeExcluded universal deltas (in_progress and skipped have no separate review-table copy bucket). Call inside a transaction.
func (w *Writer) CountCopyStatusBucketsSubtreeNotExcluded(rootPath string) (CopyStatusBucketsSubtreeNotExcluded, error) {
	ctx := context.Background()
	var out CopyStatusBucketsSubtreeNotExcluded
	notExcl := `(COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited'))`
	base := `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM ` + tableSrcStatusEvents + ` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') IN ('pending',''))::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'failed')::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'successful')::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'skipped')::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'in_progress')::BIGINT
FROM ` + tableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id WHERE `
	if rootPath == "/" {
		err := w.tx.QueryRowContext(ctx, base+`n.path LIKE '/%'`).Scan(
			&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
		return out, err
	}
	prefix := rootPath + "/%"
	err := w.tx.QueryRowContext(ctx, base+`n.path = $1 OR n.path LIKE $2`, rootPath, prefix).Scan(
		&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
	return out, err
}

// CountDstNodesUnderPath returns the number of DST nodes whose parent_path equals parentPath (direct children only). Call inside a transaction.
func (w *Writer) CountDstNodesUnderPath(parentPath string) (int64, error) {
	ctx := context.Background()
	parentHash := PathHash(parentPath)
	var n int64
	err := w.tx.QueryRowContext(ctx, `SELECT COUNT(*)::BIGINT FROM dst_nodes WHERE parent_path_hash = $1`, parentHash).Scan(&n)
	return n, err
}

// CountDstNodesUnderPathWithTraversalStatus returns the number of DST nodes under parentPath (parent_path = parentPath) whose current traversal_status equals status. Call inside a transaction.
func (w *Writer) CountDstNodesUnderPathWithTraversalStatus(parentPath, status string) (int64, error) {
	ctx := context.Background()
	parentHash := PathHash(parentPath)
	q := `WITH latest AS (SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)
SELECT COUNT(*)::BIGINT FROM dst_nodes n JOIN latest e ON n.id = e.id WHERE n.parent_path_hash = $1 AND COALESCE(e.traversal_status,'') = $2`
	var n int64
	err := w.tx.QueryRowContext(ctx, q, parentHash, status).Scan(&n)
	return n, err
}

// NodeTraversalStatus returns the current traversal_status for the node from the events table. Call inside a transaction.
func (w *Writer) NodeTraversalStatus(table, nodeID string) (string, error) {
	ctx := context.Background()
	evTbl := tableSrcStatusEvents
	if table == "DST" {
		evTbl = tableDstStatusEvents
	}
	var s string
	err := w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM `+evTbl+` WHERE id = $1`, nodeID).Scan(&s)
	return s, err
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
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(copy_status, '') <> ''`, nodeID).Scan(&copyStatus)
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
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(copy_status, '') <> ''`, nodeID).Scan(&oldCopy)
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

// SetNodeExcluded emits a copy_status-only event for SRC: excluding sets copy_status to excluded_explicit (traversal_status unchanged); unexcluding sets copy_status to pending. Applies copy-status stat deltas only.
func (w *Writer) SetNodeExcluded(table, nodeID string, excluded bool) error {
	ctx := context.Background()
	t := tableName(table)
	var depth int
	if err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth); err != nil {
		return err
	}
	var oldCopyStatus string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(copy_status, '') <> ''`, nodeID).Scan(&oldCopyStatus)
	var curTraversal string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&curTraversal)
	newCopyStatus := CopyStatusExcludedExplicit
	if !excluded {
		newCopyStatus = CopyStatusPending
	}
	ev := &StatusEvent{ID: nodeID, TraversalStatus: curTraversal, CopyStatus: newCopyStatus, EventTime: time.Now().UnixNano(), Depth: depth}
	if err := w.InsertStatusEvent("SRC", ev); err != nil {
		return err
	}
	if oldCopyStatus != "" && oldCopyStatus != CopyStatusInProgress {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKeyCopyStatus(oldCopyStatus), -1); err != nil {
			return err
		}
	}
	if newCopyStatus != "" {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKeyCopyStatus(newCopyStatus), 1); err != nil {
			return err
		}
	}
	return nil
}

// DstDescendantsReviewStats holds aggregate counts for DST nodes under a path (descendants only, not the node at the path). Used to apply review-stats deltas when deleting those nodes.
type DstDescendantsReviewStats struct {
	Folders          int64
	Files            int64
	Excluded         int64
	SizeDst          int64
	TraversalPending int64
	TraversalFailed  int64
}

// CountDstDescendantsReviewStats returns aggregate counts for DST nodes that are strict descendants of rootPath (path LIKE rootPath||'/%'; for rootPath "/" uses path != '/' AND path LIKE '/%'). Call inside a transaction.
func (w *Writer) CountDstDescendantsReviewStats(rootPath string) (DstDescendantsReviewStats, error) {
	ctx := context.Background()
	var out DstDescendantsReviewStats
	q := `WITH latest AS (SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)
SELECT
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  0::BIGINT,
  COALESCE(SUM(n.size), 0)::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(e.traversal_status,'') = 'pending')::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(e.traversal_status,'') = 'failed')::BIGINT
FROM dst_nodes n LEFT JOIN latest e ON n.id = e.id WHERE `
	if rootPath == "/" {
		q += `n.path != '/' AND n.path LIKE '/%'`
		err := w.tx.QueryRowContext(ctx, q).Scan(&out.Folders, &out.Files, &out.Excluded, &out.SizeDst, &out.TraversalPending, &out.TraversalFailed)
		return out, err
	}
	q += `n.path LIKE $1`
	err := w.tx.QueryRowContext(ctx, q, rootPath+"/%").Scan(&out.Folders, &out.Files, &out.Excluded, &out.SizeDst, &out.TraversalPending, &out.TraversalFailed)
	return out, err
}

// DeleteDescendantsUnderPath deletes only nodes that are strict descendants of rootPath (path LIKE rootPath||'/%'; for rootPath "/" deletes path != '/' AND path LIKE '/%'). The node at rootPath itself is not deleted. For DST, also deletes matching rows from dst_status_events. Recomputes stats for affected depths. Table is "SRC" or "DST".
func (w *Writer) DeleteDescendantsUnderPath(table, rootPath string) error {
	ctx := context.Background()
	t := tableName(table)
	var pathCond string
	var args []any
	if rootPath == "/" {
		pathCond = `path != '/' AND path LIKE '/%'`
	} else {
		pathCond = `path LIKE $1`
		args = append(args, rootPath+"/%")
	}
	// Get affected depths before delete
	depthsQ := `SELECT DISTINCT depth FROM ` + t + ` WHERE ` + pathCond
	rows, err := w.tx.QueryContext(ctx, depthsQ, args...)
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
	if table == "DST" {
		idsRows, err := w.tx.QueryContext(ctx, `SELECT id FROM dst_nodes WHERE `+pathCond, args...)
		if err != nil {
			return err
		}
		var ids []string
		for idsRows.Next() {
			var id string
			if err := idsRows.Scan(&id); err != nil {
				idsRows.Close()
				return err
			}
			ids = append(ids, id)
		}
		idsRows.Close()
		if err = idsRows.Err(); err != nil {
			return err
		}
		if len(ids) > 0 {
			placeholders := make([]string, len(ids))
			for i := range ids {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
			}
			argList := make([]any, len(ids))
			for i, id := range ids {
				argList[i] = id
			}
			_, err = w.tx.ExecContext(ctx, `DELETE FROM dst_status_events WHERE id IN (`+strings.Join(placeholders, ",")+`)`, argList...)
			if err != nil {
				return err
			}
		}
	}
	delQ := `DELETE FROM ` + t + ` WHERE ` + pathCond
	if len(args) == 0 {
		_, err = w.tx.ExecContext(ctx, delQ)
	} else {
		_, err = w.tx.ExecContext(ctx, delQ, args...)
	}
	if err != nil {
		return err
	}
	tbl := "SRC"
	if table == "DST" {
		tbl = "DST"
	}
	for _, depth := range depths {
		if err := w.RecomputeStatsForDepth(tbl, depth); err != nil {
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

// InsertTaskFailureLog inserts a task_failure log row with structured detail (bare error) separate from message.
func (w *Writer) InsertTaskFailureLog(id, level, message, detail, entity, entityID, queue string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO logs (id, level, message, detail, component, entity, entity_id, queue) VALUES ($1, $2, $3, $4, 'task_failure', $5, $6, $7)`,
		id, level, message, nullIfEmpty(detail), entity, entityID, queue,
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

// WriteReviewStatsSnapshot writes the full canonical review stats snapshot to the universal stats table (replaces counts for review keys).
func (w *Writer) WriteReviewStatsSnapshot(s ReviewStatsSnapshot) error {
	ctx := context.Background()
	pairs := []struct {
		key   string
		count int64
	}{
		{ReviewKeyTraversalPending, s.TraversalPending},
		{ReviewKeyTraversalPendingRetry, s.TraversalPendingRetry},
		{ReviewKeyTraversalSuccessful, s.TraversalSuccessful},
		{ReviewKeyTraversalFailed, s.TraversalFailed},
		{ReviewKeyCopyPending, s.CopyPending},
		{ReviewKeyCopySuccessful, s.CopySuccessful},
		{ReviewKeyCopyFailed, s.CopyFailed},
		{ReviewKeyExcluded, s.Excluded},
		{ReviewKeyFolders, s.Folders},
		{ReviewKeyFiles, s.Files},
		{ReviewKeySizeSrc, s.SizeSrc},
		{ReviewKeySizeDst, s.SizeDst},
	}
	for _, p := range pairs {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableStats+` (key, count) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET count = excluded.count`,
			p.key, p.count,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// NodePath returns the path and table ("SRC" or "DST") for a node by ID, or error if not found.
func (w *Writer) NodePath(nodeID string) (path string, tbl string, err error) {
	ctx := context.Background()
	err = w.tx.QueryRowContext(ctx, `SELECT path FROM `+tableSrcNodes+` WHERE id = $1`, nodeID).Scan(&path)
	if err == nil {
		return path, "SRC", nil
	}
	if err != sql.ErrNoRows {
		return "", "", err
	}
	err = w.tx.QueryRowContext(ctx, `SELECT path FROM `+tableDstNodes+` WHERE id = $1`, nodeID).Scan(&path)
	if err == nil {
		return path, "DST", nil
	}
	return "", "", err
}

// CountPendingTraversalAtPath returns how many nodes (SRC + DST) at the given path have current traversal_status = 'pending'.
// Call inside the same transaction after status events have been written so the count reflects the new state.
func (w *Writer) CountPendingTraversalAtPath(path string) (int, error) {
	ctx := context.Background()
	q := `WITH src_latest AS (SELECT id, arg_max(traversal_status, event_time) AS s FROM ` + tableSrcStatusEvents + ` GROUP BY id),
dst_latest AS (SELECT id, arg_max(traversal_status, event_time) AS d FROM ` + tableDstStatusEvents + ` GROUP BY id)
SELECT
  (SELECT COUNT(*)::BIGINT FROM ` + tableSrcNodes + ` n LEFT JOIN src_latest e ON n.id = e.id WHERE n.path = $1 AND COALESCE(e.s,'') = 'pending')
  + (SELECT COUNT(*)::BIGINT FROM ` + tableDstNodes + ` n LEFT JOIN dst_latest e ON n.id = e.id WHERE n.path = $1 AND COALESCE(e.d,'') = 'pending')`
	var n int64
	if err := w.tx.QueryRowContext(ctx, q, path).Scan(&n); err != nil {
		return 0, err
	}
	return int(n), nil
}

// ReviewStatsDelta is one (key, delta) for the universal stats table.
type ReviewStatsDelta struct {
	Key   string
	Delta int64
}

// ApplyReviewStatsDeltas applies deltas to the universal stats table (count += delta per key). Used for incremental review stats updates.
func (w *Writer) ApplyReviewStatsDeltas(deltas []ReviewStatsDelta) error {
	ctx := context.Background()
	for _, d := range deltas {
		if d.Delta == 0 {
			continue
		}
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableStats+` (key, count) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET count = count + excluded.count`,
			d.Key, d.Delta,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
