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

// NodeStateAppendRowArgs returns the column values for one NodeState in table order for duckdb.Appender.AppendRow.
// For src_nodes, appends four NULL transfer-checkpoint columns plus gpl_state (15 columns total).
// For dst_nodes, returns the 10 metadata columns only.
func NodeStateAppendRowArgs(n *NodeState) []any {
	return NodeStateAppendRowArgsForTable(tableDstNodes, n)
}

// NodeStateAppendRowArgsForTable returns appender values matching the physical column layout of table.
func NodeStateAppendRowArgsForTable(table string, n *NodeState) []any {
	path, parentPath := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
	args := []any{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath,
		NormalizeQueueNodeType(n.Type), n.Size, n.MTime, int32(n.Depth),
	}
	if table == tableSrcNodes {
		// xfer_offset, xfer_src_size, xfer_src_mtime, xfer_dst_ref — null on insert; ME updates later.
		args = append(args, nil, nil, nil, nil, n.GPLState)
	}
	return args
}

// AppenderInsert inserts node metadata into src_nodes or dst_nodes (batch INSERT). No status columns.
// Explicit column lists omit xfer_* so INSERT stays valid for both tables.
func (w *Writer) AppenderInsert(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, n := range nodes {
		path, parentPath := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
		if table == tableSrcNodes {
			_, err := w.tx.ExecContext(ctx,
				`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, gpl_state)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
				n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth, n.GPLState,
			)
			if err != nil {
				return err
			}
			continue
		}
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth,
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
		path, parentPath := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
		var err error
		if table == tableSrcNodes {
			_, err = w.tx.ExecContext(ctx,
				`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, gpl_state)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
				 ON CONFLICT (id) DO NOTHING`,
				n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth, n.GPLState,
			)
		} else {
			_, err = w.tx.ExecContext(ctx,
				`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				 ON CONFLICT (id) DO NOTHING`,
				n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth,
			)
		}
		if err != nil {
			return fmt.Errorf("insert node %s into %s: %w", n.ID, table, err)
		}
	}
	return nil
}

// SrcStatusEventAppendRowArgs returns column values for one row in src_status_events for appender.
func SrcStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.CopyStatus, e.DeleteStatus, e.GPLStatus, e.EventTime, int32(e.Depth), e.ErrorLogID}
}

// DstStatusEventAppendRowArgs returns column values for one row in dst_status_events for appender.
func DstStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.GPLStatus, e.EventTime, int32(e.Depth), e.ErrorLogID}
}

// BatchInsertSrcStatusEvents inserts status events into src_status_events inside the current transaction. Used by seal flush so events are atomic with nodes/stats.
func (w *Writer) BatchInsertSrcStatusEvents(events []StatusEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableSrcStatusEvents+` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			e.ID, e.TraversalStatus, e.CopyStatus, nullIfEmpty(e.DeleteStatus), nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
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
			`INSERT INTO `+tableDstStatusEvents+` (id, traversal_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.TraversalStatus, nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
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
		if err := w.SetStatsCountForDepth(table, depth, StatsKey(StatsKindTraversal,status), count); err != nil {
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
			if err := w.SetStatsCountForDepth(table, depth, StatsKey(StatsKindCopy,status), count); err != nil {
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

// InsertUnexcludeEventsForSubtree appends one status event per node in the SRC subtree,
// restoring each node's latest non-exclusion copy_status (not hardcoding pending).
// Traversal_status is taken from the latest event of any kind. Call inside RunWrite.
func (w *Writer) InsertUnexcludeEventsForSubtree(table, rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	// Set-based restore: aggregate prior copy_status once for the subtree, then insert.
	// Avoids N correlated arg_max lookups at large scale.
	const insertSQL = `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
WITH subtree AS (
  SELECT n.id, n.depth FROM src_nodes n WHERE ` + "%s" + `
),
trav AS (
  SELECT e.id, arg_max(e.traversal_status, e.event_time) AS traversal_status
  FROM src_status_events e
  JOIN subtree s ON s.id = e.id
  GROUP BY e.id
),
prev_copy AS (
  SELECT e.id, arg_max(e.copy_status, e.event_time) AS copy_status
  FROM src_status_events e
  JOIN subtree s ON s.id = e.id
  WHERE COALESCE(e.copy_status, '') <> ''
    AND e.copy_status NOT IN ` + SQLCopyStatusExcludedIN + `
  GROUP BY e.id
)
SELECT
  s.id,
  COALESCE(t.traversal_status, ''),
  COALESCE(NULLIF(p.copy_status, ''), 'pending'),
  $1,
  s.depth
FROM subtree s
LEFT JOIN trav t ON t.id = s.id
LEFT JOIN prev_copy p ON p.id = s.id`

	var err error
	if rootPath == "/" {
		_, err = w.tx.ExecContext(ctx, fmt.Sprintf(insertSQL, `n.path LIKE '/%'`), eventTime)
		if err != nil {
			return err
		}
		return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, "/")
	}
	prefix := rootPath + "/%"
	_, err = w.tx.ExecContext(ctx, fmt.Sprintf(insertSQL, `n.path = $2 OR n.path LIKE $3`), eventTime, rootPath, prefix)
	if err != nil {
		return err
	}
	return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, rootPath)
}

func subtreePathPredicate(alias string, pathEqParam, pathLikeParam int) string {
	return fmt.Sprintf(`(%s.path = $%d OR %s.path LIKE $%d)`, alias, pathEqParam, alias, pathLikeParam)
}

const successfulCopyEligibleForDelete = `COALESCE(cur.copy_status,'') IN ` + SQLCopyStatusCompleteIN

// CountSuccessfulDeletePendingInSubtree counts SRC nodes in the subtree (inclusive) eligible to skip from deletion.
func (w *Writer) CountSuccessfulDeletePendingInSubtree(rootPath string) (int64, error) {
	ctx := context.Background()
	base := `SELECT COUNT(*)::BIGINT FROM ` + tableSrcNodes + ` n LEFT JOIN ` + cteSrcCurrentStatus + ` cur ON n.id = cur.id WHERE `
	eligible := successfulCopyEligibleForDelete + ` AND COALESCE(cur.delete_status,'') IN ('pending', '')`
	var n int64
	var err error
	if rootPath == "/" {
		err = w.tx.QueryRowContext(ctx, base+`n.path LIKE '/%' AND `+eligible).Scan(&n)
	} else {
		err = w.tx.QueryRowContext(ctx, base+subtreePathPredicate("n", 1, 2)+` AND `+eligible, rootPath, rootPath+"/%").Scan(&n)
	}
	return n, err
}

// CountSuccessfulDeleteSkippedInSubtree counts SRC nodes in the subtree (inclusive) eligible to unskip from deletion.
func (w *Writer) CountSuccessfulDeleteSkippedInSubtree(rootPath string) (int64, error) {
	ctx := context.Background()
	base := `SELECT COUNT(*)::BIGINT FROM ` + tableSrcNodes + ` n LEFT JOIN ` + cteSrcCurrentStatus + ` cur ON n.id = cur.id WHERE `
	eligible := successfulCopyEligibleForDelete + ` AND COALESCE(cur.delete_status,'') = 'skipped'`
	var n int64
	var err error
	if rootPath == "/" {
		err = w.tx.QueryRowContext(ctx, base+`n.path LIKE '/%' AND `+eligible).Scan(&n)
	} else {
		err = w.tx.QueryRowContext(ctx, base+subtreePathPredicate("n", 1, 2)+` AND `+eligible, rootPath, rootPath+"/%").Scan(&n)
	}
	return n, err
}

// InsertSkipDeleteEventsForSubtree marks all successfully copied SRC nodes in the subtree as delete_status=skipped.
func (w *Writer) InsertSkipDeleteEventsForSubtree(rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	insert := `INSERT INTO ` + tableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, event_time, depth)
SELECT n.id, COALESCE(cur.traversal_status,''), COALESCE(cur.copy_status,''), 'skipped', $1, n.depth
FROM ` + tableSrcNodes + ` n
LEFT JOIN ` + cteSrcCurrentStatus + ` cur ON n.id = cur.id
WHERE `
	eligible := successfulCopyEligibleForDelete + ` AND COALESCE(cur.delete_status,'') IN ('pending', '')`
	if rootPath == "/" {
		_, err := w.tx.ExecContext(ctx, insert+`n.path LIKE '/%' AND `+eligible, eventTime)
		if err != nil {
			return err
		}
		return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, "/")
	}
	_, err := w.tx.ExecContext(ctx, insert+subtreePathPredicate("n", 2, 3)+` AND `+eligible, eventTime, rootPath, rootPath+"/%")
	if err != nil {
		return err
	}
	return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, rootPath)
}

// InsertUnskipDeleteEventsForSubtree marks all successfully copied SRC nodes in the subtree as delete_status=pending.
func (w *Writer) InsertUnskipDeleteEventsForSubtree(rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	insert := `INSERT INTO ` + tableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, event_time, depth)
SELECT n.id, COALESCE(cur.traversal_status,''), COALESCE(cur.copy_status,''), 'pending', $1, n.depth
FROM ` + tableSrcNodes + ` n
LEFT JOIN ` + cteSrcCurrentStatus + ` cur ON n.id = cur.id
WHERE `
	eligible := successfulCopyEligibleForDelete + ` AND COALESCE(cur.delete_status,'') = 'skipped'`
	if rootPath == "/" {
		_, err := w.tx.ExecContext(ctx, insert+`n.path LIKE '/%' AND `+eligible, eventTime)
		if err != nil {
			return err
		}
		return w.recomputeStatsForSubtreeDepths(ctx, tableSrcNodes, "/")
	}
	_, err := w.tx.ExecContext(ctx, insert+subtreePathPredicate("n", 2, 3)+` AND `+eligible, eventTime, rootPath, rootPath+"/%")
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
	normParentPath := NormalizeRootRelativePath(parentPath)
	_, err := w.tx.ExecContext(ctx, `INSERT INTO dst_status_events (id, traversal_status, event_time, depth) SELECT n.id, $1, $2, n.depth FROM dst_nodes n WHERE n.parent_path = $3`, status, eventTime, normParentPath)
	if err != nil {
		return err
	}
	var depths []int
	rows, err := w.tx.QueryContext(ctx, `SELECT DISTINCT depth FROM dst_nodes WHERE parent_path = $1`, normParentPath)
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
			`INSERT INTO `+tableDstStatusEvents+` (id, traversal_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.TraversalStatus, nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		return err
	}
	_, err := w.tx.ExecContext(ctx,
		`INSERT INTO `+tableSrcStatusEvents+` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ID, e.TraversalStatus, e.CopyStatus, nullIfEmpty(e.DeleteStatus), nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
	)
	return err
}

// UpdateNodeGPLState updates src_nodes.gpl_state for a single node.
func (w *Writer) UpdateNodeGPLState(nodeID, gplState string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`UPDATE `+tableSrcNodes+` SET gpl_state = $1 WHERE id = $2`, gplState, nodeID)
	return err
}

// InsertGPLPendingEventsForSubtree appends gpl_status=pending for every node under rootPath
// except the root itself (the accepted remap node). Preserves other status dimensions via
// empty columns (arg_max filters ignore empty gpl_status on other event types). Call inside RunWrite.
func (w *Writer) InsertGPLPendingEventsForSubtree(side, rootPath string) error {
	return w.insertGPLStatusEventsForSubtree(side, rootPath, GPLStatusPending, false)
}

// InsertGPLIgnoredEventsForSubtree appends gpl_status=ignored for rootPath and all descendants.
func (w *Writer) InsertGPLIgnoredEventsForSubtree(side, rootPath string) error {
	return w.insertGPLStatusEventsForSubtree(side, rootPath, GPLStatusIgnored, true)
}

func (w *Writer) insertGPLStatusEventsForSubtree(side, rootPath, gplStatus string, includeRoot bool) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	rootPath = NormalizeSubtreeRootPathForPropagation(rootPath)

	if side == "DST" {
		return w.insertDSTGPLStatusSubtree(ctx, rootPath, eventTime, gplStatus, includeRoot)
	}
	return w.insertSRCGPLStatusSubtree(ctx, rootPath, eventTime, gplStatus, includeRoot)
}

func (w *Writer) insertSRCGPLStatusSubtree(ctx context.Context, rootPath string, eventTime int64, gplStatus string, includeRoot bool) error {
	insert := `INSERT INTO ` + tableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth)
SELECT n.id,
  COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM ` + tableSrcStatusEvents + ` e WHERE e.id = n.id), ''),
  COALESCE((SELECT arg_max(e.copy_status, e.event_time) FROM ` + tableSrcStatusEvents + ` e WHERE e.id = n.id AND COALESCE(e.copy_status,'') <> ''), ''),
  COALESCE((SELECT arg_max(e.delete_status, e.event_time) FROM ` + tableSrcStatusEvents + ` e WHERE e.id = n.id AND COALESCE(e.delete_status,'') <> ''), ''),
  $1, $2, n.depth
FROM ` + tableSrcNodes + ` n WHERE `
	if rootPath == "/" {
		cond := `n.path LIKE '/%'`
		if !includeRoot {
			cond += ` AND n.path <> '/'`
		}
		_, err := w.tx.ExecContext(ctx, insert+cond, gplStatus, eventTime)
		return err
	}
	if includeRoot {
		_, err := w.tx.ExecContext(ctx, insert+`(n.path = $3 OR n.path LIKE $4)`, gplStatus, eventTime, rootPath, rootPath+"/%")
		return err
	}
	_, err := w.tx.ExecContext(ctx, insert+`n.path LIKE $3`, gplStatus, eventTime, rootPath+"/%")
	return err
}

func (w *Writer) insertDSTGPLStatusSubtree(ctx context.Context, rootPath string, eventTime int64, gplStatus string, includeRoot bool) error {
	insert := `INSERT INTO ` + tableDstStatusEvents + ` (id, traversal_status, gpl_status, event_time, depth)
SELECT n.id,
  COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM ` + tableDstStatusEvents + ` e WHERE e.id = n.id), ''),
  $1, $2, n.depth
FROM ` + tableDstNodes + ` n WHERE `
	if rootPath == "/" {
		cond := `n.path LIKE '/%'`
		if !includeRoot {
			cond += ` AND n.path <> '/'`
		}
		_, err := w.tx.ExecContext(ctx, insert+cond, gplStatus, eventTime)
		return err
	}
	if includeRoot {
		_, err := w.tx.ExecContext(ctx, insert+`(n.path = $3 OR n.path LIKE $4)`, gplStatus, eventTime, rootPath, rootPath+"/%")
		return err
	}
	_, err := w.tx.ExecContext(ctx, insert+`n.path LIKE $3`, gplStatus, eventTime, rootPath+"/%")
	return err
}

// BatchInsertPathEvents inserts rows into path_events.
func (w *Writer) BatchInsertPathEvents(events []PathEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tablePathEvents+` (id, event_time, category, proposed_path, status, gpl_issues) VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.EventTime, e.Category, e.ProposedPath, e.Status, nullIfEmpty(e.GPLIssues),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// BatchInsertIDMapEvents inserts rows into id_map.
func (w *Writer) BatchInsertIDMapEvents(events []IDMapEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+tableIDMap+` (src_internal_id, dst_internal_id, event_time, source, status) VALUES ($1, $2, $3, $4, $5)`,
			e.SrcInternalID, e.DstInternalID, e.EventTime, e.Source, e.Status,
		)
		if err != nil {
			return err
		}
	}
	return nil
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
	notExcl := `(COALESCE(e.copy_status,'') NOT IN ` + SQLCopyStatusExcludedIN + `)`
	base := `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM ` + tableSrcStatusEvents + ` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') IN ('pending',''))::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'failed')::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') IN ` + SQLCopyStatusCompleteIN + `)::BIGINT,
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

// CountCopyStatusBucketsSubtreeExcludedPrior counts currently-excluded SRC nodes under rootPath
// by the latest non-exclusion copy_status each will restore to on unexclude. Call inside a transaction
// before InsertUnexcludeEventsForSubtree so review deltas mirror restored buckets.
func (w *Writer) CountCopyStatusBucketsSubtreeExcludedPrior(rootPath string) (CopyStatusBucketsSubtreeNotExcluded, error) {
	ctx := context.Background()
	var out CopyStatusBucketsSubtreeNotExcluded
	base := `WITH latest AS (
  SELECT id, arg_max(copy_status, event_time) AS copy_status
  FROM ` + tableSrcStatusEvents + `
  WHERE COALESCE(copy_status,'') <> ''
  GROUP BY id
),
subtree AS (
  SELECT n.id FROM ` + tableSrcNodes + ` n
  JOIN latest e ON e.id = n.id
  WHERE COALESCE(e.copy_status,'') IN ` + SQLCopyStatusExcludedIN + ` AND `
	prev := `),
prev_copy AS (
  SELECT e.id, arg_max(e.copy_status, e.event_time) AS copy_status
  FROM ` + tableSrcStatusEvents + ` e
  JOIN subtree s ON s.id = e.id
  WHERE COALESCE(e.copy_status,'') <> ''
    AND e.copy_status NOT IN ` + SQLCopyStatusExcludedIN + `
  GROUP BY e.id
)
SELECT
  COUNT(*) FILTER (WHERE COALESCE(NULLIF(p.copy_status,''), 'pending') IN ('pending',''))::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') = 'failed')::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') IN ` + SQLCopyStatusCompleteIN + `)::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') = 'skipped')::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') = 'in_progress')::BIGINT
FROM subtree s
LEFT JOIN prev_copy p ON p.id = s.id`
	if rootPath == "/" {
		err := w.tx.QueryRowContext(ctx, base+`n.path LIKE '/%'`+prev).Scan(
			&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
		return out, err
	}
	prefix := rootPath + "/%"
	err := w.tx.QueryRowContext(ctx, base+`(n.path = $1 OR n.path LIKE $2)`+prev, rootPath, prefix).Scan(
		&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
	return out, err
}

// LatestNonExclusionCopyStatus returns the latest copy_status for nodeID that is not an exclusion
// status. Empty string means none found (caller should treat as pending). Call inside a transaction.
func (w *Writer) LatestNonExclusionCopyStatus(nodeID string) (string, error) {
	ctx := context.Background()
	var s string
	err := w.tx.QueryRowContext(ctx, `
SELECT COALESCE(arg_max(copy_status, event_time), '')
FROM src_status_events
WHERE id = $1
  AND COALESCE(copy_status, '') <> ''
  AND copy_status NOT IN `+SQLCopyStatusExcludedIN, nodeID).Scan(&s)
	return s, err
}

// CountDstNodesUnderPath returns the number of DST nodes whose parent_path equals parentPath (direct children only). Call inside a transaction.
func (w *Writer) CountDstNodesUnderPath(parentPath string) (int64, error) {
	ctx := context.Background()
	normParentPath := NormalizeRootRelativePath(parentPath)
	var n int64
	err := w.tx.QueryRowContext(ctx, `SELECT COUNT(*)::BIGINT FROM dst_nodes WHERE parent_path = $1`, normParentPath).Scan(&n)
	return n, err
}

// CountDstNodesUnderPathWithTraversalStatus returns the number of DST nodes under parentPath (parent_path = parentPath) whose current traversal_status equals status. Call inside a transaction.
func (w *Writer) CountDstNodesUnderPathWithTraversalStatus(parentPath, status string) (int64, error) {
	ctx := context.Background()
	normParentPath := NormalizeRootRelativePath(parentPath)
	q := `WITH latest AS (SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)
SELECT COUNT(*)::BIGINT FROM dst_nodes n JOIN latest e ON n.id = e.id WHERE n.parent_path = $1 AND COALESCE(e.traversal_status,'') = $2`
	var n int64
	err := w.tx.QueryRowContext(ctx, q, normParentPath, status).Scan(&n)
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
		if err := w.UpdateStatsCountByDelta(table, depth, StatsKey(StatsKindTraversal,oldStatus), -1); err != nil {
			return err
		}
	}
	if status != "" {
		if err := w.UpdateStatsCountByDelta(table, depth, StatsKey(StatsKindTraversal,status), 1); err != nil {
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
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKey(StatsKindCopy,oldCopy), -1); err != nil {
			return err
		}
	}
	if status != "" && status != CopyStatusInProgress {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKey(StatsKindCopy,status), 1); err != nil {
			return err
		}
	}
	return nil
}

// SetNodeDeleteStatus emits a delete_status event and applies stat deltas for SRC.
func (w *Writer) SetNodeDeleteStatus(table, nodeID, status string) error {
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
	var oldDelete, copySt, trav string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(delete_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(delete_status, '') <> ''`, nodeID).Scan(&oldDelete)
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(copy_status, '') <> ''`, nodeID).Scan(&copySt)
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&trav)
	ev := &StatusEvent{ID: nodeID, TraversalStatus: trav, CopyStatus: copySt, DeleteStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	if err := w.InsertStatusEvent("SRC", ev); err != nil {
		return err
	}
	if oldDelete != "" {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKey(StatsKindDelete, oldDelete), -1); err != nil {
			return err
		}
	}
	if status != "" {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKey(StatsKindDelete, status), 1); err != nil {
			return err
		}
	}
	return nil
}

// SetNodeExcluded emits a copy_status-only event for SRC: excluding sets copy_status to excluded_explicit
// (traversal_status unchanged); unexcluding restores the latest non-exclusion copy_status (pending if none).
// Applies copy-status stat deltas only.
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
		restore, err := w.LatestNonExclusionCopyStatus(nodeID)
		if err != nil {
			return err
		}
		if restore == "" {
			restore = CopyStatusPending
		}
		newCopyStatus = restore
	}
	ev := &StatusEvent{ID: nodeID, TraversalStatus: curTraversal, CopyStatus: newCopyStatus, EventTime: time.Now().UnixNano(), Depth: depth}
	if err := w.InsertStatusEvent("SRC", ev); err != nil {
		return err
	}
	if oldCopyStatus != "" && oldCopyStatus != CopyStatusInProgress {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKey(StatsKindCopy, oldCopyStatus), -1); err != nil {
			return err
		}
	}
	if newCopyStatus != "" {
		if err := w.UpdateStatsCountByDelta("SRC", depth, StatsKey(StatsKindCopy, newCopyStatus), 1); err != nil {
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

// AppendQueueStats appends queue metrics JSON into queue_stats for the given phase family.
func (w *Writer) AppendQueueStats(queueKey, phase, metricsJSON string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO queue_stats (queue_key, phase, metrics_json) VALUES ($1, $2, $3)`,
		queueKey, phase, metricsJSON,
	)
	return err
}

// PruneQueueStats deletes older rows, keeping only the latest event per (queue_key, phase).
func (w *Writer) PruneQueueStats() error {
	_, err := w.tx.ExecContext(context.Background(),
		`DELETE FROM queue_stats AS qs
		 WHERE EXISTS (
		   SELECT 1 FROM queue_stats AS newer
		   WHERE newer.queue_key = qs.queue_key
		     AND newer.phase = qs.phase
		     AND newer.event_time > qs.event_time
		 )`,
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
		{ReviewKeyDeletePending, s.DeletePending},
		{ReviewKeyDeleteDeleted, s.DeleteDeleted},
		{ReviewKeyDeleteFailed, s.DeleteFailed},
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
