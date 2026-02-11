// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
)

// StatsKeyTraversalStatus returns the src_stats/dst_stats key for traversal status. Valid statuses: pending, successful, failed, not_on_src (DST only).
func StatsKeyTraversalStatus(status string) string {
	return fmt.Sprintf("traversal/%s", status)
}

// StatsKeyCopyStatus returns the src_stats key for copy status (src_nodes only). Valid statuses: pending, successful, failed.
func StatsKeyCopyStatus(status string) string {
	return fmt.Sprintf("copy/%s", status)
}

// StatsKeyExpected is the stats key for expected count at a depth (set at round start).
const StatsKeyExpected = "expected"

// StatsKeyCompleted is the stats key for completed count at a depth (written at seal).
const StatsKeyCompleted = "completed"

// GetStatsCount returns the total count for the given key across all depths from src_stats or dst_stats (table = "SRC" or "DST"). E.g. "all pending items total in src_nodes" = GetStatsCount("SRC", StatsKeyTraversalStatus("pending")).
func (db *DB) GetStatsCount(table, key string) (int64, error) {
	if key == "" {
		return 0, nil
	}
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT COALESCE(SUM(count), 0) FROM "+tbl+" WHERE key = $1", key).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// GetStatsCountAtDepth returns the count for (depth, key) from src_stats or dst_stats.
func (db *DB) GetStatsCountAtDepth(table string, depth int, key string) (int64, error) {
	if key == "" {
		return 0, nil
	}
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT count FROM "+tbl+" WHERE depth = $1 AND key = $2", depth, key).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// GetCopyCountAtDepth returns the count of nodes in src_nodes at the given depth and copy_status; if nodeType != "", filters by type (e.g. "folder" or "file").
func (db *DB) GetCopyCountAtDepth(depth int, nodeType string, copyStatus string) (int64, error) {
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var q string
	var args []interface{}
	if nodeType == "" {
		q = `SELECT COUNT(*)::BIGINT FROM src_nodes WHERE depth = $1 AND copy_status = $2`
		args = []interface{}{depth, copyStatus}
	} else {
		q = `SELECT COUNT(*)::BIGINT FROM src_nodes WHERE depth = $1 AND type = $2 AND copy_status = $3`
		args = []interface{}{depth, nodeType, copyStatus}
	}
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, q, args...).Scan(&n)
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// GetMaxDepth returns the maximum depth present in the stats table for the given table ("SRC" or "DST"). Used as stop condition for retry sweep.
func (db *DB) GetMaxDepth(table string) (int, error) {
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var d sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT MAX(depth) FROM "+tbl).Scan(&d)
	if err != nil || !d.Valid {
		return 0, err
	}
	return int(d.Int64), nil
}

// GetPendingTraversalCountAtDepthFromLive returns the count of nodes at the given depth with traversal_status = 'pending' from the live nodes table. Use when advancing to a new round (stats for that depth may not exist yet).
func (db *DB) GetPendingTraversalCountAtDepthFromLive(table string, depth int) (int64, error) {
	t := tableSrcNodes
	if table == "DST" {
		t = tableDstNodes
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx,
		`SELECT COUNT(*)::BIGINT FROM `+t+` WHERE depth = $1 AND COALESCE(traversal_status, '') = 'pending'`,
		depth,
	).Scan(&n)
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// StatsRow is one row from src_stats or dst_stats (depth, key, count). For breakdown by level.
type StatsRow struct {
	Depth int
	Key   string
	Count int64
}

// GetStatsBreakdown returns all (depth, key, count) rows for the table so callers can see e.g. "level 4 has X pending". Order: depth, key.
func (db *DB) GetStatsBreakdown(table string) ([]StatsRow, error) {
	tbl := tableSrcStats
	if table == "DST" {
		tbl = tableDstStats
	}
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, "SELECT depth, key, count FROM "+tbl+" ORDER BY depth, key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StatsRow
	for rows.Next() {
		var r StatsRow
		if err := rows.Scan(&r.Depth, &r.Key, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetQueueStats returns the metrics JSON for the queue key from queue_stats table.
func (db *DB) GetQueueStats(queueKey string) ([]byte, error) {
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var js sql.NullString
	err = conn.QueryRowContext(ctx, "SELECT metrics_json FROM queue_stats WHERE queue_key = $1", queueKey).Scan(&js)
	if err == sql.ErrNoRows || !js.Valid {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(js.String), nil
}

// GetAllQueueStats returns all queue stats from queue_stats table.
func (db *DB) GetAllQueueStats() (map[string][]byte, error) {
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, "SELECT queue_key, metrics_json FROM queue_stats")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	allStats := make(map[string][]byte)
	for rows.Next() {
		var key string
		var js sql.NullString
		if err := rows.Scan(&key, &js); err != nil {
			return nil, err
		}
		if js.Valid {
			allStats[key] = []byte(js.String)
		}
	}
	return allStats, rows.Err()
}
