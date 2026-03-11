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

// Universal stats table keys for canonical review stats (tableStats).
const (
	ReviewKeyTraversalPending      = "review/traversal/pending"
	ReviewKeyTraversalPendingRetry = "review/traversal/pending_retry"
	ReviewKeyTraversalFailed       = "review/traversal/failed"
	ReviewKeyCopyPending           = "review/copy/pending"
	ReviewKeyCopyFailed            = "review/copy/failed"
	ReviewKeyExcluded              = "review/excluded"
	ReviewKeyFolders               = "review/folders"
	ReviewKeyFiles                 = "review/files"
	ReviewKeySizeSrc               = "review/size/src"
	ReviewKeySizeDst               = "review/size/dst"
)

// ReviewStatsSnapshot is the canonical persisted review stats in the universal stats table.
type ReviewStatsSnapshot struct {
	TraversalPending      int64
	TraversalPendingRetry int64
	TraversalFailed       int64
	CopyPending           int64
	CopyFailed            int64
	Excluded              int64
	Folders               int64
	Files                 int64
	SizeSrc               int64
	SizeDst               int64
}

// GetReviewStatsSnapshot reads the full canonical review stats from the universal stats table.
func (db *DB) GetReviewStatsSnapshot() (ReviewStatsSnapshot, error) {
	var out ReviewStatsSnapshot
	conn, err := db.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	keys := []string{
		ReviewKeyTraversalPending, ReviewKeyTraversalPendingRetry, ReviewKeyTraversalFailed, ReviewKeyCopyPending, ReviewKeyCopyFailed,
		ReviewKeyExcluded, ReviewKeyFolders, ReviewKeyFiles, ReviewKeySizeSrc, ReviewKeySizeDst,
	}
	for _, key := range keys {
		var n sql.NullInt64
		err := conn.QueryRowContext(ctx, "SELECT count FROM "+tableStats+" WHERE key = $1", key).Scan(&n)
		if err != nil && err != sql.ErrNoRows {
			return out, err
		}
		if err == nil && n.Valid {
			v := n.Int64
			switch key {
			case ReviewKeyTraversalPending:
				out.TraversalPending = v
			case ReviewKeyTraversalPendingRetry:
				out.TraversalPendingRetry = v
			case ReviewKeyTraversalFailed:
				out.TraversalFailed = v
			case ReviewKeyCopyPending:
				out.CopyPending = v
			case ReviewKeyCopyFailed:
				out.CopyFailed = v
			case ReviewKeyExcluded:
				out.Excluded = v
			case ReviewKeyFolders:
				out.Folders = v
			case ReviewKeyFiles:
				out.Files = v
			case ReviewKeySizeSrc:
				out.SizeSrc = v
			case ReviewKeySizeDst:
				out.SizeDst = v
			}
		}
	}
	return out, nil
}

// GetPathReviewStatsFromDB computes path review stats from nodes and status events only (no stats table). Use for GetPathReviewStats() and verification.
func (db *DB) GetPathReviewStatsFromDB() (ReviewStatsSnapshot, error) {
	srcT, err := db.GetTraversalStatusCountsFromEvents("SRC")
	if err != nil {
		return ReviewStatsSnapshot{}, err
	}
	dstT, err := db.GetTraversalStatusCountsFromEvents("DST")
	if err != nil {
		return ReviewStatsSnapshot{}, err
	}
	copyCounts, err := db.GetCopyStatusCountsFromEvents()
	if err != nil {
		return ReviewStatsSnapshot{}, err
	}
	merged, err := GetMergedReviewStats(db, ReviewFilter{})
	if err != nil {
		return ReviewStatsSnapshot{}, err
	}
	return ReviewStatsSnapshot{
		TraversalPending:      srcT.Pending + dstT.Pending,
		TraversalPendingRetry: 0, // no persisted source; maintained only via deltas historically
		TraversalFailed:       srcT.Failed + dstT.Failed,
		CopyPending:           copyCounts.Pending,
		CopyFailed:            copyCounts.Failed,
		Excluded:              int64(merged.Excluded),
		Folders:               int64(merged.Folders),
		Files:                 int64(merged.Files),
		SizeSrc:               merged.SizeSrc,
		SizeDst:               merged.SizeDst,
	}, nil
}

// ResyncReviewStats recomputes the canonical review stats from live nodes and latest status events, then writes the full snapshot to the universal stats table. Use for recovery when stats are missing or stale.
func (db *DB) ResyncReviewStats() error {
	snap, err := db.GetPathReviewStatsFromDB()
	if err != nil {
		return err
	}
	return db.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.WriteReviewStatsSnapshot(snap)
		})
	})
}

// TraversalStatusCounts holds traversal status counts derived from the status_events table (one row per node, latest event).
type TraversalStatusCounts struct {
	Pending    int64
	Successful int64
	Failed     int64
	NotOnSrc   int64 // DST only; 0 for SRC
	Excluded   int64 // excluded or exclusion_inherited
}

// GetTraversalStatusCountsFromEvents returns counts of nodes by current traversal_status, derived from the status_events table (arg_max per id) joined to the node table. Use for verification instead of stats-table counters.
func (db *DB) GetTraversalStatusCountsFromEvents(table string) (TraversalStatusCounts, error) {
	var out TraversalStatusCounts
	nodeTable := tableSrcNodes
	eventTable := tableSrcStatusEvents
	if table == "DST" {
		nodeTable = tableDstNodes
		eventTable = tableDstStatusEvents
	}
	conn, err := db.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	q := `WITH latest AS (SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM ` + eventTable + ` GROUP BY id)
SELECT COALESCE(e.traversal_status,'') AS status, count(*)::BIGINT FROM ` + nodeTable + ` n LEFT JOIN latest e ON n.id = e.id GROUP BY 1`
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return out, err
		}
		switch status {
		case StatusPending, "":
			out.Pending += n
		case StatusSuccessful:
			out.Successful += n
		case StatusFailed:
			out.Failed += n
		case StatusNotOnSrc:
			out.NotOnSrc += n
		case StatusExcluded, StatusExclusionInherited:
			out.Excluded += n
		default:
			out.Successful += n
		}
	}
	return out, rows.Err()
}

// CopyStatusCounts holds copy status counts for SRC (from src_status_events).
type CopyStatusCounts struct {
	Pending    int64
	Successful int64
	Failed     int64
	Skipped    int64
	Excluded   int64 // excluded_explicit + excluded_inherited
}

// GetCopyStatusCountsFromEvents returns counts of SRC nodes by current copy_status from src_status_events (arg_max per id). Use for review/API counts.
func (db *DB) GetCopyStatusCountsFromEvents() (CopyStatusCounts, error) {
	var out CopyStatusCounts
	conn, err := db.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	q := `WITH latest AS (
SELECT id, arg_max(copy_status, event_time) AS copy_status
FROM ` + tableSrcStatusEvents + `
WHERE COALESCE(copy_status, '') <> ''
GROUP BY id
)
SELECT COALESCE(e.copy_status,'') AS status, count(*)::BIGINT FROM ` + tableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id GROUP BY 1`
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return out, err
		}
		switch status {
		case CopyStatusPending, "":
			out.Pending += n
		case CopyStatusSuccessful:
			out.Successful += n
		case CopyStatusFailed:
			out.Failed += n
		case CopyStatusSkipped:
			out.Skipped += n
		case CopyStatusExcludedExplicit, CopyStatusExcludedInherited:
			out.Excluded += n
		default:
			out.Pending += n
		}
	}
	return out, rows.Err()
}

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

// GetCopyCountAtDepth returns the count of nodes in src_nodes at the given depth with current copy_status (event-derived).
// Optional nodeType filter. If breakAtFirst is true, returns 1 if any matching node exists or 0 otherwise.
func (db *DB) GetCopyCountAtDepth(depth int, nodeType string, copyStatus string, breakAtFirst bool) (int64, error) {
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()

	var q string
	args := []any{depth, copyStatus}

	if breakAtFirst {
		// Break at the first occurrence: just check for existence.
		q = `SELECT 1 FROM src_nodes n LEFT JOIN ` + cteSrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.copy_status,'') = $2`
		if nodeType != "" {
			q += ` AND n.type = $3`
			args = append(args, nodeType)
		}
		q += ` LIMIT 1`
		var dummy int
		err = conn.QueryRowContext(ctx, q, args...).Scan(&dummy)
		if err == sql.ErrNoRows {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return 1, nil
	} else {
		q = `SELECT COUNT(*)::BIGINT FROM src_nodes n LEFT JOIN ` + cteSrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.copy_status,'') = $2`
		if nodeType != "" {
			q += ` AND n.type = $3`
			args = append(args, nodeType)
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

// GetPendingTraversalCountAtDepthFromLive returns the count of nodes at the given depth with current traversal_status = 'pending' (event-derived).
func (db *DB) GetPendingTraversalCountAtDepthFromLive(table string, depth int) (int64, error) {
	t := tableSrcNodes
	cte := cteSrcCurrentStatus
	if table == "DST" {
		t = tableDstNodes
		cte = cteDstCurrentStatus
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	q := `SELECT COUNT(*)::BIGINT FROM ` + t + ` n LEFT JOIN ` + cte + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.traversal_status,'') = 'pending'`
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, q, depth).Scan(&n)
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
