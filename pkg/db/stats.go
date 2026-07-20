// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
)

// StatsKind identifies the per-depth stats key family in src_stats/dst_stats.
type StatsKind string

const (
	StatsKindTraversal StatsKind = "traversal"
	StatsKindCopy      StatsKind = "copy"
	StatsKindDelete    StatsKind = "delete"
)

// StatsKey returns the src_stats/dst_stats key for the given kind and status.
// Traversal valid statuses: pending, successful, failed, not_on_src (DST only).
// Copy valid statuses: pending, successful, failed (src only).
func StatsKey(kind StatsKind, status string) string {
	return string(kind) + "/" + status
}

// StatsKeyExpected is the stats key for expected count at a depth (set at round start).
const StatsKeyExpected = "expected"

// StatsKeyCompleted is the stats key for completed count at a depth (written at seal).
const StatsKeyCompleted = "completed"

// Universal stats table keys for canonical review stats (tableStats). Namespaced as traversal/*, copy/*, and flat aggregates.
const (
	ReviewKeyTraversalPending      = "traversal/pending"
	ReviewKeyTraversalPendingRetry = "traversal/pending_retry"
	ReviewKeyTraversalSuccessful   = "traversal/successful"
	ReviewKeyTraversalFailed       = "traversal/failed"
	ReviewKeyCopyPending           = "copy/pending"
	ReviewKeyCopySuccessful        = "copy/successful"
	ReviewKeyCopyFailed            = "copy/failed"
	ReviewKeyDeletePending         = "delete/pending"
	ReviewKeyDeleteDeleted         = "delete/deleted"
	ReviewKeyDeleteFailed          = "delete/failed"
	ReviewKeyExcluded              = "excluded"
	ReviewKeyFolders               = "folders"
	ReviewKeyFiles                 = "files"
	ReviewKeySizeSrc               = "size_src"
	ReviewKeySizeDst               = "size_dst"
)

var canonicalReviewKeys = []string{
	ReviewKeyTraversalPending,
	ReviewKeyTraversalPendingRetry,
	ReviewKeyTraversalSuccessful,
	ReviewKeyTraversalFailed,
	ReviewKeyCopyPending,
	ReviewKeyCopySuccessful,
	ReviewKeyCopyFailed,
	ReviewKeyDeletePending,
	ReviewKeyDeleteDeleted,
	ReviewKeyDeleteFailed,
	ReviewKeyExcluded,
	ReviewKeyFolders,
	ReviewKeyFiles,
	ReviewKeySizeSrc,
	ReviewKeySizeDst,
}

func reviewKeyForStatus(phase, status string) string {
	if phase == "delete" {
		switch status {
		case DeleteStatusPending:
			return ReviewKeyDeletePending
		case DeleteStatusDeleted:
			return ReviewKeyDeleteDeleted
		case DeleteStatusFailed:
			return ReviewKeyDeleteFailed
		default:
			return ""
		}
	}
	if phase == "copy" {
		switch status {
		case CopyStatusPending:
			return ReviewKeyCopyPending
		case CopyStatusSuccessful, CopyStatusAlreadyExisted:
			return ReviewKeyCopySuccessful
		case CopyStatusFailed:
			return ReviewKeyCopyFailed
		default:
			return ""
		}
	}
	switch status {
	case StatusPending:
		return ReviewKeyTraversalPending
	case StatusSuccessful:
		return ReviewKeyTraversalSuccessful
	case StatusFailed:
		return ReviewKeyTraversalFailed
	default:
		return ""
	}
}

// ReviewStatsSnapshot is the canonical persisted review stats in the universal stats table.
type ReviewStatsSnapshot struct {
	TraversalPending      int64
	TraversalPendingRetry int64
	TraversalSuccessful   int64
	TraversalFailed       int64
	CopyPending           int64
	CopySuccessful        int64
	CopyFailed            int64
	DeletePending         int64
	DeleteDeleted         int64
	DeleteFailed          int64
	Excluded              int64
	Folders               int64
	Files                 int64
	SizeSrc               int64
	SizeDst               int64
}

// PhaseProgressCounts holds durable pending/successful/failed counts for copy or delete progress.
// Successful is copy_status=successful for copy, or delete_status=deleted for delete.
type PhaseProgressCounts struct {
	Pending    int64
	Successful int64
	Failed     int64
}

// Completed returns successful + failed (terminal outcomes count as processed work).
func (c PhaseProgressCounts) Completed() int64 {
	return c.Successful + c.Failed
}

// Eligible returns pending + completed (excludes skipped/excluded by construction of the counts).
func (c PhaseProgressCounts) Eligible() int64 {
	return c.Pending + c.Completed()
}

// ProgressPercent returns 0–100 progress where completed work includes terminal failures.
func (c PhaseProgressCounts) ProgressPercent() float64 {
	return DeterministicProgressPercent(c.Pending, c.Successful, c.Failed, false)
}

// ProgressPercentRetry returns 0–100 progress for copy/delete retry runs.
// Already-successful work is the baseline; pending and failed count as remaining retry work.
func (c PhaseProgressCounts) ProgressPercentRetry() float64 {
	return DeterministicProgressPercent(c.Pending, c.Successful, c.Failed, true)
}

// DeterministicProgressPercent computes percent complete from durable status counts.
//
// Normal mode: completed = successful + failed (terminal outcomes), so a finished run with
// permanent failures still reaches 100%.
//
// Retry mode: completed = successful only. Failed items are the retry workload, so the bar
// starts at the already-processed baseline and the remaining % is pending + failed.
func DeterministicProgressPercent(pending, successful, failed int64, retryMode bool) float64 {
	var completed, eligible int64
	if retryMode {
		completed = successful
		eligible = pending + successful + failed
	} else {
		completed = successful + failed
		eligible = pending + completed
	}
	if eligible <= 0 {
		return 0
	}
	pct := 100.0 * float64(completed) / float64(eligible)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

func (db *DB) readStatsKeyCount(key string) (int64, error) {
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT count FROM "+tableStats+" WHERE key = $1", key).Scan(&n)
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

// GetCopyProgressCounts reads O(1) canonical copy review-stat counters (excludes skipped/excluded).
func (db *DB) GetCopyProgressCounts() (PhaseProgressCounts, error) {
	var out PhaseProgressCounts
	var err error
	if out.Pending, err = db.readStatsKeyCount(ReviewKeyCopyPending); err != nil {
		return out, err
	}
	if out.Successful, err = db.readStatsKeyCount(ReviewKeyCopySuccessful); err != nil {
		return out, err
	}
	if out.Failed, err = db.readStatsKeyCount(ReviewKeyCopyFailed); err != nil {
		return out, err
	}
	return out, nil
}

// GetDeleteProgressCounts aggregates delete statuses only for copy-successful SRC nodes
// (same eligibility as GetDeleteCountAtDepth), excluding delete_status=skipped.
func (db *DB) GetDeleteProgressCounts() (PhaseProgressCounts, error) {
	counts, err := db.GetEligibleDeleteStatusCounts()
	if err != nil {
		return PhaseProgressCounts{}, err
	}
	return PhaseProgressCounts{
		Pending:    counts.Pending,
		Successful: counts.Deleted,
		Failed:     counts.Failed,
	}, nil
}

// GetEligibleDeleteStatusCounts returns current delete-status counts only for
// copy-successful SRC nodes. This is the population shown in source-cleanup
// planning/results; skipped nodes are excluded from deletion but still counted
// for the review footer.
func (db *DB) GetEligibleDeleteStatusCounts() (DeleteStatusCounts, error) {
	var out DeleteStatusCounts
	conn, err := db.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, `SELECT COALESCE(cur.delete_status,''), COUNT(*)::BIGINT
FROM `+tableSrcNodes+` n
LEFT JOIN `+cteSrcCurrentStatus+` cur ON n.id = cur.id
WHERE COALESCE(cur.copy_status,'') IN ` + SQLCopyStatusCompleteIN + `
  AND COALESCE(cur.delete_status,'') IN ('pending','deleted','failed','skipped')
GROUP BY 1`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	if err := scanDeleteStatusCounts(rows, &out); err != nil {
		return out, err
	}
	return out, rows.Err()
}

// GetReviewStatsSnapshot reads the full canonical review stats from the universal stats table.
func (db *DB) GetReviewStatsSnapshot() (ReviewStatsSnapshot, error) {
	var out ReviewStatsSnapshot
	conn, err := db.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	for _, key := range canonicalReviewKeys {
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
			case ReviewKeyTraversalSuccessful:
				out.TraversalSuccessful = v
			case ReviewKeyTraversalFailed:
				out.TraversalFailed = v
			case ReviewKeyCopyPending:
				out.CopyPending = v
			case ReviewKeyCopySuccessful:
				out.CopySuccessful = v
			case ReviewKeyCopyFailed:
				out.CopyFailed = v
			case ReviewKeyDeletePending:
				out.DeletePending = v
			case ReviewKeyDeleteDeleted:
				out.DeleteDeleted = v
			case ReviewKeyDeleteFailed:
				out.DeleteFailed = v
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
	deleteCounts, err := db.GetDeleteStatusCountsFromEvents()
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
		TraversalSuccessful:   srcT.Successful + dstT.Successful,
		TraversalFailed:       srcT.Failed + dstT.Failed,
		CopyPending:           copyCounts.Pending,
		CopySuccessful:        copyCounts.Complete(),
		CopyFailed:            copyCounts.Failed,
		DeletePending:         deleteCounts.Pending,
		DeleteDeleted:         deleteCounts.Deleted,
		DeleteFailed:          deleteCounts.Failed,
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
// Successful is actual copy-phase completes only; AlreadyExisted is root/DST matches.
// Complete() folds both for progress / API successfulCount.
type CopyStatusCounts struct {
	Pending        int64
	Successful     int64
	AlreadyExisted int64
	Failed         int64
	Skipped        int64
	Excluded       int64 // excluded_explicit + excluded_inherited
}

// Complete returns copy-satisfied count (actual copies + already on DST).
func (c CopyStatusCounts) Complete() int64 {
	return c.Successful + c.AlreadyExisted
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
		case CopyStatusAlreadyExisted:
			out.AlreadyExisted += n
		case CopyStatusFailed:
			out.Failed += n
		case CopyStatusSkipped:
			out.Skipped += n
		case CopyStatusExcludedExplicit, CopyStatusExcludedInherited:
			out.Excluded += n
		default:
			// Unknown statuses must not inflate pending work.
		}
	}
	return out, rows.Err()
}

// DeleteStatusCounts holds delete status counts for SRC (from src_status_events).
type DeleteStatusCounts struct {
	Pending int64
	Deleted int64
	Failed  int64
	Skipped int64
}

func scanDeleteStatusCounts(rows *sql.Rows, out *DeleteStatusCounts) error {
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return err
		}
		switch status {
		case DeleteStatusPending:
			out.Pending += n
		case DeleteStatusDeleted:
			out.Deleted += n
		case DeleteStatusFailed:
			out.Failed += n
		case DeleteStatusSkipped:
			out.Skipped += n
		}
	}
	return rows.Err()
}

// GetDeleteStatusCountsFromEvents returns counts of SRC nodes by current delete_status from src_status_events.
func (db *DB) GetDeleteStatusCountsFromEvents() (DeleteStatusCounts, error) {
	var out DeleteStatusCounts
	conn, err := db.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	q := `WITH latest AS (
SELECT id, arg_max(delete_status, event_time) AS delete_status
FROM ` + tableSrcStatusEvents + `
WHERE COALESCE(delete_status, '') <> ''
GROUP BY id
)
SELECT COALESCE(e.delete_status,'') AS status, count(*)::BIGINT FROM ` + tableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id GROUP BY 1`
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	if err := scanDeleteStatusCounts(rows, &out); err != nil {
		return out, err
	}
	return out, rows.Err()
}

// GetRemainingSourceSizeAfterDelete returns the size of SRC nodes whose latest
// delete status is not deleted. Nodes with pending, failed, skipped, or no
// delete status remain part of the post-delete source-size result.
func (db *DB) GetRemainingSourceSizeAfterDelete() (int64, error) {
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(n.size), 0)::BIGINT
FROM `+tableSrcNodes+` n
LEFT JOIN `+cteSrcCurrentStatus+` cur ON n.id = cur.id
WHERE COALESCE(cur.delete_status, '') <> $1`, DeleteStatusDeleted).Scan(&size)
	if err != nil {
		return 0, err
	}
	if size.Valid {
		return size.Int64, nil
	}
	return 0, nil
}

// GetStatsCount returns the total count for the given key from live nodes+events (table = "SRC" or "DST"). Supports traversal status keys only.
func (db *DB) GetStatsCount(table, key string) (int64, error) {
	if key == "" {
		return 0, nil
	}
	status := statsKeyToTraversalStatus(key)
	if status == "" {
		return 0, nil
	}
	return db.getTraversalStatusCountFromLive(table, status)
}

func (db *DB) getTraversalStatusCountFromLive(table, status string) (int64, error) {
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
	q := `SELECT COUNT(*)::BIGINT FROM ` + t + ` n LEFT JOIN ` + cte + ` e ON n.id = e.id WHERE COALESCE(e.traversal_status,'') = $1`
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, q, status).Scan(&n)
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// statsKeyToTraversalStatus returns the status part if key is "traversal/<status>", else "".
func statsKeyToTraversalStatus(key string) string {
	const prefix = "traversal/"
	if len(key) > len(prefix) && key[:len(prefix)] == prefix {
		return key[len(prefix):]
	}
	return ""
}

// GetStatsCountAtDepth returns the count for (depth, key) from live nodes+events. Supports traversal status keys only (e.g. traversal/pending, traversal/failed).
func (db *DB) GetStatsCountAtDepth(table string, depth int, key string) (int64, error) {
	if key == "" {
		return 0, nil
	}
	status := statsKeyToTraversalStatus(key)
	if status == "" {
		return 0, nil
	}
	return db.GetTraversalCountAtDepthFromLive(table, depth, status)
}

// GetTraversalCountAtDepthFromLive returns the count of nodes at the given depth with the given traversal_status (event-derived).
func (db *DB) GetTraversalCountAtDepthFromLive(table string, depth int, status string) (int64, error) {
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
	q := `SELECT COUNT(*)::BIGINT FROM ` + t + ` n LEFT JOIN ` + cte + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.traversal_status,'') = $2`
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, q, depth, status).Scan(&n)
	if err != nil {
		return 0, err
	}
	if n.Valid {
		return n.Int64, nil
	}
	return 0, nil
}

// GetGPLCountAtDepthFromLive returns the count of nodes at depth with the given gpl_status (event-derived).
func (db *DB) GetGPLCountAtDepthFromLive(table string, depth int, status string) (int64, error) {
	t := tableSrcNodes
	evTable := tableSrcStatusEvents
	if table == "DST" {
		t = tableDstNodes
		evTable = tableDstStatusEvents
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	q := `WITH gpl AS (
	SELECT se.id, arg_max(se.gpl_status, se.event_time) AS gpl_status
	FROM ` + evTable + ` se
	INNER JOIN ` + t + ` n ON n.id = se.id AND n.depth = $1
	WHERE COALESCE(se.gpl_status,'') <> ''
	GROUP BY se.id
)
SELECT COUNT(*)::BIGINT FROM ` + t + ` n
INNER JOIN gpl g ON g.id = n.id
WHERE n.depth = $1 AND COALESCE(g.gpl_status,'') = $2`
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, q, depth, status).Scan(&n)
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

// GetDeleteCountAtDepth returns the count of nodes in src_nodes at the given depth with current delete_status (event-derived).
func (db *DB) GetDeleteCountAtDepth(depth int, nodeType string, deleteStatus string, breakAtFirst bool) (int64, error) {
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var q string
	args := []any{depth, deleteStatus}
	if breakAtFirst {
		q = `SELECT 1 FROM src_nodes n LEFT JOIN ` + cteSrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.delete_status,'') = $2 AND COALESCE(e.copy_status,'') IN ` + SQLCopyStatusCompleteIN + ` AND COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited')`
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
	}
	q = `SELECT COUNT(*)::BIGINT FROM src_nodes n LEFT JOIN ` + cteSrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.delete_status,'') = $2 AND COALESCE(e.copy_status,'') IN ` + SQLCopyStatusCompleteIN + ` AND COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited')`
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

// GetMaxDepth returns the maximum depth present in the nodes table for the given table ("SRC" or "DST"). Used as stop condition for retry sweep.
func (db *DB) GetMaxDepth(table string) (int, error) {
	tbl := tableSrcNodes
	if table == "DST" {
		tbl = tableDstNodes
	}
	conn, err := db.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var d sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT COALESCE(MAX(depth), 0) FROM "+tbl).Scan(&d)
	if err != nil {
		return 0, err
	}
	if d.Valid {
		return int(d.Int64), nil
	}
	return 0, nil
}

// StatsRow is one row (depth, key, count) for breakdown by level. Key is e.g. traversal/pending.
type StatsRow struct {
	Depth int
	Key   string
	Count int64
}

// GetStatsBreakdown returns (depth, key, count) from live nodes+events grouped by depth and traversal_status. Order: depth, key.
func (db *DB) GetStatsBreakdown(table string) ([]StatsRow, error) {
	t := tableSrcNodes
	cte := cteSrcCurrentStatus
	if table == "DST" {
		t = tableDstNodes
		cte = cteDstCurrentStatus
	}
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	q := `SELECT n.depth, COALESCE(e.traversal_status,'') AS status, COUNT(*)::BIGINT FROM ` + t + ` n LEFT JOIN ` + cte + ` e ON n.id = e.id GROUP BY n.depth, 2 ORDER BY 1, 2`
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StatsRow
	for rows.Next() {
		var depth int
		var status string
		var count int64
		if err := rows.Scan(&depth, &status, &count); err != nil {
			return nil, err
		}
		if count == 0 {
			continue
		}
		key := StatsKey(StatsKindTraversal, status)
		out = append(out, StatsRow{Depth: depth, Key: key, Count: count})
	}
	return out, rows.Err()
}

const (
	// QueueStatsPhaseTraversal is the phase family for traversal and retry queue modes.
	QueueStatsPhaseTraversal = "traversal"
	// QueueStatsPhaseCopy is the phase family for copy and copy-retry queue modes.
	QueueStatsPhaseCopy = "copy"
	// QueueStatsPhaseDelete is the phase family for delete and delete-retry queue modes.
	QueueStatsPhaseDelete = "delete"
)

// GetLatestQueueStats returns the most recent metrics JSON for queue_key and phase.
func (db *DB) GetLatestQueueStats(queueKey, phase string) ([]byte, error) {
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var js sql.NullString
	err = conn.QueryRowContext(ctx,
		`SELECT arg_max(metrics_json, event_time)
		 FROM queue_stats
		 WHERE queue_key = $1 AND phase = $2`,
		queueKey, phase,
	).Scan(&js)
	if err == sql.ErrNoRows || !js.Valid {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(js.String), nil
}

// GetAllQueueStats returns the latest metrics JSON per API queue key (src-traversal, dst-traversal, copy, delete).
func (db *DB) GetAllQueueStats() (map[string][]byte, error) {
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx,
		`SELECT queue_key, phase, arg_max(metrics_json, event_time) AS metrics_json
		 FROM queue_stats
		 GROUP BY queue_key, phase`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	allStats := make(map[string][]byte)
	for rows.Next() {
		var key, phase string
		var js sql.NullString
		if err := rows.Scan(&key, &phase, &js); err != nil {
			return nil, err
		}
		if !js.Valid {
			continue
		}
		apiKey := key
		switch phase {
		case QueueStatsPhaseTraversal:
			if key == "copy" || key == "delete" {
				continue
			}
		case QueueStatsPhaseCopy:
			if key != "copy" {
				continue
			}
			apiKey = "copy"
		case QueueStatsPhaseDelete:
			if key == "delete" || key == "delete-traversal" {
				apiKey = "delete"
			} else {
				continue
			}
		default:
			continue
		}
		allStats[apiKey] = []byte(js.String)
	}
	return allStats, rows.Err()
}
