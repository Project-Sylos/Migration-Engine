// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"context"
	"database/sql"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
)

const sqlCopyWorkEligibleIN = `('pending','','in_progress','successful','failed')`
const sqlDeleteWorkEligibleIN = `('pending','deleted','failed')`

func workStatusWhere(kind db.StatsKind, pop SelectedPopulation) string {
	if pop == SelectedEligible {
		switch kind {
		case db.StatsKindDelete:
			return `COALESCE(cur.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + `
  AND COALESCE(cur.delete_status,'') IN ` + sqlDeleteWorkEligibleIN
		default:
			return `COALESCE(cur.copy_status,'') IN ` + sqlCopyWorkEligibleIN
		}
	}
	switch kind {
	case db.StatsKindDelete:
		return `COALESCE(cur.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + `
  AND COALESCE(cur.delete_status,'') = 'pending'`
	default:
		return `COALESCE(cur.copy_status,'') IN ('pending','')`
	}
}

func phaseProgressStatsKeys(kind db.StatsKind) (pending, successful, failed string) {
	switch kind {
	case db.StatsKindDelete:
		return db.ReviewKeyDeletePending, db.ReviewKeyDeleteDeleted, db.ReviewKeyDeleteFailed
	default:
		return db.ReviewKeyCopyPending, db.ReviewKeyCopySuccessful, db.ReviewKeyCopyFailed
	}
}

// GetCountsByType returns SRC folder/file counts for copy or delete work at the given population.
func GetCountsByType(database *db.DB, kind db.StatsKind, pop SelectedPopulation) (db.EligibleCountsByType, error) {
	var out db.EligibleCountsByType
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `SELECT
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE `+workStatusWhere(kind, pop)).Scan(&out.Folders, &out.Files)
	return out, err
}

// GetFileSize returns the sum of SRC file sizes for copy or delete work at the given population.
func GetFileSize(database *db.DB, kind db.StatsKind, pop SelectedPopulation) (int64, error) {
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(n.size), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE n.type = 'file'
  AND `+workStatusWhere(kind, pop)).Scan(&size)
	if err != nil {
		return 0, err
	}
	if size.Valid {
		return size.Int64, nil
	}
	return 0, nil
}

// SumPendingEligibleFileSizeUnderPath sums SRC file sizes under rootPath that are still
// copy-pending (or empty). Used when a folder permanently fails so touched-byte progress
// can include cascade-failed descendants before seal propagation runs.
func SumPendingEligibleFileSizeUnderPath(database *db.DB, rootPath string) (int64, error) {
	if database == nil || rootPath == "" || rootPath == "/" {
		return 0, nil
	}
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(n.size), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE n.type = 'file'
  AND COALESCE(cur.copy_status,'') IN ('pending','')
  AND starts_with(n.path, $1)`, rootPath+"/").Scan(&size)
	if err != nil {
		return 0, err
	}
	if size.Valid {
		return size.Int64, nil
	}
	return 0, nil
}

// GetFailedWorkFileSize returns SRC file bytes currently in failed status for copy or delete work.
func GetFailedWorkFileSize(database *db.DB, kind db.StatsKind) (int64, error) {
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var where string
	switch kind {
	case db.StatsKindDelete:
		where = `COALESCE(cur.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + `
  AND COALESCE(cur.delete_status,'') = 'failed'`
	default:
		where = `COALESCE(cur.copy_status,'') = 'failed'`
	}
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(n.size), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE n.type = 'file'
  AND `+where).Scan(&size)
	if err != nil {
		return 0, err
	}
	if size.Valid {
		return size.Int64, nil
	}
	return 0, nil
}

// GetPhaseProgressCountsFromStats reads O(1) canonical copy or delete review-stat counters.
func GetPhaseProgressCountsFromStats(database *db.DB, kind db.StatsKind) (db.PhaseProgressCounts, error) {
	var out db.PhaseProgressCounts
	pendingKey, successfulKey, failedKey := phaseProgressStatsKeys(kind)
	var err error
	if out.Pending, err = readStatsKeyCount(database, pendingKey); err != nil {
		return out, err
	}
	if out.Successful, err = readStatsKeyCount(database, successfulKey); err != nil {
		return out, err
	}
	if out.Failed, err = readStatsKeyCount(database, failedKey); err != nil {
		return out, err
	}
	return out, nil
}

func readStatsKeyCount(database *db.DB, key string) (int64, error) {
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var n sql.NullInt64
	err = conn.QueryRowContext(ctx, "SELECT count FROM "+db.TableStats+" WHERE key = $1", key).Scan(&n)
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

// GetCopyWorkProgressCounts returns pending/successful/failed for copy *work*
// (excludes already_existed). Used by the progress monitor so items % matches
// Folders/Files expected (only items this migration copies).
func GetCopyWorkProgressCounts(database *db.DB) (db.PhaseProgressCounts, error) {
	counts, err := GetCopyStatusCountsFromEvents(database)
	if err != nil {
		return db.PhaseProgressCounts{}, err
	}
	pending := counts.Pending
	// In-flight copy is remaining work for the items bar.
	conn, err := database.GetDB()
	if err != nil {
		return db.PhaseProgressCounts{}, err
	}
	ctx := context.Background()
	var inProgress int64
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE COALESCE(cur.copy_status,'') = 'in_progress'`).Scan(&inProgress)
	if err != nil {
		return db.PhaseProgressCounts{}, err
	}
	return db.PhaseProgressCounts{
		Pending:    pending + inProgress,
		Successful: counts.Successful, // actual copies only
		Failed:     counts.Failed,
	}, nil
}

// GetDeleteProgressCounts aggregates delete statuses only for copy-successful SRC nodes
// (same eligibility as GetDeleteCountAtDepth), excluding delete_status=skipped.
func GetDeleteProgressCounts(database *db.DB) (db.PhaseProgressCounts, error) {
	counts, err := GetEligibleDeleteStatusCounts(database)
	if err != nil {
		return db.PhaseProgressCounts{}, err
	}
	return db.PhaseProgressCounts{
		Pending:    counts.Pending,
		Successful: counts.Deleted,
		Failed:     counts.Failed,
	}, nil
}

// GetEligibleDeleteStatusCounts returns current delete-status counts only for
// copy-successful SRC nodes. This is the population shown in source-cleanup
// planning/results; skipped nodes are excluded from deletion but still counted
// for the review footer.
func GetEligibleDeleteStatusCounts(database *db.DB) (db.DeleteStatusCounts, error) {
	var out db.DeleteStatusCounts
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, `SELECT COALESCE(cur.delete_status,''), COUNT(*)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE COALESCE(cur.copy_status,'') IN `+db.SQLCopyStatusCompleteIN+`
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
func GetReviewStatsSnapshot(database *db.DB) (db.ReviewStatsSnapshot, error) {
	var out db.ReviewStatsSnapshot
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	for _, key := range db.CanonicalReviewKeys {
		var n sql.NullInt64
		err := conn.QueryRowContext(ctx, "SELECT count FROM "+db.TableStats+" WHERE key = $1", key).Scan(&n)
		if err != nil && err != sql.ErrNoRows {
			return out, err
		}
		if err == nil && n.Valid {
			v := n.Int64
			switch key {
			case db.ReviewKeyTraversalPending:
				out.TraversalPending = v
			case db.ReviewKeyTraversalPendingRetry:
				out.TraversalPendingRetry = v
			case db.ReviewKeyTraversalSuccessful:
				out.TraversalSuccessful = v
			case db.ReviewKeyTraversalFailed:
				out.TraversalFailed = v
			case db.ReviewKeyCopyPending:
				out.CopyPending = v
			case db.ReviewKeyCopySuccessful:
				out.CopySuccessful = v
			case db.ReviewKeyCopyFailed:
				out.CopyFailed = v
			case db.ReviewKeyDeletePending:
				out.DeletePending = v
			case db.ReviewKeyDeleteDeleted:
				out.DeleteDeleted = v
			case db.ReviewKeyDeleteFailed:
				out.DeleteFailed = v
			case db.ReviewKeyExcluded:
				out.Excluded = v
			case db.ReviewKeyFolders:
				out.Folders = v
			case db.ReviewKeyFiles:
				out.Files = v
			case db.ReviewKeySizeSrc:
				out.SizeSrc = v
			case db.ReviewKeySizeDst:
				out.SizeDst = v
			case db.ReviewKeySizeSelected:
				out.SizeSelected = v
			case db.ReviewKeySizeDeleteSelected:
				out.SizeDeleteSelected = v
			}
		}
	}
	return out, nil
}

// GetPathReviewStatsFromDB computes path review stats from nodes and status events only (no stats table).
// Prefer GetReviewStatsSnapshot for live API paths; this is for verification / offline checks.
func GetPathReviewStatsFromDB(database *db.DB) (db.ReviewStatsSnapshot, error) {
	srcT, err := GetTraversalStatusCountsFromEvents(database, "SRC")
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	dstT, err := GetTraversalStatusCountsFromEvents(database, "DST")
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	copyCounts, err := GetCopyStatusCountsFromEvents(database)
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	deleteCounts, err := GetDeleteStatusCountsFromEvents(database)
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	merged, err := review.GetMergedReviewStats(database, review.ReviewFilter{})
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	deleteSelected, err := GetFileSize(database, db.StatsKindDelete, SelectedPending)
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	return db.ReviewStatsSnapshot{
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
		SizeSelected:          merged.SizeSelected,
		SizeDeleteSelected:    deleteSelected,
	}, nil
}

// GetTraversalStatusCountsFromEvents returns counts of nodes by current traversal_status, derived from the status_events table (arg_max per id) joined to the node table. Use for verification instead of stats-table counters.
func GetTraversalStatusCountsFromEvents(database *db.DB, table string) (db.TraversalStatusCounts, error) {
	return getTraversalStatusCounts(database, table, false)
}

// GetTraversalStatusCountsFromCurrent returns counts from the materialized src_current / dst_current
// tables (maintained per sealed depth and per status-event insert). Prefer this for live paths.
func GetTraversalStatusCountsFromCurrent(database *db.DB, table string) (db.TraversalStatusCounts, error) {
	return getTraversalStatusCounts(database, table, true)
}

func getTraversalStatusCounts(database *db.DB, table string, fromCurrent bool) (db.TraversalStatusCounts, error) {
	var out db.TraversalStatusCounts
	nodeTable := db.TableSrcNodes
	if table == "DST" {
		nodeTable = db.TableDstNodes
	}
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	var q string
	if fromCurrent {
		currentTable := db.TableSrcCurrent
		if table == "DST" {
			currentTable = db.TableDstCurrent
		}
		q = `SELECT COALESCE(c.traversal_status,'') AS status, count(*)::BIGINT
FROM ` + nodeTable + ` n
LEFT JOIN ` + currentTable + ` c ON n.id = c.id
GROUP BY 1`
	} else {
		eventTable := db.TableSrcStatusEvents
		if table == "DST" {
			eventTable = db.TableDstStatusEvents
		}
		q = `WITH latest AS (SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM ` + eventTable + ` GROUP BY id)
SELECT COALESCE(e.traversal_status,'') AS status, count(*)::BIGINT FROM ` + nodeTable + ` n LEFT JOIN latest e ON n.id = e.id GROUP BY 1`
	}
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	return scanTraversalStatusCounts(rows, out)
}

func scanTraversalStatusCounts(rows *sql.Rows, out db.TraversalStatusCounts) (db.TraversalStatusCounts, error) {
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return out, err
		}
		switch status {
		case db.StatusPending, "":
			out.Pending += n
		case db.StatusSuccessful:
			out.Successful += n
		case db.StatusFailed:
			out.Failed += n
		case db.StatusNotOnSrc:
			out.NotOnSrc += n
		case db.StatusExcluded, db.StatusExclusionInherited:
			out.Excluded += n
		default:
			out.Successful += n
		}
	}
	return out, rows.Err()
}

// MinPendingTraversalDepth returns the minimum node depth with pending (or missing) traversal status
// in the materialized current table, or nil if none.
func MinPendingTraversalDepth(database *db.DB, table string) (*int, error) {
	nodeTable := db.TableSrcNodes
	currentTable := db.TableSrcCurrent
	if table == "DST" {
		nodeTable = db.TableDstNodes
		currentTable = db.TableDstCurrent
	}
	conn, err := database.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var depth sql.NullInt64
	err = conn.QueryRowContext(ctx, `
SELECT MIN(n.depth) FROM `+nodeTable+` n
LEFT JOIN `+currentTable+` c ON n.id = c.id
WHERE COALESCE(c.traversal_status,'') IN ('`+db.StatusPending+`', '')`).Scan(&depth)
	if err != nil {
		return nil, err
	}
	if !depth.Valid {
		return nil, nil
	}
	d := int(depth.Int64)
	return &d, nil
}

// GetCopyStatusCountsFromEvents returns counts of SRC nodes by current copy_status from src_status_events (arg_max per id). Use for review/API counts.
func GetCopyStatusCountsFromEvents(database *db.DB) (db.CopyStatusCounts, error) {
	var out db.CopyStatusCounts
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	q := `WITH latest AS (
SELECT id, arg_max(copy_status, event_time) AS copy_status
FROM ` + db.TableSrcStatusEvents + `
WHERE COALESCE(copy_status, '') <> ''
GROUP BY id
)
SELECT COALESCE(e.copy_status,'') AS status, count(*)::BIGINT FROM ` + db.TableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id GROUP BY 1`
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
		case db.CopyStatusPending, "":
			out.Pending += n
		case db.CopyStatusSuccessful:
			out.Successful += n
		case db.CopyStatusAlreadyExisted:
			out.AlreadyExisted += n
		case db.CopyStatusFailed:
			out.Failed += n
		case db.CopyStatusSkipped:
			out.Skipped += n
		case db.CopyStatusExcludedExplicit, db.CopyStatusExcludedInherited:
			out.Excluded += n
		default:
			// Unknown statuses must not inflate pending work.
		}
	}
	return out, rows.Err()
}

func scanDeleteStatusCounts(rows *sql.Rows, out *db.DeleteStatusCounts) error {
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return err
		}
		switch status {
		case db.DeleteStatusPending:
			out.Pending += n
		case db.DeleteStatusDeleted:
			out.Deleted += n
		case db.DeleteStatusFailed:
			out.Failed += n
		case db.DeleteStatusSkipped:
			out.Skipped += n
		}
	}
	return rows.Err()
}

// GetDeleteStatusCountsFromEvents returns counts of SRC nodes by current delete_status from src_status_events.
func GetDeleteStatusCountsFromEvents(database *db.DB) (db.DeleteStatusCounts, error) {
	var out db.DeleteStatusCounts
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	q := `WITH latest AS (
SELECT id, arg_max(delete_status, event_time) AS delete_status
FROM ` + db.TableSrcStatusEvents + `
WHERE COALESCE(delete_status, '') <> ''
GROUP BY id
)
SELECT COALESCE(e.delete_status,'') AS status, count(*)::BIGINT FROM ` + db.TableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id GROUP BY 1`
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
func GetRemainingSourceSizeAfterDelete(database *db.DB) (int64, error) {
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(n.size), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE COALESCE(cur.delete_status, '') <> $1`, db.DeleteStatusDeleted).Scan(&size)
	if err != nil {
		return 0, err
	}
	if size.Valid {
		return size.Int64, nil
	}
	return 0, nil
}

// GetStatsCount returns the total count for the given key from live nodes+events (table = "SRC" or "DST"). Supports traversal status keys only.
func GetStatsCount(database *db.DB, table, key string) (int64, error) {
	if key == "" {
		return 0, nil
	}
	status := statsKeyToTraversalStatus(key)
	if status == "" {
		return 0, nil
	}
	return getTraversalStatusCountFromLive(database, table, status)
}

func getTraversalStatusCountFromLive(database *db.DB, table, status string) (int64, error) {
	t := db.TableSrcNodes
	cte := db.CTESrcCurrentStatus
	if table == "DST" {
		t = db.TableDstNodes
		cte = db.CTEDstCurrentStatus
	}
	conn, err := database.GetDB()
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
func GetStatsCountAtDepth(database *db.DB, table string, depth int, key string) (int64, error) {
	if key == "" {
		return 0, nil
	}
	status := statsKeyToTraversalStatus(key)
	if status == "" {
		return 0, nil
	}
	return GetTraversalCountAtDepthFromLive(database, table, depth, status)
}

// GetTraversalCountAtDepthFromLive returns the count of nodes at the given depth with the given traversal_status (event-derived).
func GetTraversalCountAtDepthFromLive(database *db.DB, table string, depth int, status string) (int64, error) {
	t := db.TableSrcNodes
	cte := db.CTESrcCurrentStatus
	if table == "DST" {
		t = db.TableDstNodes
		cte = db.CTEDstCurrentStatus
	}
	conn, err := database.GetDB()
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
func GetGPLCountAtDepthFromLive(database *db.DB, table string, depth int, status string) (int64, error) {
	t := db.TableSrcNodes
	evTable := db.TableSrcStatusEvents
	if table == "DST" {
		t = db.TableDstNodes
		evTable = db.TableDstStatusEvents
	}
	conn, err := database.GetDB()
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
func GetCopyCountAtDepth(database *db.DB, depth int, nodeType string, copyStatus string, breakAtFirst bool) (int64, error) {
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()

	var q string
	args := []any{depth, copyStatus}

	if breakAtFirst {
		// Break at the first occurrence: just check for existence.
		q = `SELECT 1 FROM src_nodes n LEFT JOIN ` + db.CTESrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.copy_status,'') = $2`
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
		q = `SELECT COUNT(*)::BIGINT FROM src_nodes n LEFT JOIN ` + db.CTESrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.copy_status,'') = $2`
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
func GetDeleteCountAtDepth(database *db.DB, depth int, nodeType string, deleteStatus string, breakAtFirst bool) (int64, error) {
	conn, err := database.GetDB()
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	var q string
	args := []any{depth, deleteStatus}
	if breakAtFirst {
		q = `SELECT 1 FROM src_nodes n LEFT JOIN ` + db.CTESrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.delete_status,'') = $2 AND COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + ` AND COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited')`
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
	q = `SELECT COUNT(*)::BIGINT FROM src_nodes n LEFT JOIN ` + db.CTESrcCurrentStatus + ` e ON n.id = e.id WHERE n.depth = $1 AND COALESCE(e.delete_status,'') = $2 AND COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + ` AND COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited')`
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
func GetMaxDepth(database *db.DB, table string) (int, error) {
	tbl := db.TableSrcNodes
	if table == "DST" {
		tbl = db.TableDstNodes
	}
	conn, err := database.GetDB()
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

// GetStatsBreakdown returns (depth, key, count) from live nodes+events grouped by depth and traversal_status. Order: depth, key.
func GetStatsBreakdown(database *db.DB, table string) ([]db.StatsRow, error) {
	t := db.TableSrcNodes
	cte := db.CTESrcCurrentStatus
	if table == "DST" {
		t = db.TableDstNodes
		cte = db.CTEDstCurrentStatus
	}
	conn, err := database.GetDB()
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
	var out []db.StatsRow
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
		key := db.StatsKey(db.StatsKindTraversal, status)
		out = append(out, db.StatsRow{Depth: depth, Key: key, Count: count})
	}
	return out, rows.Err()
}

// GetLatestQueueStats returns the most recent metrics JSON for queue_key and phase.
func GetLatestQueueStats(database *db.DB, queueKey, phase string) ([]byte, error) {
	conn, err := database.GetDB()
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
func GetAllQueueStats(database *db.DB) (map[string][]byte, error) {
	conn, err := database.GetDB()
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
		case db.QueueStatsPhaseTraversal:
			if key == "copy" || key == "delete" {
				continue
			}
		case db.QueueStatsPhaseCopy:
			if key != "copy" {
				continue
			}
			apiKey = "copy"
		case db.QueueStatsPhaseDelete:
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
