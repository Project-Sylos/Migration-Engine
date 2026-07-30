// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package subtree

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// SubtreeCopyMutationResult is the eligible-set aggregate from a pending-only
// exclude/unexclude insert (feeds review + copy_work deltas).
type SubtreeCopyMutationResult struct {
	Affected     int64
	Folders      int64
	Files        int64
	PendingBytes int64
}

// SubtreeDeleteMutationResult is the eligible-set aggregate from delete skip/unskip.
type SubtreeDeleteMutationResult struct {
	Affected      int64
	Folders       int64
	Files         int64
	SelectedBytes int64
}

// InsertExclusionEventsForSubtree inserts exclusion events only for SRC nodes under
// rootPath whose current copy_status is pending/empty. already_existed and other
// statuses are left untouched. Returns aggregates of the mutated set.
// Call inside RunWrite. Refreshes src_current; does not recompute per-depth stats.
func InsertExclusionEventsForSubtree(w *db.Writer, table, rootPath string) (SubtreeCopyMutationResult, error) {
	_ = table
	ctx := context.Background()
	rootPath = db.NormalizeSubtreeRootPathForPropagation(rootPath)
	var out SubtreeCopyMutationResult
	pending := `COALESCE(cur.copy_status,'') IN ('pending','')`
	aggSQL := `SELECT
  COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE ` + pending + ` AND `
	insertSQL := `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT n.id,
  COALESCE(cur.traversal_status,''),
  CASE WHEN n.path = $2 THEN 'excluded_explicit' ELSE 'excluded_inherited' END,
  $1, n.depth
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE ` + pending + ` AND `
	eventTime := time.Now().UnixNano()
	var err error
	if rootPath == "/" {
		err = w.Tx().QueryRowContext(ctx, aggSQL+`n.path LIKE '/%'`).
			Scan(&out.Affected, &out.Folders, &out.Files, &out.PendingBytes)
		if err != nil {
			return out, err
		}
		if out.Affected == 0 {
			return out, nil
		}
		_, err = w.Tx().ExecContext(ctx, `INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT n.id,
  COALESCE(cur.traversal_status,''),
  CASE WHEN n.path = '/' THEN 'excluded_explicit' ELSE 'excluded_inherited' END,
  $1, n.depth
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE `+pending+` AND n.path LIKE '/%'`, eventTime)
		if err != nil {
			return out, err
		}
		return out, w.RefreshCurrentByPathPrefix("SRC", "/")
	}
	err = w.Tx().QueryRowContext(ctx, aggSQL+`(n.path = $1 OR n.path LIKE $2)`, rootPath, rootPath+"/%").
		Scan(&out.Affected, &out.Folders, &out.Files, &out.PendingBytes)
	if err != nil {
		return out, err
	}
	if out.Affected == 0 {
		return out, nil
	}
	_, err = w.Tx().ExecContext(ctx, insertSQL+`(n.path = $3 OR n.path LIKE $4)`,
		eventTime, rootPath, rootPath, rootPath+"/%")
	if err != nil {
		return out, err
	}
	return out, w.RefreshCurrentByPathPrefix("SRC", rootPath)
}

// InsertUnexcludeEventsForSubtree restores copy_status=pending only for currently excluded
// SRC nodes under rootPath whose latest non-exclusion copy_status was pending/empty.
// Call inside RunWrite. Refreshes src_current; does not recompute per-depth stats.
func InsertUnexcludeEventsForSubtree(w *db.Writer, table, rootPath string) (SubtreeCopyMutationResult, error) {
	_ = table
	ctx := context.Background()
	rootPath = db.NormalizeSubtreeRootPathForPropagation(rootPath)
	var out SubtreeCopyMutationResult
	eventTime := time.Now().UnixNano()

	const eligibleCTE = `
WITH latest AS (
  SELECT id, arg_max(copy_status, event_time) AS copy_status
  FROM src_status_events
  WHERE COALESCE(copy_status,'') <> ''
  GROUP BY id
),
subtree AS (
  SELECT n.id, n.type, n.size, n.depth, n.path
  FROM src_nodes n
  JOIN latest e ON e.id = n.id
  WHERE COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusExcludedIN + ` AND %s
),
prev_copy AS (
  SELECT e.id, arg_max(e.copy_status, e.event_time) AS copy_status
  FROM src_status_events e
  JOIN subtree s ON s.id = e.id
  WHERE COALESCE(e.copy_status,'') <> ''
    AND e.copy_status NOT IN ` + db.SQLCopyStatusExcludedIN + `
  GROUP BY e.id
),
eligible AS (
  SELECT s.*
  FROM subtree s
  LEFT JOIN prev_copy p ON p.id = s.id
  WHERE COALESCE(NULLIF(p.copy_status,''), 'pending') IN ('pending','')
)`

	pathRoot := `n.path LIKE '/%'`
	pathSub := `(n.path = $1 OR n.path LIKE $2)`
	aggTail := `
SELECT COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN type = 'file' THEN size ELSE 0 END), 0)::BIGINT
FROM eligible`
	insertTail := `
INSERT INTO src_status_events (id, traversal_status, copy_status, event_time, depth)
SELECT e.id,
  COALESCE((SELECT arg_max(ev.traversal_status, ev.event_time) FROM src_status_events ev WHERE ev.id = e.id), ''),
  'pending',
  $%d,
  e.depth
FROM eligible e`

	var err error
	if rootPath == "/" {
		err = w.Tx().QueryRowContext(ctx, fmt.Sprintf(eligibleCTE, pathRoot)+aggTail).
			Scan(&out.Affected, &out.Folders, &out.Files, &out.PendingBytes)
		if err != nil {
			return out, err
		}
		if out.Affected == 0 {
			return out, nil
		}
		_, err = w.Tx().ExecContext(ctx, fmt.Sprintf(eligibleCTE, pathRoot)+fmt.Sprintf(insertTail, 1), eventTime)
		if err != nil {
			return out, err
		}
		return out, w.RefreshCurrentByPathPrefix("SRC", "/")
	}
	err = w.Tx().QueryRowContext(ctx, fmt.Sprintf(eligibleCTE, pathSub)+aggTail, rootPath, rootPath+"/%").
		Scan(&out.Affected, &out.Folders, &out.Files, &out.PendingBytes)
	if err != nil {
		return out, err
	}
	if out.Affected == 0 {
		return out, nil
	}
	// $1 path, $2 like, $3 eventTime
	_, err = w.Tx().ExecContext(ctx,
		fmt.Sprintf(eligibleCTE, pathSub)+fmt.Sprintf(insertTail, 3),
		rootPath, rootPath+"/%", eventTime)
	if err != nil {
		return out, err
	}
	return out, w.RefreshCurrentByPathPrefix("SRC", rootPath)
}

// InsertDeleteStatusEventsForAncestors marks copy-complete SRC ancestors of childPath
// (strict parents only) that match eligible with newDeleteStatus. Used when excluding
// a node from deletion so parents that would be blocked are also skipped.
func InsertDeleteStatusEventsForAncestors(w *db.Writer, childPath, newDeleteStatus, eligible string) (SubtreeDeleteMutationResult, error) {
	ctx := context.Background()
	childPath = db.NormalizeSubtreeRootPathForPropagation(childPath)
	var out SubtreeDeleteMutationResult
	if childPath == "" || childPath == "/" {
		return out, nil
	}
	eventTime := time.Now().UnixNano()
	// Ancestors: childPath is under n.path (n.path + '/' is a prefix of childPath).
	ancPredicate := `starts_with($%d, n.path || '/') AND n.path <> '/' AND n.path <> $%d`
	agg := `SELECT COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE ` + eligible + ` AND ` + fmt.Sprintf(ancPredicate, 1, 2)
	insert := `INSERT INTO ` + db.TableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, event_time, depth)
SELECT n.id, COALESCE(cur.traversal_status,''), COALESCE(cur.copy_status,''), $1, $2, n.depth
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE ` + eligible + ` AND ` + fmt.Sprintf(ancPredicate, 3, 4)

	err := w.Tx().QueryRowContext(ctx, agg, childPath, childPath).Scan(&out.Affected, &out.Folders, &out.Files, &out.SelectedBytes)
	if err != nil {
		return out, err
	}
	if out.Affected == 0 {
		return out, nil
	}
	_, err = w.Tx().ExecContext(ctx, insert, newDeleteStatus, eventTime, childPath, childPath)
	if err != nil {
		return out, err
	}
	// Refresh from filesystem root so each ancestor path is covered.
	return out, w.RefreshCurrentByPathPrefix("SRC", "/")
}

// InsertDeleteStatusEventsForSubtree marks copy-complete SRC nodes under rootPath matching eligible with newDeleteStatus.
func InsertDeleteStatusEventsForSubtree(w *db.Writer, rootPath, newDeleteStatus, eligible string) (SubtreeDeleteMutationResult, error) {
	ctx := context.Background()
	rootPath = db.NormalizeSubtreeRootPathForPropagation(rootPath)
	var out SubtreeDeleteMutationResult
	eventTime := time.Now().UnixNano()
	agg := `SELECT COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE ` + eligible + ` AND `
	insert := `INSERT INTO ` + db.TableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, event_time, depth)
SELECT n.id, COALESCE(cur.traversal_status,''), COALESCE(cur.copy_status,''), $1, $2, n.depth
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE ` + eligible + ` AND `
	var err error
	if rootPath == "/" {
		err = w.Tx().QueryRowContext(ctx, agg+`n.path LIKE '/%'`).Scan(&out.Affected, &out.Folders, &out.Files, &out.SelectedBytes)
		if err != nil {
			return out, err
		}
		if out.Affected == 0 {
			return out, nil
		}
		_, err = w.Tx().ExecContext(ctx, insert+`n.path LIKE '/%'`, newDeleteStatus, eventTime)
		if err != nil {
			return out, err
		}
		return out, w.RefreshCurrentByPathPrefix("SRC", "/")
	}
	err = w.Tx().QueryRowContext(ctx, agg+fmt.Sprintf(`(n.path = $%d OR n.path LIKE $%d)`, 1, 2), rootPath, rootPath+"/%").
		Scan(&out.Affected, &out.Folders, &out.Files, &out.SelectedBytes)
	if err != nil {
		return out, err
	}
	if out.Affected == 0 {
		return out, nil
	}
	_, err = w.Tx().ExecContext(ctx, insert+fmt.Sprintf(`(n.path = $%d OR n.path LIKE $%d)`, 3, 4),
		newDeleteStatus, eventTime, rootPath, rootPath+"/%")
	if err != nil {
		return out, err
	}
	return out, w.RefreshCurrentByPathPrefix("SRC", rootPath)
}

// PropagateSubtreeFailure inserts copy_status='failed' events for all SRC descendants of parentPath
// whose current copy_status is 'pending'. Returns the number of affected nodes. Call inside a transaction.
func PropagateSubtreeFailure(w *db.Writer, parentPath string) (int64, error) {
	parentPath = db.NormalizeSubtreeRootPathForPropagation(parentPath)
	if parentPath == "" || parentPath == "/" {
		return 0, nil
	}
	// Strict descendants only: use starts_with so '_' and '%' in path segments are not LIKE wildcards.
	prefix := parentPath + "/"
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	res, err := w.Tx().ExecContext(ctx, `INSERT INTO `+db.TableSrcStatusEvents+` (id, traversal_status, copy_status, event_time, depth)
SELECT n.id,
       COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM `+db.TableSrcStatusEvents+` e WHERE e.id = n.id), ''),
       'failed',
       $1,
       n.depth
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE starts_with(n.path, $2)
  AND COALESCE(cur.copy_status, '') = 'pending'`, eventTime, prefix)
	if err != nil {
		return 0, fmt.Errorf("propagate subtree failure for %s: %w", parentPath, err)
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		deltas := []db.ReviewStatsDelta{
			{Key: db.ReviewKeyCopyPending, Delta: -affected},
			{Key: db.ReviewKeyCopyFailed, Delta: affected},
		}
		if err := w.ApplyReviewStatsDeltas(deltas); err != nil {
			return affected, err
		}
	}
	return affected, nil
}

// InsertDstChildrenTraversalStatusEvents appends one traversal_status event for each DST node whose parent_path equals parentPath. Used when marking/unmarking SRC node for retry (DST-only children get pending or not_on_src). Call inside RunWrite.
func InsertDstChildrenTraversalStatusEvents(w *db.Writer, parentPath, status string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	normParentPath := db.NormalizeRootRelativePath(parentPath)
	_, err := w.Tx().ExecContext(ctx, `INSERT INTO dst_status_events (id, traversal_status, event_time, depth) SELECT n.id, $1, $2, n.depth FROM dst_nodes n WHERE n.parent_path = $3`, status, eventTime, normParentPath)
	return err
}

// InsertGPLRestoredEventsForSubtree appends the latest non-ignored gpl_status for rootPath and
// descendants (rollback after exclude-coupled ignore). Defaults to successful when none.
func InsertGPLRestoredEventsForSubtree(w *db.Writer, side, rootPath string) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	rootPath = db.NormalizeSubtreeRootPathForPropagation(rootPath)
	if side == "DST" {
		return insertDSTGPLRestoredSubtree(w, ctx, rootPath, eventTime)
	}
	return insertSRCGPLRestoredSubtree(w, ctx, rootPath, eventTime)
}

func insertSRCGPLRestoredSubtree(w *db.Writer, ctx context.Context, rootPath string, eventTime int64) error {
	insert := `INSERT INTO ` + db.TableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth)
SELECT n.id,
  COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM ` + db.TableSrcStatusEvents + ` e WHERE e.id = n.id), ''),
  COALESCE((SELECT arg_max(e.copy_status, e.event_time) FROM ` + db.TableSrcStatusEvents + ` e WHERE e.id = n.id AND COALESCE(e.copy_status,'') <> ''), ''),
  COALESCE((SELECT arg_max(e.delete_status, e.event_time) FROM ` + db.TableSrcStatusEvents + ` e WHERE e.id = n.id AND COALESCE(e.delete_status,'') <> ''), ''),
  COALESCE((
    SELECT arg_max(e.gpl_status, e.event_time)
    FROM ` + db.TableSrcStatusEvents + ` e
    WHERE e.id = n.id
      AND COALESCE(e.gpl_status, '') <> ''
      AND e.gpl_status <> '` + db.GPLStatusIgnored + `'
  ), '` + db.GPLStatusSuccessful + `'),
  $1, n.depth
FROM ` + db.TableSrcNodes + ` n WHERE `
	if rootPath == "/" {
		if _, err := w.Tx().ExecContext(ctx, insert+`n.path LIKE '/%'`, eventTime); err != nil {
			return err
		}
		return w.RefreshCurrentByPathPrefix("SRC", "/")
	}
	if _, err := w.Tx().ExecContext(ctx, insert+`(n.path = $2 OR n.path LIKE $3)`, eventTime, rootPath, rootPath+"/%"); err != nil {
		return err
	}
	return w.RefreshCurrentByPathPrefix("SRC", rootPath)
}

func insertDSTGPLRestoredSubtree(w *db.Writer, ctx context.Context, rootPath string, eventTime int64) error {
	insert := `INSERT INTO ` + db.TableDstStatusEvents + ` (id, traversal_status, gpl_status, event_time, depth)
SELECT n.id,
  COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM ` + db.TableDstStatusEvents + ` e WHERE e.id = n.id), ''),
  COALESCE((
    SELECT arg_max(e.gpl_status, e.event_time)
    FROM ` + db.TableDstStatusEvents + ` e
    WHERE e.id = n.id
      AND COALESCE(e.gpl_status, '') <> ''
      AND e.gpl_status <> '` + db.GPLStatusIgnored + `'
  ), '` + db.GPLStatusSuccessful + `'),
  $1, n.depth
FROM ` + db.TableDstNodes + ` n WHERE `
	if rootPath == "/" {
		if _, err := w.Tx().ExecContext(ctx, insert+`n.path LIKE '/%'`, eventTime); err != nil {
			return err
		}
		return w.RefreshCurrentByPathPrefix("DST", "/")
	}
	if _, err := w.Tx().ExecContext(ctx, insert+`(n.path = $2 OR n.path LIKE $3)`, eventTime, rootPath, rootPath+"/%"); err != nil {
		return err
	}
	return w.RefreshCurrentByPathPrefix("DST", rootPath)
}

func InsertGPLStatusEventsForSubtree(w *db.Writer, side, rootPath, gplStatus string, includeRoot bool) error {
	ctx := context.Background()
	eventTime := time.Now().UnixNano()
	rootPath = db.NormalizeSubtreeRootPathForPropagation(rootPath)

	if side == "DST" {
		if err := insertDSTGPLStatusSubtree(w, ctx, rootPath, eventTime, gplStatus, includeRoot); err != nil {
			return err
		}
		return w.RefreshCurrentByPathPrefix("DST", rootPath)
	}
	if err := insertSRCGPLStatusSubtree(w, ctx, rootPath, eventTime, gplStatus, includeRoot); err != nil {
		return err
	}
	return w.RefreshCurrentByPathPrefix("SRC", rootPath)
}

func insertSRCGPLStatusSubtree(w *db.Writer, ctx context.Context, rootPath string, eventTime int64, gplStatus string, includeRoot bool) error {
	insert := `INSERT INTO ` + db.TableSrcStatusEvents + ` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth)
SELECT n.id,
  COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM ` + db.TableSrcStatusEvents + ` e WHERE e.id = n.id), ''),
  COALESCE((SELECT arg_max(e.copy_status, e.event_time) FROM ` + db.TableSrcStatusEvents + ` e WHERE e.id = n.id AND COALESCE(e.copy_status,'') <> ''), ''),
  COALESCE((SELECT arg_max(e.delete_status, e.event_time) FROM ` + db.TableSrcStatusEvents + ` e WHERE e.id = n.id AND COALESCE(e.delete_status,'') <> ''), ''),
  $1, $2, n.depth
FROM ` + db.TableSrcNodes + ` n WHERE `
	if rootPath == "/" {
		cond := `n.path LIKE '/%'`
		if !includeRoot {
			cond += ` AND n.path <> '/'`
		}
		_, err := w.Tx().ExecContext(ctx, insert+cond, gplStatus, eventTime)
		return err
	}
	if includeRoot {
		_, err := w.Tx().ExecContext(ctx, insert+`(n.path = $3 OR n.path LIKE $4)`, gplStatus, eventTime, rootPath, rootPath+"/%")
		return err
	}
	_, err := w.Tx().ExecContext(ctx, insert+`n.path LIKE $3`, gplStatus, eventTime, rootPath+"/%")
	return err
}

func insertDSTGPLStatusSubtree(w *db.Writer, ctx context.Context, rootPath string, eventTime int64, gplStatus string, includeRoot bool) error {
	insert := `INSERT INTO ` + db.TableDstStatusEvents + ` (id, traversal_status, gpl_status, event_time, depth)
SELECT n.id,
  COALESCE((SELECT arg_max(e.traversal_status, e.event_time) FROM ` + db.TableDstStatusEvents + ` e WHERE e.id = n.id), ''),
  $1, $2, n.depth
FROM ` + db.TableDstNodes + ` n WHERE `
	if rootPath == "/" {
		cond := `n.path LIKE '/%'`
		if !includeRoot {
			cond += ` AND n.path <> '/'`
		}
		_, err := w.Tx().ExecContext(ctx, insert+cond, gplStatus, eventTime)
		return err
	}
	if includeRoot {
		_, err := w.Tx().ExecContext(ctx, insert+`(n.path = $3 OR n.path LIKE $4)`, gplStatus, eventTime, rootPath, rootPath+"/%")
		return err
	}
	_, err := w.Tx().ExecContext(ctx, insert+`n.path LIKE $3`, gplStatus, eventTime, rootPath+"/%")
	return err
}

func CountExcludedInSubtree(w *db.Writer, table, rootPath string) (excluded, notExcluded int64, err error) {
	ctx := context.Background()
	q := `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM ` + db.TableSrcStatusEvents + ` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') IN ('excluded_explicit','excluded_inherited'))::BIGINT,
       COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited'))::BIGINT
FROM ` + db.TableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id WHERE n.path = $1 OR n.path LIKE $2`
	if rootPath == "/" {
		err = w.Tx().QueryRowContext(ctx, `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM `+db.TableSrcStatusEvents+` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') IN ('excluded_explicit','excluded_inherited'))::BIGINT,
       COUNT(*) FILTER (WHERE COALESCE(e.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited'))::BIGINT
FROM `+db.TableSrcNodes+` n LEFT JOIN latest e ON n.id = e.id WHERE n.path LIKE '/%'`).Scan(&excluded, &notExcluded)
	} else {
		err = w.Tx().QueryRowContext(ctx, q, rootPath, rootPath+"/%").Scan(&excluded, &notExcluded)
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

// CountCopyStatusBucketsSubtreeNotExcluded counts SRC nodes under rootPath whose latest copy_status is not excluded, by bucket. Matches db.Writer.SetNodeExcluded universal deltas (in_progress and skipped have no separate review-table copy bucket). Call inside a transaction.
func CountCopyStatusBucketsSubtreeNotExcluded(w *db.Writer, rootPath string) (CopyStatusBucketsSubtreeNotExcluded, error) {
	ctx := context.Background()
	var out CopyStatusBucketsSubtreeNotExcluded
	notExcl := `(COALESCE(e.copy_status,'') NOT IN ` + db.SQLCopyStatusExcludedIN + `)`
	base := `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM ` + db.TableSrcStatusEvents + ` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') IN ('pending',''))::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'failed')::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + `)::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'skipped')::BIGINT,
  COUNT(*) FILTER (WHERE ` + notExcl + ` AND COALESCE(e.copy_status,'') = 'in_progress')::BIGINT
FROM ` + db.TableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id WHERE `
	if rootPath == "/" {
		err := w.Tx().QueryRowContext(ctx, base+`n.path LIKE '/%'`).Scan(
			&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
		return out, err
	}
	prefix := rootPath + "/%"
	err := w.Tx().QueryRowContext(ctx, base+`n.path = $1 OR n.path LIKE $2`, rootPath, prefix).Scan(
		&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
	return out, err
}

// SumPendingFileSizeSubtreeNotExcluded sums SRC file sizes under rootPath that are not
// excluded and whose latest copy_status is pending (or empty). Used for size_selected deltas.
func SumPendingFileSizeSubtreeNotExcluded(w *db.Writer, rootPath string) (int64, error) {
	ctx := context.Background()
	notExcl := `(COALESCE(e.copy_status,'') NOT IN ` + db.SQLCopyStatusExcludedIN + `)`
	pending := `(COALESCE(e.copy_status,'') IN ('pending',''))`
	base := `WITH latest AS (SELECT id, arg_max(copy_status, event_time) AS copy_status FROM ` + db.TableSrcStatusEvents + ` WHERE COALESCE(copy_status,'') <> '' GROUP BY id)
SELECT COALESCE(SUM(n.size), 0)::BIGINT
FROM ` + db.TableSrcNodes + ` n LEFT JOIN latest e ON n.id = e.id
WHERE n.type = 'file' AND ` + notExcl + ` AND ` + pending + ` AND `
	var sum int64
	var err error
	if rootPath == "/" {
		err = w.Tx().QueryRowContext(ctx, base+`n.path LIKE '/%'`).Scan(&sum)
	} else {
		err = w.Tx().QueryRowContext(ctx, base+`n.path = $1 OR n.path LIKE $2`, rootPath, rootPath+"/%").Scan(&sum)
	}
	return sum, err
}

// SumPendingFileSizeSubtreeExcludedPrior sums SRC file sizes under rootPath that are currently
// excluded and will restore to pending on unexclude.
func SumPendingFileSizeSubtreeExcludedPrior(w *db.Writer, rootPath string) (int64, error) {
	ctx := context.Background()
	base := `WITH latest AS (
  SELECT id, arg_max(copy_status, event_time) AS copy_status
  FROM ` + db.TableSrcStatusEvents + `
  WHERE COALESCE(copy_status,'') <> ''
  GROUP BY id
),
subtree AS (
  SELECT n.id, n.size FROM ` + db.TableSrcNodes + ` n
  JOIN latest e ON e.id = n.id
  WHERE n.type = 'file' AND COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusExcludedIN + ` AND `
	prev := `),
prev_copy AS (
  SELECT e.id, arg_max(e.copy_status, e.event_time) AS copy_status
  FROM ` + db.TableSrcStatusEvents + ` e
  JOIN subtree s ON s.id = e.id
  WHERE COALESCE(e.copy_status,'') <> ''
    AND e.copy_status NOT IN ` + db.SQLCopyStatusExcludedIN + `
  GROUP BY e.id
)
SELECT COALESCE(SUM(s.size), 0)::BIGINT
FROM subtree s
LEFT JOIN prev_copy p ON p.id = s.id
WHERE COALESCE(NULLIF(p.copy_status,''), 'pending') IN ('pending','')`
	var sum int64
	var err error
	if rootPath == "/" {
		err = w.Tx().QueryRowContext(ctx, base+`n.path LIKE '/%'`+prev).Scan(&sum)
	} else {
		err = w.Tx().QueryRowContext(ctx, base+`(n.path = $1 OR n.path LIKE $2)`+prev, rootPath, rootPath+"/%").Scan(&sum)
	}
	return sum, err
}

// CountCopyStatusBucketsSubtreeExcludedPrior counts currently-excluded SRC nodes under rootPath
// by the latest non-exclusion copy_status each will restore to on unexclude. Call inside a transaction
// before InsertUnexcludeEventsForSubtree so review deltas mirror restored buckets.
func CountCopyStatusBucketsSubtreeExcludedPrior(w *db.Writer, rootPath string) (CopyStatusBucketsSubtreeNotExcluded, error) {
	ctx := context.Background()
	var out CopyStatusBucketsSubtreeNotExcluded
	base := `WITH latest AS (
  SELECT id, arg_max(copy_status, event_time) AS copy_status
  FROM ` + db.TableSrcStatusEvents + `
  WHERE COALESCE(copy_status,'') <> ''
  GROUP BY id
),
subtree AS (
  SELECT n.id FROM ` + db.TableSrcNodes + ` n
  JOIN latest e ON e.id = n.id
  WHERE COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusExcludedIN + ` AND `
	prev := `),
prev_copy AS (
  SELECT e.id, arg_max(e.copy_status, e.event_time) AS copy_status
  FROM ` + db.TableSrcStatusEvents + ` e
  JOIN subtree s ON s.id = e.id
  WHERE COALESCE(e.copy_status,'') <> ''
    AND e.copy_status NOT IN ` + db.SQLCopyStatusExcludedIN + `
  GROUP BY e.id
)
SELECT
  COUNT(*) FILTER (WHERE COALESCE(NULLIF(p.copy_status,''), 'pending') IN ('pending',''))::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') = 'failed')::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + `)::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') = 'skipped')::BIGINT,
  COUNT(*) FILTER (WHERE COALESCE(p.copy_status,'') = 'in_progress')::BIGINT
FROM subtree s
LEFT JOIN prev_copy p ON p.id = s.id`
	if rootPath == "/" {
		err := w.Tx().QueryRowContext(ctx, base+`n.path LIKE '/%'`+prev).Scan(
			&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
		return out, err
	}
	prefix := rootPath + "/%"
	err := w.Tx().QueryRowContext(ctx, base+`(n.path = $1 OR n.path LIKE $2)`+prev, rootPath, prefix).Scan(
		&out.Pending, &out.Failed, &out.Successful, &out.Skipped, &out.InProgress)
	return out, err
}

// CountCopyWorkEligibleSubtreeNotExcluded returns folder/file/byte totals for SRC nodes
// under rootPath that are not excluded and count toward sealed copy_work denominators.
// Call before InsertExclusionEventsForSubtree so exclude can subtract the same units.
func CountCopyWorkEligibleSubtreeNotExcluded(w *db.Writer, rootPath string) (db.DepthWorkAbsolute, error) {
	ctx := context.Background()
	var out db.DepthWorkAbsolute
	// Same status set as GetWorkEligibleAtDepth(StatsKindCopy) (already_existed intentionally omitted).
	base := `SELECT
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM ` + db.TableSrcNodes + ` n
LEFT JOIN ` + db.CTESrcCurrentStatus + ` cur ON n.id = cur.id
WHERE COALESCE(cur.copy_status,'') IN ('pending','','in_progress','successful','failed') AND `
	if rootPath == "/" {
		err := w.Tx().QueryRowContext(ctx, base+`n.path LIKE '/%'`).Scan(&out.Folders, &out.Files, &out.Bytes)
		return out, err
	}
	err := w.Tx().QueryRowContext(ctx, base+`(n.path = $1 OR n.path LIKE $2)`, rootPath, rootPath+"/%").
		Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
}

// CountCopyWorkEligibleSubtreeExcludedPrior returns folder/file/byte totals for currently
// excluded SRC nodes under rootPath whose restored (pre-exclusion) copy_status counts
// toward sealed copy_work. Call before InsertUnexcludeEventsForSubtree.
func CountCopyWorkEligibleSubtreeExcludedPrior(w *db.Writer, rootPath string) (db.DepthWorkAbsolute, error) {
	ctx := context.Background()
	var out db.DepthWorkAbsolute
	base := `WITH latest AS (
  SELECT id, arg_max(copy_status, event_time) AS copy_status
  FROM ` + db.TableSrcStatusEvents + `
  WHERE COALESCE(copy_status,'') <> ''
  GROUP BY id
),
subtree AS (
  SELECT n.id, n.type, n.size FROM ` + db.TableSrcNodes + ` n
  JOIN latest e ON e.id = n.id
  WHERE COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusExcludedIN + ` AND `
	prev := `),
prev_copy AS (
  SELECT e.id, arg_max(e.copy_status, e.event_time) AS copy_status
  FROM ` + db.TableSrcStatusEvents + ` e
  JOIN subtree s ON s.id = e.id
  WHERE COALESCE(e.copy_status,'') <> ''
    AND e.copy_status NOT IN ` + db.SQLCopyStatusExcludedIN + `
  GROUP BY e.id
)
SELECT
  COUNT(*) FILTER (WHERE s.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE s.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN s.type = 'file' THEN s.size ELSE 0 END), 0)::BIGINT
FROM subtree s
LEFT JOIN prev_copy p ON p.id = s.id
WHERE COALESCE(NULLIF(p.copy_status,''), 'pending') IN ('pending','','in_progress','successful','failed')`
	if rootPath == "/" {
		err := w.Tx().QueryRowContext(ctx, base+`n.path LIKE '/%'`+prev).Scan(&out.Folders, &out.Files, &out.Bytes)
		return out, err
	}
	err := w.Tx().QueryRowContext(ctx, base+`(n.path = $1 OR n.path LIKE $2)`+prev, rootPath, rootPath+"/%").
		Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
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
func CountDstDescendantsReviewStats(w *db.Writer, rootPath string) (DstDescendantsReviewStats, error) {
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
		err := w.Tx().QueryRowContext(ctx, q).Scan(&out.Folders, &out.Files, &out.Excluded, &out.SizeDst, &out.TraversalPending, &out.TraversalFailed)
		return out, err
	}
	q += `n.path LIKE $1`
	err := w.Tx().QueryRowContext(ctx, q, rootPath+"/%").Scan(&out.Folders, &out.Files, &out.Excluded, &out.SizeDst, &out.TraversalPending, &out.TraversalFailed)
	return out, err
}
