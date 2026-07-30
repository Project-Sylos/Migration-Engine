// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

func TableName(table string) string {
	if table == "DST" {
		return TableDstNodes
	}
	return TableSrcNodes
}

// Current status from events (join with nodes to get traversal_status, copy_status, excluded).
// Single pass over src_status_events: traversal uses every row; copy/delete only rows where
// that column is set (same semantics as the former three-CTE FULL OUTER JOIN).
const (
	CTESrcCurrentStatus = `(SELECT id,
		COALESCE(arg_max(traversal_status, event_time), '') AS traversal_status,
		COALESCE(arg_max(copy_status, event_time) FILTER (WHERE COALESCE(copy_status, '') <> ''), '') AS copy_status,
		COALESCE(arg_max(delete_status, event_time) FILTER (WHERE COALESCE(delete_status, '') <> ''), '') AS delete_status
	FROM src_status_events
	GROUP BY id)`
	CTEDstCurrentStatus = `(SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)`
)

func StatusJoinExpr(table string) (nodesAlias, cteAlias, cte string) {
	if table == "DST" {
		return "n", "e", CTEDstCurrentStatus
	}
	return "n", "e", CTESrcCurrentStatus
}

// Node columns from joined form: n.* plus e.traversal_status, e.copy_status (SRC only), excluded derived, errors as ”, name last.
func SelectNodeColsWithStatus(table string) string {
	t := TableName(table)
	n, e, cte := StatusJoinExpr(table)
	if table == "DST" {
		return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors, COALESCE(` + n + `.name,'') AS name FROM ` + t + ` ` + n + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
	}
	return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, COALESCE(` + e + `.delete_status,'') AS delete_status, (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded, '' AS errors, COALESCE(` + n + `.name,'') AS name FROM ` + t + ` ` + n + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
}

// selectNodeColsRaw returns node columns without joining status events (traversal/copy columns are empty defaults).
func SelectNodeColsRaw(table string) string {
	t := TableName(table)
	return `SELECT n.id, n.service_id, n.parent_id, n.parent_service_id, n.path, n.parent_path, n.type, n.size, n.mtime, n.depth, '' AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors, COALESCE(n.name,'') AS name FROM ` + t + ` n`
}

// pullKeysetWindowSize is how many node rows we scan per round-trip when filtering pulls by event-derived status
// (SRC depth keyset, copy/delete/GPL pulls). DST traversal pull uses dstPullScanWindow instead.
const PullKeysetWindowSize = 50000

const (
	DstPullScanWindowMin = 1000
	DstPullScanWindowMax = 5000
)

// dstPullScanWindow sizes each DST keyset subsection from the requested batch limit.
// Smaller than pullKeysetWindowSize so status IN queries stay proportional to the refill batch.
func DstPullScanWindow(limit int) int {
	if limit < 1 {
		limit = 1
	}
	w := limit * 2
	if w < DstPullScanWindowMin {
		w = DstPullScanWindowMin
	}
	if w > DstPullScanWindowMax {
		w = DstPullScanWindowMax
	}
	return w
}
