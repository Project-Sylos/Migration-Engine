// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

func TableName(table string) string {
	if table == "DST" {
		return TableDstNodes
	}
	return TableSrcNodes
}

func StatusJoinExpr(table string) (nodesAlias, statusAlias, statusRel string) {
	if table == "DST" {
		return "n", "e", TableDstCurrent
	}
	return "n", "e", TableSrcCurrent
}

// Node columns from joined form: n.* plus e.traversal_status, e.copy_status (SRC only), excluded derived, errors as ”, name + display_path last.
func SelectNodeColsWithStatus(table string) string {
	t := TableName(table)
	n, e, statusRel := StatusJoinExpr(table)
	if table == "DST" {
		return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors, COALESCE(` + n + `.name,'') AS name, COALESCE(` + n + `.display_path,'') AS display_path FROM ` + t + ` ` + n + ` LEFT JOIN ` + statusRel + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
	}
	return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, COALESCE(` + e + `.delete_status,'') AS delete_status, (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded, '' AS errors, COALESCE(` + n + `.name,'') AS name, COALESCE(` + n + `.display_path,'') AS display_path FROM ` + t + ` ` + n + ` LEFT JOIN ` + statusRel + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
}

// selectNodeColsRaw returns node columns without joining status events (traversal/copy columns are empty defaults).
func SelectNodeColsRaw(table string) string {
	t := TableName(table)
	return `SELECT n.id, n.service_id, n.parent_id, n.parent_service_id, n.path, n.parent_path, n.type, n.size, n.mtime, n.depth, '' AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors, COALESCE(n.name,'') AS name, COALESCE(n.display_path,'') AS display_path FROM ` + t + ` n`
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
