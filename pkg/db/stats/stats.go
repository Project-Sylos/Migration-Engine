// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func opsSide(table string) string {
	if table == "DST" {
		return opsdb.SideDST
	}
	return opsdb.SideSRC
}

func sumStatsKeys(database *db.DB, keys ...string) (int64, error) {
	var sum int64
	for _, k := range keys {
		n, err := readStatsKeyCount(database, k)
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}

func copyPopulationStatuses(pop SelectedPopulation) []string {
	if pop == SelectedEligible {
		return copyEligibleStatuses
	}
	return []string{db.CopyStatusPending}
}

func deletePopulationStatuses(pop SelectedPopulation) []string {
	if pop == SelectedEligible {
		return []string{
			db.DeleteStatusPendingExplicit,
			db.DeleteStatusPendingInherited,
			db.DeleteStatusDeleted,
			db.DeleteStatusFailed,
		}
	}
	return []string{db.DeleteStatusPendingExplicit, db.DeleteStatusPendingInherited}
}

func phaseProgressStatsKeys(kind db.StatsKind) (pending, successful, failed string) {
	switch kind {
	case db.StatsKindDelete:
		return db.ReviewKeyDeletePending, db.ReviewKeyDeleteDeleted, db.ReviewKeyDeleteFailed
	default:
		return db.ReviewKeyCopyPending, db.ReviewKeyCopySuccessful, db.ReviewKeyCopyFailed
	}
}

func sumTypedCountTotal(database *db.DB, kind db.StatsKind, status, nodeType string) (int64, error) {
	if database == nil || database.Ops() == nil {
		return 0, nil
	}
	ops := database.Ops()
	if nodeType != "" {
		return ops.GetDepthStatTotal(opsdb.SideSRC, db.StatsKeyTyped(kind, status, nodeType))
	}
	folders, err := ops.GetDepthStatTotal(opsdb.SideSRC, db.StatsKeyTyped(kind, status, db.NodeTypeFolder))
	if err != nil {
		return 0, err
	}
	files, err := ops.GetDepthStatTotal(opsdb.SideSRC, db.StatsKeyTyped(kind, status, db.NodeTypeFile))
	if err != nil {
		return 0, err
	}
	return folders + files, nil
}

func sumEligibleByType(database *db.DB, kind db.StatsKind, statuses []string) (db.EligibleCountsByType, error) {
	var out db.EligibleCountsByType
	if len(statuses) == 0 || database == nil || database.Ops() == nil {
		return out, nil
	}
	ops := database.Ops()
	for _, st := range statuses {
		fk := db.StatsKeyTyped(kind, st, db.NodeTypeFolder)
		n, err := ops.GetDepthStatTotal(opsdb.SideSRC, fk)
		if err != nil {
			return out, err
		}
		out.Folders += n
		fileK := db.StatsKeyTyped(kind, st, db.NodeTypeFile)
		n, err = ops.GetDepthStatTotal(opsdb.SideSRC, fileK)
		if err != nil {
			return out, err
		}
		out.Files += n
	}
	return out, nil
}

func sumFileBytesForStatuses(database *db.DB, kind db.StatsKind, statuses []string) (int64, error) {
	if len(statuses) == 0 || database == nil || database.Ops() == nil {
		return 0, nil
	}
	ops := database.Ops()
	var sum int64
	for _, st := range statuses {
		var key string
		switch kind {
		case db.StatsKindDelete:
			key = db.StatsKeyDeleteFileBytes(st)
		default:
			key = db.StatsKeyCopyFileBytes(st)
		}
		if key == "" {
			continue
		}
		n, err := ops.GetDepthStatTotal(opsdb.SideSRC, key)
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}

// GetCountsByType returns SRC folder/file counts for copy or delete work at the given population.
func GetCountsByType(database *db.DB, kind db.StatsKind, pop SelectedPopulation) (db.EligibleCountsByType, error) {
	if kind == db.StatsKindDelete {
		return sumEligibleByType(database, kind, deletePopulationStatuses(pop))
	}
	return sumEligibleByType(database, kind, copyPopulationStatuses(pop))
}

// GetFileSize returns the sum of SRC file sizes for copy or delete work at the given population.
func GetFileSize(database *db.DB, kind db.StatsKind, pop SelectedPopulation) (int64, error) {
	if kind == db.StatsKindDelete {
		return sumFileBytesForStatuses(database, kind, deletePopulationStatuses(pop))
	}
	return sumFileBytesForStatuses(database, kind, copyPopulationStatuses(pop))
}

// GetFailedWorkFileSize returns SRC file bytes currently in failed status for copy or delete work.
func GetFailedWorkFileSize(database *db.DB, kind db.StatsKind) (int64, error) {
	if kind == db.StatsKindDelete {
		return sumFileBytesForStatuses(database, kind, []string{db.DeleteStatusFailed})
	}
	return sumFileBytesForStatuses(database, kind, []string{db.CopyStatusFailed})
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
	if database == nil || key == "" {
		return 0, nil
	}
	ops := database.Ops()
	if ops == nil {
		return 0, nil
	}
	return ops.GetStat(key)
}

// GetCopyWorkProgressCounts returns pending/successful/failed for copy *work*
// (excludes already_existed). Used by the progress monitor so items % matches
// Folders/Files expected (only items this migration copies).
func GetCopyWorkProgressCounts(database *db.DB) (db.PhaseProgressCounts, error) {
	counts, err := GetCopyStatusCountsFromEvents(database)
	if err != nil {
		return db.PhaseProgressCounts{}, err
	}
	inProgress, err := sumTypedCountTotal(database, db.StatsKindCopy, db.CopyStatusInProgress, "")
	if err != nil {
		return db.PhaseProgressCounts{}, err
	}
	return db.PhaseProgressCounts{
		Pending:    counts.Pending + inProgress,
		Successful: counts.Successful,
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
	return GetDeleteStatusCountsFromEvents(database)
}

// GetReviewStatsSnapshot reads the full canonical review stats from the universal stats table.
func GetReviewStatsSnapshot(database *db.DB) (db.ReviewStatsSnapshot, error) {
	var out db.ReviewStatsSnapshot
	if database == nil {
		return out, nil
	}
	for _, key := range db.CanonicalReviewKeys {
		n, err := readStatsKeyCount(database, key)
		if err != nil {
			return out, err
		}
		switch key {
		case db.ReviewKeyTraversalPending:
			out.TraversalPending = n
		case db.ReviewKeyTraversalPendingRetry:
			out.TraversalPendingRetry = n
		case db.ReviewKeyTraversalSuccessful:
			out.TraversalSuccessful = n
		case db.ReviewKeyTraversalFailed:
			out.TraversalFailed = n
		case db.ReviewKeyCopyPending:
			out.CopyPending = n
		case db.ReviewKeyCopyPendingRetry:
			out.CopyPendingRetry = n
		case db.ReviewKeyCopySuccessful:
			out.CopySuccessful = n
		case db.ReviewKeyCopyFailed:
			out.CopyFailed = n
		case db.ReviewKeyDeletePending:
			out.DeletePending = n
		case db.ReviewKeyDeleteDeleted:
			out.DeleteDeleted = n
		case db.ReviewKeyDeleteFailed:
			out.DeleteFailed = n
		case db.ReviewKeyDeleteSkipped:
			out.DeleteSkipped = n
		case db.ReviewKeyExcluded:
			out.Excluded = n
		case db.ReviewKeyFolders:
			out.Folders = n
		case db.ReviewKeyFiles:
			out.Files = n
		case db.ReviewKeySizeSrc:
			out.SizeSrc = n
		case db.ReviewKeySizeDst:
			out.SizeDst = n
		case db.ReviewKeySizeSelected:
			out.SizeSelected = n
		case db.ReviewKeySizeDeleteSelected:
			out.SizeDeleteSelected = n
		}
	}
	return out, nil
}

// GetPathReviewStatsFromDB computes path review stats from nodes and status events only (no stats table).
// Prefer GetReviewStatsSnapshot for live API paths; this is for verification / offline checks.
func GetPathReviewStatsFromDB(database *db.DB) (db.ReviewStatsSnapshot, error) {
	return GetPathReviewStatsFromOps(database)
}

// GetTraversalStatusCountsFromEvents returns counts of nodes by current traversal_status from Badger depth stats.
func GetTraversalStatusCountsFromEvents(database *db.DB, table string) (db.TraversalStatusCounts, error) {
	return getTraversalStatusCounts(database, table)
}

// GetTraversalStatusCountsFromCurrent returns counts from Badger depth stats (same as FromEvents on Badger-only stores).
func GetTraversalStatusCountsFromCurrent(database *db.DB, table string) (db.TraversalStatusCounts, error) {
	return getTraversalStatusCounts(database, table)
}

// GetTraversalStatusCounts returns traversal status counts; fromCurrent is ignored on Badger-only stores.
func GetTraversalStatusCounts(database *db.DB, table string, fromCurrent bool) (db.TraversalStatusCounts, error) {
	_ = fromCurrent
	return getTraversalStatusCounts(database, table)
}

func getTraversalStatusCounts(database *db.DB, table string) (db.TraversalStatusCounts, error) {
	return traversalCountsFromDepthStats(database, table)
}

func traversalCountsFromDepthStats(database *db.DB, table string) (db.TraversalStatusCounts, error) {
	var out db.TraversalStatusCounts
	if database == nil || database.Ops() == nil {
		return out, nil
	}
	side := opsSide(table)
	ops := database.Ops()
	add := func(status string, apply func(int64)) error {
		n, err := ops.GetDepthStatTotal(side, db.StatsKey(db.StatsKindTraversal, status))
		if err != nil {
			return err
		}
		if n != 0 {
			apply(n)
		}
		return nil
	}
	if err := add(db.StatusPending, func(n int64) { out.Pending += n }); err != nil {
		return out, err
	}
	if err := add(db.StatusSuccessful, func(n int64) { out.Successful += n }); err != nil {
		return out, err
	}
	if err := add(db.StatusFailed, func(n int64) { out.Failed += n }); err != nil {
		return out, err
	}
	if table == "DST" {
		if err := add(db.StatusNotOnSrc, func(n int64) { out.NotOnSrc += n }); err != nil {
			return out, err
		}
	}
	if err := add(db.StatusExcluded, func(n int64) { out.Excluded += n }); err != nil {
		return out, err
	}
	if err := add(db.StatusExclusionInherited, func(n int64) { out.Excluded += n }); err != nil {
		return out, err
	}
	return out, nil
}

// MinPendingTraversalDepth returns the minimum node depth with pending (or missing) traversal status
// in depth stats, or nil if none.
func MinPendingTraversalDepth(database *db.DB, table string) (*int, error) {
	if database == nil || database.Ops() == nil {
		return nil, nil
	}
	maxDepth, err := GetMaxDepth(database, table)
	if err != nil {
		return nil, err
	}
	side := opsSide(table)
	key := db.StatsKey(db.StatsKindTraversal, db.StatusPending)
	for d := 0; d <= maxDepth; d++ {
		n, err := database.Ops().GetDepthStat(side, d, key)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			depth := d
			return &depth, nil
		}
	}
	return nil, nil
}

// GetCopyStatusCountsFromEvents returns counts of SRC nodes by current copy_status from Badger depth stats.
func GetCopyStatusCountsFromEvents(database *db.DB) (db.CopyStatusCounts, error) {
	var out db.CopyStatusCounts
	if database == nil || database.Ops() == nil {
		return out, nil
	}
	type pair struct {
		status string
		apply  func(int64)
	}
	for _, p := range []pair{
		{db.CopyStatusPending, func(n int64) { out.Pending += n }},
		{db.CopyStatusSuccessful, func(n int64) { out.Successful += n }},
		{db.CopyStatusAlreadyExisted, func(n int64) { out.AlreadyExisted += n }},
		{db.CopyStatusFailed, func(n int64) { out.Failed += n }},
		{db.CopyStatusSkipped, func(n int64) { out.Skipped += n }},
		{db.CopyStatusExcludedExplicit, func(n int64) { out.Excluded += n }},
		{db.CopyStatusExcludedInherited, func(n int64) { out.Excluded += n }},
	} {
		n, err := sumTypedCountTotal(database, db.StatsKindCopy, p.status, "")
		if err != nil {
			return out, err
		}
		p.apply(n)
	}
	return out, nil
}

// GetDeleteStatusCountsFromEvents returns counts of SRC nodes by current delete_status from Badger depth stats.
func GetDeleteStatusCountsFromEvents(database *db.DB) (db.DeleteStatusCounts, error) {
	var out db.DeleteStatusCounts
	if database == nil || database.Ops() == nil {
		return out, nil
	}
	type pair struct {
		status string
		apply  func(int64)
	}
	for _, p := range []pair{
		{db.DeleteStatusPendingExplicit, func(n int64) { out.Pending += n }},
		{db.DeleteStatusPendingInherited, func(n int64) { out.Pending += n }},
		{db.DeleteStatusDeleted, func(n int64) { out.Deleted += n }},
		{db.DeleteStatusFailed, func(n int64) { out.Failed += n }},
		{db.DeleteStatusSkipped, func(n int64) { out.Skipped += n }},
	} {
		n, err := sumTypedCountTotal(database, db.StatsKindDelete, p.status, "")
		if err != nil {
			return out, err
		}
		p.apply(n)
	}
	return out, nil
}

// GetRemainingSourceSizeAfterDelete returns the persisted size_src review counter.
func GetRemainingSourceSizeAfterDelete(database *db.DB) (int64, error) {
	return readStatsKeyCount(database, db.ReviewKeySizeSrc)
}

// GetStatsCount returns the total count for the given key from Badger depth stats (table = "SRC" or "DST"). Supports traversal status keys only.
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
	if database == nil || database.Ops() == nil {
		return 0, nil
	}
	if status == "" {
		status = db.StatusPending
	}
	return database.Ops().GetDepthStatTotal(opsSide(table), db.StatsKey(db.StatsKindTraversal, status))
}

// statsKeyToTraversalStatus returns the status part if key is "traversal/<status>", else "".
func statsKeyToTraversalStatus(key string) string {
	const prefix = "traversal/"
	if len(key) > len(prefix) && key[:len(prefix)] == prefix {
		return key[len(prefix):]
	}
	return ""
}

// GetStatsCountAtDepth returns the count for (depth, key) from Badger depth stats. Supports traversal status keys only (e.g. traversal/pending, traversal/failed).
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

// GetTraversalCountAtDepthFromLive returns the count of nodes at the given depth with the given traversal_status.
func GetTraversalCountAtDepthFromLive(database *db.DB, table string, depth int, status string) (int64, error) {
	if database == nil || database.Ops() == nil {
		return 0, nil
	}
	if status == "" {
		status = db.StatusPending
	}
	return database.Ops().GetDepthStat(opsSide(table), depth, db.StatsKey(db.StatsKindTraversal, status))
}

// GetGPLCountAtDepthFromLive returns the count of nodes at depth with the given gpl_status.
func GetGPLCountAtDepthFromLive(database *db.DB, table string, depth int, status string) (int64, error) {
	if database == nil || database.Ops() == nil || status == "" {
		return 0, nil
	}
	side := opsSide(table)
	gplKey := "gpl/" + status
	if n, err := database.Ops().GetDepthStat(side, depth, gplKey); err == nil && n > 0 {
		return n, nil
	}
	return countNodesAtDepthGPL(database, side, depth, status)
}

func countNodesAtDepthGPL(database *db.DB, side string, depth int, status string) (int64, error) {
	ops := database.Ops()
	var count int64
	for after := ""; ; {
		ids, err := ops.ListNodeIDs(side, after, 500)
		if err != nil {
			return 0, err
		}
		if len(ids) == 0 {
			break
		}
		nodes, sts, err := ops.BatchGetNodeStatus(side, ids)
		if err != nil {
			return 0, err
		}
		for _, id := range ids {
			n, ok := nodes[id]
			if !ok || n.Depth != depth {
				continue
			}
			if st, ok := sts[id]; ok && st.GPLStatus == status {
				count++
			}
		}
		after = ids[len(ids)-1]
	}
	return count, nil
}

// GetCopyCountAtDepth returns the count of nodes in src_nodes at the given depth with current copy_status.
// Optional nodeType filter. If breakAtFirst is true, returns 1 if any matching node exists or 0 otherwise.
func GetCopyCountAtDepth(database *db.DB, depth int, nodeType string, copyStatus string, breakAtFirst bool) (int64, error) {
	if database == nil || database.Ops() == nil {
		return 0, nil
	}
	if copyStatus == "" {
		copyStatus = db.CopyStatusPending
	}
	var count int64
	var err error
	if nodeType != "" {
		count, err = database.Ops().GetDepthStat(opsdb.SideSRC, depth, db.StatsKeyTyped(db.StatsKindCopy, copyStatus, nodeType))
	} else {
		count, err = sumTypedCountTotal(database, db.StatsKindCopy, copyStatus, "")
	}
	if err != nil {
		return 0, err
	}
	if breakAtFirst {
		if count > 0 {
			return 1, nil
		}
		return 0, nil
	}
	return count, nil
}

// GetDeleteCountAtDepth returns the count of nodes in src_nodes at the given depth with current delete_status.
func GetDeleteCountAtDepth(database *db.DB, depth int, nodeType string, deleteStatus string, breakAtFirst bool) (int64, error) {
	if database == nil || database.Ops() == nil {
		return 0, nil
	}
	if deleteStatus == "" {
		deleteStatus = db.DeleteStatusPendingExplicit
	}
	var count int64
	var err error
	if nodeType != "" {
		count, err = database.Ops().GetDepthStat(opsdb.SideSRC, depth, db.StatsKeyTyped(db.StatsKindDelete, deleteStatus, nodeType))
	} else {
		count, err = sumTypedCountTotal(database, db.StatsKindDelete, deleteStatus, "")
	}
	if err != nil {
		return 0, err
	}
	if breakAtFirst {
		if count > 0 {
			return 1, nil
		}
		return 0, nil
	}
	return count, nil
}

// GetMaxDepth returns the maximum depth present in Badger depth stats for the given table ("SRC" or "DST").
func GetMaxDepth(database *db.DB, table string) (int, error) {
	if database == nil || database.Ops() == nil {
		return 0, nil
	}
	return database.Ops().GetDepthMax(opsSide(table))
}

// GetStatsBreakdown returns (depth, key, count) from Badger depth stats grouped by depth and traversal_status. Order: depth, key.
func GetStatsBreakdown(database *db.DB, table string) ([]db.StatsRow, error) {
	if database == nil || database.Ops() == nil {
		return nil, nil
	}
	side := opsSide(table)
	rows, err := database.Ops().ListDepthStats()
	if err != nil {
		return nil, err
	}
	var out []db.StatsRow
	for _, row := range rows {
		if row.Side != side {
			continue
		}
		if statsKeyToTraversalStatus(row.Key) == "" {
			continue
		}
		if row.Count == 0 {
			continue
		}
		out = append(out, db.StatsRow{Depth: row.Depth, Key: row.Key, Count: row.Count})
	}
	if len(out) > 0 {
		return out, nil
	}
	maxDepth, err := GetMaxDepth(database, table)
	if err != nil {
		return nil, err
	}
	for d := 0; d <= maxDepth; d++ {
		for _, status := range []string{db.StatusPending, db.StatusSuccessful, db.StatusFailed, db.StatusNotOnSrc, db.StatusExcluded, db.StatusExclusionInherited} {
			if table != "DST" && status == db.StatusNotOnSrc {
				continue
			}
			key := db.StatsKey(db.StatsKindTraversal, status)
			n, err := database.Ops().GetDepthStat(side, d, key)
			if err != nil {
				return nil, err
			}
			if n == 0 {
				continue
			}
			out = append(out, db.StatsRow{Depth: d, Key: key, Count: n})
		}
	}
	return out, nil
}

// GetLatestQueueStats returns the most recent metrics JSON for queue_key and phase.
func GetLatestQueueStats(database *db.DB, queueKey, phase string) ([]byte, error) {
	if database == nil || database.Ops() == nil {
		return nil, nil
	}
	rec, err := database.Ops().LatestQueueStats(queueKey, phase)
	if err != nil {
		return nil, err
	}
	if rec == nil || rec.MetricsJSON == "" {
		return nil, nil
	}
	return []byte(rec.MetricsJSON), nil
}

// GetAllQueueStats returns the latest metrics JSON per API queue key (src-traversal, dst-traversal, copy, delete).
func GetAllQueueStats(database *db.DB) (map[string][]byte, error) {
	if database == nil || database.Ops() == nil {
		return map[string][]byte{}, nil
	}
	recs, err := database.Ops().AllLatestQueueStats()
	if err != nil {
		return nil, err
	}
	allStats := make(map[string][]byte)
	for _, rec := range recs {
		if rec.MetricsJSON == "" {
			continue
		}
		apiKey := rec.QueueKey
		switch rec.Phase {
		case db.QueueStatsPhaseTraversal:
			if rec.QueueKey == "copy" || rec.QueueKey == "delete" {
				continue
			}
		case db.QueueStatsPhaseCopy:
			if rec.QueueKey != "copy" {
				continue
			}
			apiKey = "copy"
		case db.QueueStatsPhaseDelete:
			if rec.QueueKey == "delete" || rec.QueueKey == "delete-traversal" {
				apiKey = "delete"
			} else {
				continue
			}
		default:
			continue
		}
		allStats[apiKey] = []byte(rec.MetricsJSON)
	}
	return allStats, nil
}

// GetPathReviewStatsFromOps rebuilds review counters from Badger depth/review stats.
func GetPathReviewStatsFromOps(database *db.DB) (db.ReviewStatsSnapshot, error) {
	srcT, err := traversalCountsFromDepthStats(database, "SRC")
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	dstT, err := traversalCountsFromDepthStats(database, "DST")
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	copyCounts, err := GetCopyStatusCountsFromEvents(database)
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	deleteCounts, err := GetEligibleDeleteStatusCounts(database)
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
		TraversalPending:    srcT.Pending + dstT.Pending,
		TraversalSuccessful: srcT.Successful + dstT.Successful,
		TraversalFailed:     srcT.Failed + dstT.Failed,
		CopyPending:         copyCounts.Pending,
		CopySuccessful:      copyCounts.Complete(),
		CopyFailed:          copyCounts.Failed,
		DeletePending:       deleteCounts.Pending,
		DeleteDeleted:       deleteCounts.Deleted,
		DeleteFailed:        deleteCounts.Failed,
		DeleteSkipped:       deleteCounts.Skipped,
		Excluded:            int64(merged.Excluded),
		Folders:             int64(merged.Folders),
		Files:               int64(merged.Files),
		SizeSrc:             merged.SizeSrc,
		SizeDst:             merged.SizeDst,
		SizeSelected:        merged.SizeSelected,
		SizeDeleteSelected:  deleteSelected,
	}, nil
}

// RebuildReviewStats recomputes review counters from live Badger depth stats and persists them.
func RebuildReviewStats(database *db.DB) (db.ReviewStatsSnapshot, error) {
	snap, err := GetPathReviewStatsFromOps(database)
	if err != nil {
		return db.ReviewStatsSnapshot{}, err
	}
	if database == nil {
		return snap, nil
	}
	if err := database.WriteReviewStatsSnapshot(snap); err != nil {
		return db.ReviewStatsSnapshot{}, fmt.Errorf("persist rebuilt review stats: %w", err)
	}
	return snap, nil
}
