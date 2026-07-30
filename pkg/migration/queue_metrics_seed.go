// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
)

func seedQueueCountersFromDB(database *db.DB, q *queue.Queue, queueKey, phase string) {
	if database == nil || q == nil {
		return
	}
	blob, err := stats.GetLatestQueueStats(database, queueKey, phase)
	if err == nil && len(blob) > 0 {
		_ = observe.RehydrateCountersFromMetricsJSON(q, blob)
	}
	// Prefer live failed-byte sum from nodes when metrics JSON predates bytes_failed.
	if q.GetBytesFailedTotal() > 0 {
		return
	}
	kind := db.StatsKindCopy
	if queueKey == "delete" {
		kind = db.StatsKindDelete
	}
	if failedBytes, err := stats.GetFailedWorkFileSize(database, kind); err == nil && failedBytes > 0 {
		q.RecordFailedBytes(failedBytes)
	}
}

// seedQueueWorkTotalsFromDB loads immutable phase denominators once during setup.
// The observer then serves all live progress from queue memory.
func seedQueueWorkTotalsFromDB(database *db.DB, q *queue.Queue, queueName string) {
	if database == nil || q == nil {
		return
	}
	var totals db.SealedWorkTotals
	var err error
	switch queueName {
	case "copy":
		totals, err = stats.ReadSealedWorkTotals(database, db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
	case "delete":
		totals, err = stats.ReadSealedWorkTotals(database, db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen)
	default:
		return
	}
	if err != nil {
		return
	}
	q.SetWorkTotals(queue.WorkTotals{
		Folders: totals.Folders,
		Files:   totals.Files,
		Bytes:   totals.Bytes,
	})
}
