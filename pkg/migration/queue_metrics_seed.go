// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func seedQueueCountersFromDB(database *db.DB, q *queue.Queue, queueKey, phase string) {
	if database == nil || q == nil {
		return
	}
	blob, err := database.GetLatestQueueStats(queueKey, phase)
	if err != nil || len(blob) == 0 {
		return
	}
	_ = queue.RehydrateCountersFromMetricsJSON(q, blob)
}
