// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "encoding/json"

// RehydrateCountersFromMetricsJSON seeds monotonic discovery/copy counters from persisted metrics JSON.
func RehydrateCountersFromMetricsJSON(q *Queue, metricsJSON []byte) error {
	if q == nil || len(metricsJSON) == 0 {
		return nil
	}
	var metrics ExternalQueueMetrics
	if err := json.Unmarshal(metricsJSON, &metrics); err != nil {
		return err
	}
	switch q.name {
	case "src", "dst":
		q.SeedDiscoveryCounters(metrics.FilesDiscoveredTotal, metrics.FoldersDiscoveredTotal)
	case "copy", "delete":
		q.SeedCopyCounters(metrics.Folders, metrics.Files, metrics.Bytes)
	}
	return nil
}
