// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"testing"
)

func TestExternalQueueMetricsJSONPendingWorkers(t *testing.T) {
	m := ExternalQueueMetrics{
		QueueStats: QueueStats{
			Name:       "src",
			Round:      5,
			Pending:    136,
			InProgress: 2,
			Workers:    8,
		},
		Round:          5,
		RoundExpected:  652,
		RoundCompleted: 356,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"pending", "workers", "in_progress", "round"} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("missing json key %q in %s", key, string(b))
		}
	}
	if int(raw["pending"].(float64)) != 136 {
		t.Fatalf("pending=%v want 136", raw["pending"])
	}
	if int(raw["workers"].(float64)) != 8 {
		t.Fatalf("workers=%v want 8", raw["workers"])
	}
}
