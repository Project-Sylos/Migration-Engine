// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"encoding/json"
	"testing"
)

func TestExternalQueueMetricsJSONPendingWorkers(t *testing.T) {
	m := ExternalQueueMetrics{
		QueueStats: queue.QueueStats{
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

func TestExternalQueueMetricsJSONCopyDeleteProgress(t *testing.T) {
	m := ExternalQueueMetrics{
		Folders:              120,
		Files:                800,
		Total:                920,
		Bytes:                640,
		FoldersExpected:      500,
		FilesExpected:        4200,
		TotalExpected:        4700,
		ItemsCompleted:       842,
		ItemsTotal:           1050,
		ItemsProgressPercent: 80.19,
		ProgressPercent:      80.19,
		BytesTotal:           1100,
		BytesProgressPercent: 58.18,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"items_completed", "items_total", "items_progress_percent", "progress_percent",
		"bytes_total", "bytes_progress_percent",
		"folders_expected", "files_expected", "total_expected",
	} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("missing json key %q in %s", key, string(b))
		}
	}
	if int(raw["items_completed"].(float64)) != 842 {
		t.Fatalf("items_completed=%v", raw["items_completed"])
	}
	if int(raw["bytes_total"].(float64)) != 1100 {
		t.Fatalf("bytes_total=%v", raw["bytes_total"])
	}
	if int(raw["folders_expected"].(float64)) != 500 {
		t.Fatalf("folders_expected=%v", raw["folders_expected"])
	}
}

