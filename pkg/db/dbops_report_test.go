// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestSummarizeDBOpRecords(t *testing.T) {
	recs := []opsdb.DBOpRecord{
		{Op: "badger_sync", DurationNs: int64(10 * time.Millisecond), Rows: 100, At: time.Now()},
		{Op: "badger_sync", DurationNs: int64(30 * time.Millisecond), Rows: 200, At: time.Now()},
		{Op: "badger_pull", DurationNs: int64(5 * time.Millisecond), Rows: 50, At: time.Now()},
	}
	sum := SummarizeDBOpRecords(recs)
	if len(sum) != 2 {
		t.Fatalf("len=%d want 2", len(sum))
	}
	if sum[0].Op != "badger_sync" || sum[0].Count != 2 || sum[0].TotalRows != 300 {
		t.Fatalf("badger_sync agg: %+v", sum[0])
	}
	if sum[0].AvgDurationNs != int64(20*time.Millisecond) {
		t.Fatalf("avg=%d want %d", sum[0].AvgDurationNs, 20*time.Millisecond)
	}
}
