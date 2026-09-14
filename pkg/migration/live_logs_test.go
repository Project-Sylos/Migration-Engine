// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"
	"time"
)

func TestGetRecentLogs_liveUsesMemoryOnly(t *testing.T) {
	m := &Migration{running: true, logRing: newLogRing(8)}
	m.logRing.add(LogEntry{ID: "phase-1", Level: "info", Message: "phase transitioned", Timestamp: time.Unix(1, 0)})
	got, err := m.GetRecentLogs(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "phase-1" {
		t.Fatalf("got %+v", got)
	}
}
