// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"testing"
	"time"
)

func TestRecordWaitBudgetWallTotalsAndEMA(t *testing.T) {
	a := &Autoscaler{interval: time.Second}
	// Wall FS 2s; bp wall 1s; db work wall 300ms; breakdown pieces for display
	a.applyWaitBudgetDeltas(time.Second, 2e9, 1e9, 3e8, 1e8, 9e8, 2e8, 5e7, 5e7)

	view := a.buildWaitBudgetViewLocked()
	if view == nil {
		t.Fatal("expected wait budget view")
	}
	if view.FSTotalMs != 2000 {
		t.Fatalf("FS total=%d want 2000", view.FSTotalMs)
	}
	if view.BackpressureTotalMs != 1000 {
		t.Fatalf("backpressure total=%d want 1000", view.BackpressureTotalMs)
	}
	if view.DBWorkTotalMs != 300 {
		t.Fatalf("DB work total=%d want 300", view.DBWorkTotalMs)
	}
	if view.FSEMAMsPerSec < 1900 || view.FSEMAMsPerSec > 2100 {
		t.Fatalf("FS EMA=%v want ~2000 ms/s", view.FSEMAMsPerSec)
	}

	a.applyWaitBudgetDeltas(time.Second, 0, 0, 0, 0, 0, 0, 0, 0)
	view = a.buildWaitBudgetViewLocked()
	if view.FSTotalMs != 2000 {
		t.Fatalf("FS total should stick: %d", view.FSTotalMs)
	}
	if view.FSEMAMsPerSec >= 2000 {
		t.Fatalf("FS EMA should decay, got %v", view.FSEMAMsPerSec)
	}
}
