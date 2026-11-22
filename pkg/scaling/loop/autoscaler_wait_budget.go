// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"time"
)

const waitBudgetEMAAlpha = 0.2

// WaitBudgetView is operator-facing FS vs DB wait time (totals + recent EMA rate).
// Totals are overlap wall-clock: time when at least one waiter was busy in that
// category (parallel workers overlapping the same second count once). EMAMsPerSec
// is milliseconds of that wall busy time per wall-clock second (at most ~1000).
type WaitBudgetView struct {
	FSTotalMs     int64   `json:"fsTotalMs"`
	FSEMAMsPerSec float64 `json:"fsEmaMsPerSec"`

	// Backpressure is wall time with hard-cap waiters and/or a forced flush active.
	BackpressureTotalMs     int64   `json:"backpressureTotalMs"`
	BackpressureEMAMsPerSec float64 `json:"backpressureEmaMsPerSec"`

	// DBWork is wall time with organic flush and/or frontier pull (incl. gate) active.
	DBWorkTotalMs     int64   `json:"dbWorkTotalMs"`
	DBWorkEMAMsPerSec float64 `json:"dbWorkEmaMsPerSec"`

	// DBTotalMs is Backpressure + DBWork (may overlap slightly across categories).
	DBTotalMs     int64   `json:"dbTotalMs"`
	DBEMAMsPerSec float64 `json:"dbEmaMsPerSec"`

	DBHardCapWaitMs  int64 `json:"dbHardCapWaitMs,omitempty"`
	DBForcedFlushMs  int64 `json:"dbForcedFlushMs,omitempty"`
	DBOrganicFlushMs int64 `json:"dbOrganicFlushMs,omitempty"`
	DBFlushMs        int64 `json:"dbFlushMs,omitempty"`
	DBPullMs         int64 `json:"dbPullMs,omitempty"`
	DBPullGateWaitMs int64 `json:"dbPullGateWaitMs,omitempty"`
}

type waitBudgetPullSource interface {
	TakeDBPullDurationNs() int64
}

type waitBudgetAcc struct {
	fsTotalNs         int64
	bpTotalNs         int64
	dbWorkTotalNs     int64
	hardCapNs         int64
	forcedFlushNs     int64
	organicFlushNs    int64
	pullNs            int64
	pullGateNs        int64
	fsEMAMsPerSec     float64
	backpressureEMA   float64
	dbWorkEMAMsPerSec float64
	dbEMAMsPerSec     float64
	hasEMA            bool
}

func (a *Autoscaler) recordWaitBudget(tickInterval time.Duration, forcedFlushNs, organicFlushNs int64) {
	if a == nil {
		return
	}
	var fs, bp, hardCap, dbWork, pullGate int64
	if a.database != nil {
		fs, bp, hardCap, dbWork, pullGate = a.database.TakeBottleneckWallNs()
	}
	var pullDelta int64
	for _, q := range a.queues {
		s, ok := q.(waitBudgetPullSource)
		if !ok || s == nil {
			continue
		}
		pullDelta += s.TakeDBPullDurationNs()
	}
	a.applyWaitBudgetDeltas(tickInterval, fs, bp, dbWork, hardCap, forcedFlushNs, organicFlushNs, pullDelta, pullGate)
}

func (a *Autoscaler) applyWaitBudgetDeltas(tickInterval time.Duration, fsWall, bpWall, dbWorkWall, hardCapWall, forcedFlushNs, organicFlushNs, pullNs, pullGateWall int64) {
	if a == nil {
		return
	}
	clamp0 := func(v int64) int64 {
		if v < 0 {
			return 0
		}
		return v
	}
	fsWall = clamp0(fsWall)
	bpWall = clamp0(bpWall)
	dbWorkWall = clamp0(dbWorkWall)
	hardCapWall = clamp0(hardCapWall)
	forcedFlushNs = clamp0(forcedFlushNs)
	organicFlushNs = clamp0(organicFlushNs)
	pullNs = clamp0(pullNs)
	pullGateWall = clamp0(pullGateWall)

	dbDelta := bpWall + dbWorkWall

	tickSec := tickInterval.Seconds()
	if tickSec <= 0 {
		tickSec = a.interval.Seconds()
	}
	if tickSec <= 0 {
		tickSec = 1
	}

	fsRate := float64(fsWall) / 1e6 / tickSec
	bpRate := float64(bpWall) / 1e6 / tickSec
	workRate := float64(dbWorkWall) / 1e6 / tickSec
	dbRate := float64(dbDelta) / 1e6 / tickSec

	a.mu.Lock()
	defer a.mu.Unlock()
	wb := &a.waitBudget
	wb.fsTotalNs += fsWall
	wb.bpTotalNs += bpWall
	wb.dbWorkTotalNs += dbWorkWall
	wb.hardCapNs += hardCapWall
	wb.forcedFlushNs += forcedFlushNs
	wb.organicFlushNs += organicFlushNs
	wb.pullNs += pullNs
	wb.pullGateNs += pullGateWall
	if !wb.hasEMA {
		wb.fsEMAMsPerSec = fsRate
		wb.backpressureEMA = bpRate
		wb.dbWorkEMAMsPerSec = workRate
		wb.dbEMAMsPerSec = dbRate
		wb.hasEMA = true
		return
	}
	wb.fsEMAMsPerSec = waitBudgetEMAAlpha*fsRate + (1-waitBudgetEMAAlpha)*wb.fsEMAMsPerSec
	wb.backpressureEMA = waitBudgetEMAAlpha*bpRate + (1-waitBudgetEMAAlpha)*wb.backpressureEMA
	wb.dbWorkEMAMsPerSec = waitBudgetEMAAlpha*workRate + (1-waitBudgetEMAAlpha)*wb.dbWorkEMAMsPerSec
	wb.dbEMAMsPerSec = waitBudgetEMAAlpha*dbRate + (1-waitBudgetEMAAlpha)*wb.dbEMAMsPerSec
}

func (a *Autoscaler) buildWaitBudgetViewLocked() *WaitBudgetView {
	if a == nil {
		return nil
	}
	wb := a.waitBudget
	dbTotalNs := wb.bpTotalNs + wb.dbWorkTotalNs
	if wb.fsTotalNs <= 0 && dbTotalNs <= 0 && !wb.hasEMA {
		return nil
	}
	return &WaitBudgetView{
		FSTotalMs:               wb.fsTotalNs / int64(time.Millisecond),
		FSEMAMsPerSec:           wb.fsEMAMsPerSec,
		BackpressureTotalMs:     wb.bpTotalNs / int64(time.Millisecond),
		BackpressureEMAMsPerSec: wb.backpressureEMA,
		DBWorkTotalMs:           wb.dbWorkTotalNs / int64(time.Millisecond),
		DBWorkEMAMsPerSec:       wb.dbWorkEMAMsPerSec,
		DBTotalMs:               dbTotalNs / int64(time.Millisecond),
		DBEMAMsPerSec:           wb.dbEMAMsPerSec,
		DBHardCapWaitMs:         wb.hardCapNs / int64(time.Millisecond),
		DBForcedFlushMs:         wb.forcedFlushNs / int64(time.Millisecond),
		DBOrganicFlushMs:        wb.organicFlushNs / int64(time.Millisecond),
		DBFlushMs:               (wb.organicFlushNs + wb.forcedFlushNs) / int64(time.Millisecond),
		DBPullMs:                wb.pullNs / int64(time.Millisecond),
		DBPullGateWaitMs:        wb.pullGateNs / int64(time.Millisecond),
	}
}
