// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
)

const (
	sealAtMaxLookback       = 5
	sealAtMaxNeed           = 3
	sealProbeMinWindow      = 30 * time.Second
	sealCostImproveRatio    = 0.05
	sealStallImproveRatio   = 0.10
	sealCostFlatBand        = 0.05
	sealProbeMaxCooldown    = 10 * time.Minute
	defaultSealProbeBackoff = 30 * time.Second
)

// sealEfficiencyProbe tracks one at-max SealRowThreshold raise until accept/rollback.
type sealEfficiencyProbe struct {
	pending bool
	// windowStart is when the first flush after the raise was observed (probe clock).
	windowStart time.Time
	rowsBefore  int

	baselineCostNsPerRow float64
	baselineStallPerSec  float64
	hasBaseline          bool

	flushNs     int64
	flushRows   int64
	stallWaitNs int64
}

func (a *Autoscaler) sealRowCeiling() int {
	rowMax := a.effectiveKnobMax("seal", "SealRowThreshold", maxSealRowThreshold)
	if rowMax > maxSealRowThreshold {
		rowMax = maxSealRowThreshold
	}
	if rowMax < minSealRowThreshold {
		rowMax = minSealRowThreshold
	}
	return rowMax
}

func (a *Autoscaler) sealConsistentlyAtMax() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ring := a.sealRing
	if len(ring) == 0 {
		return false
	}
	n := sealAtMaxLookback
	if n > len(ring) {
		n = len(ring)
	}
	atMax := 0
	for _, p := range ring[len(ring)-n:] {
		if p.AtMax {
			atMax++
		}
	}
	need := sealAtMaxNeed
	if need > n {
		need = n
	}
	return atMax >= need
}

// maybeProbeSealRowThreshold raises the soft flush threshold when the buffer is
// consistently at max, memory is green, and no seal efficiency probe is pending.
func (a *Autoscaler) maybeProbeSealRowThreshold(now time.Time, sample memory.MemorySample) bool {
	if a == nil || a.database == nil {
		return false
	}
	if a.sealProbe.pending {
		return false
	}
	if !a.sealProbeCooldownElapsed(now) {
		return false
	}
	if !a.sealConsistentlyAtMax() {
		return false
	}
	curRows := a.sealRows
	if curRows <= 0 {
		curRows = defaultSealRowThreshold
	}
	rowMin := a.effectiveKnobMin("seal", "SealRowThreshold", minSealRowThreshold)
	rowMax := a.sealRowCeiling()
	next, ok := memory.IncreaseTowardMaxQuarter(curRows, rowMin, rowMax)
	if !ok {
		return false
	}
	extra := memory.EstimateBatchIncrementKB(memory.BatchIncrementSealRow, curRows, next)
	if !memory.MemoryBudgetAllowsIncrease(sample, extra) {
		return false
	}

	baselineCost, baselineStall, hasBase := a.sealProbeBaseline(now)
	a.sealRows = next
	a.database.UpdateSealBufferOptions(db.SealBufferOptions{
		RowThreshold: next,
		HardCap:      maxSealRowThreshold,
	})
	a.emit(scaling.ScalingEvent{
		Queue: "seal", Knob: "SealRowThreshold",
		OldValue: curRows, NewValue: next,
		Pressure: scaling.PressureNone, At: now,
	})

	a.sealProbe = sealEfficiencyProbe{
		pending:              true,
		rowsBefore:           curRows,
		baselineCostNsPerRow: baselineCost,
		baselineStallPerSec:  baselineStall,
		hasBaseline:          hasBase,
	}
	if hasBase {
		a.sealProbe.windowStart = now
	}
	a.sealProbeLastResult = "pending"
	return true
}

// sealProbeBaseline reads recent flush cost and last-tick hard-cap stall rate.
func (a *Autoscaler) sealProbeBaseline(now time.Time) (costNsPerRow, stallPerSec float64, ok bool) {
	a.mu.Lock()
	flushes := append([]SealFlushPoint(nil), a.sealFlushRing...)
	waitNs := a.lastSealHardCapWaitNs
	tickAt := a.lastSealTickAt
	a.mu.Unlock()

	var flushNs, flushRows int64
	for _, f := range flushes {
		if f.Rows <= 0 || f.DurationMs <= 0 {
			continue
		}
		flushNs += f.DurationMs * int64(time.Millisecond)
		flushRows += f.Rows
	}
	if flushRows <= 0 || flushNs <= 0 {
		return 0, 0, false
	}
	costNsPerRow = float64(flushNs) / float64(flushRows)
	tickInterval := a.interval
	if !tickAt.IsZero() && now.After(tickAt) {
		if d := now.Sub(tickAt); d > 0 {
			tickInterval = d
		}
	}
	if tickInterval <= 0 {
		tickInterval = time.Second
	}
	stallPerSec = float64(waitNs) / float64(tickInterval.Nanoseconds())
	return costNsPerRow, stallPerSec, true
}

func (a *Autoscaler) noteSealProbeTelemetry(seal db.SealBufferTelemetry, tickInterval time.Duration) {
	if a == nil || !a.sealProbe.pending {
		return
	}
	a.sealProbe.stallWaitNs += seal.HardCapWaitNsSinceLastPoll
	// Only soft-threshold / timer flushes; ignore forced sync drains.
	if seal.OrganicFlushCountSinceLastPoll <= 0 ||
		seal.LastOrganicFlushRows <= 0 ||
		seal.LastOrganicFlushDurationNs <= 0 {
		return
	}
	a.sealProbe.flushNs += seal.LastOrganicFlushDurationNs
	a.sealProbe.flushRows += seal.LastOrganicFlushRows
	if !a.sealProbe.hasBaseline {
		a.sealProbe.baselineCostNsPerRow = float64(seal.LastOrganicFlushDurationNs) / float64(seal.LastOrganicFlushRows)
		if tickInterval <= 0 {
			tickInterval = a.interval
		}
		if tickInterval <= 0 {
			tickInterval = time.Second
		}
		// Rough stall rate over this single tick as baseline when we had no history.
		a.sealProbe.baselineStallPerSec = float64(seal.HardCapWaitNsSinceLastPoll) / float64(tickInterval.Nanoseconds())
		a.sealProbe.hasBaseline = true
		a.sealProbe.windowStart = time.Now()
		// Reset accumulators so the evaluation window starts clean after baseline.
		a.sealProbe.flushNs = 0
		a.sealProbe.flushRows = 0
		a.sealProbe.stallWaitNs = 0
	}
}

// evaluateSealEfficiencyProbe accepts or rolls back after the probe window.
func (a *Autoscaler) evaluateSealEfficiencyProbe(now time.Time) {
	if a == nil || !a.sealProbe.pending || !a.sealProbe.hasBaseline {
		return
	}
	if a.sealProbe.windowStart.IsZero() || now.Sub(a.sealProbe.windowStart) < sealProbeMinWindow {
		return
	}
	windowNs := now.Sub(a.sealProbe.windowStart).Nanoseconds()
	if windowNs <= 0 {
		return
	}

	var costNow float64
	if a.sealProbe.flushRows > 0 && a.sealProbe.flushNs > 0 {
		costNow = float64(a.sealProbe.flushNs) / float64(a.sealProbe.flushRows)
	} else {
		// No flushes in window: cannot judge; abort without rollback or ratchet.
		a.abortSealEfficiencyProbe(now, "no_flush")
		return
	}
	stallNow := float64(a.sealProbe.stallWaitNs) / float64(windowNs)

	baseCost := a.sealProbe.baselineCostNsPerRow
	baseStall := a.sealProbe.baselineStallPerSec
	costImproved := baseCost > 0 && costNow <= baseCost*(1-sealCostImproveRatio)
	costFlat := baseCost <= 0 || (costNow >= baseCost*(1-sealCostFlatBand) && costNow <= baseCost*(1+sealCostFlatBand))
	stallImproved := stallNow <= baseStall*(1-sealStallImproveRatio) || (baseStall <= 0 && stallNow <= 0)

	accept := costImproved || (costFlat && stallImproved)
	rowsBefore := a.sealProbe.rowsBefore
	a.sealProbe = sealEfficiencyProbe{}
	if accept {
		a.sealProbeLastResult = "accepted"
		a.sealProbeBackoff = defaultSealProbeBackoff
		a.emit(scaling.ScalingEvent{
			Queue: "seal", Knob: "SealRowThresholdProbe",
			OldValue: rowsBefore, NewValue: a.sealRows,
			Pressure: scaling.PressureNone, At: now,
		})
		return
	}

	// Rollback
	old := a.sealRows
	a.sealRows = rowsBefore
	if a.database != nil {
		a.database.UpdateSealBufferOptions(db.SealBufferOptions{
			RowThreshold: rowsBefore,
			HardCap:      maxSealRowThreshold,
		})
	}
	a.sealProbeLastResult = "rolled_back"
	a.ratchetSealProbeBackoff(now)
	a.emit(scaling.ScalingEvent{
		Queue: "seal", Knob: "SealRowThreshold",
		OldValue: old, NewValue: rowsBefore,
		Pressure: scaling.PressureNone, At: now,
	})
}

func (a *Autoscaler) abortSealEfficiencyProbe(now time.Time, reason string) {
	if a == nil || !a.sealProbe.pending {
		return
	}
	a.sealProbe = sealEfficiencyProbe{}
	a.sealProbeLastResult = "aborted:" + reason
}

func (a *Autoscaler) sealProbeCooldownElapsed(now time.Time) bool {
	if a == nil || a.sealProbeCooldownUntil.IsZero() {
		return true
	}
	return !now.Before(a.sealProbeCooldownUntil)
}

func (a *Autoscaler) ratchetSealProbeBackoff(now time.Time) {
	if a.sealProbeBackoff <= 0 {
		a.sealProbeBackoff = defaultSealProbeBackoff
	} else {
		a.sealProbeBackoff *= 2
		if a.sealProbeBackoff > sealProbeMaxCooldown {
			a.sealProbeBackoff = sealProbeMaxCooldown
		}
	}
	until := now.Add(a.sealProbeBackoff)
	a.sealProbeCooldownUntil = until
	if until.After(a.memoryKnobCooldownUntil) {
		a.memoryKnobCooldownUntil = until
	}
}
