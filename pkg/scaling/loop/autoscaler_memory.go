// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/memweight"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
)

const (
	minDstPullChildMultiplier = queue.MinDstPullChildMultiplier
	maxDstPullChildMultiplier = queue.MaxDstPullChildMultiplier
)

const (
	// Seal autoscaling moves the soft flush threshold only. HardCap equals
	// maxSealRowThreshold (250k): absolute ceiling for overrides and the
	// producer block. Profile max stays at 50k so autoscaling does not grow past
	// the default without an operator Max override.
	defaultSealRowThreshold    = 50_000
	profileMaxSealRowThreshold = 50_000
	maxSealRowThreshold        = 250_000 // = HardCap; absolute clamp for overrides
	defaultSealFlushInterval   = 10 * time.Second
	maxSealFlushInterval       = 10 * time.Second
	minSealRowThreshold        = 5_000
	minSealFlushInterval       = 2 * time.Second
)

// stepDownMemoryKnobs sheds only the furthest-over-ceiling consumer (weight ledger),
// instead of halving every lease/refill/seal knob each tick.
func (a *Autoscaler) stepDownMemoryKnobs(now time.Time, pressure scaling.PressureClass) {
	if a == nil {
		return
	}
	target := memweight.Pending
	if a.weightLedger != nil {
		snap := a.weightLedger.FurthestOver()
		target = snap.Bucket
		if pressure == scaling.PressureSeal {
			all := a.weightLedger.Snapshots()
			sealBest := all[memweight.SealBuffer]
			if bucketPrefer(all[memweight.SealFlush], sealBest) {
				sealBest = all[memweight.SealFlush]
			}
			if sealBest.Level > 0 {
				target = sealBest.Bucket
			}
		}
	}

	switch target {
	case memweight.InFlight:
		a.halveLeaseBatches(now, pressure)
		if a.weightLedger != nil {
			a.weightLedger.DecayCeiling(memweight.InFlight, int64(a.minBatchAcrossQueues(true)))
		}
	case memweight.SealBuffer, memweight.SealFlush:
		// One consumer only; inflow into seal is cut on a later allowed tick
		// when pending/inFlight becomes furthest-over.
		a.halveSealKnobs(now, pressure)
		if a.weightLedger != nil {
			a.weightLedger.DecayCeiling(memweight.SealBuffer, int64(minSealRowThreshold))
			a.weightLedger.DecayCeiling(memweight.SealFlush, int64(minSealRowThreshold))
		}
	default: // pending: refill is derived from lease (queue task limit / 4)
		a.halveLeaseBatches(now, pressure)
		a.halveDstPullChildMult(now, pressure)
		if a.weightLedger != nil {
			a.weightLedger.DecayCeiling(memweight.Pending, int64(a.minBatchAcrossQueues(true)))
		}
	}
}

func bucketPrefer(candidate, current memweight.Snapshot) bool {
	return candidate.Over > current.Over || (candidate.Over == current.Over && candidate.Climb > current.Climb)
}

func (a *Autoscaler) pendingDominantAtFloors() bool {
	if a == nil || a.weightLedger == nil {
		return true
	}
	best := a.weightLedger.FurthestOver()
	if best.Bucket != memweight.Pending {
		return false
	}
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof := a.profiles[name]
		minLease := prof.MinLeaseBatch
		if minLease <= 0 {
			minLease = 100
		}
		if q.EffectiveLeaseBatchSize() > minLease {
			return false
		}
		if name == "dst" && q.EffectiveDstPullChildMultiplier() > minDstPullChildMultiplier {
			return false
		}
	}
	return true
}

func (a *Autoscaler) halveLeaseBatches(now time.Time, pressure scaling.PressureClass) {
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof := a.profiles[name]
		minLease := prof.MinLeaseBatch
		if minLease <= 0 {
			minLease = 100
		}
		curLease := q.EffectiveLeaseBatchSize()
		if next, ok := halveTowardMin(curLease, minLease); ok {
			q.SetLeaseBatchSize(next)
			a.emit(scaling.ScalingEvent{Queue: name, Knob: "LeaseBatchSize", OldValue: curLease, NewValue: next, Pressure: pressure, At: now})
		}
	}
}

func (a *Autoscaler) halveDstPullChildMult(now time.Time, pressure scaling.PressureClass) {
	q := a.queues["dst"]
	if q == nil {
		return
	}
	curMult := q.EffectiveDstPullChildMultiplier()
	if next, ok := halveTowardMin(curMult, minDstPullChildMultiplier); ok {
		q.SetDstPullChildMultiplier(next)
		a.emit(scaling.ScalingEvent{Queue: "dst", Knob: "DstPullChildMultiplier", OldValue: curMult, NewValue: next, Pressure: pressure, At: now})
	}
}

func (a *Autoscaler) halveSealKnobs(now time.Time, pressure scaling.PressureClass) {
	if a.database == nil {
		return
	}
	if a.sealRows <= 0 {
		a.sealRows = defaultSealRowThreshold
	}
	if a.sealFlushMs <= 0 {
		a.sealFlushMs = int64(defaultSealFlushInterval / time.Millisecond)
	}

	oldRows := a.sealRows
	newRows, _ := halveTowardMin(oldRows, minSealRowThreshold)
	oldFlush := time.Duration(a.sealFlushMs) * time.Millisecond
	newFlush, _ := halveDurationTowardMin(oldFlush, minSealFlushInterval)

	opts := db.SealBufferOptions{}
	changed := false
	if newRows < oldRows {
		opts.RowThreshold = newRows
		a.sealRows = newRows
		changed = true
	}
	if newFlush < oldFlush {
		opts.FlushInterval = newFlush
		a.sealFlushMs = int64(newFlush / time.Millisecond)
		changed = true
	}
	if !changed {
		return
	}
	a.database.UpdateSealBufferOptions(opts)
	if opts.RowThreshold > 0 {
		a.emit(scaling.ScalingEvent{Queue: "seal", Knob: "SealRowThreshold", OldValue: oldRows, NewValue: newRows, Pressure: pressure, At: now})
	}
	if opts.FlushInterval > 0 {
		a.emit(scaling.ScalingEvent{
			Queue:    "seal",
			Knob:     "SealFlushIntervalMs",
			OldValue: int(oldFlush / time.Millisecond),
			NewValue: int(newFlush / time.Millisecond),
			Pressure: pressure,
			At:       now,
		})
	}
}

// minBatchAcrossQueues returns the lowest profile min lease (lease=true) or refill (lease=false).
func (a *Autoscaler) minBatchAcrossQueues(lease bool) int {
	fallback := 500
	if lease {
		fallback = 100
	}
	min := 0
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof := a.profiles[name]
		m := prof.MinRefillBatch
		if lease {
			m = prof.MinLeaseBatch
		}
		if m <= 0 {
			m = fallback
		}
		if min == 0 || m < min {
			min = m
		}
	}
	if min == 0 {
		return fallback
	}
	return min
}

// stepUpMemoryKnobs raises a single furthest-under consumer per tick (mirrors
// step-down) so calm recovery does not snap every batch/seal knob in one go.
func (a *Autoscaler) stepUpMemoryKnobs(now time.Time, sample memory.MemorySample) {
	switch a.furthestUnderMemoryConsumer() {
	case memweight.InFlight:
		a.stepUpLeaseBatches(now, sample)
	case memweight.SealBuffer, memweight.SealFlush:
		a.stepUpSealKnobs(now, sample)
	default: // pending: raise queue task limit (refill is lease/4)
		a.stepUpLeaseBatches(now, sample)
		a.stepUpDstPullChildMult(now, sample)
	}
}

// furthestUnderMemoryConsumer picks the consumer with the most knob headroom
// below its max (fractional). Ties prefer pending → inFlight → seal.
func (a *Autoscaler) furthestUnderMemoryConsumer() memweight.Bucket {
	leaseHead, refillHead, sealHead := a.memoryKnobHeadroom()
	best := memweight.Pending
	bestHead := refillHead
	if leaseHead > bestHead {
		best = memweight.InFlight
		bestHead = leaseHead
	}
	if sealHead > bestHead {
		best = memweight.SealBuffer
	}
	return best
}

func (a *Autoscaler) memoryKnobHeadroom() (leaseHead, refillHead, sealHead float64) {
	leaseN, refillN := 0, 0
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof := a.profiles[name]
		maxLease := prof.MaxLeaseBatch
		if maxLease <= 0 {
			maxLease = 10_000
		}
		maxLease = a.effectiveKnobMax(name, "LeaseBatchSize", maxLease)
		curLease := q.EffectiveLeaseBatchSize()
		if curLease < maxLease {
			h := 1 - float64(curLease)/float64(maxLease)
			leaseHead += h
			refillHead += h // pending path shares queue-task headroom
			leaseN++
			refillN++
		}
		if name == "dst" {
			curMult := q.EffectiveDstPullChildMultiplier()
			if curMult < maxDstPullChildMultiplier {
				refillHead += 1 - float64(curMult)/float64(maxDstPullChildMultiplier)
				refillN++
			}
		}
	}
	if leaseN > 0 {
		leaseHead /= float64(leaseN)
	}
	if refillN > 0 {
		refillHead /= float64(refillN)
	}
	curRows := a.sealRows
	if curRows <= 0 {
		curRows = defaultSealRowThreshold
	}
	rowCeil := a.effectiveKnobMax("seal", "SealRowThreshold", profileMaxSealRowThreshold)
	if a.database != nil && curRows < rowCeil {
		sealHead = 1 - float64(curRows)/float64(rowCeil)
	}
	curFlush := time.Duration(a.sealFlushMs) * time.Millisecond
	if curFlush <= 0 {
		curFlush = defaultSealFlushInterval
	}
	if a.database != nil && curFlush < maxSealFlushInterval {
		flushHead := 1 - float64(curFlush)/float64(maxSealFlushInterval)
		if flushHead > sealHead {
			sealHead = flushHead
		}
	}
	return leaseHead, refillHead, sealHead
}

func (a *Autoscaler) stepUpLeaseBatches(now time.Time, sample memory.MemorySample) {
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof := a.profiles[name]
		minLease := prof.MinLeaseBatch
		if minLease <= 0 {
			minLease = 100
		}
		maxLease := prof.MaxLeaseBatch
		if maxLease <= 0 {
			maxLease = 10_000
		}
		maxLease = a.effectiveKnobMax(name, "LeaseBatchSize", maxLease)
		minLease = a.effectiveKnobMin(name, "LeaseBatchSize", minLease)
		curLease := q.EffectiveLeaseBatchSize()
		next, ok := memory.IncreaseTowardMaxQuarter(curLease, minLease, maxLease)
		if !ok {
			continue
		}
		extra := memory.EstimateBatchIncrementKB(memory.BatchIncrementLease, curLease, next)
		if !memory.MemoryBudgetAllowsIncrease(sample, extra) {
			continue
		}
		q.SetLeaseBatchSize(next)
		a.emit(scaling.ScalingEvent{Queue: name, Knob: "LeaseBatchSize", OldValue: curLease, NewValue: next, Pressure: scaling.PressureNone, At: now})
	}
}

func (a *Autoscaler) stepUpDstPullChildMult(now time.Time, sample memory.MemorySample) {
	q := a.queues["dst"]
	if q == nil {
		return
	}
	curMult := q.EffectiveDstPullChildMultiplier()
	next, ok := memory.IncreaseTowardMaxQuarter(curMult, minDstPullChildMultiplier, maxDstPullChildMultiplier)
	if !ok {
		return
	}
	childQuota := pull.DstPullChildQuota(q.EffectiveRefillBatchSize(), next)
	extra := memory.EstimateDstChildQuotaKB(childQuota)
	if !memory.MemoryBudgetAllowsIncrease(sample, extra) {
		return
	}
	q.SetDstPullChildMultiplier(next)
	a.emit(scaling.ScalingEvent{Queue: "dst", Knob: "DstPullChildMultiplier", OldValue: curMult, NewValue: next, Pressure: scaling.PressureNone, At: now})
}

// stepUpSealKnobs recovers flush-interval only. Soft RowThreshold growth is
// at-max + efficiency probe via maybeProbeSealRowThreshold (avoids double raise).
func (a *Autoscaler) stepUpSealKnobs(now time.Time, sample memory.MemorySample) {
	if a.database == nil {
		return
	}
	curFlush := time.Duration(a.sealFlushMs) * time.Millisecond
	if curFlush <= 0 {
		curFlush = defaultSealFlushInterval
	}
	next, ok := increaseDurationTowardMaxQuarter(curFlush, minSealFlushInterval, maxSealFlushInterval)
	if !ok || !memory.MemoryBudgetAllowsIncrease(sample, 0) {
		return
	}
	a.sealFlushMs = int64(next / time.Millisecond)
	a.database.UpdateSealBufferOptions(db.SealBufferOptions{FlushInterval: next})
	a.emit(scaling.ScalingEvent{
		Queue:    "seal",
		Knob:     "SealFlushIntervalMs",
		OldValue: int(curFlush / time.Millisecond),
		NewValue: int(next / time.Millisecond),
		Pressure: scaling.PressureNone,
		At:       now,
	})
}

func increaseDurationTowardMaxQuarter(cur, min, max time.Duration) (time.Duration, bool) {
	if cur >= max {
		return cur, false
	}
	step := cur / 4
	if step < time.Second {
		step = time.Second
	}
	next := cur + step
	if next > max {
		next = max
	}
	if next < min {
		next = min
	}
	if next <= cur {
		return cur, false
	}
	return next, true
}

func (a *Autoscaler) sampleMemory() memory.MemorySample {
	if a.memSampler != nil {
		return a.memSampler.Sample()
	}
	return memory.DefaultMemorySampler.Sample()
}

func halveTowardMin[T ~int | ~int64](cur, min T) (T, bool) {
	if cur <= min {
		return cur, false
	}
	next := cur / 2
	if next < min {
		next = min
	}
	if next >= cur {
		return cur, false
	}
	return next, true
}

func halveDurationTowardMin(cur, min time.Duration) (time.Duration, bool) {
	n, ok := halveTowardMin(int64(cur), int64(min))
	return time.Duration(n), ok
}

// SetMemorySampler replaces the memory sampler (tests).
func (a *Autoscaler) SetMemorySampler(s memory.MemorySampler) {
	if a == nil {
		return
	}
	a.memSampler = s
}

// SetWeightLedger attaches the shared memory-weight ledger (tests / migration wire).
func (a *Autoscaler) SetWeightLedger(l *memweight.Ledger) {
	if a == nil {
		return
	}
	a.weightLedger = l
}

// WeightLedger returns the attached ledger (may be nil).
func (a *Autoscaler) WeightLedger() *memweight.Ledger {
	if a == nil {
		return nil
	}
	return a.weightLedger
}
