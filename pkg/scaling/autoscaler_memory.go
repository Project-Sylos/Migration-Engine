// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

const (
	defaultSealRowThreshold    = 20_000
	maxSealRowThreshold        = 40_000
	defaultSealFlushInterval   = 10 * time.Second
	maxSealFlushInterval       = 10 * time.Second
	minSealRowThreshold        = 5_000
	minSealFlushInterval       = 2 * time.Second
)

func (a *Autoscaler) stepDownMemoryKnobs(now time.Time, pressure PressureClass) {
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		profile := a.profiles[name]
		minLease := profile.MinLeaseBatch
		if minLease <= 0 {
			minLease = 100
		}
		minRefill := profile.MinRefillBatch
		if minRefill <= 0 {
			minRefill = 500
		}

		curLease := q.EffectiveLeaseBatchSize()
		if next, ok := halveTowardMin(curLease, minLease); ok {
			q.SetLeaseBatchSize(next)
			a.emit(ScalingEvent{Queue: name, Knob: "LeaseBatchSize", OldValue: curLease, NewValue: next, Pressure: pressure, At: now})
		}

		curRefill := q.EffectiveRefillBatchSize()
		if next, ok := halveTowardMin(curRefill, minRefill); ok {
			q.SetRefillBatchSize(next)
			a.emit(ScalingEvent{Queue: name, Knob: "RefillBatchSize", OldValue: curRefill, NewValue: next, Pressure: pressure, At: now})
		}
	}

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
		a.emit(ScalingEvent{Queue: "seal", Knob: "SealRowThreshold", OldValue: oldRows, NewValue: newRows, Pressure: pressure, At: now})
	}
	if opts.FlushInterval > 0 {
		a.emit(ScalingEvent{
			Queue:    "seal",
			Knob:     "SealFlushIntervalMs",
			OldValue: int(oldFlush / time.Millisecond),
			NewValue: int(newFlush / time.Millisecond),
			Pressure: pressure,
			At:       now,
		})
	}
}

func (a *Autoscaler) stepUpMemoryKnobs(now time.Time, sample MemorySample) {
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		profile := a.profiles[name]
		minLease := profile.MinLeaseBatch
		if minLease <= 0 {
			minLease = 100
		}
		maxLease := profile.MaxLeaseBatch
		if maxLease <= 0 {
			maxLease = 10_000
		}
		minRefill := profile.MinRefillBatch
		if minRefill <= 0 {
			minRefill = 500
		}
		maxRefill := profile.MaxRefillBatch
		if maxRefill <= 0 {
			maxRefill = 10_000
		}

		curLease := q.EffectiveLeaseBatchSize()
		if next, ok := increaseTowardMax(curLease, minLease, maxLease, maxLease/10); ok {
			extra := EstimateLeaseIncrementKB(curLease, next)
			if MemoryBudgetAllowsIncrease(sample, extra) {
				q.SetLeaseBatchSize(next)
				a.emit(ScalingEvent{Queue: name, Knob: "LeaseBatchSize", OldValue: curLease, NewValue: next, Pressure: PressureNone, At: now})
			}
		}

		curRefill := q.EffectiveRefillBatchSize()
		if next, ok := increaseTowardMax(curRefill, minRefill, maxRefill, maxRefill/10); ok {
			extra := EstimateRefillIncrementKB(curRefill, next)
			if MemoryBudgetAllowsIncrease(sample, extra) {
				q.SetRefillBatchSize(next)
				a.emit(ScalingEvent{Queue: name, Knob: "RefillBatchSize", OldValue: curRefill, NewValue: next, Pressure: PressureNone, At: now})
			}
		}
	}

	if a.database == nil {
		return
	}
	curRows := a.sealRows
	if curRows <= 0 {
		curRows = defaultSealRowThreshold
	}
	if next, ok := increaseTowardMax(curRows, minSealRowThreshold, maxSealRowThreshold, 5000); ok {
		extra := EstimateSealRowIncrementKB(curRows, next)
		if MemoryBudgetAllowsIncrease(sample, extra) {
			a.sealRows = next
			a.database.UpdateSealBufferOptions(db.SealBufferOptions{RowThreshold: next})
			a.emit(ScalingEvent{Queue: "seal", Knob: "SealRowThreshold", OldValue: curRows, NewValue: next, Pressure: PressureNone, At: now})
		}
	}

	curFlush := time.Duration(a.sealFlushMs) * time.Millisecond
	if curFlush <= 0 {
		curFlush = defaultSealFlushInterval
	}
	if next, ok := increaseDurationTowardMax(curFlush, minSealFlushInterval, maxSealFlushInterval); ok {
		if MemoryBudgetAllowsIncrease(sample, 0) {
			a.sealFlushMs = int64(next / time.Millisecond)
			a.database.UpdateSealBufferOptions(db.SealBufferOptions{FlushInterval: next})
			a.emit(ScalingEvent{
				Queue:    "seal",
				Knob:     "SealFlushIntervalMs",
				OldValue: int(curFlush / time.Millisecond),
				NewValue: int(next / time.Millisecond),
				Pressure: PressureNone,
				At:       now,
			})
		}
	}
}

func increaseDurationTowardMax(cur, min, max time.Duration) (time.Duration, bool) {
	if cur >= max {
		return cur, false
	}
	next := cur * 2
	if next > max {
		next = max
	}
	if next <= cur {
		next = cur + time.Second
		if next > max {
			next = max
		}
	}
	if next <= cur {
		return cur, false
	}
	if next < min {
		next = min
	}
	return next, true
}

func (a *Autoscaler) sampleMemory() MemorySample {
	if a.memSampler != nil {
		return a.memSampler.Sample()
	}
	return DefaultMemorySampler.Sample()
}

func halveTowardMin(cur, min int) (int, bool) {
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

// SetMemorySampler replaces the memory sampler (tests).
func (a *Autoscaler) SetMemorySampler(s MemorySampler) {
	if a == nil {
		return
	}
	a.memSampler = s
}
