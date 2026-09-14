// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/aimd"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/backend"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

type scaleUpTrigger int

const (
	scaleUpUnderfeed scaleUpTrigger = iota
	scaleUpCalmProbe
)

func (a *Autoscaler) stepUpWorkers(internal map[string]observe.InternalMetricsSnapshot, inProgress, pending map[string]int, trigger scaleUpTrigger) {
	now := time.Now()
	handled := make(map[string]bool)
	if a.registry != nil {
		for groupID, queues := range a.registry.QueuesByGroup() {
			if len(queues) < 2 {
				continue
			}
			var reason string
			var try bool
			if trigger == scaleUpUnderfeed && a.groupWantsScaleUp(queues, internal, pending) {
				try = true
				for _, name := range queues {
					if queueUnderfeed(name, internal, pending) {
						snap := internal[name]
						reason = fmt.Sprintf("UNDERFEED wait=%s in_progress=%d pending=%d", snap.TimeWaitingOnQueue.Round(time.Millisecond), inProgress[name], pending[name])
						break
					}
				}
			} else if trigger == scaleUpCalmProbe && a.groupCanCalmProbe(groupID, queues, internal, now) {
				try = true
				reason = "calm probe (pressure=NONE)"
			}
			if try {
				a.stepUpSharedGroup(groupID, queues, internal, reason, now)
			}
			for _, name := range queues {
				handled[name] = true
			}
		}
	}
	for name, q := range a.queues {
		if handled[name] || q == nil {
			continue
		}
		var reason string
		var try bool
		if trigger == scaleUpUnderfeed && queueUnderfeed(name, internal, pending) {
			try = true
			snap := internal[name]
			reason = fmt.Sprintf("UNDERFEED wait=%s in_progress=%d pending=%d", snap.TimeWaitingOnQueue.Round(time.Millisecond), inProgress[name], pending[name])
		} else if trigger == scaleUpCalmProbe && a.queueCanCalmProbe(name, q, internal, now) {
			try = true
			reason = "calm probe (pressure=NONE)"
		}
		if !try {
			continue
		}
		a.stepUpIndependentQueue(name, q, internal, reason, now)
	}
	a.mu.Lock()
	a.lastActuate = now
	a.mu.Unlock()
}

func (a *Autoscaler) groupCanCalmProbe(groupID string, queues []string, internal map[string]observe.InternalMetricsSnapshot, now time.Time) bool {
	st := a.queueState(backend.GroupAIMDKey(groupID))
	rateLimitedUntil := aimd.MaxRateLimitedUntilForQueues(internal, queues)
	if aimd.ScaleUpBlockedByRateLimit(now, rateLimitedUntil) {
		return false
	}
	if aimd.FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		return false
	}
	minPer := 1
	if p, ok := a.profiles[queues[0]]; ok && p.MinWorkers > 0 {
		minPer = p.MinWorkers
	}
	totalCur := 0
	maxInterOp := time.Duration(0)
	for _, name := range queues {
		q := a.queues[name]
		if q == nil {
			continue
		}
		totalCur += q.GetWorkerCount()
		if d := q.GetInterOpDelay(); d > maxInterOp {
			maxInterOp = d
		}
	}
	if maxInterOp > 0 {
		return aimd.InterOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	maxTotal := a.registry.GroupMaxWorkers(groupID)
	if maxTotal <= 0 {
		maxTotal = totalCur + 1
	}
	if totalCur >= maxTotal {
		return false
	}
	_, ok := a.aimd.IncreaseTarget(totalCur, minPer*len(queues), maxTotal, st, now)
	return ok
}

func (a *Autoscaler) queueCanCalmProbe(name string, q scaling.QueueActuator, internal map[string]observe.InternalMetricsSnapshot, now time.Time) bool {
	st := a.queueState(name)
	rateLimitedUntil := aimd.MaxRateLimitedUntil(internal, name)
	if aimd.ScaleUpBlockedByRateLimit(now, rateLimitedUntil) {
		return false
	}
	if aimd.FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		return false
	}
	if q.GetInterOpDelay() > 0 {
		return aimd.InterOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	prof := a.profiles[name]
	maxWorkers := prof.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = profile.UnboundedMaxWorkers
	}
	cur := q.GetWorkerCount()
	if cur >= maxWorkers {
		return false
	}
	minWorkers := prof.MinWorkers
	if minWorkers <= 0 {
		minWorkers = 1
	}
	_, ok := a.aimd.IncreaseTarget(cur, minWorkers, maxWorkers, st, now)
	return ok
}

func (a *Autoscaler) stepDownWorkers(internal map[string]observe.InternalMetricsSnapshot) {
	now := time.Now()
	handled := make(map[string]bool)
	if a.registry != nil {
		for groupID, queues := range a.registry.QueuesByGroup() {
			if len(queues) < 2 {
				continue
			}
			a.stepDownSharedGroup(groupID, queues, internal, now)
			for _, name := range queues {
				handled[name] = true
			}
		}
	}
	for name, q := range a.queues {
		if handled[name] || q == nil {
			continue
		}
		a.stepDownIndependentQueue(name, q, internal, now)
	}
	a.mu.Lock()
	a.lastActuate = now
	a.mu.Unlock()
}

func (a *Autoscaler) stepDownSharedGroup(groupID string, queues []string, internal map[string]observe.InternalMetricsSnapshot, now time.Time) {
	minPer := 1
	if p, ok := a.profiles[queues[0]]; ok && p.MinWorkers > 0 {
		minPer = p.MinWorkers
	}
	totalCur := 0
	for _, name := range queues {
		if q := a.queues[name]; q != nil {
			totalCur += q.GetWorkerCount()
		}
	}
	maxTotal := a.registry.GroupMaxWorkers(groupID)
	if maxTotal <= 0 {
		maxTotal = totalCur
	}
	minTotal := minPer * len(queues)
	st := a.queueState(backend.GroupAIMDKey(groupID))
	rateLimitedUntil := aimd.MaxRateLimitedUntilForQueues(internal, queues)
	if a.lastClass == scaling.PressureFSThrottle && aimd.FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked("group:"+groupID, st, now, rateLimitedUntil)
		return
	}
	var targetTotal int
	if a.lastClass == scaling.PressureFSThrottle {
		targetTotal = aimd.SoftCapDecreaseTarget(totalCur, minTotal, maxTotal, st, now)
	} else {
		targetTotal = a.aimd.DecreaseTarget(totalCur, minTotal, maxTotal, st, now)
	}
	if targetTotal < totalCur {
		cause := "PRESSURE"
		switch a.lastClass {
		case scaling.PressureFSThrottle:
			cause = "FS_THROTTLE"
			a.noteSoftCapThrottle(st, now, rateLimitedUntil)
			a.abortGroupEfficiencyProbeOnPressure(st, targetTotal, now)
			st.NoteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
		case scaling.PressureMemory:
			cause = "MEMORY_PRESSURE"
		}
		fmt.Printf("  SCALE_DOWN cause=%s scope=group:%s workers %d->%d (pressure=%s)\n",
			cause, groupID, totalCur, targetTotal, a.lastClass)
		a.debugAIMDPrint(fmt.Sprintf("  aimd decrease [group:%s]: total workers %d->%d", groupID, totalCur, targetTotal))
		if a.debugAIMD {
			a.debugAIMDPrint(aimd.FormatProbeCooldownLine("group:"+groupID, st, a.aimd.ProbeCooldown, now))
		}
		split := backend.SplitWorkersTotal(targetTotal, queues, minPer)
		for name, target := range split {
			q := a.queues[name]
			if q == nil {
				continue
			}
			cur := q.GetWorkerCount()
			if target == cur {
				continue
			}
			if err := q.SetTargetWorkerCount(target); err != nil {
				continue
			}
			if a.debugAIMD {
				a.debugAIMDPrint(fmt.Sprintf("  aimd decrease [%s]: workers %d->%d (group split)", name, cur, target))
			}
			a.emit(scaling.ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
			if target > minPer {
				a.clearInterOpDelay(name, q, st, now)
			}
			a.maybeIncreaseListPage(name, q, a.profiles[name], now)
		}
		return
	}
	if a.lastClass == scaling.PressureFSThrottle && aimd.FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked("group:"+groupID, st, now, rateLimitedUntil)
		return
	}
	for _, name := range queues {
		q := a.queues[name]
		if q == nil {
			continue
		}
		a.stepDownSharedInterOpDelay(groupID, queues, internal, now)
		break
	}
}

func (a *Autoscaler) stepDownSharedInterOpDelay(groupID string, queues []string, internal map[string]observe.InternalMetricsSnapshot, now time.Time) {
	st := a.queueState(backend.GroupAIMDKey(groupID))
	rateLimitedUntil := aimd.MaxRateLimitedUntilForQueues(internal, queues)
	if !aimd.InterOpIncreaseAllowed(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked("group:"+groupID, st, now, rateLimitedUntil)
		return
	}
	prof := a.profiles[queues[0]]
	maxDelay := prof.MaxInterOpDelay
	if maxDelay <= 0 {
		maxDelay = profile.DefaultMaxInterOpDelay
	}
	cur := time.Duration(0)
	for _, name := range queues {
		if q := a.queues[name]; q != nil {
			if d := q.GetInterOpDelay(); d > cur {
				cur = d
			}
		}
	}
	target := a.aimd.IncreaseInterOpDelay(cur, maxDelay, a.maxInterOpSeedRate(queues), st, now)
	if target <= cur {
		return
	}
	if a.lastClass == scaling.PressureFSThrottle {
		a.noteSoftCapThrottle(st, now, rateLimitedUntil)
		st.NoteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	for _, name := range queues {
		q := a.queues[name]
		if q == nil {
			continue
		}
		old := q.GetInterOpDelay()
		q.SetInterOpDelay(target)
		a.emit(scaling.ScalingEvent{
			Queue:    name,
			Knob:     "InterOpDelayMs",
			OldValue: int(old.Microseconds()),
			NewValue: int(target.Microseconds()),
			Pressure: a.lastClass,
			At:       now,
		})
	}
}

func (a *Autoscaler) stepDownIndependentQueue(name string, q scaling.QueueActuator, internal map[string]observe.InternalMetricsSnapshot, now time.Time) {
	prof := a.profiles[name]
	cur := q.GetWorkerCount()
	st := a.queueState(name)
	rateLimitedUntil := aimd.MaxRateLimitedUntil(internal, name)
	if a.lastClass == scaling.PressureFSThrottle && aimd.FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked(name, st, now, rateLimitedUntil)
		return
	}
	var target int
	if a.lastClass == scaling.PressureFSThrottle {
		target = aimd.SoftCapDecreaseTarget(cur, prof.MinWorkers, prof.MaxWorkers, st, now)
	} else {
		target = a.aimd.DecreaseTarget(cur, prof.MinWorkers, prof.MaxWorkers, st, now)
	}
	if target < cur {
		if err := q.SetTargetWorkerCount(target); err != nil {
			return
		}
		cause := "PRESSURE"
		switch a.lastClass {
		case scaling.PressureFSThrottle:
			cause = "FS_THROTTLE"
			a.noteSoftCapThrottle(st, now, rateLimitedUntil)
			if a.efficiency.Enabled && st.ProbePending {
				st.AbortEfficiencyProbeThrottled(target, a.aimd.ProbeCooldown, a.efficiency.MaxProbeCooldown, now)
			}
			st.NoteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
		case scaling.PressureMemory:
			cause = "MEMORY_PRESSURE"
		}
		fmt.Printf("  SCALE_DOWN cause=%s scope=%s workers %d->%d (pressure=%s)\n",
			cause, name, cur, target, a.lastClass)
		a.debugAfterWorkerDecrease(name, st, cur, target, now)
		a.emit(scaling.ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
		if target > prof.MinWorkers {
			a.clearInterOpDelay(name, q, st, now)
		}
		a.maybeIncreaseListPage(name, q, prof, now)
		return
	}
	if a.lastClass == scaling.PressureFSThrottle && aimd.FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked(name, st, now, rateLimitedUntil)
		return
	}
	a.stepDownInterOpDelay(name, q, prof, st, rateLimitedUntil, now)
}

func (a *Autoscaler) stepUpSharedGroup(groupID string, queues []string, internal map[string]observe.InternalMetricsSnapshot, reason string, now time.Time) {
	if reason != "" {
		a.debugWorkerScaleUpAttempt("group:"+groupID, reason)
	}
	rateLimitedUntil := aimd.MaxRateLimitedUntilForQueues(internal, queues)
	if aimd.ScaleUpBlockedByRateLimit(now, rateLimitedUntil) {
		if a.debugAIMD {
			a.debugScaleUpBlocked("group:"+groupID, fmt.Sprintf("fs_retry_after remaining=%s", rateLimitedUntil.Sub(now).Round(time.Millisecond)))
		}
		return
	}
	minPer := 1
	if p, ok := a.profiles[queues[0]]; ok && p.MinWorkers > 0 {
		minPer = p.MinWorkers
	}
	st := a.queueState(backend.GroupAIMDKey(groupID))
	maxTotal := a.registry.GroupMaxWorkers(groupID)
	a.prepareScaleUpState(st, maxTotal, now)
	if !a.applyGroupEfficiencyProbeBeforeScaleUp(groupID, queues, internal, st, minPer, now) {
		return
	}
	if cleared := a.tryRecoverSharedInterOpDelay(groupID, queues, internal, st, now); !cleared {
		return
	}
	totalCur := 0
	for _, name := range queues {
		if q := a.queues[name]; q != nil {
			totalCur += q.GetWorkerCount()
		}
	}
	if maxTotal <= 0 {
		maxTotal = totalCur + 1
	}
	rateBefore := a.groupProbeThroughputRate(queues, internal)
	targetTotal, ok := a.aimd.IncreaseTarget(totalCur, minPer*len(queues), maxTotal, st, now)
	if !ok || targetTotal <= totalCur {
		if a.debugAIMD {
			blockReason := aimd.IncreaseTargetBlockReason(totalCur, maxTotal, st, a.aimd, now)
			a.debugScaleUpBlocked("group:"+groupID, blockReason)
		}
		return
	}
	a.debugScaleUpProbe("group:"+groupID, st, totalCur, targetTotal, rateBefore)
	a.recordEfficiencyProbe(st, totalCur, rateBefore, now)
	if st.SoftCap > 0 && targetTotal == st.SoftCap+1 {
		st.ArmSoftCapProbe(targetTotal, now)
	}
	split := backend.SplitWorkersTotal(targetTotal, queues, minPer)
	for name, target := range split {
		q := a.queues[name]
		if q == nil {
			continue
		}
		cur := q.GetWorkerCount()
		if target <= cur {
			continue
		}
		if err := q.SetTargetWorkerCount(target); err != nil {
			continue
		}
		a.clearInterOpDelay(name, q, st, now)
		a.maybeDecreaseListPage(name, q, a.profiles[name], now)
		a.emit(scaling.ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
	}
}

func (a *Autoscaler) tryRecoverSharedInterOpDelay(groupID string, queues []string, internal map[string]observe.InternalMetricsSnapshot, st *aimd.State, now time.Time) (cleared bool) {
	cur := time.Duration(0)
	for _, name := range queues {
		if q := a.queues[name]; q != nil {
			if d := q.GetInterOpDelay(); d > cur {
				cur = d
			}
		}
	}
	if cur <= 0 {
		return true
	}
	rateLimitedUntil := aimd.MaxRateLimitedUntilForQueues(internal, queues)
	if !aimd.InterOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown) {
		if a.debugAIMD {
			remaining := time.Duration(0)
			if !st.DelayLastIncrease.IsZero() {
				until := st.DelayLastIncrease.Add(st.ProbeCooldownDuration(a.aimd.ProbeCooldown))
				if now.Before(until) {
					remaining = until.Sub(now)
				}
			}
			a.debugScaleUpBlocked("group:"+groupID, fmt.Sprintf("inter_op_recovery_cooldown remaining=%s delay=%s", remaining.Round(time.Millisecond), cur.Round(time.Microsecond)))
		}
		return false
	}
	target, ok := a.aimd.DecreaseInterOpDelay(cur, st, now)
	if !ok {
		if a.debugAIMD {
			a.debugScaleUpBlocked("group:"+groupID, "inter_op_delay_unchanged")
		}
		return false
	}
	for _, name := range queues {
		q := a.queues[name]
		if q == nil {
			continue
		}
		old := q.GetInterOpDelay()
		q.SetInterOpDelay(target)
		a.emit(scaling.ScalingEvent{
			Queue:    name,
			Knob:     "InterOpDelayMs",
			OldValue: int(old.Microseconds()),
			NewValue: int(target.Microseconds()),
			Pressure: a.lastClass,
			At:       now,
		})
	}
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  aimd [group:%s]: inter-op delay %s->%s (recovery)", groupID, cur.Round(time.Microsecond), target.Round(time.Microsecond)))
	}
	return target <= 0
}

func (a *Autoscaler) stepUpIndependentQueue(name string, q scaling.QueueActuator, internal map[string]observe.InternalMetricsSnapshot, reason string, now time.Time) {
	if reason != "" {
		a.debugWorkerScaleUpAttempt(name, reason)
	}
	rateLimitedUntil := aimd.MaxRateLimitedUntil(internal, name)
	if aimd.ScaleUpBlockedByRateLimit(now, rateLimitedUntil) {
		if a.debugAIMD {
			a.debugScaleUpBlocked(name, fmt.Sprintf("fs_retry_after remaining=%s", rateLimitedUntil.Sub(now).Round(time.Millisecond)))
		}
		return
	}
	prof := a.profiles[name]
	st := a.queueState(name)
	a.prepareScaleUpState(st, prof.MaxWorkers, now)
	if !a.applyEfficiencyProbeBeforeScaleUp(name, q, st, now) {
		return
	}
	if q.GetInterOpDelay() > 0 {
		if !a.tryRecoverIndependentInterOpDelay(name, q, internal, st, now) {
			return
		}
	}
	cur := q.GetWorkerCount()
	rateBefore := a.throughputRate(name)
	target, ok := a.aimd.IncreaseTarget(cur, prof.MinWorkers, prof.MaxWorkers, st, now)
	if !ok || target <= cur {
		if a.debugAIMD {
			a.debugScaleUpBlocked(name, aimd.IncreaseTargetBlockReason(cur, prof.MaxWorkers, st, a.aimd, now))
		}
		return
	}
	if err := q.SetTargetWorkerCount(target); err != nil {
		return
	}
	if st.SoftCap > 0 && target == st.SoftCap+1 {
		st.ArmSoftCapProbe(target, now)
	}
	a.debugScaleUpProbe(name, st, cur, target, rateBefore)
	a.recordEfficiencyProbe(st, cur, rateBefore, now)
	a.clearInterOpDelay(name, q, st, now)
	a.maybeDecreaseListPage(name, q, prof, now)
	a.emit(scaling.ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
}

func (a *Autoscaler) maybeIncreaseListPage(name string, q scaling.QueueActuator, prof profile.FSPerformanceProfile, now time.Time) {
	if a.lastClass != scaling.PressureFSThrottle || !prof.PreferLargePages {
		return
	}
	cur := q.GetListPageSize()
	p95 := q.ListItemsP95()
	if !profile.ListPageIncreaseAllowed(cur, p95) {
		return
	}
	min := prof.MinListPageSize
	if min <= 0 {
		min = 20
	}
	max := prof.MaxListPageSize
	if max <= 0 {
		max = 10000
	}
	next, ok := profile.IncreaseListPageSize(cur, min, max, prof.ListPageStep)
	if !ok {
		return
	}
	q.SetListPageSize(next)
	a.emit(scaling.ScalingEvent{Queue: name, Knob: "ListPageSize", OldValue: cur, NewValue: next, Pressure: scaling.PressureFSThrottle, At: now})
}

func (a *Autoscaler) maybeDecreaseListPage(name string, q scaling.QueueActuator, prof profile.FSPerformanceProfile, now time.Time) {
	if !prof.PreferLargePages {
		return
	}
	cur := q.GetListPageSize()
	min := prof.MinListPageSize
	if min <= 0 {
		min = 20
	}
	defaultSize := prof.DefaultListPageSize
	if defaultSize <= 0 {
		defaultSize = 100
	}
	next, ok := profile.DecreaseListPageSize(cur, min, defaultSize)
	if !ok {
		return
	}
	q.SetListPageSize(next)
	a.emit(scaling.ScalingEvent{Queue: name, Knob: "ListPageSize", OldValue: cur, NewValue: next, Pressure: scaling.PressureUnderfeed, At: now})
}

func queueUnderfeed(name string, internal map[string]observe.InternalMetricsSnapshot, pending map[string]int) bool {
	snap, ok := internal[name]
	if !ok {
		return false
	}
	return snap.TimeWaitingOnQueue > 500*time.Millisecond && pending[name] > 0
}

func (a *Autoscaler) groupWantsScaleUp(queues []string, internal map[string]observe.InternalMetricsSnapshot, pending map[string]int) bool {
	for _, name := range queues {
		if queueUnderfeed(name, internal, pending) {
			return true
		}
	}
	return false
}
