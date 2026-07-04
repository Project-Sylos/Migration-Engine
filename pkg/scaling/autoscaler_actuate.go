// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

type scaleUpTrigger int

const (
	scaleUpUnderfeed scaleUpTrigger = iota
	scaleUpCalmProbe
)

func (a *Autoscaler) stepUpWorkers(internal map[string]queue.InternalMetricsSnapshot, inProgress, pending map[string]int, trigger scaleUpTrigger) {
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

func (a *Autoscaler) groupCanCalmProbe(groupID string, queues []string, internal map[string]queue.InternalMetricsSnapshot, now time.Time) bool {
	st := a.queueState(groupAIMDKey(groupID))
	rateLimitedUntil := maxRateLimitedUntilForQueues(internal, queues)
	if fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
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
		return interOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown)
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

func (a *Autoscaler) queueCanCalmProbe(name string, q QueueActuator, internal map[string]queue.InternalMetricsSnapshot, now time.Time) bool {
	st := a.queueState(name)
	rateLimitedUntil := maxRateLimitedUntil(internal, name)
	if fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
		return false
	}
	if q.GetInterOpDelay() > 0 {
		return interOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	profile := a.profiles[name]
	maxWorkers := profile.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 32
	}
	cur := q.GetWorkerCount()
	if cur >= maxWorkers {
		return false
	}
	minWorkers := profile.MinWorkers
	if minWorkers <= 0 {
		minWorkers = 1
	}
	_, ok := a.aimd.IncreaseTarget(cur, minWorkers, maxWorkers, st, now)
	return ok
}

func (a *Autoscaler) stepDownWorkers(internal map[string]queue.InternalMetricsSnapshot) {
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

func (a *Autoscaler) stepDownSharedGroup(groupID string, queues []string, internal map[string]queue.InternalMetricsSnapshot, now time.Time) {
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
	st := a.queueState(groupAIMDKey(groupID))
	rateLimitedUntil := maxRateLimitedUntilForQueues(internal, queues)
	if a.lastClass == PressureFSThrottle && fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked("group:"+groupID, st, now, rateLimitedUntil)
		return
	}
	targetTotal := a.aimd.DecreaseTarget(totalCur, minTotal, maxTotal, st, now)
	if targetTotal < totalCur {
		if a.lastClass == PressureFSThrottle {
			a.abortGroupEfficiencyProbeOnPressure(st, targetTotal)
			st.noteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
		}
		a.debugAIMDPrint(fmt.Sprintf("  aimd decrease [group:%s]: total workers %d->%d", groupID, totalCur, targetTotal))
		if a.debugAIMD {
			a.debugAIMDPrint(formatProbeCooldownLine("group:"+groupID, st, a.aimd.ProbeCooldown, now))
		}
		split := SplitWorkersTotal(targetTotal, queues, minPer)
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
			a.emit(ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
			if target > minPer {
				a.clearInterOpDelay(name, q, st, now)
			}
			a.maybeIncreaseListPage(name, q, a.profiles[name], now)
		}
		return
	}
	if a.lastClass == PressureFSThrottle && fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
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

func (a *Autoscaler) stepDownSharedInterOpDelay(groupID string, queues []string, internal map[string]queue.InternalMetricsSnapshot, now time.Time) {
	st := a.queueState(groupAIMDKey(groupID))
	rateLimitedUntil := maxRateLimitedUntilForQueues(internal, queues)
	if !interOpIncreaseAllowed(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked("group:"+groupID, st, now, rateLimitedUntil)
		return
	}
	profile := a.profiles[queues[0]]
	maxDelay := profile.MaxInterOpDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
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
	if a.lastClass == PressureFSThrottle {
		st.noteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	for _, name := range queues {
		q := a.queues[name]
		if q == nil {
			continue
		}
		old := q.GetInterOpDelay()
		q.SetInterOpDelay(target)
		a.emit(ScalingEvent{
			Queue:    name,
			Knob:     "InterOpDelayMs",
			OldValue: durationToDelayMicros(old),
			NewValue: durationToDelayMicros(target),
			Pressure: a.lastClass,
			At:       now,
		})
	}
}

func (a *Autoscaler) stepDownIndependentQueue(name string, q QueueActuator, internal map[string]queue.InternalMetricsSnapshot, now time.Time) {
	profile := a.profiles[name]
	cur := q.GetWorkerCount()
	st := a.queueState(name)
	rateLimitedUntil := maxRateLimitedUntil(internal, name)
	if a.lastClass == PressureFSThrottle && fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked(name, st, now, rateLimitedUntil)
		return
	}
	target := a.aimd.DecreaseTarget(cur, profile.MinWorkers, profile.MaxWorkers, st, now)
	if target < cur {
		if err := q.SetTargetWorkerCount(target); err != nil {
			return
		}
		a.debugAfterWorkerDecrease(name, st, cur, target, now)
		if a.lastClass == PressureFSThrottle {
			st.noteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
		}
		a.emit(ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
		if target > profile.MinWorkers {
			a.clearInterOpDelay(name, q, st, now)
		}
		a.maybeIncreaseListPage(name, q, profile, now)
		return
	}
	if a.lastClass == PressureFSThrottle && fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked(name, st, now, rateLimitedUntil)
		return
	}
	a.stepDownInterOpDelay(name, q, profile, st, rateLimitedUntil, now)
}

func (a *Autoscaler) stepUpSharedGroup(groupID string, queues []string, internal map[string]queue.InternalMetricsSnapshot, reason string, now time.Time) {
	if reason != "" {
		a.debugWorkerScaleUpAttempt("group:"+groupID, reason)
	}
	minPer := 1
	if p, ok := a.profiles[queues[0]]; ok && p.MinWorkers > 0 {
		minPer = p.MinWorkers
	}
	st := a.queueState(groupAIMDKey(groupID))
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
			blockReason := increaseTargetBlockReason(totalCur, maxTotal, st, a.aimd, now)
			a.debugScaleUpBlocked("group:"+groupID, blockReason)
		}
		return
	}
	a.debugScaleUpProbe("group:"+groupID, st, totalCur, targetTotal, rateBefore)
	a.recordEfficiencyProbe(st, totalCur, rateBefore, now)
	split := SplitWorkersTotal(targetTotal, queues, minPer)
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
		a.emit(ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
	}
}

func (a *Autoscaler) tryRecoverSharedInterOpDelay(groupID string, queues []string, internal map[string]queue.InternalMetricsSnapshot, st *queueAIMDState, now time.Time) (cleared bool) {
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
	rateLimitedUntil := maxRateLimitedUntilForQueues(internal, queues)
	if !interOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown) {
		if a.debugAIMD {
			remaining := time.Duration(0)
			if !st.delayLastIncrease.IsZero() {
				until := st.delayLastIncrease.Add(st.probeCooldownDuration(a.aimd.ProbeCooldown))
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
		a.emit(ScalingEvent{
			Queue:    name,
			Knob:     "InterOpDelayMs",
			OldValue: durationToDelayMicros(old),
			NewValue: durationToDelayMicros(target),
			Pressure: a.lastClass,
			At:       now,
		})
	}
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  aimd [group:%s]: inter-op delay %s->%s (recovery)", groupID, cur.Round(time.Microsecond), target.Round(time.Microsecond)))
	}
	return target <= 0
}

func (a *Autoscaler) stepUpIndependentQueue(name string, q QueueActuator, internal map[string]queue.InternalMetricsSnapshot, reason string, now time.Time) {
	if reason != "" {
		a.debugWorkerScaleUpAttempt(name, reason)
	}
	profile := a.profiles[name]
	st := a.queueState(name)
	a.prepareScaleUpState(st, profile.MaxWorkers, now)
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
	target, ok := a.aimd.IncreaseTarget(cur, profile.MinWorkers, profile.MaxWorkers, st, now)
	if !ok || target <= cur {
		if a.debugAIMD {
			a.debugScaleUpBlocked(name, increaseTargetBlockReason(cur, profile.MaxWorkers, st, a.aimd, now))
		}
		return
	}
	if err := q.SetTargetWorkerCount(target); err != nil {
		return
	}
	a.debugScaleUpProbe(name, st, cur, target, rateBefore)
	a.recordEfficiencyProbe(st, cur, rateBefore, now)
	a.clearInterOpDelay(name, q, st, now)
	a.maybeDecreaseListPage(name, q, profile, now)
	a.emit(ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
}

func (a *Autoscaler) maybeIncreaseListPage(name string, q QueueActuator, profile FSPerformanceProfile, now time.Time) {
	if a.lastClass != PressureFSThrottle || !profile.PreferLargePages {
		return
	}
	cur := q.GetListPageSize()
	p95 := q.ListItemsP95()
	if !ListPageIncreaseAllowed(cur, p95) {
		return
	}
	min := profile.MinListPageSize
	if min <= 0 {
		min = 20
	}
	max := profile.MaxListPageSize
	if max <= 0 {
		max = 10000
	}
	next, ok := IncreaseListPageSize(cur, min, max, profile.ListPageStep)
	if !ok {
		return
	}
	q.SetListPageSize(next)
	a.emit(ScalingEvent{Queue: name, Knob: "ListPageSize", OldValue: cur, NewValue: next, Pressure: PressureFSThrottle, At: now})
}

func (a *Autoscaler) maybeDecreaseListPage(name string, q QueueActuator, profile FSPerformanceProfile, now time.Time) {
	if !profile.PreferLargePages {
		return
	}
	cur := q.GetListPageSize()
	min := profile.MinListPageSize
	if min <= 0 {
		min = 20
	}
	defaultSize := profile.DefaultListPageSize
	if defaultSize <= 0 {
		defaultSize = 100
	}
	next, ok := DecreaseListPageSize(cur, min, defaultSize)
	if !ok {
		return
	}
	q.SetListPageSize(next)
	a.emit(ScalingEvent{Queue: name, Knob: "ListPageSize", OldValue: cur, NewValue: next, Pressure: PressureUnderfeed, At: now})
}

func queueUnderfeed(name string, internal map[string]queue.InternalMetricsSnapshot, pending map[string]int) bool {
	snap, ok := internal[name]
	if !ok {
		return false
	}
	return snap.TimeWaitingOnQueue > 500*time.Millisecond && pending[name] > 0
}

func (a *Autoscaler) groupWantsScaleUp(queues []string, internal map[string]queue.InternalMetricsSnapshot, pending map[string]int) bool {
	for _, name := range queues {
		if queueUnderfeed(name, internal, pending) {
			return true
		}
	}
	return false
}
