// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/backend"
)

func (a *Autoscaler) refreshProfiles() {
	if a == nil {
		return
	}
	a.mu.Lock()
	overrides := a.workerCapOverrides.Clone()
	a.mu.Unlock()
	resolved := make(map[string]profile.FSPerformanceProfile, len(a.queues))
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		ctx := q.ScalingContext()
		srcAd := q.ScalingAdapter(true)
		dstAd := q.ScalingAdapter(false)
		if srcAd == nil {
			srcAd = a.adapters.Src
		}
		if dstAd == nil {
			dstAd = a.adapters.Dst
		}
		prof := profile.ResolveEffectiveProfile(ctx, srcAd, dstAd)
		mode := profile.WorkerCapModeFromContext(ctx)
		resolved[name] = profile.ApplyWorkerCapOverride(prof, overrides, mode)
	}
	a.mu.Lock()
	a.profiles = resolved
	a.mu.Unlock()
	a.syncRegistryProfiles(resolved)
}

func (a *Autoscaler) syncRegistryProfiles(resolved map[string]profile.FSPerformanceProfile) {
	if a.registry == nil {
		return
	}
	for name, prof := range resolved {
		g := a.registry.GroupForQueue(name)
		if g == nil {
			continue
		}
		g.SetProfile(prof)
	}
}

func (a *Autoscaler) reconcileProfileBounds() {
	if a == nil {
		return
	}
	a.mu.Lock()
	profiles := make(map[string]profile.FSPerformanceProfile, len(a.profiles))
	for name, prof := range a.profiles {
		profiles[name] = prof
	}
	if a.lastMaxWorkers == nil {
		a.lastMaxWorkers = make(map[string]int)
	}
	a.mu.Unlock()
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof, ok := profiles[name]
		if !ok {
			continue
		}
		maxW := prof.MaxWorkers
		if maxW <= 0 {
			maxW = profile.UnboundedMaxWorkers
		}
		minW := prof.MinWorkers
		if minW <= 0 {
			minW = 1
		}
		if st := a.queueState(name); st != nil {
			a.mu.Lock()
			prev := a.lastMaxWorkers[name]
			a.lastMaxWorkers[name] = maxW
			a.mu.Unlock()
			st.ClearSoftCapForHigherMax(prev, maxW)
			st.ClampSoftCapToBounds(minW, maxW)
		}
		if q.GetWorkerCount() > maxW {
			_ = q.SetTargetWorkerCount(maxW)
		}
	}
	if a.registry != nil {
		for groupID, queues := range a.registry.QueuesByGroup() {
			if len(queues) < 2 {
				continue
			}
			maxTotal := a.registry.GroupMaxWorkers(groupID)
			minPer := 1
			if p, ok := profiles[queues[0]]; ok && p.MinWorkers > 0 {
				minPer = p.MinWorkers
			}
			key := backend.GroupAIMDKey(groupID)
			if st := a.queueState(key); st != nil {
				a.mu.Lock()
				prev := a.lastMaxWorkers[key]
				a.lastMaxWorkers[key] = maxTotal
				a.mu.Unlock()
				st.ClearSoftCapForHigherMax(prev, maxTotal)
				st.ClampSoftCapToBounds(minPer*len(queues), maxTotal)
			}
		}
	}
}
