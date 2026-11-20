// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"sync"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/loop"
)

// Advanced-panel overlays stay in memory while a phase is running.
// Ops Badger is written only on:
//   - user Apply (manual tweak)
//   - phase stop (autoscaler unbound)
// Those flushes are async and never block the Apply / stop path on Badger
// writeMu (seal pressure is exactly what we are trying not to wait on).
// Never flush from the autoscaler/memory hot path or 1s snapshot polls.

type autoscalerPrefsState struct {
	mu        sync.Mutex
	overrides map[string]loop.KnobOverride
	lastViews []loop.KnobView
	loaded    bool
}

func (m *Migration) prefsState() *autoscalerPrefsState {
	if m == nil {
		return nil
	}
	if v := m.autoscalerPrefs.Load(); v != nil {
		if st, ok := v.(*autoscalerPrefsState); ok {
			return st
		}
	}
	st := &autoscalerPrefsState{overrides: make(map[string]loop.KnobOverride)}
	m.autoscalerPrefs.Store(st)
	return st
}

func (m *Migration) ensureAutoscalerPrefsLoaded() {
	st := m.prefsState()
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.loaded {
		return
	}
	st.loaded = true
	if m.DB == nil || m.DB.Ops() == nil {
		return
	}
	prefs, ok, err := m.DB.Ops().GetAutoscalerPrefs()
	if err != nil || !ok {
		return
	}
	for _, p := range prefs.Knobs {
		if p.Queue == "" || p.Knob == "" {
			continue
		}
		st.overrides[knobOverrideKey(p.Queue, p.Knob)] = loop.KnobOverride{
			Queue: p.Queue,
			Knob:  p.Knob,
			Min:   cloneIntPtr(p.Min),
			Max:   cloneIntPtr(p.Max),
			Value: cloneIntPtr(p.Value),
		}
	}
	st.lastViews = mapSlice(prefs.LastViews, func(v opsdb.AutoscalerKnobView) loop.KnobView { return loop.KnobView(v) })
}

// flushAutoscalerPrefsAsync snapshots prefs under the in-memory lock and writes
// Badger on a background goroutine. Callers must not wait on this for Apply latency.
func (m *Migration) flushAutoscalerPrefsAsync() {
	if m == nil {
		return
	}
	m.ensureAutoscalerPrefsLoaded()
	st := m.prefsState()
	if st == nil {
		return
	}
	st.mu.Lock()
	prefs := snapshotAutoscalerPrefs(st)
	ops := m.DB
	st.mu.Unlock()
	if ops == nil || ops.Ops() == nil {
		return
	}
	store := ops.Ops()
	go func() {
		_ = store.PutAutoscalerPrefs(prefs)
	}()
}

func snapshotAutoscalerPrefs(st *autoscalerPrefsState) opsdb.AutoscalerPrefs {
	prefs := opsdb.AutoscalerPrefs{
		Knobs:     make([]opsdb.AutoscalerKnobPref, 0, len(st.overrides)),
		LastViews: mapSlice(st.lastViews, func(v loop.KnobView) opsdb.AutoscalerKnobView { return opsdb.AutoscalerKnobView(v) }),
	}
	for _, ov := range st.overrides {
		prefs.Knobs = append(prefs.Knobs, opsdb.AutoscalerKnobPref{
			Queue: ov.Queue,
			Knob:  ov.Knob,
			Min:   cloneIntPtr(ov.Min),
			Max:   cloneIntPtr(ov.Max),
			Value: cloneIntPtr(ov.Value),
		})
	}
	return prefs
}

func (m *Migration) listKnobOverrides() []loop.KnobOverride {
	m.ensureAutoscalerPrefsLoaded()
	st := m.prefsState()
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make([]loop.KnobOverride, 0, len(st.overrides))
	for _, ov := range st.overrides {
		out = append(out, loop.KnobOverride{
			Queue: ov.Queue,
			Knob:  ov.Knob,
			Min:   cloneIntPtr(ov.Min),
			Max:   cloneIntPtr(ov.Max),
			Value: cloneIntPtr(ov.Value),
		})
	}
	return out
}

// mergeKnobOverrides updates in-memory overlays only (no Badger write).
func (m *Migration) mergeKnobOverrides(overrides []loop.KnobOverride) error {
	m.ensureAutoscalerPrefsLoaded()
	st := m.prefsState()
	if st == nil {
		return fmt.Errorf("migration is nil")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.overrides == nil {
		st.overrides = make(map[string]loop.KnobOverride)
	}
	for i := range overrides {
		ov := &overrides[i]
		if ov.Queue == "" || ov.Knob == "" {
			continue
		}
		_ = loop.ClampSealRowOverride(ov)
		if ov.Min != nil && ov.Value != nil && *ov.Value < *ov.Min {
			v := *ov.Min
			ov.Value = &v
		}
		if ov.Max != nil && ov.Value != nil && *ov.Value > *ov.Max {
			v := *ov.Max
			ov.Value = &v
		}
		key := knobOverrideKey(ov.Queue, ov.Knob)
		cur := st.overrides[key]
		cur.Queue = ov.Queue
		cur.Knob = ov.Knob
		if ov.Min != nil {
			cur.Min = cloneIntPtr(ov.Min)
		}
		if ov.Max != nil {
			cur.Max = cloneIntPtr(ov.Max)
		}
		if ov.Value != nil {
			cur.Value = cloneIntPtr(ov.Value)
		}
		st.overrides[key] = cur
	}
	return nil
}

// rememberAutoscalerViews caches live knob rows in memory for paused display.
// Does not touch Badger (hot-path / poll safe).
func (m *Migration) rememberAutoscalerViews(views []loop.KnobView) {
	if len(views) == 0 {
		return
	}
	m.ensureAutoscalerPrefsLoaded()
	st := m.prefsState()
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastViews = append([]loop.KnobView(nil), views...)
}

func (m *Migration) refreshPausedViewsFromOverrides() {
	m.ensureAutoscalerPrefsLoaded()
	st := m.prefsState()
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastViews = loop.OverlayKnobViews(loop.DefaultOperatorKnobViews(), st.lastViews)
	applyOverrideBoundsToViews(st.lastViews, st.overrides)
}

func (m *Migration) pausedAutoscalerSnapshot() loop.LiveSnapshot {
	m.ensureAutoscalerPrefsLoaded()
	st := m.prefsState()
	out := loop.LiveSnapshot{Rates: map[string][]loop.RateSample{}}
	var lastViews []loop.KnobView
	overrides := map[string]loop.KnobOverride{}
	if st != nil {
		st.mu.Lock()
		lastViews = append([]loop.KnobView(nil), st.lastViews...)
		overrides = make(map[string]loop.KnobOverride, len(st.overrides))
		for k, v := range st.overrides {
			overrides[k] = v
		}
		st.mu.Unlock()
	}
	// Always expose the full Advanced-panel template while paused so operators
	// can tweak overlays offline; last-live rows and durable overrides win.
	views := loop.OverlayKnobViews(loop.DefaultOperatorKnobViews(), lastViews)
	applyOverrideBoundsToViews(views, overrides)
	out.Knobs = views
	return out
}

func knobOverrideKey(queue, knob string) string {
	return queue + "/" + knob
}

func cloneIntPtr(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func mapSlice[S, D any](in []S, conv func(S) D) []D {
	if len(in) == 0 {
		return nil
	}
	out := make([]D, len(in))
	for i, v := range in {
		out[i] = conv(v)
	}
	return out
}

func applyOverrideBoundsToViews(views []loop.KnobView, overrides map[string]loop.KnobOverride) {
	for i := range views {
		k := &views[i]
		ov, ok := overrides[knobOverrideKey(k.Queue, k.Knob)]
		if !ok {
			continue
		}
		if ov.Min != nil && *ov.Min >= 0 {
			k.Min = *ov.Min
		}
		if ov.Max != nil && *ov.Max > 0 {
			k.Max = *ov.Max
			if k.Max < k.Min {
				k.Max = k.Min
			}
		}
		if ov.Value != nil {
			k.Value = *ov.Value
		}
	}
}
