// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

const (
	autoscalerEventRingSize = 100
	autoscalerRateRingSize  = 100

	bounceQueue = "autoscaler"

	minMemoryKnobCooldownMs = 5_000
	maxMemoryKnobCooldownMs = 30 * 60_000
	minMemoryShedCooldownMs = 5_000
	maxMemoryShedCooldownMs = 30 * 60_000
	minProbeCooldownMs      = 1_000
	maxProbeCooldownMs      = 10 * 60_000
)

// RateSample is one throughput point for sparklines.
type RateSample struct {
	At          time.Time `json:"at"`
	ItemsPerSec float64   `json:"itemsPerSec"`
	BytesPerSec float64   `json:"bytesPerSec"`
}

// KnobView is one live autoscaler knob with effective bounds.
type KnobView struct {
	Queue    string `json:"queue"`
	Knob     string `json:"knob"`
	Value    int    `json:"value"`
	Min      int    `json:"min"`
	Max      int    `json:"max"`
	Unit     string `json:"unit,omitempty"`
	ReadOnly bool   `json:"readOnly,omitempty"`
}

// MemorySamplePoint is one host-memory reading for the operator graph.
type MemorySamplePoint struct {
	At          time.Time `json:"at"`
	UsedPercent float64   `json:"usedPercent"`
	Level       string    `json:"level"`
	ProcRSSGiB  float64   `json:"procRssGiB,omitempty"`
}

// SystemMemoryView is the current host used% plus recent tick history.
type SystemMemoryView struct {
	UsedPercent   float64             `json:"usedPercent"`
	Level         string              `json:"level"`
	YellowPercent float64             `json:"yellowPercent"`
	RedPercent    float64             `json:"redPercent"`
	ProcRSSGiB    float64             `json:"procRssGiB,omitempty"`
	UsedGiB       float64             `json:"usedGiB,omitempty"`
	TotalGiB      float64             `json:"totalGiB,omitempty"`
	History       []MemorySamplePoint `json:"history,omitempty"`
}

// SealSamplePoint is one seal-buffer fill reading (rows vs soft threshold).
type SealSamplePoint struct {
	At        time.Time `json:"at"`
	Rows      int64     `json:"rows"`
	Threshold int       `json:"threshold"`
	AtMax     bool      `json:"atMax"`
}

// SealFlushPoint is one observed flush (from tick telemetry).
type SealFlushPoint struct {
	At         time.Time `json:"at"`
	Rows       int64     `json:"rows"`
	DurationMs int64     `json:"durationMs,omitempty"`
}

// SealBufferView is the operator seal-buffer graph + flush stats.
type SealBufferView struct {
	Rows                int64             `json:"rows"`
	Threshold           int               `json:"threshold"`
	HardCap             int               `json:"hardCap,omitempty"`
	History             []SealSamplePoint `json:"history,omitempty"`
	TimeAtMaxPercent    float64           `json:"timeAtMaxPercent,omitempty"`
	FlushCount          int               `json:"flushCount,omitempty"`
	LastFlushRows       int64             `json:"lastFlushRows,omitempty"`
	LastFlushDurationMs int64             `json:"lastFlushDurationMs,omitempty"`
	// ProbePending is true while an at-max buffer-size raise is under efficiency review.
	ProbePending bool `json:"probePending,omitempty"`
	// LastProbeResult is accepted | rolled_back | pending | aborted:* (operator hint).
	LastProbeResult string `json:"lastProbeResult,omitempty"`
}

// LiveSnapshot is the operator-facing autoscaler view (API / Advanced panel).
type LiveSnapshot struct {
	Pressure      scaling.PressureClass   `json:"pressure"`
	CooldownUntil time.Time               `json:"cooldownUntil,omitempty"`
	Knobs         []KnobView              `json:"knobs"`
	WaitBudget    *WaitBudgetView         `json:"waitBudget,omitempty"`
	SystemMemory  *SystemMemoryView       `json:"systemMemory,omitempty"`
	SealBuffer    *SealBufferView         `json:"sealBuffer,omitempty"`
	Events        []scaling.ScalingEvent  `json:"events"`
	Rates         map[string][]RateSample `json:"rates"`
}

// KnobOverride is a live min/max/value overlay for one knob.
type KnobOverride struct {
	Queue string `json:"queue"`
	Knob  string `json:"knob"`
	Min   *int   `json:"min,omitempty"`
	Max   *int   `json:"max,omitempty"`
	Value *int   `json:"value,omitempty"`
}

func knobKey(queue, knob string) string {
	return queue + "/" + knob
}

func (a *Autoscaler) effectiveKnobMax(queue, knob string, profileMax int) int {
	if a == nil {
		return profileMax
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.knobOverrides == nil {
		return profileMax
	}
	ov, ok := a.knobOverrides[knobKey(queue, knob)]
	if !ok || ov.Max == nil || *ov.Max <= 0 {
		return profileMax
	}
	return *ov.Max
}

func (a *Autoscaler) effectiveKnobMin(queue, knob string, profileMin int) int {
	if a == nil {
		return profileMin
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.knobOverrides == nil {
		return profileMin
	}
	ov, ok := a.knobOverrides[knobKey(queue, knob)]
	if !ok || ov.Min == nil || *ov.Min < 0 {
		return profileMin
	}
	return *ov.Min
}

// LiveSnapshot builds the current operator view.
func (a *Autoscaler) LiveSnapshot() LiveSnapshot {
	if a == nil {
		return LiveSnapshot{Rates: map[string][]RateSample{}}
	}
	a.mu.Lock()
	pressure := a.lastClass
	cooldown := a.memoryKnobCooldownUntil
	events := append([]scaling.ScalingEvent(nil), a.events...)
	if len(events) > autoscalerEventRingSize {
		events = events[len(events)-autoscalerEventRingSize:]
	}
	rates := make(map[string][]RateSample, len(a.rateRings))
	for name, ring := range a.rateRings {
		rates[name] = append([]RateSample(nil), ring...)
	}
	memHist := append([]MemorySamplePoint(nil), a.memoryRing...)
	sealView := a.buildSealBufferViewLocked()
	waitBudget := a.buildWaitBudgetViewLocked()
	a.mu.Unlock()

	memSample := a.sampleMemory()
	memLevel := memory.LevelFromSample(memSample)
	usedPct := memory.DisplayUsedPercent(memSample)
	totalGiB := float64(memSample.MemTotalKB) / 1024 / 1024
	out := LiveSnapshot{
		Pressure:      pressure,
		CooldownUntil: cooldown,
		Events:        events,
		Rates:         rates,
		Knobs:         a.collectKnobViews(),
		WaitBudget:    waitBudget,
		SystemMemory: &SystemMemoryView{
			UsedPercent:   usedPct,
			Level:         memLevel.String(),
			YellowPercent: memory.YellowThresholdPercent(),
			RedPercent:    memory.RedThresholdPercent(),
			ProcRSSGiB:    float64(memSample.RSSKB) / 1024 / 1024,
			UsedGiB:       totalGiB * usedPct / 100,
			TotalGiB:      totalGiB,
			History:       memHist,
		},
		SealBuffer: sealView,
	}
	return out
}

func (a *Autoscaler) collectKnobViews() []KnobView {
	var knobs []KnobView
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof := a.profiles[name]
		minW := prof.MinWorkers
		if minW <= 0 {
			minW = 1
		}
		maxW := prof.MaxWorkers
		if maxW <= 0 {
			maxW = 32
		}
		if ov := a.WorkerCapOverrides().MaxFor(profile.WorkerCapModeFromContext(q.ScalingContext())); ov > 0 {
			maxW = ov
		}
		minLease := prof.MinLeaseBatch
		if minLease <= 0 {
			minLease = 100
		}
		maxLease := prof.MaxLeaseBatch
		if maxLease <= 0 {
			maxLease = 10_000
		}
		maxDelayMs := int(prof.MaxInterOpDelay / time.Millisecond)
		if maxDelayMs <= 0 {
			maxDelayMs = int(profile.DefaultMaxInterOpDelay / time.Millisecond)
		}
		workers := q.GetWorkerCount()
		knobs = append(knobs,
			KnobView{Queue: name, Knob: "WorkerCount", Value: workers, Min: minW, Max: maxW},
			// LeaseBatchSize is the operator "queue task count"; refill is lease/4.
			KnobView{Queue: name, Knob: "LeaseBatchSize", Value: q.EffectiveLeaseBatchSize(), Min: minLease, Max: maxLease},
			KnobView{
				Queue: name, Knob: "InterOpDelayMs",
				Value: int(q.GetInterOpDelay() / time.Millisecond),
				Min:   0, Max: maxDelayMs, Unit: "ms",
				// Inter-op delay only matters at the worker floor.
				ReadOnly: workers > 1,
			},
		)
	}
	if a.database != nil {
		rows := a.sealRows
		if rows <= 0 {
			rows = defaultSealRowThreshold
		}
		flushMs := int(a.sealFlushMs)
		if flushMs <= 0 {
			flushMs = int(defaultSealFlushInterval / time.Millisecond)
		}
		knobs = append(knobs,
			KnobView{Queue: "seal", Knob: "SealRowThreshold", Value: rows, Min: minSealRowThreshold, Max: maxSealRowThreshold},
			KnobView{Queue: "seal", Knob: "SealFlushIntervalMs", Value: flushMs, Min: int(minSealFlushInterval / time.Millisecond), Max: int(maxSealFlushInterval / time.Millisecond), Unit: "ms"},
		)
	}
	knobs = append(knobs, a.collectBounceKnobViews()...)
	a.applyOverrideBoundsToViews(knobs)
	return knobs
}

func (a *Autoscaler) collectBounceKnobViews() []KnobView {
	memCD := int(a.effectiveMemoryKnobCooldown() / time.Millisecond)
	probeCD := int(a.softCap.ProbeCooldown / time.Millisecond)
	if probeCD <= 0 {
		probeCD = int((2 * a.interval) / time.Millisecond)
	}
	return []KnobView{
		{Queue: bounceQueue, Knob: "MemoryKnobCooldownMs", Value: memCD, Min: minMemoryKnobCooldownMs, Max: maxMemoryKnobCooldownMs, Unit: "ms"},
		{Queue: bounceQueue, Knob: "ProbeCooldownMs", Value: probeCD, Min: minProbeCooldownMs, Max: maxProbeCooldownMs, Unit: "ms"},
	}
}

// DefaultOperatorKnobViews is the Advanced-panel template used while paused /
// before a phase autoscaler is running so operators can edit overlays offline.
func DefaultOperatorKnobViews() []KnobView {
	const (
		defWorkers = 4
		defLease   = 1000
		minLease   = 100
		maxLease   = 10_000
		maxDelayMs = int(profile.DefaultMaxInterOpDelay / time.Millisecond)
	)
	var knobs []KnobView
	for _, queue := range []string{"src", "dst", "copy", "delete"} {
		knobs = append(knobs,
			KnobView{Queue: queue, Knob: "WorkerCount", Value: defWorkers, Min: 1, Max: profile.UnboundedMaxWorkers},
			KnobView{Queue: queue, Knob: "LeaseBatchSize", Value: defLease, Min: minLease, Max: maxLease},
			KnobView{Queue: queue, Knob: "InterOpDelayMs", Value: 0, Min: 0, Max: maxDelayMs, Unit: "ms", ReadOnly: defWorkers > 1},
		)
	}
	knobs = append(knobs,
		KnobView{Queue: "seal", Knob: "SealRowThreshold", Value: defaultSealRowThreshold, Min: minSealRowThreshold, Max: maxSealRowThreshold},
		KnobView{Queue: "seal", Knob: "SealFlushIntervalMs", Value: int(defaultSealFlushInterval / time.Millisecond), Min: int(minSealFlushInterval / time.Millisecond), Max: int(maxSealFlushInterval / time.Millisecond), Unit: "ms"},
		KnobView{Queue: bounceQueue, Knob: "MemoryKnobCooldownMs", Value: minMemoryKnobCooldownMs, Min: minMemoryKnobCooldownMs, Max: maxMemoryKnobCooldownMs, Unit: "ms"},
		KnobView{Queue: bounceQueue, Knob: "ProbeCooldownMs", Value: 20_000, Min: minProbeCooldownMs, Max: maxProbeCooldownMs, Unit: "ms"},
	)
	return knobs
}

// OverlayKnobViews copies base then replaces matching queue/knob rows from overlay
// (used to layer last-live values onto the paused defaults template).
func OverlayKnobViews(base, overlay []KnobView) []KnobView {
	if len(base) == 0 {
		return append([]KnobView(nil), overlay...)
	}
	out := append([]KnobView(nil), base...)
	if len(overlay) == 0 {
		return out
	}
	idx := make(map[string]int, len(out))
	for i, k := range out {
		idx[knobKey(k.Queue, k.Knob)] = i
	}
	for _, k := range overlay {
		key := knobKey(k.Queue, k.Knob)
		if i, ok := idx[key]; ok {
			out[i] = k
			continue
		}
		idx[key] = len(out)
		out = append(out, k)
	}
	return out
}

func (a *Autoscaler) applyOverrideBoundsToViews(knobs []KnobView) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.knobOverrides == nil {
		return
	}
	for i := range knobs {
		k := &knobs[i]
		ov, ok := a.knobOverrides[knobKey(k.Queue, k.Knob)]
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
	}
}

// ClampSealRowOverride clamps SealRowThreshold min/max/value into
// [minSealRowThreshold, maxSealRowThreshold]. When Max is set without Value,
// Max becomes the live target. Returns a warning if anything was capped down.
func ClampSealRowOverride(ov *KnobOverride) string {
	if ov == nil || ov.Queue != "seal" || ov.Knob != "SealRowThreshold" {
		return ""
	}
	var warned int
	clampDown := func(p *int) {
		if p == nil {
			return
		}
		if *p > maxSealRowThreshold {
			if warned == 0 {
				warned = *p
			}
			*p = maxSealRowThreshold
		}
		if *p < minSealRowThreshold {
			*p = minSealRowThreshold
		}
	}
	if ov.Max != nil && ov.Value == nil {
		v := *ov.Max
		ov.Value = &v
	}
	clampDown(ov.Min)
	clampDown(ov.Max)
	clampDown(ov.Value)
	if ov.Min != nil && ov.Value != nil && *ov.Value < *ov.Min {
		v := *ov.Min
		ov.Value = &v
	}
	if ov.Max != nil && ov.Value != nil && *ov.Value > *ov.Max {
		v := *ov.Max
		ov.Value = &v
	}
	if warned == 0 {
		return ""
	}
	return fmt.Sprintf(
		"seal/SealRowThreshold capped at %s (requested %s); absolute max is %s rows",
		formatRowsCompact(maxSealRowThreshold),
		formatRowsCompact(warned),
		formatRowsCompact(maxSealRowThreshold),
	)
}

func formatRowsCompact(n int) string {
	if n >= 1000 && n%1000 == 0 {
		return fmt.Sprintf("%dk", n/1000)
	}
	return fmt.Sprintf("%d", n)
}

// ApplyKnobOverrides applies live min/max/value overlays. Later pressure step-downs
// may move current below a sticky value and clear that target via clearKnobTarget.
//
// Seal Max without Value becomes the live soft threshold immediately (buffer-size
// Apply must move current/graphs without waiting for the autoscaler). Other knobs only clamp
// the live value down into a lowered Max. Warnings list absolute-cap clamps.
func (a *Autoscaler) ApplyKnobOverrides(overrides []KnobOverride) (warnings []string, err error) {
	if a == nil {
		return nil, fmt.Errorf("autoscaler not running")
	}
	for i := range overrides {
		ov := &overrides[i]
		if ov.Queue == "" || ov.Knob == "" {
			continue
		}
		if w := ClampSealRowOverride(ov); w != "" {
			warnings = append(warnings, w)
		} else if ov.Queue != "seal" || ov.Knob != "SealRowThreshold" {
			if ov.Min != nil && ov.Value != nil && *ov.Value < *ov.Min {
				v := *ov.Min
				ov.Value = &v
			}
			if ov.Max != nil && ov.Value != nil && *ov.Value > *ov.Max {
				v := *ov.Max
				ov.Value = &v
			}
		}
		key := knobKey(ov.Queue, ov.Knob)
		a.mu.Lock()
		if a.knobOverrides == nil {
			a.knobOverrides = make(map[string]KnobOverride)
		}
		cur := a.knobOverrides[key]
		cur.Queue = ov.Queue
		cur.Knob = ov.Knob
		if ov.Min != nil {
			v := *ov.Min
			cur.Min = &v
		}
		if ov.Max != nil {
			v := *ov.Max
			cur.Max = &v
		}
		if ov.Value != nil {
			v := *ov.Value
			cur.Value = &v
		}
		a.knobOverrides[key] = cur
		a.mu.Unlock()

		if ov.Max != nil && ov.Knob == "WorkerCount" {
			if q := a.queues[ov.Queue]; q != nil {
				mode := profile.WorkerCapModeFromContext(q.ScalingContext())
				caps := a.WorkerCapOverrides().Clone()
				if caps.Caps == nil {
					caps.Caps = make(map[profile.WorkerCapMode]int)
				}
				caps.Caps[mode] = *ov.Max
				a.SetWorkerCapOverrides(caps)
			}
		}
		if ov.Knob == "InterOpDelayMs" {
			if q := a.queues[ov.Queue]; q != nil && q.GetWorkerCount() > 1 {
				return warnings, fmt.Errorf("InterOpDelayMs only editable when WorkerCount is 1")
			}
		}
		if ov.Value != nil {
			if err := a.applyKnobValue(ov.Queue, ov.Knob, *ov.Value); err != nil {
				return warnings, err
			}
		}
	}
	a.refreshProfiles()
	return warnings, nil
}

func clampMs(value, minV, maxV int) int {
	if value < minV {
		return minV
	}
	if value > maxV {
		return maxV
	}
	return value
}

func (a *Autoscaler) applyKnobValue(queue, knob string, value int) error {
	if queue == bounceQueue {
		switch knob {
		case "MemoryKnobCooldownMs":
			value = clampMs(value, minMemoryKnobCooldownMs, maxMemoryKnobCooldownMs)
			a.memoryKnobCooldownOverride = time.Duration(value) * time.Millisecond
			return nil
		case "MemoryShedCooldownMs":
			value = clampMs(value, minMemoryShedCooldownMs, maxMemoryShedCooldownMs)
			a.memoryShedCooldownOverride = time.Duration(value) * time.Millisecond
			return nil
		case "ProbeCooldownMs":
			value = clampMs(value, minProbeCooldownMs, maxProbeCooldownMs)
			a.softCap.ProbeCooldown = time.Duration(value) * time.Millisecond
			return nil
		case "MemoryKnobCooldownRemainingMs":
			if value < 0 {
				value = 0
			}
			value = clampMs(value, 0, maxMemoryKnobCooldownMs)
			a.mu.Lock()
			if value == 0 {
				a.memoryKnobCooldownUntil = time.Time{}
			} else {
				a.memoryKnobCooldownUntil = time.Now().Add(time.Duration(value) * time.Millisecond)
			}
			a.mu.Unlock()
			return nil
		case "MemoryShedCooldownRemainingMs":
			if value < 0 {
				value = 0
			}
			value = clampMs(value, 0, maxMemoryShedCooldownMs)
			a.mu.Lock()
			if value == 0 {
				a.memoryShedCooldownUntil = time.Time{}
			} else {
				a.memoryShedCooldownUntil = time.Now().Add(time.Duration(value) * time.Millisecond)
			}
			a.mu.Unlock()
			return nil
		default:
			return fmt.Errorf("unknown bounce knob %q", knob)
		}
	}
	if queue == "seal" {
		switch knob {
		case "SealRowThreshold":
			if value < minSealRowThreshold {
				value = minSealRowThreshold
			}
			if value > maxSealRowThreshold {
				value = maxSealRowThreshold
			}
			if a.database == nil {
				return fmt.Errorf("no database")
			}
			a.sealRows = value
			// Keep HardCap aligned with the absolute soft-threshold ceiling so a
			// raised buffer size is not silently blocked by an older 100k cap.
			a.database.UpdateSealBufferOptions(db.SealBufferOptions{
				RowThreshold: value,
				HardCap:      maxSealRowThreshold,
			})
			return nil
		case "SealFlushIntervalMs":
			d := time.Duration(value) * time.Millisecond
			if d < minSealFlushInterval {
				d = minSealFlushInterval
			}
			if d > maxSealFlushInterval {
				d = maxSealFlushInterval
			}
			if a.database == nil {
				return fmt.Errorf("no database")
			}
			a.sealFlushMs = int64(d / time.Millisecond)
			a.database.UpdateSealBufferOptions(db.SealBufferOptions{FlushInterval: d})
			return nil
		default:
			return fmt.Errorf("read-only or unknown seal knob %q", knob)
		}
	}
	q := a.queues[queue]
	if q == nil {
		return fmt.Errorf("unknown queue %q", queue)
	}
	switch knob {
	case "WorkerCount":
		return q.SetTargetWorkerCount(value)
	case "LeaseBatchSize":
		q.SetLeaseBatchSize(value)
	case "RefillBatchSize":
		q.SetRefillBatchSize(value)
	case "DstPullChildMultiplier":
		q.SetDstPullChildMultiplier(value)
	case "InterOpDelayMs":
		if q.GetWorkerCount() > 1 {
			return fmt.Errorf("InterOpDelayMs only editable when WorkerCount is 1")
		}
		q.SetInterOpDelay(time.Duration(value) * time.Millisecond)
	case "ListPageSize":
		q.SetListPageSize(value)
	default:
		return fmt.Errorf("unknown knob %q", knob)
	}
	return nil
}

// clearKnobTarget drops a sticky value override after pressure moved the knob.
func (a *Autoscaler) clearKnobTarget(queue, knob string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.knobOverrides == nil {
		return
	}
	key := knobKey(queue, knob)
	ov, ok := a.knobOverrides[key]
	if !ok || ov.Value == nil {
		return
	}
	ov.Value = nil
	a.knobOverrides[key] = ov
}

func (a *Autoscaler) recordRateSamples(now time.Time) {
	if a == nil || a.observer == nil {
		return
	}
	samples := make(map[string]RateSample, len(a.queues))
	for name := range a.queues {
		samples[name] = RateSample{
			At:          now,
			ItemsPerSec: a.observer.SnapshotThroughputRate(name),
			BytesPerSec: a.observer.SnapshotEMARate(name, "-bytes"),
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rateRings == nil {
		a.rateRings = make(map[string][]RateSample)
	}
	for name, sample := range samples {
		ring := append(a.rateRings[name], sample)
		if len(ring) > autoscalerRateRingSize {
			ring = ring[len(ring)-autoscalerRateRingSize:]
		}
		a.rateRings[name] = ring
	}
}

func (a *Autoscaler) recordMemorySample(now time.Time, sample memory.MemorySample, level memory.MemoryLevel) {
	if a == nil {
		return
	}
	pt := MemorySamplePoint{
		At:          now,
		UsedPercent: memory.DisplayUsedPercent(sample),
		Level:       level.String(),
		ProcRSSGiB:  float64(sample.RSSKB) / 1024 / 1024,
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ring := append(a.memoryRing, pt)
	if len(ring) > autoscalerRateRingSize {
		ring = ring[len(ring)-autoscalerRateRingSize:]
	}
	a.memoryRing = ring
}

func (a *Autoscaler) recordSealSample(now time.Time, seal db.SealBufferTelemetry) {
	if a == nil || a.database == nil {
		return
	}
	threshold := a.sealRows
	if threshold <= 0 {
		threshold = defaultSealRowThreshold
	}
	rows := seal.CurrentRows
	if seal.HWMSinceLastPoll > rows {
		rows = seal.HWMSinceLastPoll
	}
	pt := SealSamplePoint{
		At:        now,
		Rows:      rows,
		Threshold: threshold,
		AtMax:     threshold > 0 && rows >= int64(threshold),
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ring := append(a.sealRing, pt)
	if len(ring) > autoscalerRateRingSize {
		ring = ring[len(ring)-autoscalerRateRingSize:]
	}
	a.sealRing = ring

	// Efficiency / soft-buffer chart samples only organic flushes (threshold/timer),
	// not forced round-advance / StopPhase / explicit Flush drains.
	if seal.OrganicFlushCountSinceLastPoll <= 0 {
		return
	}
	flush := SealFlushPoint{
		At:         now,
		Rows:       seal.LastOrganicFlushRows,
		DurationMs: seal.LastOrganicFlushDurationNs / int64(time.Millisecond),
	}
	fr := append(a.sealFlushRing, flush)
	if len(fr) > autoscalerRateRingSize {
		fr = fr[len(fr)-autoscalerRateRingSize:]
	}
	a.sealFlushRing = fr
}

// buildSealBufferViewLocked requires a.mu held.
func (a *Autoscaler) buildSealBufferViewLocked() *SealBufferView {
	if a == nil || a.database == nil {
		return nil
	}
	threshold := a.sealRows
	if threshold <= 0 {
		threshold = defaultSealRowThreshold
	}
	hist := append([]SealSamplePoint(nil), a.sealRing...)
	view := &SealBufferView{
		Threshold: threshold,
		HardCap:   maxSealRowThreshold,
		History:   hist,
	}
	if len(hist) > 0 {
		last := hist[len(hist)-1]
		view.Rows = last.Rows
		atMax := 0
		for _, p := range hist {
			if p.AtMax {
				atMax++
			}
		}
		view.TimeAtMaxPercent = 100 * float64(atMax) / float64(len(hist))
	}
	flushes := a.sealFlushRing
	view.FlushCount = len(flushes)
	if len(flushes) > 0 {
		last := flushes[len(flushes)-1]
		view.LastFlushRows = last.Rows
		view.LastFlushDurationMs = last.DurationMs
	}
	view.ProbePending = a.sealProbe.pending
	view.LastProbeResult = a.sealProbeLastResult
	return view
}

func (a *Autoscaler) trimEventRingLocked() {
	if len(a.events) > autoscalerEventRingSize {
		a.events = append([]scaling.ScalingEvent(nil), a.events[len(a.events)-autoscalerEventRingSize:]...)
	}
}
