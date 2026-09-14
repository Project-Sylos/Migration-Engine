// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"context"
	"fmt"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/aimd"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/backend"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

// Autoscaler runs the control loop during a migration.
type Autoscaler struct {
	mu          sync.Mutex
	observer    *observe.QueueObserver
	database    *db.DB
	queues      map[string]scaling.QueueActuator
	profiles    map[string]profile.FSPerformanceProfile
	adapters    profile.AdaptersForScaling
	registry    *backend.BackendRegistry
	interval    time.Duration
	aimd        aimd.AIMDPolicy
	efficiency  aimd.EfficiencyProbeConfig
	onEvent     func(scaling.ScalingEvent)
	lastClass   scaling.PressureClass
	lastActuate time.Time
	minWorkers  map[string]int
	aimdState   map[string]*aimd.State
	events      []scaling.ScalingEvent
	memSampler  memory.MemorySampler
	sealRows    int
	sealFlushMs int64 // flush interval in milliseconds
	debugAIMD   bool
	memoryKnobCooldownUntil time.Time
	workerCapOverrides profile.WorkerCapOverrides
	// lastMaxWorkers tracks effective MaxWorkers per AIMD state key so SoftCap can
	// be cleared when the hard ceiling rises (overrides / profile changes).
	lastMaxWorkers map[string]int
}

// Config configures the autoscaler loop.
type Config struct {
	Enabled  bool
	Interval time.Duration
	OnEvent  func(scaling.ScalingEvent)
	// AIMD tunes TCP-style worker scaling; zero values use defaults derived from Interval.
	AIMD aimd.AIMDPolicy
	// EfficiencyProbe optionally enables throughput-aware scale-up probing (second-order AIMD).
	// Off by default (EfficiencyProbeConfig.Enabled); set Enabled=true to turn probes back on.
	// MaxProbeCooldown still caps the universal FS_THROTTLE bounce timer when probes are off.
	EfficiencyProbe aimd.EfficiencyProbeConfig
	// DebugAIMD prints probe cooldown and scale-up attempt diagnostics to stdout.
	// Also enabled when ME_AUTOSCALER_DEBUG_AIMD=1.
	DebugAIMD bool
	// Adapters are fallbacks for operation profile resolution when queue-local adapters are unset.
	Adapters profile.AdaptersForScaling
	// WorkerCapOverrides optionally overlay MaxWorkers after profile resolve (live-updatable).
	WorkerCapOverrides profile.WorkerCapOverrides
}

// NewAutoscaler builds an autoscaler for the given queues.
func NewAutoscaler(database *db.DB, observer *observe.QueueObserver, registry *backend.BackendRegistry, profiles map[string]profile.FSPerformanceProfile, actuators map[string]scaling.QueueActuator, cfg Config) *Autoscaler {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	minW := make(map[string]int)
	for name, p := range profiles {
		minW[name] = p.MinWorkers
	}
	cooldown := 2 * interval
	if cooldown <= 0 {
		cooldown = 20 * time.Second
	}
	aimdPol := aimd.DefaultAIMDPolicy(cooldown)
	if cfg.AIMD.DecreaseFactor > 0 && cfg.AIMD.DecreaseFactor < 1 {
		aimdPol.DecreaseFactor = cfg.AIMD.DecreaseFactor
	}
	if cfg.AIMD.AdditiveStep > 0 {
		aimdPol.AdditiveStep = cfg.AIMD.AdditiveStep
	}
	if cfg.AIMD.ProbeCooldown > 0 {
		aimdPol.ProbeCooldown = cfg.AIMD.ProbeCooldown
	}
	aimdState := make(map[string]*aimd.State, len(actuators))
	for name := range actuators {
		aimdState[name] = &aimd.State{}
	}
	return &Autoscaler{
		observer:   observer,
		database:   database,
		queues:     actuators,
		profiles:   profiles,
		adapters:   cfg.Adapters,
		registry:   registry,
		interval:   interval,
		aimd:       aimdPol,
		efficiency: cfg.EfficiencyProbe.Normalized(interval),
		onEvent:    cfg.OnEvent,
		minWorkers: minW,
		aimdState:  aimdState,
		memSampler: memory.DefaultMemorySampler,
		debugAIMD:  aimd.DebugEnabled(cfg.DebugAIMD),
		workerCapOverrides: cfg.WorkerCapOverrides.Clone(),
		lastMaxWorkers:     make(map[string]int),
	}
}

// SetWorkerCapOverrides replaces MaxWorkers overlays; the next tick's refreshProfiles applies them.
func (a *Autoscaler) SetWorkerCapOverrides(o profile.WorkerCapOverrides) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.workerCapOverrides = o.Clone()
	a.mu.Unlock()
	a.refreshProfiles()
	a.reconcileProfileBounds()
}

// WorkerCapOverrides returns a copy of the current MaxWorkers overlays.
func (a *Autoscaler) WorkerCapOverrides() profile.WorkerCapOverrides {
	if a == nil {
		return profile.WorkerCapOverrides{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.workerCapOverrides.Clone()
}

// Run executes the autoscaler until ctx is canceled.
func (a *Autoscaler) Run(ctx context.Context) {
	if a == nil {
		return
	}
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick()
		}
	}
}

func (a *Autoscaler) tick() {
	a.refreshProfiles()
	a.reconcileProfileBounds()
	a.updateFSOpRatePeaks()
	inProgress := make(map[string]int)
	pending := make(map[string]int)
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		inProgress[name] = q.InProgressCount()
		pending[name] = q.GetPendingCount()
	}
	var internal map[string]observe.InternalMetricsSnapshot
	if a.observer != nil {
		internal = a.observer.SnapshotInternalMetrics()
	}
	seal := db.SealBufferTelemetry{}
	if a.database != nil {
		seal = a.database.SealBufferTelemetry()
	}
	memSample := a.sampleMemory()
	memLevel := memory.LevelFromSample(memSample)
	pressure := scaling.Classify(scaling.ClassifierInput{
		Internal:      internal,
		SealTelemetry: seal,
		MemoryLevel:   memLevel,
		InProgress:    inProgress,
		Pending:       pending,
	})
	a.mu.Lock()
	a.lastClass = pressure
	a.mu.Unlock()

	now := time.Now()
	sealBackpressure := seal.HardCapHitsSinceLastPoll > 0

	// Host memory only moves lease/refill/seal knobs. Worker AIMD ignores memory level.
	switch pressure {
	case scaling.PressureMemory:
		a.stepDownMemoryKnobs(now, scaling.PressureMemory)
		a.noteMemoryKnobCooldown(now)
	case scaling.PressureUnderfeed:
		if a.memoryKnobIncreaseAllowed(now) && memory.ScaleUpAllowed(memLevel) && memory.MemoryBudgetAllowsIncrease(memSample, 0) {
			a.stepUpMemoryKnobs(now, memSample)
		} else if a.debugAIMD && !memory.ScaleUpAllowed(memLevel) {
			a.debugAIMDPrint(fmt.Sprintf("  aimd batch/seal scale-up blocked: host memory=%v (need green)", memLevel))
		}
	}
	if sealBackpressure {
		a.stepDownMemoryKnobs(now, scaling.PressureSeal)
		a.noteMemoryKnobCooldown(now)
		if a.debugAIMD {
			a.debugAIMDPrint("  aimd seal backpressure: stepped down batch/seal knobs (not host memory pressure)")
		}
	}

	workerPressure := pressure
	if pressure == scaling.PressureMemory {
		// Reclassify without host-memory red so FS/underfeed/none still drive workers.
		workerPressure = scaling.Classify(scaling.ClassifierInput{
			Internal:      internal,
			SealTelemetry: seal,
			MemoryLevel:   memory.MemoryGreen,
			InProgress:    inProgress,
			Pending:       pending,
		})
		a.mu.Lock()
		a.lastClass = workerPressure
		a.mu.Unlock()
	}
	switch workerPressure {
	case scaling.PressureFSThrottle:
		a.stepDownWorkers(internal)
	case scaling.PressureUnderfeed:
		a.stepUpWorkers(internal, inProgress, pending, scaleUpUnderfeed)
	case scaling.PressureNone:
		a.stepUpWorkers(internal, inProgress, pending, scaleUpCalmProbe)
	}
	if pressure == scaling.PressureMemory {
		a.mu.Lock()
		a.lastClass = scaling.PressureMemory
		a.mu.Unlock()
	}
}

func (a *Autoscaler) noteMemoryKnobCooldown(now time.Time) {
	if a == nil {
		return
	}
	cooldown := 2 * a.interval
	if cooldown <= 0 {
		cooldown = 20 * time.Second
	}
	until := now.Add(cooldown)
	if until.After(a.memoryKnobCooldownUntil) {
		a.memoryKnobCooldownUntil = until
	}
}

func (a *Autoscaler) memoryKnobIncreaseAllowed(now time.Time) bool {
	if a == nil || a.memoryKnobCooldownUntil.IsZero() {
		return true
	}
	return !now.Before(a.memoryKnobCooldownUntil)
}

func (a *Autoscaler) stepDownInterOpDelay(name string, q scaling.QueueActuator, prof profile.FSPerformanceProfile, st *aimd.State, rateLimitedUntil time.Time, now time.Time) {
	if !aimd.InterOpIncreaseAllowed(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked(name, st, now, rateLimitedUntil)
		return
	}
	cur := q.GetInterOpDelay()
	maxDelay := prof.MaxInterOpDelay
	if maxDelay <= 0 {
		maxDelay = profile.DefaultMaxInterOpDelay
	}
	target := a.aimd.IncreaseInterOpDelay(cur, maxDelay, aimd.FSOpRateForInterOpDelay(a.taskCompletionRate(name), q.GetWorkerCount(), st.PeakPerWorkerFSOpRate), st, now)
	if target <= cur {
		return
	}
	if a.lastClass == scaling.PressureFSThrottle {
		a.noteSoftCapThrottle(st, now, rateLimitedUntil)
		st.NoteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	q.SetInterOpDelay(target)
	a.emit(scaling.ScalingEvent{
		Queue:    name,
		Knob:     "InterOpDelayMs",
		OldValue: int(cur.Microseconds()),
		NewValue: int(target.Microseconds()),
		Pressure: a.lastClass,
		At:       now,
	})
}

func (a *Autoscaler) stepUpInterOpDelay(name string, q scaling.QueueActuator, st *aimd.State, now time.Time) bool {
	cur := q.GetInterOpDelay()
	target, ok := a.aimd.DecreaseInterOpDelay(cur, st, now)
	if !ok {
		if a.debugAIMD && cur > 0 {
			if st != nil && a.aimd.ProbeCooldown > 0 && !st.DelayLastIncrease.IsZero() {
				elapsed := now.Sub(st.DelayLastIncrease)
				if elapsed < st.ProbeCooldownDuration(a.aimd.ProbeCooldown) {
					a.debugScaleUpBlocked(name, fmt.Sprintf("inter_op_recovery_cooldown remaining=%s", (st.ProbeCooldownDuration(a.aimd.ProbeCooldown)-elapsed).Round(time.Millisecond)))
				}
			} else {
				a.debugScaleUpBlocked(name, "inter_op_delay_unchanged")
			}
		}
		return false
	}
	q.SetInterOpDelay(target)
	a.emit(scaling.ScalingEvent{
		Queue:    name,
		Knob:     "InterOpDelayMs",
		OldValue: int(cur.Microseconds()),
		NewValue: int(target.Microseconds()),
		Pressure: a.lastClass,
		At:       now,
	})
	return target <= 0
}

func (a *Autoscaler) tryRecoverIndependentInterOpDelay(name string, q scaling.QueueActuator, internal map[string]observe.InternalMetricsSnapshot, st *aimd.State, now time.Time) bool {
	cur := q.GetInterOpDelay()
	if cur <= 0 {
		return true
	}
	rateLimitedUntil := aimd.MaxRateLimitedUntil(internal, name)
	if !aimd.InterOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown) {
		if a.debugAIMD {
			remaining := time.Duration(0)
			if !st.DelayLastIncrease.IsZero() {
				until := st.DelayLastIncrease.Add(st.ProbeCooldownDuration(a.aimd.ProbeCooldown))
				if now.Before(until) {
					remaining = until.Sub(now)
				}
			}
			a.debugScaleUpBlocked(name, fmt.Sprintf("inter_op_recovery_cooldown remaining=%s delay=%s", remaining.Round(time.Millisecond), cur.Round(time.Microsecond)))
		}
		return false
	}
	return a.stepUpInterOpDelay(name, q, st, now)
}

func (a *Autoscaler) clearInterOpDelay(name string, q scaling.QueueActuator, st *aimd.State, now time.Time) {
	if q.GetInterOpDelay() <= 0 {
		return
	}
	old := q.GetInterOpDelay()
	q.SetInterOpDelay(0)
	st.DelaySsthresh = 0
	st.DelaySeed = 0
	st.DelayLastIncrease = time.Time{}
	st.PeakPerWorkerFSOpRate = 0
	a.emit(scaling.ScalingEvent{
		Queue:    name,
		Knob:     "InterOpDelayMs",
		OldValue: int(old.Microseconds()),
		NewValue: 0,
		Pressure: scaling.PressureUnderfeed,
		At:       now,
	})
}

func (a *Autoscaler) throughputRate(queueName string) float64 {
	if a == nil || a.observer == nil {
		return 0
	}
	if q := a.queues[queueName]; q != nil {
		ctx := q.ScalingContext()
		if (ctx.Mode == queue.ScalingModeCopy || ctx.Mode == queue.ScalingModeCopyRetry) && ctx.CopyPass == 2 {
			if rate := a.observer.SnapshotEMARate(queueName, "-bytes"); rate > 0 {
				return rate
			}
		}
	}
	return a.observer.SnapshotThroughputRate(queueName)
}

func (a *Autoscaler) taskCompletionRate(queueName string) float64 {
	if a == nil || a.observer == nil {
		return 0
	}
	return a.observer.SnapshotEMARate(queueName, "-tasks")
}

func (a *Autoscaler) updateFSOpRatePeaks() {
	for name, q := range a.queues {
		if q == nil || q.GetInterOpDelay() > 0 {
			continue
		}
		w := q.GetWorkerCount()
		if w <= 0 {
			continue
		}
		rate := a.taskCompletionRate(name)
		if rate <= 0 {
			continue
		}
		perWorker := rate / float64(w)
		st := a.queueState(name)
		if perWorker > st.PeakPerWorkerFSOpRate {
			st.PeakPerWorkerFSOpRate = perWorker
		}
	}
}

func (a *Autoscaler) maxInterOpSeedRate(queueNames []string) float64 {
	var max float64
	for _, name := range queueNames {
		q := a.queues[name]
		if q == nil {
			continue
		}
		st := a.queueState(name)
		if r := aimd.FSOpRateForInterOpDelay(a.taskCompletionRate(name), q.GetWorkerCount(), st.PeakPerWorkerFSOpRate); r > max {
			max = r
		}
	}
	return max
}

func (a *Autoscaler) maxThroughputRate(queueNames []string) float64 {
	var max float64
	for _, name := range queueNames {
		if r := a.throughputRate(name); r > max {
			max = r
		}
	}
	return max
}

func (a *Autoscaler) prepareScaleUpState(st *aimd.State, maxWorkers int, now time.Time) {
	underPressure := a.lastClass == scaling.PressureFSThrottle
	if !underPressure {
		st.ClearFSBackoff()
	}
	st.NoteStability(underPressure, now)
	st.MaybeRecoverSsthresh(now, maxWorkers, a.efficiency)
}

func (a *Autoscaler) groupWorkersTotal(queues []string) int {
	total := 0
	for _, name := range queues {
		if q := a.queues[name]; q != nil {
			total += q.GetWorkerCount()
		}
	}
	return total
}

// groupProbeThroughputRate picks the best throughput signal for a shared worker group,
// preferring queues that are actively completing work (skips idle/low-activity peers like dst in traversal-only tests).
func (a *Autoscaler) groupProbeThroughputRate(queues []string, internal map[string]observe.InternalMetricsSnapshot) float64 {
	const minActiveTasks = int64(10)
	var best float64
	for _, name := range queues {
		r := a.throughputRate(name)
		if internal != nil {
			snap := internal[name]
			if snap.TasksCompletedWhileActive < minActiveTasks && r < 10 {
				continue
			}
		}
		if r > best {
			best = r
		}
	}
	if best > 0 {
		return best
	}
	return a.maxThroughputRate(queues)
}

func (a *Autoscaler) applyGroupEfficiencyProbeBeforeScaleUp(groupID string, queues []string, internal map[string]observe.InternalMetricsSnapshot, st *aimd.State, minPer int, now time.Time) bool {
	if !a.efficiency.Enabled {
		return true
	}
	totalNow := a.groupWorkersTotal(queues)
	if st.ProbePending && a.efficiency.MinProbeWindow > 0 && now.Sub(st.ProbeStarted) < a.efficiency.MinProbeWindow {
		remaining := a.efficiency.MinProbeWindow - now.Sub(st.ProbeStarted)
		a.debugScaleUpBlocked("group:"+groupID, fmt.Sprintf("efficiency_probe_window remaining=%s (workers_before=%d rate_before=%.2f)",
			remaining.Round(time.Millisecond), st.ProbeWorkersBefore, st.ProbeRateBefore))
		return false
	}
	wasPending := st.ProbePending
	rateNow := a.groupProbeThroughputRate(queues, internal)
	if rollback, ok := st.EvaluateEfficiencyProbe(totalNow, rateNow, a.aimd.ProbeCooldown, a.efficiency, now); ok {
		fmt.Printf("  SCALE_DOWN cause=EFFICIENCY_PROBE scope=group:%s workers %d->%d (rate_now=%.2f rate_before=%.2f failed_probes=%d)\n",
			groupID, totalNow, rollback, rateNow, st.ProbeRateBefore, st.FailedProbes)
		a.debugEfficiencyProbeResult("group:"+groupID, st, totalNow, rateNow, rollback, true)
		split := backend.SplitWorkersTotal(rollback, queues, minPer)
		for name, target := range split {
			q := a.queues[name]
			if q == nil {
				continue
			}
			cur := q.GetWorkerCount()
			if target >= cur {
				continue
			}
			if err := q.SetTargetWorkerCount(target); err != nil {
				continue
			}
			a.emit(scaling.ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
		}
		a.debugScaleUpBlocked("group:"+groupID, fmt.Sprintf("efficiency_probe_failed rollback_total=%d", rollback))
		return false
	}
	if wasPending {
		a.debugEfficiencyProbeResult("group:"+groupID, st, totalNow, rateNow, 0, false)
	}
	return true
}

func (a *Autoscaler) abortGroupEfficiencyProbeOnPressure(st *aimd.State, workersAfter int, now time.Time) {
	if !a.efficiency.Enabled || st == nil || !st.ProbePending {
		return
	}
	st.AbortEfficiencyProbeThrottled(workersAfter, a.aimd.ProbeCooldown, a.efficiency.MaxProbeCooldown, now)
	if a.debugAIMD {
		a.debugWorkerScaleUpResult("group", st.ProbeWorkersBefore, workersAfter,
			fmt.Sprintf("efficiency probe ABORTED by pressure (failed_probes=%d cooldown=%s)",
				st.FailedProbes, st.ProbeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond)))
	}
}

func (a *Autoscaler) applyEfficiencyProbeBeforeScaleUp(name string, q scaling.QueueActuator, st *aimd.State, now time.Time) bool {
	if !a.efficiency.Enabled {
		return true
	}
	if st.ProbePending && a.efficiency.MinProbeWindow > 0 && now.Sub(st.ProbeStarted) < a.efficiency.MinProbeWindow {
		remaining := a.efficiency.MinProbeWindow - now.Sub(st.ProbeStarted)
		a.debugScaleUpBlocked(name, fmt.Sprintf("efficiency_probe_window remaining=%s (workers_before=%d rate_before=%.2f)",
			remaining.Round(time.Millisecond), st.ProbeWorkersBefore, st.ProbeRateBefore))
		return false
	}
	wasPending := st.ProbePending
	rateNow := a.throughputRate(name)
	if rollback, ok := st.EvaluateEfficiencyProbe(q.GetWorkerCount(), rateNow, a.aimd.ProbeCooldown, a.efficiency, now); ok {
		cur := q.GetWorkerCount()
		fmt.Printf("  SCALE_DOWN cause=EFFICIENCY_PROBE scope=%s workers %d->%d (rate_now=%.2f rate_before=%.2f failed_probes=%d)\n",
			name, cur, rollback, rateNow, st.ProbeRateBefore, st.FailedProbes)
		a.debugEfficiencyProbeResult(name, st, cur, rateNow, rollback, true)
		if rollback > 0 && rollback < cur {
			if err := q.SetTargetWorkerCount(rollback); err == nil {
				a.emit(scaling.ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: rollback, Pressure: scaling.PressureUnderfeed, At: now})
			}
		}
		a.debugScaleUpBlocked(name, fmt.Sprintf("efficiency_probe_failed rollback=%d", rollback))
		return false
	}
	if wasPending {
		a.debugEfficiencyProbeResult(name, st, q.GetWorkerCount(), rateNow, 0, false)
	}
	return true
}

func (a *Autoscaler) recordEfficiencyProbe(st *aimd.State, workersBefore int, rateBefore float64, now time.Time) {
	if !a.efficiency.Enabled || st == nil {
		return
	}
	st.StartEfficiencyProbe(workersBefore, rateBefore, now)
}

func (a *Autoscaler) queueState(name string) *aimd.State {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.aimdState[name]
	if !ok || st == nil {
		st = &aimd.State{}
		a.aimdState[name] = st
	}
	return st
}

func (a *Autoscaler) emit(ev scaling.ScalingEvent) {
	a.mu.Lock()
	a.events = append(a.events, ev)
	a.mu.Unlock()
	if a.onEvent != nil {
		a.onEvent(ev)
	}
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", scaling.FormatScalingEvent(ev.Queue, ev.Knob, ev.OldValue, ev.NewValue, ev.Pressure), "autoscaler", ev.Queue, ev.Queue)
	}
}

// Events returns a copy of scaling events recorded so far.
func (a *Autoscaler) Events() []scaling.ScalingEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]scaling.ScalingEvent, len(a.events))
	copy(out, a.events)
	return out
}

// MinWorkerCountSeen returns the minimum worker count observed across queues (for tests).
func (a *Autoscaler) MinWorkerCountSeen(initial int) int {
	min := initial
	for _, q := range a.queues {
		if q == nil {
			continue
		}
		c := q.GetWorkerCount()
		if c < min {
			min = c
		}
	}
	for _, ev := range a.Events() {
		if ev.Knob == "WorkerCount" && ev.NewValue < min {
			min = ev.NewValue
		}
	}
	return min
}

// LastPressure returns the last classified pressure class.
func (a *Autoscaler) LastPressure() scaling.PressureClass {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastClass
}
