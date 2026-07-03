// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"context"
	"fmt"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// ScalingEvent describes one knob change for tests and logs.
type ScalingEvent struct {
	Queue    string
	Knob     string
	OldValue int
	NewValue int
	Pressure PressureClass
	At       time.Time
}

// QueueActuator applies knob changes to a queue.
type QueueActuator interface {
	GetWorkerCount() int
	SetTargetWorkerCount(n int) error
	GetInterOpDelay() time.Duration
	SetInterOpDelay(d time.Duration)
	EffectiveLeaseBatchSize() int
	SetLeaseBatchSize(n int)
	EffectiveRefillBatchSize() int
	SetRefillBatchSize(n int)
	GetListPageSize() int
	SetListPageSize(n int)
	ListItemsP95() int
	GetPendingCount() int
	InProgressCount() int
}

// Autoscaler runs the control loop during a migration.
type Autoscaler struct {
	mu          sync.Mutex
	observer    *queue.QueueObserver
	database    *db.DB
	queues      map[string]QueueActuator
	profiles    map[string]FSPerformanceProfile
	registry    *BackendRegistry
	interval    time.Duration
	aimd        AIMDPolicy
	efficiency  EfficiencyProbeConfig
	onEvent     func(ScalingEvent)
	lastClass   PressureClass
	lastActuate time.Time
	minWorkers  map[string]int
	aimdState   map[string]*queueAIMDState
	events      []ScalingEvent
	memSampler  MemorySampler
	sealRows    int
	sealFlushMs int64 // flush interval in milliseconds
	debugAIMD   bool
	memoryKnobCooldownUntil time.Time
}

// Config configures the autoscaler loop.
type Config struct {
	Enabled  bool
	Interval time.Duration
	OnEvent  func(ScalingEvent)
	// AIMD tunes TCP-style worker scaling; zero values use defaults derived from Interval.
	AIMD AIMDPolicy
	// EfficiencyProbe enables throughput-aware scale-up probing (second-order AIMD).
	EfficiencyProbe EfficiencyProbeConfig
	// DebugAIMD prints probe cooldown and scale-up attempt diagnostics to stdout.
	// Also enabled when ME_AUTOSCALER_DEBUG_AIMD=1.
	DebugAIMD bool
}

// NewAutoscaler builds an autoscaler for the given queues.
func NewAutoscaler(database *db.DB, observer *queue.QueueObserver, registry *BackendRegistry, profiles map[string]FSPerformanceProfile, actuators map[string]QueueActuator, cfg Config) *Autoscaler {
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
	aimd := DefaultAIMDPolicy(cooldown)
	if cfg.AIMD.DecreaseFactor > 0 && cfg.AIMD.DecreaseFactor < 1 {
		aimd.DecreaseFactor = cfg.AIMD.DecreaseFactor
	}
	if cfg.AIMD.AdditiveStep > 0 {
		aimd.AdditiveStep = cfg.AIMD.AdditiveStep
	}
	if cfg.AIMD.ProbeCooldown > 0 {
		aimd.ProbeCooldown = cfg.AIMD.ProbeCooldown
	}
	aimdState := make(map[string]*queueAIMDState, len(actuators))
	for name := range actuators {
		aimdState[name] = &queueAIMDState{}
	}
	return &Autoscaler{
		observer:   observer,
		database:   database,
		queues:     actuators,
		profiles:   profiles,
		registry:   registry,
		interval:   interval,
		aimd:       aimd,
		efficiency: cfg.EfficiencyProbe.normalized(interval),
		onEvent:    cfg.OnEvent,
		minWorkers: minW,
		aimdState:  aimdState,
		memSampler: DefaultMemorySampler,
		debugAIMD:  aimdDebugEnabled(cfg.DebugAIMD),
	}
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
	var internal map[string]queue.InternalMetricsSnapshot
	if a.observer != nil {
		internal = a.observer.SnapshotInternalMetrics()
	}
	seal := db.SealBufferTelemetry{}
	if a.database != nil {
		seal = a.database.SealBufferTelemetry()
	}
	memSample := a.sampleMemory()
	memLevel := LevelFromSample(memSample)
	pressure := Classify(ClassifierInput{
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

	switch pressure {
	case PressureFSThrottle:
		a.stepDownWorkers(internal)
	case PressureMemory:
		a.stepDownWorkers(nil)
		a.stepDownMemoryKnobs(now, PressureMemory)
		a.noteMemoryKnobCooldown(now)
	case PressureUnderfeed:
		if ScaleUpAllowed(memLevel) {
			a.stepUpWorkers(internal, inProgress, pending, scaleUpUnderfeed)
			if a.memoryKnobIncreaseAllowed(now) && MemoryBudgetAllowsIncrease(memSample, 0) {
				a.stepUpMemoryKnobs(now, memSample)
			}
		} else if a.debugAIMD {
			a.debugAIMDPrint(fmt.Sprintf("  aimd worker scale-up blocked: host memory=%v (need green)", memLevel))
		}
	case PressureNone:
		if ScaleUpAllowed(memLevel) {
			a.stepUpWorkers(internal, inProgress, pending, scaleUpCalmProbe)
		}
	}

	if sealBackpressure {
		a.stepDownMemoryKnobs(now, PressureSeal)
		a.noteMemoryKnobCooldown(now)
		if a.debugAIMD {
			a.debugAIMDPrint("  aimd seal backpressure: stepped down batch/seal knobs (not host memory pressure)")
		}
	}
}

func (a *Autoscaler) noteMemoryKnobCooldown(now time.Time) {
	if a == nil {
		return
	}
	cooldown := 3 * a.interval
	if cooldown <= 0 {
		cooldown = 30 * time.Second
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

func (a *Autoscaler) stepDownInterOpDelay(name string, q QueueActuator, profile FSPerformanceProfile, st *queueAIMDState, rateLimitedUntil time.Time, now time.Time) {
	if !interOpIncreaseAllowed(st, now, rateLimitedUntil) {
		a.debugFSBackoffBlocked(name, st, now, rateLimitedUntil)
		return
	}
	cur := q.GetInterOpDelay()
	maxDelay := profile.MaxInterOpDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}
	target := a.aimd.IncreaseInterOpDelay(cur, maxDelay, a.interOpSeedRate(name, q.GetWorkerCount(), st), st, now)
	if target <= cur {
		return
	}
	if a.lastClass == PressureFSThrottle {
		st.noteFSBackoff(now, rateLimitedUntil, a.aimd.ProbeCooldown)
	}
	q.SetInterOpDelay(target)
	a.emit(ScalingEvent{
		Queue:    name,
		Knob:     "InterOpDelayMs",
		OldValue: durationToDelayMicros(cur),
		NewValue: durationToDelayMicros(target),
		Pressure: a.lastClass,
		At:       now,
	})
}

func (a *Autoscaler) stepUpInterOpDelay(name string, q QueueActuator, st *queueAIMDState, now time.Time) bool {
	cur := q.GetInterOpDelay()
	target, ok := a.aimd.DecreaseInterOpDelay(cur, st, now)
	if !ok {
		if a.debugAIMD && cur > 0 {
			if st != nil && a.aimd.ProbeCooldown > 0 && !st.delayLastIncrease.IsZero() {
				elapsed := now.Sub(st.delayLastIncrease)
				if elapsed < st.probeCooldownDuration(a.aimd.ProbeCooldown) {
					a.debugScaleUpBlocked(name, fmt.Sprintf("inter_op_recovery_cooldown remaining=%s", (st.probeCooldownDuration(a.aimd.ProbeCooldown)-elapsed).Round(time.Millisecond)))
				}
			} else {
				a.debugScaleUpBlocked(name, "inter_op_delay_unchanged")
			}
		}
		return false
	}
	q.SetInterOpDelay(target)
	a.emit(ScalingEvent{
		Queue:    name,
		Knob:     "InterOpDelayMs",
		OldValue: durationToDelayMicros(cur),
		NewValue: durationToDelayMicros(target),
		Pressure: a.lastClass,
		At:       now,
	})
	return target <= 0
}

func (a *Autoscaler) tryRecoverIndependentInterOpDelay(name string, q QueueActuator, internal map[string]queue.InternalMetricsSnapshot, st *queueAIMDState, now time.Time) bool {
	cur := q.GetInterOpDelay()
	if cur <= 0 {
		return true
	}
	rateLimitedUntil := maxRateLimitedUntil(internal, name)
	if !interOpDecreaseAllowed(st, now, rateLimitedUntil, a.aimd.ProbeCooldown) {
		if a.debugAIMD {
			remaining := time.Duration(0)
			if !st.delayLastIncrease.IsZero() {
				until := st.delayLastIncrease.Add(st.probeCooldownDuration(a.aimd.ProbeCooldown))
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

func (a *Autoscaler) clearInterOpDelay(name string, q QueueActuator, st *queueAIMDState, now time.Time) {
	if q.GetInterOpDelay() <= 0 {
		return
	}
	old := q.GetInterOpDelay()
	q.SetInterOpDelay(0)
	st.delaySsthresh = 0
	st.delaySeed = 0
	st.delayLastIncrease = time.Time{}
	st.peakPerWorkerFSOpRate = 0
	a.emit(ScalingEvent{
		Queue:    name,
		Knob:     "InterOpDelayMs",
		OldValue: durationToDelayMicros(old),
		NewValue: 0,
		Pressure: PressureUnderfeed,
		At:       now,
	})
}

func (a *Autoscaler) throughputRate(queueName string) float64 {
	if a == nil || a.observer == nil {
		return 0
	}
	return a.observer.SnapshotThroughputRate(queueName)
}

func (a *Autoscaler) taskCompletionRate(queueName string) float64 {
	if a == nil || a.observer == nil {
		return 0
	}
	return a.observer.SnapshotTaskCompletionRate(queueName)
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
		if perWorker > st.peakPerWorkerFSOpRate {
			st.peakPerWorkerFSOpRate = perWorker
		}
	}
}

func (a *Autoscaler) interOpSeedRate(queueName string, workers int, st *queueAIMDState) float64 {
	return FSOpRateForInterOpDelay(a.taskCompletionRate(queueName), workers, st.peakPerWorkerFSOpRate)
}

func (a *Autoscaler) maxInterOpSeedRate(queueNames []string) float64 {
	var max float64
	for _, name := range queueNames {
		q := a.queues[name]
		if q == nil {
			continue
		}
		st := a.queueState(name)
		if r := a.interOpSeedRate(name, q.GetWorkerCount(), st); r > max {
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

func (a *Autoscaler) prepareScaleUpState(st *queueAIMDState, maxWorkers int, now time.Time) {
	underPressure := a.lastClass == PressureFSThrottle || a.lastClass == PressureMemory
	if !underPressure {
		st.clearFSBackoff()
	}
	st.noteStability(underPressure, now)
	st.maybeRecoverSsthresh(now, maxWorkers, a.efficiency)
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
func (a *Autoscaler) groupProbeThroughputRate(queues []string, internal map[string]queue.InternalMetricsSnapshot) float64 {
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

func (a *Autoscaler) applyGroupEfficiencyProbeBeforeScaleUp(groupID string, queues []string, internal map[string]queue.InternalMetricsSnapshot, st *queueAIMDState, minPer int, now time.Time) bool {
	totalNow := a.groupWorkersTotal(queues)
	if st.probePending && a.efficiency.MinProbeWindow > 0 && now.Sub(st.probeStarted) < a.efficiency.MinProbeWindow {
		remaining := a.efficiency.MinProbeWindow - now.Sub(st.probeStarted)
		a.debugScaleUpBlocked("group:"+groupID, fmt.Sprintf("efficiency_probe_window remaining=%s (workers_before=%d rate_before=%.2f)",
			remaining.Round(time.Millisecond), st.probeWorkersBefore, st.probeRateBefore))
		return false
	}
	wasPending := st.probePending
	rateNow := a.groupProbeThroughputRate(queues, internal)
	if rollback, ok := st.evaluateEfficiencyProbe(totalNow, rateNow, a.aimd.ProbeCooldown, a.efficiency); ok {
		a.debugEfficiencyProbeResult("group:"+groupID, st, totalNow, rateNow, rollback, true)
		split := SplitWorkersTotal(rollback, queues, minPer)
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
			a.emit(ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: target, Pressure: a.lastClass, At: now})
		}
		a.debugScaleUpBlocked("group:"+groupID, fmt.Sprintf("efficiency_probe_failed rollback_total=%d", rollback))
		return false
	}
	if wasPending {
		a.debugEfficiencyProbeResult("group:"+groupID, st, totalNow, rateNow, 0, false)
	}
	return true
}

func (a *Autoscaler) abortGroupEfficiencyProbeOnPressure(st *queueAIMDState, workersAfter int, now time.Time) {
	if st == nil || !st.probePending {
		return
	}
	st.abortEfficiencyProbeThrottled(workersAfter, a.aimd.ProbeCooldown, a.efficiency.MaxProbeCooldown)
	if a.debugAIMD {
		a.debugWorkerScaleUpResult("group", st.probeWorkersBefore, workersAfter,
			fmt.Sprintf("efficiency probe ABORTED by pressure (failed_probes=%d cooldown=%s)",
				st.failedProbes, st.probeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond)))
	}
}

func (a *Autoscaler) applyEfficiencyProbeBeforeScaleUp(name string, q QueueActuator, st *queueAIMDState, now time.Time) bool {
	if st.probePending && a.efficiency.MinProbeWindow > 0 && now.Sub(st.probeStarted) < a.efficiency.MinProbeWindow {
		remaining := a.efficiency.MinProbeWindow - now.Sub(st.probeStarted)
		a.debugScaleUpBlocked(name, fmt.Sprintf("efficiency_probe_window remaining=%s (workers_before=%d rate_before=%.2f)",
			remaining.Round(time.Millisecond), st.probeWorkersBefore, st.probeRateBefore))
		return false
	}
	wasPending := st.probePending
	rateNow := a.throughputRate(name)
	if rollback, ok := st.evaluateEfficiencyProbe(q.GetWorkerCount(), rateNow, a.aimd.ProbeCooldown, a.efficiency); ok {
		cur := q.GetWorkerCount()
		a.debugEfficiencyProbeResult(name, st, cur, rateNow, rollback, true)
		if rollback > 0 && rollback < cur {
			if err := q.SetTargetWorkerCount(rollback); err == nil {
				a.emit(ScalingEvent{Queue: name, Knob: "WorkerCount", OldValue: cur, NewValue: rollback, Pressure: PressureUnderfeed, At: now})
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

func (a *Autoscaler) recordEfficiencyProbe(st *queueAIMDState, workersBefore int, rateBefore float64, now time.Time) {
	st.startEfficiencyProbe(workersBefore, rateBefore, now)
}

func durationToDelayMicros(d time.Duration) int {
	return int(d.Microseconds())
}

func (a *Autoscaler) queueState(name string) *queueAIMDState {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.aimdState[name]
	if !ok || st == nil {
		st = &queueAIMDState{}
		a.aimdState[name] = st
	}
	return st
}

func (a *Autoscaler) emit(ev ScalingEvent) {
	a.mu.Lock()
	a.events = append(a.events, ev)
	a.mu.Unlock()
	if a.onEvent != nil {
		a.onEvent(ev)
	}
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", FormatScalingEvent(ev.Queue, ev.Knob, ev.OldValue, ev.NewValue, ev.Pressure), "autoscaler", ev.Queue, ev.Queue)
	}
}

// Events returns a copy of scaling events recorded so far.
func (a *Autoscaler) Events() []ScalingEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ScalingEvent, len(a.events))
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
func (a *Autoscaler) LastPressure() PressureClass {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastClass
}
