// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"testing"
	"time"

	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/memweight"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

type memActuator struct {
	workers      int
	lease        int
	dstChildMult int
}

func (m *memActuator) GetWorkerCount() int { return m.workers }
func (m *memActuator) SetTargetWorkerCount(n int) error {
	m.workers = n
	return nil
}
func (m *memActuator) GetInterOpDelay() time.Duration { return 0 }
func (m *memActuator) SetInterOpDelay(time.Duration)  {}
func (m *memActuator) EffectiveLeaseBatchSize() int   { return m.lease }
func (m *memActuator) SetLeaseBatchSize(n int)        { m.lease = n }
func (m *memActuator) EffectiveRefillBatchSize() int {
	n := m.lease / 4
	if n < 1 {
		return 1
	}
	return n
}
func (m *memActuator) SetRefillBatchSize(n int) {
	if n <= 0 {
		return
	}
	m.lease = n * 4
}
func (m *memActuator) EffectiveDstPullChildMultiplier() int {
	if m.dstChildMult <= 0 {
		return queue.DefaultDstPullChildMultiplier
	}
	return m.dstChildMult
}
func (m *memActuator) SetDstPullChildMultiplier(n int) { m.dstChildMult = n }
func (m *memActuator) EffectiveDstPullChildQuota(taskQuota int) int {
	return taskQuota * m.EffectiveDstPullChildMultiplier()
}
func (m *memActuator) GetListPageSize() int { return 100 }
func (m *memActuator) SetListPageSize(int)  {}
func (m *memActuator) ListItemsP95() int    { return 0 }
func (m *memActuator) GetPendingCount() int { return 0 }
func (m *memActuator) InProgressCount() int { return 0 }
func (m *memActuator) ScalingContext() queue.ScalingContext {
	return queue.ScalingContext{QueueName: "src", Mode: queue.ScalingModeTraversal, SrcProvider: "local"}
}
func (m *memActuator) ScalingAdapter(_ bool) fstypes.FSAdapter { return nil }

type fixedMemorySampler struct {
	sample memory.MemorySample
}

func (f fixedMemorySampler) Sample() memory.MemorySample { return f.sample }

func TestStepDownMemoryKnobsPendingDefault(t *testing.T) {
	srcAct := &memActuator{workers: 1, lease: 4000}
	dstAct := &memActuator{workers: 1, lease: 4000, dstChildMult: 10}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	var events []scaling.ScalingEvent
	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren)),
		"dst": profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren)),
	}, map[string]scaling.QueueActuator{"src": srcAct, "dst": dstAct}, Config{
		Enabled: true,
		OnEvent: func(ev scaling.ScalingEvent) { events = append(events, ev) },
	})
	// Empty ledger → furthest defaults to pending → queue tasks (lease) + dst mult.
	sc.SetWeightLedger(memweight.New())

	sc.stepDownMemoryKnobs(time.Now(), scaling.PressureMemory)

	if srcAct.lease != 2000 {
		t.Fatalf("lease=%d want 2000 (pending cuts queue tasks)", srcAct.lease)
	}
	if srcAct.EffectiveRefillBatchSize() != 500 {
		t.Fatalf("derived refill=%d want 500", srcAct.EffectiveRefillBatchSize())
	}
	if dstAct.dstChildMult != 5 {
		t.Fatalf("dstChildMult=%d want 5", dstAct.dstChildMult)
	}
	if len(events) < 2 {
		t.Fatalf("expected lease + dst-mult scaling events, got %d", len(events))
	}
}

func TestStepDownMemoryKnobsPicksHeaviestBucket(t *testing.T) {
	srcAct := &memActuator{workers: 1, lease: 1000}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren)),
	}, map[string]scaling.QueueActuator{"src": srcAct}, Config{Enabled: true})

	ledger := memweight.New()
	ledger.SetCeiling(memweight.Pending, 100)
	ledger.SetCeiling(memweight.InFlight, 100)
	ledger.SetCeiling(memweight.SealBuffer, 100)
	ledger.Add(memweight.Pending, 10)
	ledger.Add(memweight.InFlight, 150)
	sc.SetWeightLedger(ledger)

	sc.stepDownMemoryKnobs(time.Now(), scaling.PressureMemory)

	if srcAct.lease != 500 {
		t.Fatalf("lease=%d want 500 (inFlight furthest)", srcAct.lease)
	}
}

func TestStepDownMemoryKnobsSealBucket(t *testing.T) {
	srcAct := &memActuator{workers: 1, lease: 1000}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	var events []scaling.ScalingEvent
	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren)),
	}, map[string]scaling.QueueActuator{"src": srcAct}, Config{
		Enabled: true,
		OnEvent: func(ev scaling.ScalingEvent) { events = append(events, ev) },
	})
	sc.sealRows = 40_000
	sc.sealFlushMs = int64(10 * time.Second / time.Millisecond)

	ledger := memweight.New()
	ledger.SetCeiling(memweight.Pending, 100)
	ledger.SetCeiling(memweight.SealBuffer, 100)
	ledger.Add(memweight.SealBuffer, 200)
	sc.SetWeightLedger(ledger)

	sc.stepDownMemoryKnobs(time.Now(), scaling.PressureMemory)

	if sc.sealRows != 20_000 {
		t.Fatalf("sealRows=%d want 20000", sc.sealRows)
	}
	sawSeal := false
	for _, ev := range events {
		if ev.Knob == "SealRowThreshold" {
			sawSeal = true
		}
	}
	if !sawSeal {
		t.Fatalf("expected SealRowThreshold event, got %+v", events)
	}
}

func TestMemoryPressureDoesNotChangeWorkers(t *testing.T) {
	act := &memActuator{workers: 8, lease: 4000}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	var events []scaling.ScalingEvent
	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren)),
	}, map[string]scaling.QueueActuator{"src": act}, Config{
		Enabled:  true,
		Interval: time.Second,
		OnEvent:  func(ev scaling.ScalingEvent) { events = append(events, ev) },
	})
	sc.SetWeightLedger(memweight.New())
	// Host used 95% → MEMORY_PRESSURE; pending default → queue tasks shrink; workers must not step down.
	sc.SetMemorySampler(fixedMemorySampler{sample: memory.MemorySample{
		MemTotalKB:     100 * 1024,
		MemAvailableKB: 5 * 1024,
	}})

	sc.tick()

	for _, ev := range events {
		if ev.Knob == "WorkerCount" && ev.NewValue < ev.OldValue {
			t.Fatalf("unexpected worker step-down: %+v", ev)
		}
	}
	if act.workers < 8 {
		t.Fatalf("workers=%d want >=8 (memory must not step workers down)", act.workers)
	}
	if act.lease != 2000 {
		t.Fatalf("lease=%d want 2000 (pending cuts queue tasks)", act.lease)
	}
	if got := sc.LastPressure(); got != scaling.PressureMemory {
		t.Fatalf("pressure=%s want MEMORY_PRESSURE", got)
	}
}

func TestYellowMemoryAllowsWorkerScaleUp(t *testing.T) {
	act := &memActuator{workers: 2, lease: 1000}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	prof := profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren))
	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": prof,
	}, map[string]scaling.QueueActuator{"src": act}, Config{Enabled: true, Interval: time.Second})
	// Host used 85% → yellow (blocks batch/seal up, not workers).
	sc.SetMemorySampler(fixedMemorySampler{sample: memory.MemorySample{
		MemTotalKB:     100 * 1024,
		MemAvailableKB: 15 * 1024,
	}})

	sc.tick()

	if act.workers < 2 {
		t.Fatalf("workers=%d should not drop on yellow", act.workers)
	}
}

func TestCalmNoneRecoversBatchKnobs(t *testing.T) {
	act := &memActuator{workers: 2, lease: 500}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	prof := profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren))
	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": prof,
	}, map[string]scaling.QueueActuator{"src": act}, Config{Enabled: true, Interval: time.Second})
	// Green host + no FS hits → NONE; batch/seal should climb without requiring underfeed.
	sc.SetMemorySampler(fixedMemorySampler{sample: memory.MemorySample{
		MemTotalKB:     100 * 1024,
		MemAvailableKB: 50 * 1024,
	}})

	sc.tick()

	if act.lease <= 500 && sc.sealRows <= 0 {
		t.Fatalf("calm NONE should raise queue tasks or seal, lease=%d sealRows=%d", act.lease, sc.sealRows)
	}
}

func TestMemoryKnobCooldownIsHarsherThanSoftCap(t *testing.T) {
	sc := &Autoscaler{interval: 3 * time.Second}
	now := time.Now()
	sc.noteMemoryKnobCooldown(now)
	remaining := sc.memoryKnobCooldownUntil.Sub(now)
	if remaining < 2*time.Minute {
		t.Fatalf("cooldown=%s want >=2m", remaining)
	}
	// 20×Interval when larger than 2m.
	sc2 := &Autoscaler{interval: 10 * time.Second}
	sc2.noteMemoryKnobCooldown(now)
	if got := sc2.memoryKnobCooldownUntil.Sub(now); got < 200*time.Second {
		t.Fatalf("cooldown=%s want >=200s (20×10s)", got)
	}
}

func TestMemoryKnobCooldownOverrideShortensBounce(t *testing.T) {
	sc := &Autoscaler{interval: 3 * time.Second}
	if err := sc.applyKnobValue(bounceQueue, "MemoryKnobCooldownMs", 15_000); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	sc.noteMemoryKnobCooldown(now)
	got := sc.memoryKnobCooldownUntil.Sub(now)
	if got < 14*time.Second || got > 16*time.Second {
		t.Fatalf("override cooldown=%s want ~15s", got)
	}
	if err := sc.applyKnobValue(bounceQueue, "MemoryKnobCooldownRemainingMs", 0); err != nil {
		t.Fatal(err)
	}
	if !sc.memoryKnobChangeAllowed(now) {
		t.Fatal("clearing remaining should allow changes immediately")
	}
}

func TestMemoryKnobCooldownBlocksSecondStepDown(t *testing.T) {
	srcAct := &memActuator{workers: 1, lease: 4000}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/cd.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, map[string]profile.FSPerformanceProfile{
		"src": profile.ToActuatorProfile(profile.LookupOperationProfile("local", "", profile.OpListChildren)),
	}, map[string]scaling.QueueActuator{"src": srcAct}, Config{Enabled: true, Interval: time.Second})
	sc.SetWeightLedger(memweight.New())
	sc.SetMemorySampler(fixedMemorySampler{sample: memory.MemorySample{
		MemTotalKB:     100 * 1024,
		MemAvailableKB: 5 * 1024,
	}})

	sc.tick()
	leaseAfterFirst := srcAct.lease
	if leaseAfterFirst >= 4000 {
		t.Fatalf("first tick should step down queue tasks, got %d", leaseAfterFirst)
	}
	sc.tick()
	if srcAct.lease != leaseAfterFirst {
		t.Fatalf("second tick during cooldown must not cut again: lease=%d (after first=%d)",
			srcAct.lease, leaseAfterFirst)
	}
}

func TestIncreaseTowardMaxQuarterIsGentle(t *testing.T) {
	next, ok := memory.IncreaseTowardMaxQuarter(1000, 500, 10_000)
	if !ok {
		t.Fatal("expected increase")
	}
	// +25% of current: 1000 + 250 = 1250 (double would be 2000).
	if next != 1250 {
		t.Fatalf("next=%d want 1250", next)
	}
}

func TestApplyKnobOverridesMaxSetsSealRowThreshold(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, nil, nil, Config{Enabled: true})
	sc.sealRows = defaultSealRowThreshold

	max25k := 25_000
	if _, err := sc.ApplyKnobOverrides([]KnobOverride{{
		Queue: "seal", Knob: "SealRowThreshold", Max: &max25k,
	}}); err != nil {
		t.Fatal(err)
	}
	if sc.sealRows != 25_000 {
		t.Fatalf("sealRows=%d want 25000 after Max-only apply", sc.sealRows)
	}

	max250k := 250_000
	if _, err := sc.ApplyKnobOverrides([]KnobOverride{{
		Queue: "seal", Knob: "SealRowThreshold", Max: &max250k,
	}}); err != nil {
		t.Fatal(err)
	}
	if sc.sealRows != 250_000 {
		t.Fatalf("sealRows=%d want 250000 (HardCap)", sc.sealRows)
	}

	maxOver := 500_000
	warns, err := sc.ApplyKnobOverrides([]KnobOverride{{
		Queue: "seal", Knob: "SealRowThreshold", Max: &maxOver,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if sc.sealRows != maxSealRowThreshold {
		t.Fatalf("sealRows=%d want clamp %d", sc.sealRows, maxSealRowThreshold)
	}
	if len(warns) == 0 {
		t.Fatal("expected clamp warning when requesting above absolute max")
	}
}
