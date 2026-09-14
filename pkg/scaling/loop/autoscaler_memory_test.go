// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"testing"
	"time"

	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

type memActuator struct {
	workers       int
	lease, refill int
	dstChildMult  int
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
func (m *memActuator) EffectiveRefillBatchSize() int  { return m.refill }
func (m *memActuator) SetRefillBatchSize(n int)       { m.refill = n }
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
func (m *memActuator) GetListPageSize() int           { return 100 }
func (m *memActuator) SetListPageSize(int)            {}
func (m *memActuator) ListItemsP95() int              { return 0 }
func (m *memActuator) GetPendingCount() int           { return 0 }
func (m *memActuator) InProgressCount() int           { return 0 }
func (m *memActuator) ScalingContext() queue.ScalingContext {
	return queue.ScalingContext{QueueName: "src", Mode: queue.ScalingModeTraversal, SrcProvider: "local"}
}
func (m *memActuator) ScalingAdapter(_ bool) fstypes.FSAdapter { return nil }

type fixedMemorySampler struct {
	sample memory.MemorySample
}

func (f fixedMemorySampler) Sample() memory.MemorySample { return f.sample }

func TestStepDownMemoryKnobs(t *testing.T) {
	srcAct := &memActuator{workers: 1, lease: 1000, refill: 8000}
	dstAct := &memActuator{workers: 1, lease: 1000, refill: 8000, dstChildMult: 10}
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

	sc.stepDownMemoryKnobs(time.Now(), scaling.PressureMemory)

	if srcAct.lease != 500 {
		t.Fatalf("lease=%d want 500", srcAct.lease)
	}
	if srcAct.refill != 4000 {
		t.Fatalf("refill=%d want 4000", srcAct.refill)
	}
	if dstAct.dstChildMult != 5 {
		t.Fatalf("dstChildMult=%d want 5", dstAct.dstChildMult)
	}
	if len(events) < 3 {
		t.Fatalf("expected batch scaling events, got %d", len(events))
	}
}

func TestMemoryPressureDoesNotChangeWorkers(t *testing.T) {
	act := &memActuator{workers: 8, lease: 1000, refill: 8000}
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
	// Host used 95% → MEMORY_PRESSURE; batches shrink; workers must not step down.
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
	if act.lease != 500 || act.refill != 4000 {
		t.Fatalf("lease=%d refill=%d want 500/4000", act.lease, act.refill)
	}
	if got := sc.LastPressure(); got != scaling.PressureMemory {
		t.Fatalf("pressure=%s want MEMORY_PRESSURE", got)
	}
}

func TestYellowMemoryAllowsWorkerScaleUp(t *testing.T) {
	act := &memActuator{workers: 2, lease: 1000, refill: 8000}
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

	if act.workers <= 2 {
		t.Fatalf("workers=%d want >2 (yellow must not gate worker AIMD)", act.workers)
	}
	if act.lease != 1000 || act.refill != 8000 {
		t.Fatalf("lease=%d refill=%d want unchanged on yellow NONE", act.lease, act.refill)
	}
}
