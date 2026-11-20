// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

type shedActuator struct {
	memActuator
	state        queue.QueueState
	cleared      int
	cursorResets int
	beginCalls   int
	finishCalls  int
	partialFalse int
	inProgress   int
	waitCalls    int
}

func (s *shedActuator) BeginMemoryShed() queue.QueueState {
	s.beginCalls++
	prev := s.state
	if prev == "" {
		prev = queue.QueueStateRunning
	}
	s.state = queue.QueueStatePaused
	return prev
}

func (s *shedActuator) FinishMemoryShed(resume queue.QueueState) {
	s.finishCalls++
	s.state = resume
}

func (s *shedActuator) ClearPendingBufferForSuspend() { s.cleared++ }

func (s *shedActuator) ResetKeysetCursorForShed() { s.cursorResets++ }

func (s *shedActuator) WaitInProgressZeroFunc(context.Context, time.Duration, func(int)) error {
	s.waitCalls++
	s.inProgress = 0
	return nil
}

func (s *shedActuator) SetLastPullWasPartial(v bool) {
	if !v {
		s.partialFalse++
	}
}

func (s *shedActuator) GetPendingCount() int { return 42 }

var _ scaling.MemoryShedding = (*shedActuator)(nil)

func TestMemoryPressureShedsPendingBuffer(t *testing.T) {
	act := &shedActuator{
		memActuator: memActuator{workers: 8, lease: 4000},
		state:       queue.QueueStateRunning,
		inProgress:  3,
	}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/shed.db"})
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
	sc.SetMemorySampler(fixedMemorySampler{sample: memory.MemorySample{
		MemTotalKB:     100 * 1024,
		MemAvailableKB: 5 * 1024,
	}})

	sc.tick()

	if act.beginCalls != 1 || act.cleared != 1 || act.cursorResets != 1 || act.finishCalls != 1 {
		t.Fatalf("shed calls begin=%d clear=%d reset=%d finish=%d", act.beginCalls, act.cleared, act.cursorResets, act.finishCalls)
	}
	if act.waitCalls != 1 || act.partialFalse != 1 {
		t.Fatalf("wait=%d partialFalse=%d", act.waitCalls, act.partialFalse)
	}
	if act.state != queue.QueueStateRunning {
		t.Fatalf("state=%s want running", act.state)
	}
	// Nil ledger → furthest defaults to pending → queue tasks (lease) step down.
	if act.lease != 2000 {
		t.Fatalf("lease=%d want 2000", act.lease)
	}
	sawShed := false
	for _, ev := range events {
		if ev.Knob == "MemoryShed" {
			sawShed = true
			if ev.OldValue != 42 || ev.NewValue != 0 {
				t.Fatalf("MemoryShed event=%+v", ev)
			}
		}
	}
	if !sawShed {
		t.Fatal("expected MemoryShed scaling event")
	}

	// Cooldown: second tick must not shed again immediately.
	act.beginCalls = 0
	sc.tick()
	if act.beginCalls != 0 {
		t.Fatal("shed should respect cooldown")
	}
}
