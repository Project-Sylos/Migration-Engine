// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"testing"
	"time"
)

func TestAIMDDecreaseMultiplicative(t *testing.T) {
	p := DefaultAIMDPolicy(time.Second)
	st := &State{}
	now := time.Now()

	got := p.DecreaseTarget(20, 1, 32, st, now)
	if got != 10 {
		t.Fatalf("decrease 20: got %d want 10", got)
	}
	if st.Ssthresh != 10 {
		t.Fatalf("Ssthresh: got %d want 10", st.Ssthresh)
	}

	got = p.DecreaseTarget(3, 1, 32, st, now.Add(2*time.Second))
	if got != 1 {
		t.Fatalf("decrease 3: got %d want 1", got)
	}
}

func TestAIMDSlowStartThenAdditive(t *testing.T) {
	p := AIMDPolicy{DecreaseFactor: 0.5, AdditiveStep: 1, ProbeCooldown: 0}
	st := &State{}
	_ = p.DecreaseTarget(20, 1, 32, st, time.Now()) // Ssthresh=10, workers would be 10

	now := time.Now().Add(time.Minute)
	cur := 10

	target, ok := p.IncreaseTarget(cur, 1, 32, st, now)
	if !ok || target != 11 {
		t.Fatalf("at Ssthresh additive: got (%d,%v) want (11,true)", target, ok)
	}

	st.Ssthresh = 0
	cur = 4
	target, ok = p.IncreaseTarget(cur, 1, 32, st, now)
	if !ok || target != 8 {
		t.Fatalf("slow start double: got (%d,%v) want (8,true)", target, ok)
	}
}

func TestAIMDProbeCooldown(t *testing.T) {
	p := DefaultAIMDPolicy(30 * time.Second)
	st := &State{}
	_ = p.DecreaseTarget(20, 1, 32, st, time.Now())

	now := st.LastDecrease.Add(10 * time.Second)
	_, ok := p.IncreaseTarget(10, 1, 32, st, now)
	if ok {
		t.Fatal("expected probe blocked during cooldown")
	}

	now = st.LastDecrease.Add(31 * time.Second)
	target, ok := p.IncreaseTarget(10, 1, 32, st, now)
	if !ok || target != 11 {
		t.Fatalf("after cooldown: got (%d,%v) want (11,true)", target, ok)
	}
}

func TestAIMDSawtoothSequence(t *testing.T) {
	p := AIMDPolicy{DecreaseFactor: 0.5, AdditiveStep: 1, ProbeCooldown: 0}
	st := &State{}
	now := time.Now()

	workers := 20
	// Hit ceiling → halve.
	workers = p.DecreaseTarget(workers, 1, 32, st, now)
	if workers != 10 {
		t.Fatalf("after throttle: %d", workers)
	}

	// Climb back: at Ssthresh, additive only (+1 per tick).
	for i := 0; i < 5; i++ {
		now = now.Add(time.Second)
		next, ok := p.IncreaseTarget(workers, 1, 32, st, now)
		if !ok {
			t.Fatalf("tick %d: increase blocked at %d", i, workers)
		}
		workers = next
	}
	if workers != 15 {
		t.Fatalf("after 5 additive steps from 10: got %d want 15", workers)
	}
}

func TestFSOpRateForInterOpDelay(t *testing.T) {
	got := FSOpRateForInterOpDelay(49, 1, 800)
	if got != 800 {
		t.Fatalf("peak per worker: got %v want 800", got)
	}
	got = FSOpRateForInterOpDelay(400, 20, 0)
	if got != 400 {
		t.Fatalf("aggregate at 20 workers: got %v want 400", got)
	}
}

func TestInterOpDelayFromThroughput(t *testing.T) {
	got := InterOpDelayFromThroughput(1000)
	if got != 2*time.Millisecond {
		t.Fatalf("1000 ops/s: got %v want 2ms", got)
	}
	got = InterOpDelayFromThroughput(500)
	if got != 4*time.Millisecond {
		t.Fatalf("500 ops/s: got %v want 4ms", got)
	}
	if InterOpDelayFromThroughput(0) != 0 {
		t.Fatal("zero throughput should return 0")
	}
}

func TestAIMDInterOpDelayAtWorkerFloor(t *testing.T) {
	p := AIMDPolicy{DecreaseFactor: 0.5, AdditiveStep: 1, ProbeCooldown: 0, InitialInterOpDelay: time.Millisecond, MinInterOpDelayStep: 100 * time.Microsecond}
	st := &State{}
	max := 5 * time.Second
	now := time.Now()

	got := p.IncreaseInterOpDelay(0, max, 1000, st, now)
	if got != 2*time.Millisecond {
		t.Fatalf("seed delay: got %v want 2ms", got)
	}
	if st.DelaySeed != 2*time.Millisecond {
		t.Fatalf("DelaySeed: got %v want 2ms", st.DelaySeed)
	}

	got = p.IncreaseInterOpDelay(got, max, 1000, st, now)
	if got != 4*time.Millisecond {
		t.Fatalf("double delay: got %v want 4ms", got)
	}

	now = now.Add(time.Minute)
	target, ok := p.DecreaseInterOpDelay(4*time.Millisecond, st, now)
	if !ok || target != 2*time.Millisecond {
		t.Fatalf("halve delay: got (%v,%v) want (2ms,true)", target, ok)
	}

	target, ok = p.DecreaseInterOpDelay(2*time.Millisecond, st, now)
	if !ok || target != 0 {
		t.Fatalf("clear delay at seed: got (%v,%v) want (0,true)", target, ok)
	}
}

func TestAIMDInterOpDelayFallbackWhenNoThroughput(t *testing.T) {
	p := DefaultAIMDPolicy(0)
	st := &State{}
	got := p.IncreaseInterOpDelay(0, 5*time.Second, 0, st, time.Now())
	if got != time.Millisecond {
		t.Fatalf("fallback seed: got %v want 1ms", got)
	}
}

func TestAIMDInterOpDelayProbeCooldown(t *testing.T) {
	p := DefaultAIMDPolicy(30 * time.Second)
	st := &State{}
	now := time.Now()
	_ = p.IncreaseInterOpDelay(0, 5*time.Second, 1000, st, now)

	now = st.DelayLastIncrease.Add(5 * time.Second)
	_, ok := p.DecreaseInterOpDelay(2*time.Millisecond, st, now)
	if ok {
		t.Fatal("expected delay recovery blocked during cooldown")
	}
}
