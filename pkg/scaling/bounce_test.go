// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"
)

func TestNoteThrottleBounceIndependentOfEfficiency(t *testing.T) {
	a := &Autoscaler{
		aimd:       DefaultAIMDPolicy(6 * time.Second),
		efficiency: EfficiencyProbeConfig{}.normalized(time.Second), // Enabled=false
		debugAIMD:  false,
	}
	st := &queueAIMDState{}
	now := time.Now()
	a.noteThrottleBounce(st, now, time.Time{})
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("first throttle bounce=%v want %v", st.effectiveProbeCooldown, defaultFirstBounceWait)
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1", st.failedProbes)
	}
	if st.lastDecrease.IsZero() {
		t.Fatal("expected lastDecrease stamped")
	}
	// Second throttle doubles.
	a.noteThrottleBounce(st, now.Add(time.Second), time.Time{})
	if st.effectiveProbeCooldown != 2*defaultFirstBounceWait {
		t.Fatalf("second bounce=%v want %v", st.effectiveProbeCooldown, 2*defaultFirstBounceWait)
	}
}

func TestNoteThrottleBounceCoversRetryAfter(t *testing.T) {
	a := &Autoscaler{
		aimd:       DefaultAIMDPolicy(6 * time.Second),
		efficiency: EfficiencyProbeConfig{MaxProbeCooldown: 10 * time.Minute},
	}
	st := &queueAIMDState{}
	now := time.Now()
	until := now.Add(5 * time.Minute)
	a.noteThrottleBounce(st, now, until)
	want := 5*time.Minute + defaultBounceCushion
	if st.effectiveProbeCooldown != want {
		t.Fatalf("bounce=%v want %v (retry-after + cushion)", st.effectiveProbeCooldown, want)
	}
}

func TestIncreaseTargetHonorsThrottleBounce(t *testing.T) {
	p := DefaultAIMDPolicy(6 * time.Second)
	st := &queueAIMDState{}
	now := time.Now()
	st.ratchetProbeBackoff(6*time.Second, 10*time.Minute)
	st.lastDecrease = now
	if _, ok := p.IncreaseTarget(4, 1, 16, st, now.Add(10*time.Second)); ok {
		t.Fatal("should block while bounce still active")
	}
	if _, ok := p.IncreaseTarget(4, 1, 16, st, now.Add(defaultFirstBounceWait+time.Second)); !ok {
		t.Fatal("should allow climb after bounce elapses")
	}
}

func TestHalveBounceAfterCalmClimbWhenProbesOff(t *testing.T) {
	a := &Autoscaler{
		aimd:       DefaultAIMDPolicy(6 * time.Second),
		efficiency: EfficiencyProbeConfig{Enabled: false, MaxProbeCooldown: 10 * time.Minute},
	}
	st := &queueAIMDState{effectiveProbeCooldown: 2 * defaultFirstBounceWait, failedProbes: 2}
	now := time.Now()
	a.maybeHalveBounceAfterCalmClimb(st, now)
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("halved wait=%v want %v", st.effectiveProbeCooldown, defaultFirstBounceWait)
	}
	// With probes on, climb path must not steal the probe's success/fail ownership.
	a.efficiency.Enabled = true
	st.effectiveProbeCooldown = 2 * defaultFirstBounceWait
	a.maybeHalveBounceAfterCalmClimb(st, now)
	if st.effectiveProbeCooldown != 2*defaultFirstBounceWait {
		t.Fatalf("probes on: wait should stay %v", st.effectiveProbeCooldown)
	}
}

func TestDecayBounceAtCeiling(t *testing.T) {
	a := &Autoscaler{
		aimd:       DefaultAIMDPolicy(6 * time.Second),
		efficiency: EfficiencyProbeConfig{Enabled: false},
	}
	st := &queueAIMDState{
		effectiveProbeCooldown: 2 * defaultFirstBounceWait,
		lastDecrease:           time.Now().Add(-5 * time.Minute),
	}
	now := time.Now()
	a.maybeDecayBounceAtCeiling(st, 16, 16, now)
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("ceiling decay=%v want %v", st.effectiveProbeCooldown, defaultFirstBounceWait)
	}
	// Below max: no decay (climb owns it).
	st.effectiveProbeCooldown = 2 * defaultFirstBounceWait
	a.maybeDecayBounceAtCeiling(st, 8, 16, now)
	if st.effectiveProbeCooldown != 2*defaultFirstBounceWait {
		t.Fatalf("below max should not decay, got %v", st.effectiveProbeCooldown)
	}
}
