// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"
)

func TestEfficiencyProbeDisabledByDefault(t *testing.T) {
	cfg := EfficiencyProbeConfig{}.normalized(time.Second)
	if cfg.Enabled {
		t.Fatal("Enabled should default to false")
	}
}

func TestEfficiencyProbeFailedRollback(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.1, MinProbeWindow: time.Millisecond}.normalized(time.Second)
	st := &queueAIMDState{ssthresh: 12}
	now := time.Now()
	st.startEfficiencyProbe(5, 100, now.Add(-2*time.Second))
	rollback, ok := st.evaluateEfficiencyProbe(10, 101, 30*time.Second, cfg, now)
	if !ok {
		t.Fatal("expected failed probe")
	}
	if rollback != 5 {
		t.Fatalf("rollback=%d want 5", rollback)
	}
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("expected first bounce %v, got %v", defaultFirstBounceWait, st.effectiveProbeCooldown)
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1", st.failedProbes)
	}
	if st.ssthresh != 12 {
		t.Fatalf("ssthresh=%d want unchanged 12 on efficiency miss", st.ssthresh)
	}
	if st.lastDecrease.IsZero() {
		t.Fatal("expected lastDecrease stamped on efficiency miss")
	}
}

func TestEfficiencyProbeBounceRatchet(t *testing.T) {
	st := &queueAIMDState{}
	base := 30 * time.Second
	max := 10 * time.Minute
	st.ratchetProbeBackoff(base, max)
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("first bounce=%v want %v", st.effectiveProbeCooldown, defaultFirstBounceWait)
	}
	st.ratchetProbeBackoff(base, max)
	if st.effectiveProbeCooldown != 2*defaultFirstBounceWait {
		t.Fatalf("second bounce=%v want %v", st.effectiveProbeCooldown, 2*defaultFirstBounceWait)
	}
	st.ratchetProbeBackoff(base, max)
	if st.effectiveProbeCooldown != 4*defaultFirstBounceWait {
		t.Fatalf("third bounce=%v want %v", st.effectiveProbeCooldown, 4*defaultFirstBounceWait)
	}
}

func TestEfficiencyProbeDefaultMinWindow(t *testing.T) {
	cfg := EfficiencyProbeConfig{}.normalized(3 * time.Second)
	if cfg.MinProbeWindow != 15*time.Second {
		t.Fatalf("MinProbeWindow=%v want 15s", cfg.MinProbeWindow)
	}
}

func TestEfficiencyProbeSuccessHalvesBackoff(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.05, MinProbeWindow: time.Millisecond}.normalized(time.Second)
	st := &queueAIMDState{effectiveProbeCooldown: 4 * time.Second, failedProbes: 2}
	now := time.Now()
	st.startEfficiencyProbe(5, 100, now.Add(-2*time.Second))
	if _, ok := st.evaluateEfficiencyProbe(10, 200, time.Second, cfg, now); ok {
		t.Fatal("expected successful probe")
	}
	if st.effectiveProbeCooldown != 2*time.Second {
		t.Fatalf("expected halved cooldown 2s, got %v", st.effectiveProbeCooldown)
	}
	if st.failedProbes != 2 {
		t.Fatalf("failedProbes=%d want 2 retained until clear", st.failedProbes)
	}
	if st.lastDecrease.IsZero() {
		t.Fatal("expected lastDecrease stamped on success")
	}
}

func TestEfficiencyProbeSuccessClearsWhenAtBase(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.05, MinProbeWindow: time.Millisecond}.normalized(time.Second)
	base := time.Second
	st := &queueAIMDState{effectiveProbeCooldown: 2 * base, failedProbes: 1}
	now := time.Now()
	st.startEfficiencyProbe(5, 100, now.Add(-2*time.Second))
	if _, ok := st.evaluateEfficiencyProbe(10, 200, base, cfg, now); ok {
		t.Fatal("expected successful probe")
	}
	if st.effectiveProbeCooldown != 0 {
		t.Fatalf("expected cooldown cleared at base, got %v", st.effectiveProbeCooldown)
	}
	if st.failedProbes != 0 {
		t.Fatalf("failedProbes=%d want 0", st.failedProbes)
	}
}

func TestEfficiencyProbePendingWindowBlocks(t *testing.T) {
	st := &queueAIMDState{}
	now := time.Now()
	st.startEfficiencyProbe(5, 100, now)
	cfg := EfficiencyProbeConfig{MinProbeWindow: time.Minute}.normalized(time.Second)
	if _, ok := st.evaluateEfficiencyProbe(10, 200, time.Second, cfg, now); ok {
		t.Fatal("should not evaluate before window")
	}
	if !st.probePending {
		t.Fatal("probe should still be pending")
	}
}

func TestEfficiencyProbeFlatRateFails(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.08, MinProbeWindow: time.Millisecond}.normalized(time.Second)
	st := &queueAIMDState{ssthresh: 20}
	now := time.Now()
	st.startEfficiencyProbe(20, 8946.0, now.Add(-20*time.Second))
	rollback, ok := st.evaluateEfficiencyProbe(21, 8946.0, 30*time.Second, cfg, now)
	if !ok {
		t.Fatal("expected flat throughput at ceiling to fail probe")
	}
	if rollback != 20 {
		t.Fatalf("rollback=%d want 20", rollback)
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1", st.failedProbes)
	}
	if st.ssthresh != 20 {
		t.Fatalf("ssthresh=%d want unchanged on flat miss", st.ssthresh)
	}
}

func TestEfficiencyProbePerQueueCountNotUsedForGroup(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.08, MinProbeWindow: time.Millisecond}.normalized(time.Second)
	st := &queueAIMDState{}
	now := time.Now()
	st.startEfficiencyProbe(20, 8946.0, now.Add(-20*time.Second))
	// Per-queue worker count (11) must not short-circuit a group probe (20→21).
	rollback, ok := st.evaluateEfficiencyProbe(11, 3.85, time.Second, cfg, now)
	if ok {
		t.Fatalf("unexpected rollback=%d", rollback)
	}
	if st.probePending {
		t.Fatal("stale per-queue evaluation should clear pending without passing")
	}
}

func TestAbortEfficiencyProbeThrottled(t *testing.T) {
	st := &queueAIMDState{ssthresh: 20}
	now := time.Now()
	st.startEfficiencyProbe(20, 100, now)
	// Bounce is elevated by noteThrottleBounce on FS_THROTTLE; abort only clears the probe.
	st.ratchetProbeBackoff(30*time.Second, 10*time.Minute)
	st.abortEfficiencyProbeThrottled(14, 30*time.Second, 10*time.Minute, now)
	if st.probePending {
		t.Fatal("probe should be cleared")
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1 (from prior ratchet, not abort)", st.failedProbes)
	}
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("expected bounce left at %v, got %v", defaultFirstBounceWait, st.effectiveProbeCooldown)
	}
	if st.ssthresh > 14 {
		t.Fatalf("ssthresh=%d should be capped to workers after abort", st.ssthresh)
	}
	if st.lastDecrease.IsZero() {
		t.Fatal("expected lastDecrease stamped on throttle abort")
	}
}

func TestRatchetFirstBounceUsesMinFloor(t *testing.T) {
	st := &queueAIMDState{}
	st.ratchetProbeBackoff(6*time.Second, 10*time.Minute)
	if st.effectiveProbeCooldown != defaultFirstBounceWait {
		t.Fatalf("first bounce with short base=%v want %v", st.effectiveProbeCooldown, defaultFirstBounceWait)
	}
}

func TestSsthreshRecovery(t *testing.T) {
	cfg := EfficiencyProbeConfig{SsthreshRecoveryInterval: time.Minute}.normalized(time.Second)
	st := &queueAIMDState{ssthresh: 5, lastStable: time.Now().Add(-2 * time.Minute)}
	st.maybeRecoverSsthresh(time.Now(), 20, cfg)
	if st.ssthresh != 6 {
		t.Fatalf("ssthresh=%d want 6", st.ssthresh)
	}
}

func TestScaleUpBlockedByRateLimit(t *testing.T) {
	now := time.Now()
	if scaleUpBlockedByRateLimit(now, time.Time{}) {
		t.Fatal("zero until should not block")
	}
	if scaleUpBlockedByRateLimit(now, now.Add(-time.Second)) {
		t.Fatal("past until should not block")
	}
	if !scaleUpBlockedByRateLimit(now, now.Add(time.Second)) {
		t.Fatal("future until should block")
	}
}
