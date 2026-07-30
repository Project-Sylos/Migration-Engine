// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"testing"
	"time"
)

func TestEfficiencyProbeDisabledByDefault(t *testing.T) {
	cfg := EfficiencyProbeConfig{}.Normalized(time.Second)
	if cfg.Enabled {
		t.Fatal("Enabled should default to false")
	}
}

func TestEfficiencyProbeFailedRollback(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.1, MinProbeWindow: time.Millisecond}.Normalized(time.Second)
	st := &State{Ssthresh: 12}
	now := time.Now()
	st.StartEfficiencyProbe(5, 100, now.Add(-2*time.Second))
	rollback, ok := st.EvaluateEfficiencyProbe(10, 101, 30*time.Second, cfg, now)
	if !ok {
		t.Fatal("expected failed probe")
	}
	if rollback != 5 {
		t.Fatalf("rollback=%d want 5", rollback)
	}
	if st.EffectiveProbeCooldown != DefaultFirstBounceWait {
		t.Fatalf("expected first bounce %v, got %v", DefaultFirstBounceWait, st.EffectiveProbeCooldown)
	}
	if st.FailedProbes != 1 {
		t.Fatalf("FailedProbes=%d want 1", st.FailedProbes)
	}
	if st.Ssthresh != 12 {
		t.Fatalf("Ssthresh=%d want unchanged 12 on efficiency miss", st.Ssthresh)
	}
	if st.LastDecrease.IsZero() {
		t.Fatal("expected LastDecrease stamped on efficiency miss")
	}
}

func TestEfficiencyProbeBounceRatchet(t *testing.T) {
	st := &State{}
	base := 30 * time.Second
	max := 10 * time.Minute
	st.RatchetProbeBackoff(base, max)
	if st.EffectiveProbeCooldown != DefaultFirstBounceWait {
		t.Fatalf("first bounce=%v want %v", st.EffectiveProbeCooldown, DefaultFirstBounceWait)
	}
	st.RatchetProbeBackoff(base, max)
	if st.EffectiveProbeCooldown != 2*DefaultFirstBounceWait {
		t.Fatalf("second bounce=%v want %v", st.EffectiveProbeCooldown, 2*DefaultFirstBounceWait)
	}
	st.RatchetProbeBackoff(base, max)
	if st.EffectiveProbeCooldown != 4*DefaultFirstBounceWait {
		t.Fatalf("third bounce=%v want %v", st.EffectiveProbeCooldown, 4*DefaultFirstBounceWait)
	}
}

func TestEfficiencyProbeDefaultMinWindow(t *testing.T) {
	cfg := EfficiencyProbeConfig{}.Normalized(3 * time.Second)
	if cfg.MinProbeWindow != 15*time.Second {
		t.Fatalf("MinProbeWindow=%v want 15s", cfg.MinProbeWindow)
	}
}

func TestEfficiencyProbeSuccessHalvesBackoff(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.05, MinProbeWindow: time.Millisecond}.Normalized(time.Second)
	st := &State{EffectiveProbeCooldown: 4 * time.Second, FailedProbes: 2}
	now := time.Now()
	st.StartEfficiencyProbe(5, 100, now.Add(-2*time.Second))
	if _, ok := st.EvaluateEfficiencyProbe(10, 200, time.Second, cfg, now); ok {
		t.Fatal("expected successful probe")
	}
	if st.EffectiveProbeCooldown != 2*time.Second {
		t.Fatalf("expected halved cooldown 2s, got %v", st.EffectiveProbeCooldown)
	}
	if st.FailedProbes != 2 {
		t.Fatalf("FailedProbes=%d want 2 retained until clear", st.FailedProbes)
	}
	if st.LastDecrease.IsZero() {
		t.Fatal("expected LastDecrease stamped on success")
	}
}

func TestEfficiencyProbeSuccessClearsWhenAtBase(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.05, MinProbeWindow: time.Millisecond}.Normalized(time.Second)
	base := time.Second
	st := &State{EffectiveProbeCooldown: 2 * base, FailedProbes: 1}
	now := time.Now()
	st.StartEfficiencyProbe(5, 100, now.Add(-2*time.Second))
	if _, ok := st.EvaluateEfficiencyProbe(10, 200, base, cfg, now); ok {
		t.Fatal("expected successful probe")
	}
	if st.EffectiveProbeCooldown != 0 {
		t.Fatalf("expected cooldown cleared at base, got %v", st.EffectiveProbeCooldown)
	}
	if st.FailedProbes != 0 {
		t.Fatalf("FailedProbes=%d want 0", st.FailedProbes)
	}
}

func TestEfficiencyProbePendingWindowBlocks(t *testing.T) {
	st := &State{}
	now := time.Now()
	st.StartEfficiencyProbe(5, 100, now)
	cfg := EfficiencyProbeConfig{MinProbeWindow: time.Minute}.Normalized(time.Second)
	if _, ok := st.EvaluateEfficiencyProbe(10, 200, time.Second, cfg, now); ok {
		t.Fatal("should not evaluate before window")
	}
	if !st.ProbePending {
		t.Fatal("probe should still be pending")
	}
}

func TestEfficiencyProbeFlatRateFails(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.08, MinProbeWindow: time.Millisecond}.Normalized(time.Second)
	st := &State{Ssthresh: 20}
	now := time.Now()
	st.StartEfficiencyProbe(20, 8946.0, now.Add(-20*time.Second))
	rollback, ok := st.EvaluateEfficiencyProbe(21, 8946.0, 30*time.Second, cfg, now)
	if !ok {
		t.Fatal("expected flat throughput at ceiling to fail probe")
	}
	if rollback != 20 {
		t.Fatalf("rollback=%d want 20", rollback)
	}
	if st.FailedProbes != 1 {
		t.Fatalf("FailedProbes=%d want 1", st.FailedProbes)
	}
	if st.Ssthresh != 20 {
		t.Fatalf("Ssthresh=%d want unchanged on flat miss", st.Ssthresh)
	}
}

func TestEfficiencyProbePerQueueCountNotUsedForGroup(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.08, MinProbeWindow: time.Millisecond}.Normalized(time.Second)
	st := &State{}
	now := time.Now()
	st.StartEfficiencyProbe(20, 8946.0, now.Add(-20*time.Second))
	// Per-queue worker count (11) must not short-circuit a group probe (20→21).
	rollback, ok := st.EvaluateEfficiencyProbe(11, 3.85, time.Second, cfg, now)
	if ok {
		t.Fatalf("unexpected rollback=%d", rollback)
	}
	if st.ProbePending {
		t.Fatal("stale per-queue evaluation should clear pending without passing")
	}
}

func TestAbortEfficiencyProbeThrottled(t *testing.T) {
	st := &State{Ssthresh: 20}
	now := time.Now()
	st.StartEfficiencyProbe(20, 100, now)
	// Bounce is elevated by noteThrottleBounce on FS_THROTTLE; abort only clears the probe.
	st.RatchetProbeBackoff(30*time.Second, 10*time.Minute)
	st.AbortEfficiencyProbeThrottled(14, 30*time.Second, 10*time.Minute, now)
	if st.ProbePending {
		t.Fatal("probe should be cleared")
	}
	if st.FailedProbes != 1 {
		t.Fatalf("FailedProbes=%d want 1 (from prior ratchet, not abort)", st.FailedProbes)
	}
	if st.EffectiveProbeCooldown != DefaultFirstBounceWait {
		t.Fatalf("expected bounce left at %v, got %v", DefaultFirstBounceWait, st.EffectiveProbeCooldown)
	}
	if st.Ssthresh > 14 {
		t.Fatalf("Ssthresh=%d should be capped to workers after abort", st.Ssthresh)
	}
	if st.LastDecrease.IsZero() {
		t.Fatal("expected LastDecrease stamped on throttle abort")
	}
}

func TestRatchetFirstBounceUsesMinFloor(t *testing.T) {
	st := &State{}
	st.RatchetProbeBackoff(6*time.Second, 10*time.Minute)
	if st.EffectiveProbeCooldown != DefaultFirstBounceWait {
		t.Fatalf("first bounce with short base=%v want %v", st.EffectiveProbeCooldown, DefaultFirstBounceWait)
	}
}

func TestSsthreshRecovery(t *testing.T) {
	cfg := EfficiencyProbeConfig{SsthreshRecoveryInterval: time.Minute}.Normalized(time.Second)
	st := &State{Ssthresh: 5, LastStable: time.Now().Add(-2 * time.Minute)}
	st.MaybeRecoverSsthresh(time.Now(), 20, cfg)
	if st.Ssthresh != 6 {
		t.Fatalf("Ssthresh=%d want 6", st.Ssthresh)
	}
}

func TestScaleUpBlockedByRateLimit(t *testing.T) {
	now := time.Now()
	if ScaleUpBlockedByRateLimit(now, time.Time{}) {
		t.Fatal("zero until should not block")
	}
	if ScaleUpBlockedByRateLimit(now, now.Add(-time.Second)) {
		t.Fatal("past until should not block")
	}
	if !ScaleUpBlockedByRateLimit(now, now.Add(time.Second)) {
		t.Fatal("future until should block")
	}
}
