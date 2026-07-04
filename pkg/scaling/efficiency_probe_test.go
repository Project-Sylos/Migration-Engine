// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"
)

func TestEfficiencyProbeFailedRollback(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.1, MinProbeWindow: 0}.normalized(time.Second)
	st := &queueAIMDState{}
	st.startEfficiencyProbe(5, 100, time.Now().Add(-2*time.Second))
	rollback, ok := st.evaluateEfficiencyProbe(10, 101, time.Second, cfg)
	if !ok {
		t.Fatal("expected failed probe")
	}
	if rollback != 5 {
		t.Fatalf("rollback=%d want 5", rollback)
	}
	if st.effectiveProbeCooldown <= time.Second {
		t.Fatalf("expected ratcheted cooldown, got %v", st.effectiveProbeCooldown)
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1", st.failedProbes)
	}
}

func TestEfficiencyProbeSuccessResetsBackoff(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.05, MinProbeWindow: 0}.normalized(time.Second)
	st := &queueAIMDState{effectiveProbeCooldown: 4 * time.Second}
	st.startEfficiencyProbe(5, 100, time.Now().Add(-2*time.Second))
	if _, ok := st.evaluateEfficiencyProbe(10, 200, time.Second, cfg); ok {
		t.Fatal("expected successful probe")
	}
	if st.effectiveProbeCooldown != 0 {
		t.Fatalf("expected cooldown reset, got %v", st.effectiveProbeCooldown)
	}
}

func TestEfficiencyProbePendingWindowBlocks(t *testing.T) {
	st := &queueAIMDState{}
	st.startEfficiencyProbe(5, 100, time.Now())
	cfg := EfficiencyProbeConfig{MinProbeWindow: time.Minute}.normalized(time.Second)
	if _, ok := st.evaluateEfficiencyProbe(10, 200, time.Second, cfg); ok {
		t.Fatal("should not evaluate before window")
	}
	if !st.probePending {
		t.Fatal("probe should still be pending")
	}
}

func TestEfficiencyProbeFlatRateFails(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.08, MinProbeWindow: 0}.normalized(time.Second)
	st := &queueAIMDState{}
	st.startEfficiencyProbe(20, 8946.0, time.Now().Add(-5*time.Second))
	rollback, ok := st.evaluateEfficiencyProbe(21, 8946.0, time.Second, cfg)
	if !ok {
		t.Fatal("expected flat throughput at ceiling to fail probe")
	}
	if rollback != 20 {
		t.Fatalf("rollback=%d want 20", rollback)
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1", st.failedProbes)
	}
}

func TestEfficiencyProbePerQueueCountNotUsedForGroup(t *testing.T) {
	cfg := EfficiencyProbeConfig{MinEfficiencyRatio: 0.08, MinProbeWindow: 0}.normalized(time.Second)
	st := &queueAIMDState{}
	st.startEfficiencyProbe(20, 8946.0, time.Now().Add(-5*time.Second))
	// Per-queue worker count (11) must not short-circuit a group probe (20→21).
	rollback, ok := st.evaluateEfficiencyProbe(11, 3.85, time.Second, cfg)
	if ok {
		t.Fatalf("unexpected rollback=%d", rollback)
	}
	if st.probePending {
		t.Fatal("stale per-queue evaluation should clear pending without passing")
	}
}

func TestAbortEfficiencyProbeThrottled(t *testing.T) {
	st := &queueAIMDState{}
	st.startEfficiencyProbe(20, 100, time.Now())
	st.abortEfficiencyProbeThrottled(14, time.Second, 10*time.Minute)
	if st.probePending {
		t.Fatal("probe should be cleared")
	}
	if st.failedProbes != 1 {
		t.Fatalf("failedProbes=%d want 1", st.failedProbes)
	}
	if st.effectiveProbeCooldown <= time.Second {
		t.Fatalf("expected ratcheted cooldown, got %v", st.effectiveProbeCooldown)
	}
	if st.ssthresh > 14 {
		t.Fatalf("ssthresh=%d should be capped to workers after abort", st.ssthresh)
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
