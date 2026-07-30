// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/aimd"
)

func TestNoteThrottleBounceIndependentOfEfficiency(t *testing.T) {
	a := &Autoscaler{
		aimd:       aimd.DefaultAIMDPolicy(6 * time.Second),
		efficiency: aimd.EfficiencyProbeConfig{}.Normalized(time.Second), // Enabled=false
		debugAIMD:  false,
	}
	st := &aimd.State{}
	now := time.Now()
	a.noteThrottleBounce(st, now, time.Time{})
	if st.EffectiveProbeCooldown != aimd.DefaultFirstBounceWait {
		t.Fatalf("first throttle bounce=%v want %v", st.EffectiveProbeCooldown, aimd.DefaultFirstBounceWait)
	}
	if st.ProbeBounce != aimd.DefaultFirstBounceWait {
		t.Fatalf("ProbeBounce=%v want %v", st.ProbeBounce, aimd.DefaultFirstBounceWait)
	}
	if st.FailedProbes != 1 {
		t.Fatalf("FailedProbes=%d want 1", st.FailedProbes)
	}
	a.noteThrottleBounce(st, now.Add(time.Second), time.Time{})
	// Pre-ceiling: efficiency timer doubles; ProbeBounce also doubles from LastProbeBounce.
	if st.EffectiveProbeCooldown != 2*aimd.DefaultFirstBounceWait {
		t.Fatalf("second efficiency bounce=%v want %v", st.EffectiveProbeCooldown, 2*aimd.DefaultFirstBounceWait)
	}
	if st.ProbeBounce != 2*aimd.DefaultFirstBounceWait {
		t.Fatalf("second ProbeBounce=%v want %v", st.ProbeBounce, 2*aimd.DefaultFirstBounceWait)
	}
}

func TestNoteThrottleBounceCoversRetryAfter(t *testing.T) {
	a := &Autoscaler{
		aimd:       aimd.DefaultAIMDPolicy(6 * time.Second),
		efficiency: aimd.EfficiencyProbeConfig{MaxProbeCooldown: 10 * time.Minute},
	}
	st := &aimd.State{}
	now := time.Now()
	until := now.Add(5 * time.Minute)
	a.noteThrottleBounce(st, now, until)
	// Pre-ceiling noteThrottleBounce: efficiency timer covers Retry-After+cushion;
	// ProbeBounce uses fail formula max(2×serverWait, …) = 10m.
	if st.EffectiveProbeCooldown != 5*time.Minute+aimd.DefaultBounceCushion {
		t.Fatalf("efficiency bounce=%v want %v", st.EffectiveProbeCooldown, 5*time.Minute+aimd.DefaultBounceCushion)
	}
	if st.ProbeBounce != 10*time.Minute {
		t.Fatalf("ProbeBounce=%v want 10m", st.ProbeBounce)
	}
}

func TestIncreaseTargetHonorsEfficiencyBouncePreSoftCap(t *testing.T) {
	p := aimd.DefaultAIMDPolicy(6 * time.Second)
	st := &aimd.State{}
	now := time.Now()
	st.RatchetProbeBackoff(6*time.Second, 10*time.Minute)
	st.LastDecrease = now
	if _, ok := p.IncreaseTarget(4, 1, 16, st, now.Add(10*time.Second)); ok {
		t.Fatal("should block while efficiency bounce still active (pre soft-cap)")
	}
	if _, ok := p.IncreaseTarget(4, 1, 16, st, now.Add(aimd.DefaultFirstBounceWait+time.Second)); !ok {
		t.Fatal("should allow climb after efficiency bounce elapses")
	}
}

func TestNoteSoftCapThrottle_doesNotGateRecovery(t *testing.T) {
	a := &Autoscaler{
		aimd:       aimd.DefaultAIMDPolicy(6 * time.Second),
		efficiency: aimd.EfficiencyProbeConfig{MaxProbeCooldown: 10 * time.Minute},
	}
	st := &aimd.State{}
	now := time.Now()
	_ = aimd.SoftCapDecreaseTarget(5, 1, 32, st, now)
	a.noteSoftCapThrottle(st, now, time.Time{})
	p := a.aimd
	target, ok := p.IncreaseTarget(2, 1, 32, st, now.Add(time.Second))
	if !ok || target <= 2 {
		t.Fatalf("recovery to SoftCap must ignore ProbeBounce: ok=%v target=%d bounce=%v", ok, target, st.ProbeBounce)
	}
}
