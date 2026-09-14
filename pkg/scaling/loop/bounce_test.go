// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/aimd"
)

func TestNoteSoftCapThrottleRatchetsProbeBounce(t *testing.T) {
	a := &Autoscaler{
		aimd:       aimd.DefaultAIMDPolicy(6 * time.Second),
		efficiency: aimd.EfficiencyProbeConfig{MaxProbeCooldown: 10 * time.Minute},
	}
	st := &aimd.State{}
	now := time.Now()
	until := now.Add(5 * time.Minute)
	a.noteSoftCapThrottle(st, now, until)
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
