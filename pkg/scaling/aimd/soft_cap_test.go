// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"testing"
	"time"
)

func TestSoftCapDecreaseTarget_notMultiplicativeCliff(t *testing.T) {
	st := &State{}
	now := time.Now()
	got := SoftCapDecreaseTarget(5, 1, 32, st, now)
	// RL at 5 → SoftCap=4 (drop by 1), CeilingSafe=5 (next SoftCap+1 boundary)
	if st.CeilingSafe != 5 {
		t.Fatalf("CeilingSafe=%d want 5", st.CeilingSafe)
	}
	if st.SoftCap != 4 {
		t.Fatalf("SoftCap=%d want 4", st.SoftCap)
	}
	if got != 4 {
		t.Fatalf("target=%d want 4 (not ×0.5→2)", got)
	}
}

func TestIncreaseTarget_recoveryToSoftCapIgnoresProbeBounce(t *testing.T) {
	p := DefaultAIMDPolicy(6 * time.Second)
	st := &State{}
	now := time.Now()
	_ = SoftCapDecreaseTarget(5, 1, 32, st, now)
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now, time.Time{})
	if st.ProbeBounce < DefaultFirstBounceWait {
		t.Fatalf("expected probe bounce elevated, got %v", st.ProbeBounce)
	}
	// At 2 with SoftCap=4: should climb despite elevated probe bounce.
	target, ok := p.IncreaseTarget(2, 1, 32, st, now.Add(time.Second))
	if !ok || target <= 2 {
		t.Fatalf("expected climb toward SoftCap, got ok=%v target=%d", ok, target)
	}
	if target > st.SoftCap {
		t.Fatalf("calm climb must not exceed SoftCap=%d, got %d", st.SoftCap, target)
	}
}

func TestIncreaseTarget_probeBlockedByBounce(t *testing.T) {
	p := DefaultAIMDPolicy(6 * time.Second)
	st := &State{}
	now := time.Now()
	_ = SoftCapDecreaseTarget(5, 1, 32, st, now)
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now, time.Time{})
	_, ok := p.IncreaseTarget(st.SoftCap, 1, 32, st, now.Add(time.Second))
	if ok {
		t.Fatal("probe SoftCap+1 should be blocked while bounce active")
	}
	target, ok := p.IncreaseTarget(st.SoftCap, 1, 32, st, now.Add(st.ProbeBounce+time.Second))
	if !ok || target != st.SoftCap+1 {
		t.Fatalf("after bounce: want probe %d, got ok=%v target=%d", st.SoftCap+1, ok, target)
	}
}

func TestRatchetProbeBounce_doublesAndCoversRetryAfter(t *testing.T) {
	st := &State{}
	now := time.Now()
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now, time.Time{})
	if st.ProbeBounce != DefaultFirstBounceWait {
		t.Fatalf("first bounce=%v want %v", st.ProbeBounce, DefaultFirstBounceWait)
	}
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now.Add(time.Second), time.Time{})
	if st.ProbeBounce != 2*DefaultFirstBounceWait {
		t.Fatalf("second bounce=%v want %v", st.ProbeBounce, 2*DefaultFirstBounceWait)
	}
	until := now.Add(5 * time.Minute)
	st2 := &State{}
	st2.RatchetProbeBounce(6*time.Second, 10*time.Minute, now, until)
	want := 10 * time.Minute // max(30s, 2×5m server wait)
	if st2.ProbeBounce != want {
		t.Fatalf("retry-after bounce=%v want %v", st2.ProbeBounce, want)
	}
}

func TestSoftCapProbe_sustainRaisesRestingCap(t *testing.T) {
	p := DefaultAIMDPolicy(6 * time.Second)
	st := &State{}
	now := time.Now()
	_ = SoftCapDecreaseTarget(5, 1, 32, st, now)
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now, time.Time{})
	bounce := st.ProbeBounce
	probeAt := now.Add(bounce + time.Second)
	target, ok := p.IncreaseTarget(st.SoftCap, 1, 32, st, probeAt)
	if !ok || target != 5 {
		t.Fatalf("probe want 5, got ok=%v target=%d SoftCap=%d", ok, target, st.SoftCap)
	}
	st.ArmSoftCapProbe(target, probeAt)
	// Before sustain window: hold
	_, ok = p.IncreaseTarget(5, 1, 32, st, probeAt.Add(bounce/2))
	if ok {
		t.Fatal("should hold during sustain")
	}
	if st.SoftCap != 4 {
		t.Fatalf("SoftCap must stay 4 during sustain, got %d", st.SoftCap)
	}
	// After sustain: raise SoftCap to 5, clear bounce
	_, ok = p.IncreaseTarget(5, 1, 32, st, probeAt.Add(bounce+time.Second))
	if ok {
		t.Fatal("sustain completion rests this tick without further climb")
	}
	if st.SoftCap != 5 {
		t.Fatalf("after sustain SoftCap=%d want 5", st.SoftCap)
	}
	if st.ProbeBounce != 0 || st.SoftCapProbeActive() {
		t.Fatalf("bounce/probe should clear: bounce=%v armed=%v", st.ProbeBounce, st.SoftCapProbeActive())
	}
}

func TestSoftCapProbe_failBeforeSustain_noCeilingRaise(t *testing.T) {
	st := &State{}
	now := time.Now()
	_ = SoftCapDecreaseTarget(5, 1, 32, st, now)
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now, time.Time{})
	st.ArmSoftCapProbe(5, now.Add(st.ProbeBounce+time.Second))
	ceilingBefore := st.CeilingSafe
	softBefore := st.SoftCap
	// Fail mid-sustain at SoftCap+1
	_ = SoftCapDecreaseTarget(5, 1, 32, st, now.Add(st.ProbeBounce+2*time.Second))
	st.RatchetProbeBounce(6*time.Second, 10*time.Minute, now.Add(st.ProbeBounce+2*time.Second), time.Time{})
	if st.SoftCapProbeActive() {
		t.Fatal("probe should abort on throttle")
	}
	if st.SoftCap > softBefore {
		t.Fatalf("SoftCap must not rise on failed probe: before=%d after=%d", softBefore, st.SoftCap)
	}
	if st.CeilingSafe > ceilingBefore {
		t.Fatalf("ceiling must not rise on failed probe: before=%d after=%d", ceilingBefore, st.CeilingSafe)
	}
}

func TestIncreaseTarget_preFirstRL_climbsToMax(t *testing.T) {
	p := DefaultAIMDPolicy(6 * time.Second)
	st := &State{}
	now := time.Now()
	target, ok := p.IncreaseTarget(4, 1, 16, st, now)
	if !ok || target <= 4 {
		t.Fatalf("pre-RL climb failed: ok=%v target=%d", ok, target)
	}
	if st.CeilingSafe != 0 {
		t.Fatalf("ceiling should stay unset, got %d", st.CeilingSafe)
	}
}

func TestSoftCap_respectsHardBounds(t *testing.T) {
	st := &State{}
	got := SoftCapDecreaseTarget(3, 2, 4, st, time.Now())
	if got < 2 {
		t.Fatalf("must not drop below minWorkers: got %d", got)
	}
	st.CeilingSafe = 100
	st.SoftCap = 99
	st.ClampSoftCapToBounds(1, 8)
	if st.CeilingSafe != 8 || st.SoftCap > 8 {
		t.Fatalf("clamp to max: ceiling=%d SoftCap=%d", st.CeilingSafe, st.SoftCap)
	}
}

func TestClearSoftCapForHigherMax(t *testing.T) {
	st := &State{CeilingSafe: 4, SoftCap: 3, ProbeBounce: time.Minute, LastProbeBounce: time.Minute}
	st.ArmSoftCapProbe(4, time.Now())
	st.ClearSoftCapForHigherMax(32, 32)
	if st.SoftCap != 3 {
		t.Fatalf("same max must not clear SoftCap, got %d", st.SoftCap)
	}
	st.ClearSoftCapForHigherMax(32, 64)
	if st.CeilingSafe != 0 || st.SoftCap != 0 || st.SoftCapProbeActive() || st.ProbeBounce != 0 {
		t.Fatalf("raise should clear soft-cap: ceiling=%d soft=%d armed=%v bounce=%v",
			st.CeilingSafe, st.SoftCap, st.SoftCapProbeActive(), st.ProbeBounce)
	}
}
