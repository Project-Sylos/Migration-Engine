// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"fmt"
	"time"
)

// Bounce / probe timers:
//
// Soft-cap FS path uses ProbeBounce: paces only SoftCap+1 probes (not recovery to SoftCap).
// Efficiency probes (when Enabled) keep EffectiveProbeCooldown via RatchetProbeBackoff/HalveProbeBackoff.
//
// On FS_THROTTLE: SoftCap drop + RatchetProbeBounce(max(2×last, 2×serverWait)).

const (
	DefaultFirstBounceWait = 30 * time.Second
	DefaultBounceCushion   = 30 * time.Second
)

// ProbeCooldownDuration returns the efficiency-probe climb cooldown (ratcheted or base).
func (st *State) ProbeCooldownDuration(base time.Duration) time.Duration {
	if st == nil || st.EffectiveProbeCooldown <= 0 {
		return base
	}
	if st.EffectiveProbeCooldown > base {
		return st.EffectiveProbeCooldown
	}
	return base
}

// HalveProbeBackoff AIMD-recovers the efficiency bounce wait after a successful
// efficiency probe. Halves the effective wait and stamps LastDecrease.
func (st *State) HalveProbeBackoff(base time.Duration, now time.Time) {
	if st == nil {
		return
	}
	cur := st.ProbeCooldownDuration(base)
	if cur <= 0 {
		cur = base
	}
	next := cur / 2
	if next <= base {
		st.EffectiveProbeCooldown = 0
		st.FailedProbes = 0
	} else {
		st.EffectiveProbeCooldown = next
	}
	st.LastDecrease = now
}

// RatchetProbeBackoff elevates the efficiency-probe bounce wait after an efficiency miss.
func (st *State) RatchetProbeBackoff(base, max time.Duration) {
	if st == nil {
		return
	}
	var next time.Duration
	if st.EffectiveProbeCooldown <= 0 {
		next = DefaultFirstBounceWait
		if base > next {
			next = base
		}
		if next <= 0 {
			next = DefaultFirstBounceWait
		}
	} else {
		next = st.EffectiveProbeCooldown * 2
	}
	if max > 0 && next > max {
		next = max
	}
	st.EffectiveProbeCooldown = next
	st.FailedProbes++
}

// RatchetProbeBounce elevates the FS soft-cap probe wait after throttle / probe failure.
// next = max(2×LastProbeBounce, 2×serverWait); first elevation also floors at max(30s, base).
func (st *State) RatchetProbeBounce(base, max time.Duration, now time.Time, rateLimitedUntil time.Time) {
	if st == nil {
		return
	}
	serverWait := time.Duration(0)
	if !rateLimitedUntil.IsZero() && rateLimitedUntil.After(now) {
		serverWait = rateLimitedUntil.Sub(now)
	}
	next := 2 * st.LastProbeBounce
	if tw := 2 * serverWait; tw > next {
		next = tw
	}
	if st.LastProbeBounce == 0 {
		first := DefaultFirstBounceWait
		if base > first {
			first = base
		}
		if first <= 0 {
			first = DefaultFirstBounceWait
		}
		if next < first {
			next = first
		}
	}
	// Also cover Retry-After + cushion when larger than the doubled schedule.
	if serverWait > 0 {
		need := serverWait + DefaultBounceCushion
		if need > next {
			next = need
		}
	}
	if max > 0 && next > max {
		next = max
	}
	st.ProbeBounce = next
	st.LastProbeBounce = next
	st.LastProbeFail = now
	st.AbortSoftCapProbe()
}

// FormatProbeCooldownLine formats AIMD probe cooldown status for debug logs.
func FormatProbeCooldownLine(scope string, st *State, base time.Duration, now time.Time) string {
	_, remaining, active := st.ProbeCooldownStatus(base, now)
	ssthresh := 0
	failed := 0
	softCap, ceiling := 0, 0
	probeBounce := time.Duration(0)
	if st != nil {
		ssthresh = st.Ssthresh
		failed = st.FailedProbes
		softCap = st.SoftCap
		ceiling = st.CeilingSafe
		probeBounce = st.ProbeBounce
	}
	extra := ""
	if ceiling > 0 {
		extra = fmt.Sprintf(" softCap=%d ceiling=%d probeBounce=%s", softCap, ceiling, probeBounce.Round(time.Millisecond))
	}
	if active {
		return fmt.Sprintf("  aimd [%s]: next worker probe in %s (ssthresh=%d failed_probes=%d%s)", scope, remaining.Round(time.Millisecond), ssthresh, failed, extra)
	}
	return fmt.Sprintf("  aimd [%s]: worker probe eligible (ssthresh=%d%s)", scope, ssthresh, extra)
}
