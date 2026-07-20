// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"fmt"
	"time"
)

// Bounce timer: second AIMD axis independent of ssthresh.
//
// ssthresh answers "how high can I go"; the bounce wait answers "how soon may I
// try climbing again" after FS_THROTTLE (and, when enabled, efficiency-probe misses).
// Shape: first elevation uses max(base ProbeCooldown, defaultFirstBounceWait), then
// doubles on each miss (capped), halves on calm recovery, clears when ≤ base.
// When the FS reports Retry-After, bounce is also raised to cover that window plus cushion
// so we do not climb the instant a multi-minute Dropbox ban clears.

const (
	defaultFirstBounceWait = 30 * time.Second
	defaultBounceCushion   = 30 * time.Second
)

// probeCooldownDuration returns the effective climb cooldown (ratcheted or base).
func (st *queueAIMDState) probeCooldownDuration(base time.Duration) time.Duration {
	if st == nil || st.effectiveProbeCooldown <= 0 {
		return base
	}
	if st.effectiveProbeCooldown > base {
		return st.effectiveProbeCooldown
	}
	return base
}

// halveProbeBackoff AIMD-recovers the bounce wait after calm climb (or a successful
// efficiency probe). Halves the effective wait and stamps lastDecrease so the next
// climb respects it. Clears elevation when halved value is at or below base.
func (st *queueAIMDState) halveProbeBackoff(base time.Duration, now time.Time) {
	if st == nil {
		return
	}
	cur := st.probeCooldownDuration(base)
	if cur <= 0 {
		cur = base
	}
	next := cur / 2
	if next <= base {
		st.effectiveProbeCooldown = 0
		st.failedProbes = 0
	} else {
		st.effectiveProbeCooldown = next
	}
	st.lastDecrease = now
}

// ratchetProbeBackoff elevates the bounce wait after FS_THROTTLE (or an efficiency miss).
// First elevation: max(base, defaultFirstBounceWait). Subsequent: double, capped at max.
func (st *queueAIMDState) ratchetProbeBackoff(base, max time.Duration) {
	if st == nil {
		return
	}
	var next time.Duration
	if st.effectiveProbeCooldown <= 0 {
		next = defaultFirstBounceWait
		if base > next {
			next = base
		}
		if next <= 0 {
			next = defaultFirstBounceWait
		}
	} else {
		next = st.effectiveProbeCooldown * 2
	}
	if max > 0 && next > max {
		next = max
	}
	st.effectiveProbeCooldown = next
	st.failedProbes++
}

func (a *Autoscaler) bounceMaxCooldown() time.Duration {
	if a == nil {
		return 10 * time.Minute
	}
	if a.efficiency.MaxProbeCooldown > 0 {
		return a.efficiency.MaxProbeCooldown
	}
	return 10 * time.Minute
}

// noteThrottleBounce elevates the universal bounce timer after a real FS_THROTTLE
// actuation (worker decrease or inter-op delay increase). Always on — independent of
// EfficiencyProbe.Enabled. Stamps lastDecrease so IncreaseTarget respects the wait.
// When rateLimitedUntil is set, bounce covers at least that window + cushion.
func (a *Autoscaler) noteThrottleBounce(st *queueAIMDState, now time.Time, rateLimitedUntil time.Time) {
	if a == nil || st == nil {
		return
	}
	max := a.bounceMaxCooldown()
	st.ratchetProbeBackoff(a.aimd.ProbeCooldown, max)
	if !rateLimitedUntil.IsZero() && rateLimitedUntil.After(now) {
		need := rateLimitedUntil.Sub(now) + defaultBounceCushion
		if need > st.effectiveProbeCooldown {
			st.effectiveProbeCooldown = need
			if max > 0 && st.effectiveProbeCooldown > max {
				st.effectiveProbeCooldown = max
			}
		}
	}
	st.lastDecrease = now
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  bounce ratchet: wait=%s failed=%d",
			st.probeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond), st.failedProbes))
	}
}

// maybeHalveBounceAfterCalmClimb decays bounce after a successful worker scale-up when
// efficiency probing is off (probes own success/fail when Enabled).
func (a *Autoscaler) maybeHalveBounceAfterCalmClimb(st *queueAIMDState, now time.Time) {
	if a == nil || st == nil || a.efficiency.Enabled {
		return
	}
	if st.effectiveProbeCooldown <= 0 {
		return
	}
	st.halveProbeBackoff(a.aimd.ProbeCooldown, now)
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  bounce halve after climb: wait=%s",
			st.probeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond)))
	}
}

// maybeDecayBounceAtCeiling halves an elevated bounce when calm, wait elapsed, and
// workers are already at max (so climb cannot drain the timer).
func (a *Autoscaler) maybeDecayBounceAtCeiling(st *queueAIMDState, cur, maxWorkers int, now time.Time) {
	if a == nil || st == nil || a.efficiency.Enabled {
		return
	}
	if st.effectiveProbeCooldown <= 0 || maxWorkers <= 0 || cur < maxWorkers {
		return
	}
	cooldown := st.probeCooldownDuration(a.aimd.ProbeCooldown)
	if cooldown > 0 && !st.lastDecrease.IsZero() && now.Sub(st.lastDecrease) < cooldown {
		return
	}
	st.halveProbeBackoff(a.aimd.ProbeCooldown, now)
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  bounce decay at ceiling: wait=%s",
			st.probeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond)))
	}
}
