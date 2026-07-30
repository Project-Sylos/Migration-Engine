// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/aimd"
)

func (a *Autoscaler) bounceMaxCooldown() time.Duration {
	if a == nil {
		return 10 * time.Minute
	}
	if a.efficiency.MaxProbeCooldown > 0 {
		return a.efficiency.MaxProbeCooldown
	}
	return 10 * time.Minute
}

// noteSoftCapThrottle updates soft-cap ceiling memory and ratchets FS probe bounce
// after a real FS_THROTTLE worker drop (or inter-op increase at floor).
func (a *Autoscaler) noteSoftCapThrottle(st *aimd.State, now time.Time, rateLimitedUntil time.Time) {
	if a == nil || st == nil {
		return
	}
	st.RatchetProbeBounce(a.aimd.ProbeCooldown, a.bounceMaxCooldown(), now, rateLimitedUntil)
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  soft-cap probe bounce: wait=%s ceiling=%d softCap=%d",
			st.ProbeBounce.Round(time.Millisecond), st.CeilingSafe, st.SoftCap))
	}
}

// noteThrottleBounce is kept as an alias for efficiency-abort paths that still expect
// a universal bounce stamp; FS worker drops should call noteSoftCapThrottle instead.
// When soft-cap is active it only ratchets ProbeBounce; otherwise elevates efficiency timer
// for back-compat with tests that call this directly.
func (a *Autoscaler) noteThrottleBounce(st *aimd.State, now time.Time, rateLimitedUntil time.Time) {
	if a == nil || st == nil {
		return
	}
	if st.CeilingSafe > 0 || st.SoftCap > 0 {
		a.noteSoftCapThrottle(st, now, rateLimitedUntil)
		return
	}
	// Pre-ceiling discovery: still pace climbs via efficiency/universal timer.
	max := a.bounceMaxCooldown()
	st.RatchetProbeBackoff(a.aimd.ProbeCooldown, max)
	if !rateLimitedUntil.IsZero() && rateLimitedUntil.After(now) {
		need := rateLimitedUntil.Sub(now) + aimd.DefaultBounceCushion
		if need > st.EffectiveProbeCooldown {
			st.EffectiveProbeCooldown = need
			if max > 0 && st.EffectiveProbeCooldown > max {
				st.EffectiveProbeCooldown = max
			}
		}
	}
	st.LastDecrease = now
	// Also seed soft-cap probe bounce so the first post-RL probe is patient.
	st.RatchetProbeBounce(a.aimd.ProbeCooldown, max, now, rateLimitedUntil)
	if a.debugAIMD {
		a.debugAIMDPrint(fmt.Sprintf("  bounce ratchet: wait=%s probeBounce=%s failed=%d",
			st.ProbeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond),
			st.ProbeBounce.Round(time.Millisecond), st.FailedProbes))
	}
}
