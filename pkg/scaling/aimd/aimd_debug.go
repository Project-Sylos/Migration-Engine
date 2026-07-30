// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"fmt"
	"os"
	"time"
)

// DebugEnabled reports whether AIMD debug printing is on (cfg or ME_AUTOSCALER_DEBUG_AIMD).
func DebugEnabled(cfg bool) bool {
	if cfg {
		return true
	}
	switch os.Getenv("ME_AUTOSCALER_DEBUG_AIMD") {
	case "1", "true", "TRUE", "yes", "YES":
		return true
	default:
		return false
	}
}

func (st *State) ProbeCooldownStatus(base time.Duration, now time.Time) (effective, remaining time.Duration, active bool) {
	if st == nil {
		return base, 0, false
	}
	effective = st.ProbeCooldownDuration(base)
	if effective <= 0 || st.LastDecrease.IsZero() {
		return effective, 0, false
	}
	elapsed := now.Sub(st.LastDecrease)
	if elapsed >= effective {
		return effective, 0, false
	}
	return effective, effective - elapsed, true
}

// IncreaseTargetBlockReason explains why IncreaseTarget returned ok=false (for debug).
func IncreaseTargetBlockReason(cur, maxWorkers int, st *State, p AIMDPolicy, now time.Time) string {
	if cur >= maxWorkers {
		return "at_max_workers"
	}
	if st != nil && st.CeilingSafe > 0 && st.SoftCap > 0 {
		if st.SoftCapProbeActive() && cur >= st.ProbeLevel {
			need := st.SustainDurationForProbe()
			elapsed := now.Sub(st.ProbeArmedAt)
			if need > 0 && elapsed < need {
				return fmt.Sprintf("soft_cap_sustain remaining=%s", (need - elapsed).Round(time.Millisecond))
			}
		}
		if cur == st.SoftCap && st.SoftCap < maxWorkers && !st.ProbeBounceElapsed(now) {
			rem := st.ProbeBounce - now.Sub(st.LastProbeFail)
			if rem < 0 {
				rem = 0
			}
			return fmt.Sprintf("soft_cap_probe_bounce remaining=%s SoftCap=%d", rem.Round(time.Millisecond), st.SoftCap)
		}
		if cur >= st.SoftCap {
			return fmt.Sprintf("at_soft_cap SoftCap=%d ceiling=%d", st.SoftCap, st.CeilingSafe)
		}
	}
	if _, remaining, active := st.ProbeCooldownStatus(p.ProbeCooldown, now); active {
		return fmt.Sprintf("probe_cooldown remaining=%s", remaining.Round(time.Millisecond))
	}
	return "increase_target_unchanged"
}
