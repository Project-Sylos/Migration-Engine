// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"fmt"
	"os"
	"time"
)

func aimdDebugEnabled(cfg bool) bool {
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

func (st *queueAIMDState) probeCooldownStatus(base time.Duration, now time.Time) (effective, remaining time.Duration, active bool) {
	if st == nil {
		return base, 0, false
	}
	effective = st.probeCooldownDuration(base)
	if effective <= 0 || st.lastDecrease.IsZero() {
		return effective, 0, false
	}
	elapsed := now.Sub(st.lastDecrease)
	if elapsed >= effective {
		return effective, 0, false
	}
	return effective, effective - elapsed, true
}

func formatProbeCooldownLine(scope string, st *queueAIMDState, base time.Duration, now time.Time) string {
	_, remaining, active := st.probeCooldownStatus(base, now)
	ssthresh := 0
	failed := 0
	if st != nil {
		ssthresh = st.ssthresh
		failed = st.failedProbes
	}
	if active {
		return fmt.Sprintf("  aimd [%s]: next worker probe in %s (ssthresh=%d failed_probes=%d)", scope, remaining.Round(time.Millisecond), ssthresh, failed)
	}
	return fmt.Sprintf("  aimd [%s]: worker probe eligible (ssthresh=%d)", scope, ssthresh)
}

func increaseTargetBlockReason(cur, maxWorkers int, st *queueAIMDState, p AIMDPolicy, now time.Time) string {
	if cur >= maxWorkers {
		return "at_max_workers"
	}
	if _, remaining, active := st.probeCooldownStatus(p.ProbeCooldown, now); active {
		return fmt.Sprintf("probe_cooldown remaining=%s", remaining.Round(time.Millisecond))
	}
	return "increase_target_unchanged"
}

func (a *Autoscaler) debugAIMDPrint(msg string) {
	if a == nil || !a.debugAIMD {
		return
	}
	fmt.Println(msg)
}

func (a *Autoscaler) debugAfterWorkerDecrease(scope string, st *queueAIMDState, cur, target int, now time.Time) {
	if !a.debugAIMD {
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: workers %d->%d (step-down)", scope, cur, target))
	a.debugAIMDPrint(formatProbeCooldownLine(scope, st, a.aimd.ProbeCooldown, now))
}

func (a *Autoscaler) debugWorkerScaleUpAttempt(scope, detail string) {
	if !a.debugAIMD {
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: worker scale-up attempt — %s", scope, detail))
}

func (a *Autoscaler) debugWorkerScaleUpResult(scope string, workersBefore, workersAfter int, detail string) {
	if !a.debugAIMD {
		return
	}
	if workersAfter > workersBefore {
		a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: worker scale-up OK %d->%d (%s)", scope, workersBefore, workersAfter, detail))
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: worker scale-up skipped (%s)", scope, detail))
}

func (a *Autoscaler) debugScaleUpBlocked(scope, reason string) {
	if !a.debugAIMD || reason == "" {
		return
	}
	a.debugWorkerScaleUpResult(scope, 0, 0, reason)
}

func (a *Autoscaler) debugScaleUpProbe(scope string, st *queueAIMDState, workersBefore int, target int, rateBefore float64) {
	if !a.debugAIMD {
		return
	}
	mode := "slow_start"
	if st != nil && st.ssthresh > 0 && workersBefore >= st.ssthresh {
		mode = "additive"
	}
	ssthresh := 0
	if st != nil {
		ssthresh = st.ssthresh
	}
	a.debugWorkerScaleUpResult(scope, workersBefore, target, fmt.Sprintf("%s ssthresh=%d rate_before=%.2f", mode, ssthresh, rateBefore))
}

func (a *Autoscaler) debugEfficiencyProbeResult(scope string, st *queueAIMDState, workersNow int, rateNow float64, rollback int, failed bool) {
	if !a.debugAIMD {
		return
	}
	if failed {
		a.debugWorkerScaleUpResult(scope, workersNow, rollback, fmt.Sprintf("efficiency probe FAILED rate_now=%.2f ratchet_cooldown=%s", rateNow, st.probeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond)))
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: efficiency probe PASSED workers=%d rate_now=%.2f", scope, workersNow, rateNow))
}

func (a *Autoscaler) debugFSBackoffBlocked(scope string, st *queueAIMDState, now, rateLimitedUntil time.Time) {
	if !a.debugAIMD {
		return
	}
	remaining := fsBackoffRemaining(st, now, rateLimitedUntil)
	if remaining <= 0 {
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: FS backoff active (%s remaining, retry-after until %s)",
		scope, remaining.Round(time.Millisecond), rateLimitedUntil.Format(time.RFC3339)))
}
