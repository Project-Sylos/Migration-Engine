// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/aimd"
)

func (a *Autoscaler) debugAIMDPrint(msg string) {
	if a == nil || !a.debugAIMD {
		return
	}
	fmt.Println(msg)
}

func (a *Autoscaler) debugAfterWorkerDecrease(scope string, st *aimd.State, cur, target int, now time.Time) {
	if !a.debugAIMD {
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: workers %d->%d (step-down)", scope, cur, target))
	a.debugAIMDPrint(aimd.FormatProbeCooldownLine(scope, st, a.aimd.ProbeCooldown, now))
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

func (a *Autoscaler) debugScaleUpProbe(scope string, st *aimd.State, workersBefore int, target int, rateBefore float64) {
	if !a.debugAIMD {
		return
	}
	mode := "slow_start"
	if st != nil && st.Ssthresh > 0 && workersBefore >= st.Ssthresh {
		mode = "additive"
	}
	ssthresh := 0
	if st != nil {
		ssthresh = st.Ssthresh
	}
	a.debugWorkerScaleUpResult(scope, workersBefore, target, fmt.Sprintf("%s ssthresh=%d rate_before=%.2f", mode, ssthresh, rateBefore))
}

func (a *Autoscaler) debugEfficiencyProbeResult(scope string, st *aimd.State, workersNow int, rateNow float64, rollback int, failed bool) {
	if !a.debugAIMD {
		return
	}
	if failed {
		a.debugWorkerScaleUpResult(scope, workersNow, rollback, fmt.Sprintf("efficiency probe FAILED rate_now=%.2f ratchet_cooldown=%s", rateNow, st.ProbeCooldownDuration(a.aimd.ProbeCooldown).Round(time.Millisecond)))
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: efficiency probe PASSED workers=%d rate_now=%.2f", scope, workersNow, rateNow))
}

func (a *Autoscaler) debugFSBackoffBlocked(scope string, st *aimd.State, now, rateLimitedUntil time.Time) {
	if !a.debugAIMD {
		return
	}
	remaining := aimd.FSBackoffRemaining(st, now, rateLimitedUntil)
	if remaining <= 0 {
		return
	}
	a.debugAIMDPrint(fmt.Sprintf("  aimd [%s]: FS backoff active (%s remaining, retry-after until %s)",
		scope, remaining.Round(time.Millisecond), rateLimitedUntil.Format(time.RFC3339)))
}
