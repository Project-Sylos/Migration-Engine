// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "time"

// EfficiencyProbeConfig tunes throughput-aware scale-up probing (second-order AIMD).
type EfficiencyProbeConfig struct {
	// Enabled turns on throughput probes after scale-up. Default false — AIMD still
	// respects operation MaxWorkers ceilings, FS_THROTTLE step-down, and the universal
	// bounce timer without probes.
	Enabled bool
	// MinEfficiencyRatio is minimum rate_gain/worker_gain required to accept a probe (default 0.08).
	MinEfficiencyRatio float64
	// MinProbeWindow is how long to wait after a scale-up before judging efficiency (default 15s).
	MinProbeWindow time.Duration
	// MaxProbeCooldown caps the shared bounce timer (FS_THROTTLE + efficiency misses; default 10m).
	// Applies even when Enabled=false — throttle bounce still uses this ceiling.
	MaxProbeCooldown time.Duration
	// SsthreshRecoveryInterval raises ssthresh by 1 after this much calm stability (default 5m).
	SsthreshRecoveryInterval time.Duration
}

const defaultMinProbeWindow = 15 * time.Second

func (c EfficiencyProbeConfig) normalized(interval time.Duration) EfficiencyProbeConfig {
	_ = interval // probe window is wall-clock, not tied to autoscaler tick
	out := c
	if out.MinEfficiencyRatio <= 0 {
		out.MinEfficiencyRatio = 0.08
	}
	if out.MinProbeWindow <= 0 {
		out.MinProbeWindow = defaultMinProbeWindow
	}
	if out.MaxProbeCooldown <= 0 {
		out.MaxProbeCooldown = 10 * time.Minute
	}
	if out.SsthreshRecoveryInterval <= 0 {
		out.SsthreshRecoveryInterval = 5 * time.Minute
	}
	return out
}

func (st *queueAIMDState) noteStability(underPressure bool, now time.Time) {
	if st == nil {
		return
	}
	if underPressure {
		st.lastStable = time.Time{}
		return
	}
	if st.lastStable.IsZero() {
		st.lastStable = now
	}
}

func (st *queueAIMDState) maybeRecoverSsthresh(now time.Time, maxWorkers int, cfg EfficiencyProbeConfig) {
	if st == nil || st.ssthresh <= 0 || st.ssthresh >= maxWorkers {
		return
	}
	if st.lastStable.IsZero() || now.Sub(st.lastStable) < cfg.SsthreshRecoveryInterval {
		return
	}
	st.ssthresh++
	if st.ssthresh > maxWorkers {
		st.ssthresh = maxWorkers
	}
	st.lastStable = now
}

func (st *queueAIMDState) startEfficiencyProbe(workersBefore int, rateBefore float64, now time.Time) {
	if st == nil {
		return
	}
	st.probePending = true
	st.probeWorkersBefore = workersBefore
	st.probeRateBefore = rateBefore
	st.probeStarted = now
}

// evaluateEfficiencyProbe returns rollback target when the last scale-up did not improve throughput enough.
// ok=false means no rollback; ok=true means rollback to target.
// Efficiency misses bounce workers and double the probe wait without lowering ssthresh.
// Successes halve the probe wait (AIMD recovery) instead of resetting to zero.
func (st *queueAIMDState) evaluateEfficiencyProbe(workersNow int, rateNow float64, baseCooldown time.Duration, cfg EfficiencyProbeConfig, now time.Time) (rollback int, ok bool) {
	if st == nil || !st.probePending {
		return 0, false
	}
	if cfg.MinProbeWindow > 0 && now.Sub(st.probeStarted) < cfg.MinProbeWindow {
		return 0, false
	}
	st.probePending = false

	if st.probeWorkersBefore <= 0 {
		return 0, false
	}
	if workersNow <= st.probeWorkersBefore {
		// Scale-up did not take effect (or wrong worker scope was passed).
		return 0, false
	}

	workerGain := float64(workersNow-st.probeWorkersBefore) / float64(st.probeWorkersBefore)
	if workerGain <= 0 {
		return 0, false
	}

	var rateGain float64
	switch {
	case st.probeRateBefore > 0:
		rateGain = (rateNow - st.probeRateBefore) / st.probeRateBefore
	case rateNow > 0:
		rateGain = 1
	default:
		rateGain = -1
	}

	efficiency := rateGain / workerGain
	if efficiency >= cfg.MinEfficiencyRatio {
		st.halveProbeBackoff(baseCooldown, now)
		return 0, false
	}

	st.ratchetProbeBackoff(baseCooldown, cfg.MaxProbeCooldown)
	st.lastDecrease = now
	rollback = st.probeWorkersBefore
	if rollback < 1 {
		rollback = 1
	}
	return rollback, true
}

// abortEfficiencyProbeThrottled clears an in-flight efficiency probe when FS throttle
// cuts workers before the window completes. Does not ratchet bounce — callers must
// already have called noteThrottleBounce for the FS_THROTTLE actuation.
func (st *queueAIMDState) abortEfficiencyProbeThrottled(workersAfter int, _ time.Duration, _ time.Duration, now time.Time) {
	if st == nil || !st.probePending {
		return
	}
	st.probePending = false
	st.lastDecrease = now
	if workersAfter > 0 && st.ssthresh > workersAfter {
		st.ssthresh = workersAfter
	}
}

// scaleUpBlockedByRateLimit is true while an FS retry-after window is still active.
func scaleUpBlockedByRateLimit(now, rateLimitedUntil time.Time) bool {
	return !rateLimitedUntil.IsZero() && now.Before(rateLimitedUntil)
}
