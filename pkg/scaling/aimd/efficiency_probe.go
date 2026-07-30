// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

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
	// SsthreshRecoveryInterval raises Ssthresh by 1 after this much calm stability (default 5m).
	SsthreshRecoveryInterval time.Duration
}

const defaultMinProbeWindow = 15 * time.Second

func (c EfficiencyProbeConfig) Normalized(interval time.Duration) EfficiencyProbeConfig {
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

func (st *State) NoteStability(underPressure bool, now time.Time) {
	if st == nil {
		return
	}
	if underPressure {
		st.LastStable = time.Time{}
		return
	}
	if st.LastStable.IsZero() {
		st.LastStable = now
	}
}

func (st *State) MaybeRecoverSsthresh(now time.Time, maxWorkers int, cfg EfficiencyProbeConfig) {
	if st == nil || st.Ssthresh <= 0 || st.Ssthresh >= maxWorkers {
		return
	}
	if st.LastStable.IsZero() || now.Sub(st.LastStable) < cfg.SsthreshRecoveryInterval {
		return
	}
	st.Ssthresh++
	if st.Ssthresh > maxWorkers {
		st.Ssthresh = maxWorkers
	}
	st.LastStable = now
}

func (st *State) StartEfficiencyProbe(workersBefore int, rateBefore float64, now time.Time) {
	if st == nil {
		return
	}
	st.ProbePending = true
	st.ProbeWorkersBefore = workersBefore
	st.ProbeRateBefore = rateBefore
	st.ProbeStarted = now
}

// EvaluateEfficiencyProbe returns rollback target when the last scale-up did not improve throughput enough.
// ok=false means no rollback; ok=true means rollback to target.
// Efficiency misses bounce workers and double the probe wait without lowering Ssthresh.
// Successes halve the probe wait (AIMD recovery) instead of resetting to zero.
func (st *State) EvaluateEfficiencyProbe(workersNow int, rateNow float64, baseCooldown time.Duration, cfg EfficiencyProbeConfig, now time.Time) (rollback int, ok bool) {
	if st == nil || !st.ProbePending {
		return 0, false
	}
	if cfg.MinProbeWindow > 0 && now.Sub(st.ProbeStarted) < cfg.MinProbeWindow {
		return 0, false
	}
	st.ProbePending = false

	if st.ProbeWorkersBefore <= 0 {
		return 0, false
	}
	if workersNow <= st.ProbeWorkersBefore {
		// Scale-up did not take effect (or wrong worker scope was passed).
		return 0, false
	}

	workerGain := float64(workersNow-st.ProbeWorkersBefore) / float64(st.ProbeWorkersBefore)
	if workerGain <= 0 {
		return 0, false
	}

	var rateGain float64
	switch {
	case st.ProbeRateBefore > 0:
		rateGain = (rateNow - st.ProbeRateBefore) / st.ProbeRateBefore
	case rateNow > 0:
		rateGain = 1
	default:
		rateGain = -1
	}

	efficiency := rateGain / workerGain
	if efficiency >= cfg.MinEfficiencyRatio {
		st.HalveProbeBackoff(baseCooldown, now)
		return 0, false
	}

	st.RatchetProbeBackoff(baseCooldown, cfg.MaxProbeCooldown)
	st.LastDecrease = now
	rollback = st.ProbeWorkersBefore
	if rollback < 1 {
		rollback = 1
	}
	return rollback, true
}

// AbortEfficiencyProbeThrottled clears an in-flight efficiency probe when FS throttle
// cuts workers before the window completes. Does not ratchet bounce — callers must
// already have called noteSoftCapThrottle / noteThrottleBounce for the FS_THROTTLE actuation.
func (st *State) AbortEfficiencyProbeThrottled(workersAfter int, _ time.Duration, _ time.Duration, now time.Time) {
	if st == nil || !st.ProbePending {
		return
	}
	st.ProbePending = false
	st.LastDecrease = now
	if workersAfter > 0 && st.Ssthresh > workersAfter {
		st.Ssthresh = workersAfter
	}
}

// ScaleUpBlockedByRateLimit is true while an FS retry-after window is still active.
func ScaleUpBlockedByRateLimit(now, rateLimitedUntil time.Time) bool {
	return !rateLimitedUntil.IsZero() && now.Before(rateLimitedUntil)
}
