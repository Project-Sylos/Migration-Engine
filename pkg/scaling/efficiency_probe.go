// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "time"

// EfficiencyProbeConfig tunes throughput-aware scale-up probing (second-order AIMD).
type EfficiencyProbeConfig struct {
	// MinEfficiencyRatio is minimum rate_gain/worker_gain required to accept a probe (default 0.08).
	MinEfficiencyRatio float64
	// MinProbeWindow is how long to wait after a scale-up before judging efficiency (default 0 = one autoscaler tick).
	MinProbeWindow time.Duration
	// MaxProbeCooldown caps ratcheted probe cooldown (default 10m).
	MaxProbeCooldown time.Duration
	// SsthreshRecoveryInterval raises ssthresh by 1 after this much calm stability (default 5m).
	SsthreshRecoveryInterval time.Duration
}

func (c EfficiencyProbeConfig) normalized(interval time.Duration) EfficiencyProbeConfig {
	out := c
	if out.MinEfficiencyRatio <= 0 {
		out.MinEfficiencyRatio = 0.08
	}
	if out.MinProbeWindow <= 0 {
		out.MinProbeWindow = interval
		if out.MinProbeWindow <= 0 {
			out.MinProbeWindow = 10 * time.Second
		}
	}
	if out.MaxProbeCooldown <= 0 {
		out.MaxProbeCooldown = 10 * time.Minute
	}
	if out.SsthreshRecoveryInterval <= 0 {
		out.SsthreshRecoveryInterval = 5 * time.Minute
	}
	return out
}

// probeCooldownDuration returns the effective cooldown (ratcheted or base).
func (st *queueAIMDState) probeCooldownDuration(base time.Duration) time.Duration {
	if st == nil || st.effectiveProbeCooldown <= 0 {
		return base
	}
	if st.effectiveProbeCooldown > base {
		return st.effectiveProbeCooldown
	}
	return base
}

func (st *queueAIMDState) resetProbeBackoff() {
	if st == nil {
		return
	}
	st.effectiveProbeCooldown = 0
	st.failedProbes = 0
}

func (st *queueAIMDState) ratchetProbeBackoff(base, max time.Duration) {
	if st == nil {
		return
	}
	cur := st.probeCooldownDuration(base)
	next := cur * 2
	if next > max {
		next = max
	}
	st.effectiveProbeCooldown = next
	st.failedProbes++
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
func (st *queueAIMDState) evaluateEfficiencyProbe(workersNow int, rateNow float64, baseCooldown time.Duration, cfg EfficiencyProbeConfig) (rollback int, ok bool) {
	if st == nil || !st.probePending {
		return 0, false
	}
	if cfg.MinProbeWindow > 0 && time.Since(st.probeStarted) < cfg.MinProbeWindow {
		return 0, false
	}
	st.probePending = false

	if st.probeWorkersBefore <= 0 {
		st.probePending = false
		return 0, false
	}
	if workersNow <= st.probeWorkersBefore {
		// Scale-up did not take effect (or wrong worker scope was passed).
		st.probePending = false
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
		st.resetProbeBackoff()
		return 0, false
	}

	st.ratchetProbeBackoff(baseCooldown, cfg.MaxProbeCooldown)
	rollback = st.probeWorkersBefore
	if rollback < 1 {
		rollback = 1
	}
	if st.ssthresh > rollback {
		st.ssthresh = rollback
	}
	return rollback, true
}

// abortEfficiencyProbeThrottled records a failed probe when pressure (e.g. FS throttle)
// cuts workers before the efficiency window completes.
func (st *queueAIMDState) abortEfficiencyProbeThrottled(workersAfter int, baseCooldown, maxCooldown time.Duration) {
	if st == nil || !st.probePending {
		return
	}
	st.probePending = false
	st.ratchetProbeBackoff(baseCooldown, maxCooldown)
	if workersAfter > 0 && st.ssthresh > workersAfter {
		st.ssthresh = workersAfter
	}
}
