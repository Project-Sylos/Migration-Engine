// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"math"
	"time"
)

const interOpClearThreshold = time.Millisecond // below this, clear delay and hand off to worker scale-up

// AIMDPolicy implements TCP Reno-style worker scaling:
//   - multiplicative decrease on pressure (halve by default)
//   - slow-start (exponential climb) below ssthresh
//   - additive increase at/above ssthresh
//   - probe cooldown after a decrease before climbing again
//
// When workers are at MinWorkers, the same AIMD shape applies to inter-op delay
// (multiply delay up on pressure, halve toward zero on calm) as a fallback lever.
// The first delay is derived from observed throughput (halves effective op rate); see InterOpDelayFromThroughput.
type AIMDPolicy struct {
	DecreaseFactor      float64       // multiplicative factor on decrease (default 0.5)
	AdditiveStep        int           // workers added per tick in congestion avoidance (default 1)
	ProbeCooldown       time.Duration // calm period after decrease before probing up
	InitialInterOpDelay time.Duration // fallback seed when throughput is unknown (default 1ms)
	MinInterOpDelayStep time.Duration // minimum delay step when multiplicative increase stalls (default 100µs)
}

// DefaultAIMDPolicy returns TCP-like defaults for worker scaling.
func DefaultAIMDPolicy(probeCooldown time.Duration) AIMDPolicy {
	if probeCooldown <= 0 {
		probeCooldown = 30 * time.Second
	}
	return AIMDPolicy{
		DecreaseFactor:      0.5,
		AdditiveStep:        1,
		ProbeCooldown:       probeCooldown,
		InitialInterOpDelay: time.Millisecond,
		MinInterOpDelayStep: 100 * time.Microsecond,
	}
}

const minInterOpDelayFromThroughput = 100 * time.Microsecond

// InterOpDelayFromThroughput returns pacing delay that halves the observed FS op rate (single worker).
// opsPerSec should be task completions/sec (list/copy calls), not items discovered/sec.
// delay ≈ 2 / opsPerSec — e.g. 1000 ops/s → 2ms (~500 ops/s cap).
func InterOpDelayFromThroughput(opsPerSec float64) time.Duration {
	if opsPerSec <= 0 {
		return 0
	}
	d := time.Duration((2.0 / opsPerSec) * float64(time.Second))
	if d < minInterOpDelayFromThroughput {
		return minInterOpDelayFromThroughput
	}
	return d
}

// FSOpRateForInterOpDelay picks the FS op rate used to seed inter-op pacing.
// Uses per-worker task completion rate and the peak seen before inter-op delay was applied,
// so throttled crawl rates do not produce absurd multi-second delays.
func FSOpRateForInterOpDelay(currentRate float64, workers int, peakPerWorker float64) float64 {
	if workers <= 0 {
		workers = 1
	}
	perWorker := currentRate / float64(workers)
	if peakPerWorker > perWorker {
		perWorker = peakPerWorker
	}
	return perWorker * float64(workers)
}

func (p AIMDPolicy) normalized() AIMDPolicy {
	out := p
	if out.DecreaseFactor <= 0 || out.DecreaseFactor >= 1 {
		out.DecreaseFactor = 0.5
	}
	if out.AdditiveStep <= 0 {
		out.AdditiveStep = 1
	}
	if out.InitialInterOpDelay <= 0 {
		out.InitialInterOpDelay = time.Millisecond
	}
	if out.MinInterOpDelayStep <= 0 {
		out.MinInterOpDelayStep = 100 * time.Microsecond
	}
	return out
}

// queueAIMDState tracks per-queue congestion window state (TCP ssthresh analogue).
type queueAIMDState struct {
	ssthresh          int // worker slow-start threshold; 0 = no throttle seen yet
	lastDecrease      time.Time
	delaySsthresh     time.Duration
	delaySeed         time.Duration // first throughput-derived delay; halving at/below clears to zero
	delayLastIncrease time.Time     // set when inter-op delay is raised (pressure at worker floor)
	peakPerWorkerFSOpRate float64   // best recent task/sec per worker before inter-op delay

	effectiveProbeCooldown time.Duration
	probePending           bool
	probeWorkersBefore     int
	probeRateBefore        float64
	probeStarted           time.Time
	failedProbes           int
	lastStable             time.Time
	fsBackoffUntil         time.Time // block further FS throttle step-down until retry-after (+ min probe cooldown)
}

// DecreaseTarget computes the worker target after pressure (multiplicative decrease).
// Records ssthresh as the new operating ceiling for slow-start / congestion avoidance.
func (p AIMDPolicy) DecreaseTarget(cur, minWorkers, maxWorkers int, state *queueAIMDState, now time.Time) int {
	p = p.normalized()
	if state == nil {
		state = &queueAIMDState{}
	}
	if cur <= minWorkers {
		return minWorkers
	}
	if p.ProbeCooldown > 0 && !state.lastDecrease.IsZero() && now.Sub(state.lastDecrease) < p.ProbeCooldown {
		return cur
	}

	target := int(math.Floor(float64(cur) * p.DecreaseFactor))
	if target < minWorkers {
		target = minWorkers
	}
	if target >= cur {
		target = cur - 1
		if target < minWorkers {
			target = minWorkers
		}
	}
	target = ClampInt(target, minWorkers, maxWorkers)

	state.ssthresh = target
	state.lastDecrease = now
	return target
}

// IncreaseTarget computes the next worker target during calm (underfeed) periods.
// Returns (target, ok). ok is false when probe cooldown is active or already at max.
func (p AIMDPolicy) IncreaseTarget(cur, minWorkers, maxWorkers int, state *queueAIMDState, now time.Time) (int, bool) {
	p = p.normalized()
	if state == nil {
		state = &queueAIMDState{}
	}
	if cur >= maxWorkers {
		return cur, false
	}
	cooldown := p.ProbeCooldown
	if state != nil {
		cooldown = state.probeCooldownDuration(p.ProbeCooldown)
		if cooldown > 0 && !state.lastDecrease.IsZero() && now.Sub(state.lastDecrease) < cooldown {
			return cur, false
		}
	} else if p.ProbeCooldown > 0 {
		return cur, false
	}

	var target int
	if state.ssthresh <= 0 || cur < state.ssthresh {
		// Slow start: double toward ssthresh (or max when ssthresh unknown).
		cap := state.ssthresh
		if cap <= 0 {
			cap = maxWorkers
		}
		target = cur * 2
		if target > cap {
			target = cap
		}
		if target <= cur {
			target = cur + p.AdditiveStep
		}
	} else {
		// Congestion avoidance: additive increase above ssthresh.
		target = cur + p.AdditiveStep
	}
	target = ClampInt(target, minWorkers, maxWorkers)
	return target, target > cur
}

// IncreaseInterOpDelay raises inter-op pacing when workers are already at MinWorkers.
// throughputOpsPerSec seeds the first delay via InterOpDelayFromThroughput; further pressure doubles delay (AIMD multiply-up).
func (p AIMDPolicy) IncreaseInterOpDelay(cur, maxDelay time.Duration, throughputOpsPerSec float64, state *queueAIMDState, now time.Time) time.Duration {
	p = p.normalized()
	if state == nil {
		state = &queueAIMDState{}
	}
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}

	var target time.Duration
	if cur <= 0 {
		target = InterOpDelayFromThroughput(throughputOpsPerSec)
		if target <= 0 {
			target = p.InitialInterOpDelay
		}
		state.delaySeed = target
	} else {
		target = time.Duration(math.Ceil(float64(cur) / p.DecreaseFactor))
	}
	if target <= cur {
		target = cur + p.MinInterOpDelayStep
	}
	if target > maxDelay {
		target = maxDelay
	}
	state.delaySsthresh = target
	state.delayLastIncrease = now
	return target
}

// DecreaseInterOpDelay lowers inter-op pacing during calm periods before worker scale-up resumes.
func (p AIMDPolicy) DecreaseInterOpDelay(cur time.Duration, state *queueAIMDState, now time.Time) (time.Duration, bool) {
	p = p.normalized()
	if state == nil {
		state = &queueAIMDState{}
	}
	if cur <= 0 {
		return 0, false
	}
	if p.ProbeCooldown > 0 && !state.delayLastIncrease.IsZero() && now.Sub(state.delayLastIncrease) < state.probeCooldownDuration(p.ProbeCooldown) {
		return cur, false
	}

	target := time.Duration(math.Floor(float64(cur) * p.DecreaseFactor))
	if target >= cur {
		target = cur - p.MinInterOpDelayStep
		if target < 0 {
			target = 0
		}
	}
	// At/below seed or sub-ms: clear delay so worker scale-up can resume.
	if target < interOpClearThreshold || (state.delaySeed > 0 && cur <= state.delaySeed) {
		target = 0
	} else if state.delaySeed <= 0 && cur <= p.InitialInterOpDelay && target < cur {
		target = 0
	}
	if target == cur {
		return cur, false
	}
	return target, true
}
