// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"math"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

const interOpClearThreshold = time.Millisecond // below this, clear delay and hand off to worker scale-up

// AIMDPolicy implements TCP Reno-style worker scaling:
//   - multiplicative decrease on pressure (halve by default)
//   - slow-start (exponential climb) below Ssthresh
//   - additive increase at/above Ssthresh
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

func (p AIMDPolicy) Normalized() AIMDPolicy {
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

// State tracks per-queue congestion window state (TCP Ssthresh analogue).
type State struct {
	Ssthresh              int // worker slow-start threshold; 0 = no throttle seen yet
	LastDecrease          time.Time
	DelaySsthresh         time.Duration
	DelaySeed             time.Duration // first throughput-derived delay; halving at/below clears to zero
	DelayLastIncrease     time.Time     // set when inter-op delay is raised (pressure at worker floor)
	PeakPerWorkerFSOpRate float64       // best recent task/sec per worker before inter-op delay

	// Soft-cap discovery (FS_THROTTLE): resting ceiling separate from probe patience.
	CeilingSafe     int           // 0 = unset; SoftCap+1 next-probe boundary after FS_THROTTLE
	SoftCap         int           // resting worker target (n-1 after RL at n)
	ProbeBounce     time.Duration // FS probe-only wait before SoftCap+1
	LastProbeBounce time.Duration // last ratcheted probe bounce (sustain + fail formula)
	LastProbeFail   time.Time     // when ProbeBounce was last ratcheted
	ProbeArmedAt    time.Time     // SoftCap+1 sustain clock; zero if not probing
	ProbeLevel      int           // workers under sustain test

	// Efficiency-probe timer (separate from FS ProbeBounce).
	EffectiveProbeCooldown time.Duration
	ProbePending           bool
	ProbeWorkersBefore     int
	ProbeRateBefore        float64
	ProbeStarted           time.Time
	FailedProbes           int
	LastStable             time.Time
	FSBackoffUntil         time.Time // block further FS throttle step-down until retry-after (+ min probe cooldown)
}

// DecreaseTarget computes the worker target after pressure (multiplicative decrease).
// Records Ssthresh as the new operating ceiling for slow-start / congestion avoidance.
func (p AIMDPolicy) DecreaseTarget(cur, minWorkers, maxWorkers int, state *State, now time.Time) int {
	p = p.Normalized()
	if state == nil {
		state = &State{}
	}
	if cur <= minWorkers {
		return minWorkers
	}
	if p.ProbeCooldown > 0 && !state.LastDecrease.IsZero() && now.Sub(state.LastDecrease) < p.ProbeCooldown {
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
	target = profile.ClampInt(target, minWorkers, maxWorkers)

	state.Ssthresh = target
	state.LastDecrease = now
	return target
}

// IncreaseTarget computes the next worker target during calm (underfeed) periods.
// Returns (target, ok). ok is false when probe cooldown is active or already at max.
//
// Soft-cap mode (CeilingSafe set after first FS_THROTTLE):
//   - cur < SoftCap: AIMD toward SoftCap; FS ProbeBounce does not gate
//   - cur == SoftCap: may return SoftCap+1 only when ProbeBounce elapsed
//   - active sustain at ProbeLevel: hold until sustain window, then raise SoftCap
// Pre-first-RL: classic AIMD to maxWorkers; efficiency EffectiveProbeCooldown still gates.
func (p AIMDPolicy) IncreaseTarget(cur, minWorkers, maxWorkers int, state *State, now time.Time) (int, bool) {
	p = p.Normalized()
	if state == nil {
		state = &State{}
	}
	if minWorkers <= 0 {
		minWorkers = 1
	}
	if cur >= maxWorkers {
		return cur, false
	}

	// Soft-cap: complete sustain or climb within/at SoftCap / probe SoftCap+1.
	if state.CeilingSafe > 0 && state.SoftCap > 0 {
		if state.SoftCapProbeActive() && cur >= state.ProbeLevel {
			need := state.SustainDurationForProbe()
			if need <= 0 || now.Sub(state.ProbeArmedAt) >= need {
				state.RaiseCeilingFromSustainedProbe(state.ProbeLevel, minWorkers, maxWorkers)
				state.ClearProbeBounceOnSuccess()
				state.AbortSoftCapProbe()
				// Rest at proven SoftCap this tick; probe SoftCap+1 on a later calm tick.
				return cur, false
			}
			return cur, false
		}

		climbMax := state.SoftCap
		if climbMax > maxWorkers {
			climbMax = maxWorkers
		}
		if cur < climbMax {
			target := AimdIncreaseStep(p, cur, state.Ssthresh, climbMax, minWorkers, maxWorkers)
			return target, target > cur
		}

		// At SoftCap: optional probe one step up.
		if cur == state.SoftCap && state.SoftCap < maxWorkers {
			if !state.ProbeBounceElapsed(now) {
				return cur, false
			}
			probe := state.SoftCap + 1
			if probe > maxWorkers {
				probe = maxWorkers
			}
			if probe <= cur {
				return cur, false
			}
			return probe, true
		}
		return cur, false
	}

	// Discovery (no FS ceiling yet): classic AIMD; efficiency bounce may gate.
	cooldown := p.ProbeCooldown
	if state != nil {
		cooldown = state.ProbeCooldownDuration(p.ProbeCooldown)
		if cooldown > 0 && !state.LastDecrease.IsZero() && now.Sub(state.LastDecrease) < cooldown {
			return cur, false
		}
	} else if p.ProbeCooldown > 0 {
		return cur, false
	}

	target := AimdIncreaseStep(p, cur, state.Ssthresh, maxWorkers, minWorkers, maxWorkers)
	return target, target > cur
}

func AimdIncreaseStep(p AIMDPolicy, cur, Ssthresh, climbMax, minWorkers, maxWorkers int) int {
	var target int
	if Ssthresh <= 0 || cur < Ssthresh {
		cap := Ssthresh
		if cap <= 0 || cap > climbMax {
			cap = climbMax
		}
		target = cur * 2
		if target > cap {
			target = cap
		}
		if target <= cur {
			target = cur + p.AdditiveStep
		}
	} else {
		target = cur + p.AdditiveStep
	}
	if target > climbMax {
		target = climbMax
	}
	return profile.ClampInt(target, minWorkers, maxWorkers)
}

// IncreaseInterOpDelay raises inter-op pacing when workers are already at MinWorkers.
// throughputOpsPerSec seeds the first delay via InterOpDelayFromThroughput; further pressure doubles delay (AIMD multiply-up).
func (p AIMDPolicy) IncreaseInterOpDelay(cur, maxDelay time.Duration, throughputOpsPerSec float64, state *State, now time.Time) time.Duration {
	p = p.Normalized()
	if state == nil {
		state = &State{}
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
		state.DelaySeed = target
	} else {
		target = time.Duration(math.Ceil(float64(cur) / p.DecreaseFactor))
	}
	if target <= cur {
		target = cur + p.MinInterOpDelayStep
	}
	if target > maxDelay {
		target = maxDelay
	}
	state.DelaySsthresh = target
	state.DelayLastIncrease = now
	return target
}

// DecreaseInterOpDelay lowers inter-op pacing during calm periods before worker scale-up resumes.
func (p AIMDPolicy) DecreaseInterOpDelay(cur time.Duration, state *State, now time.Time) (time.Duration, bool) {
	p = p.Normalized()
	if state == nil {
		state = &State{}
	}
	if cur <= 0 {
		return 0, false
	}
	if p.ProbeCooldown > 0 && !state.DelayLastIncrease.IsZero() && now.Sub(state.DelayLastIncrease) < state.ProbeCooldownDuration(p.ProbeCooldown) {
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
	if target < interOpClearThreshold || (state.DelaySeed > 0 && cur <= state.DelaySeed) {
		target = 0
	} else if state.DelaySeed <= 0 && cur <= p.InitialInterOpDelay && target < cur {
		target = 0
	}
	if target == cur {
		return cur, false
	}
	return target, true
}
