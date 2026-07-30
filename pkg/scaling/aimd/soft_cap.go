// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"math"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

const softCapEMAAlpha = 0.5

// SoftCapDecreaseTarget updates the soft-cap ceiling from an FS_THROTTLE at cur
// workers and returns the resting SoftCap (not a multiplicative cliff).
func SoftCapDecreaseTarget(cur, minWorkers, maxWorkers int, state *State, now time.Time) int {
	if state == nil {
		state = &State{}
	}
	if minWorkers <= 0 {
		minWorkers = 1
	}
	state.ApplyThrottleCeiling(cur, minWorkers, maxWorkers)
	state.LastDecrease = now
	state.AbortSoftCapProbe()
	return state.SoftCap
}

// ApplyThrottleCeiling records that worker count n just rate-limited: safe ceiling
// pulls toward n-1, SoftCap = ceiling-1 (clamped).
func (st *State) ApplyThrottleCeiling(n, minWorkers, maxWorkers int) {
	if st == nil {
		return
	}
	safe := n - 1
	if safe < minWorkers {
		safe = minWorkers
	}
	if maxWorkers > 0 && safe > maxWorkers {
		safe = maxWorkers
	}
	if st.CeilingSafe <= 0 {
		st.CeilingSafe = safe
	} else {
		st.CeilingSafe = int(math.Round(softCapEMAAlpha*float64(safe) + (1-softCapEMAAlpha)*float64(st.CeilingSafe)))
		if st.CeilingSafe < minWorkers {
			st.CeilingSafe = minWorkers
		}
	}
	st.RecomputeSoftCap(minWorkers, maxWorkers)
	st.Ssthresh = st.SoftCap
}

// RecomputeSoftCap sets SoftCap = CeilingSafe-1 within [min, max].
func (st *State) RecomputeSoftCap(minWorkers, maxWorkers int) {
	if st == nil {
		return
	}
	if st.CeilingSafe <= 0 {
		st.SoftCap = 0
		return
	}
	if minWorkers <= 0 {
		minWorkers = 1
	}
	sc := st.CeilingSafe - 1
	if st.CeilingSafe <= minWorkers {
		sc = minWorkers
	}
	st.SoftCap = profile.ClampInt(sc, minWorkers, maxWorkers)
}

// RaiseCeilingFromSustainedProbe marks probed workers as proven-safe resting state:
// SoftCap = probed, CeilingSafe = probed+1 (next unknown boundary), clamped to max.
func (st *State) RaiseCeilingFromSustainedProbe(probed, minWorkers, maxWorkers int) {
	if st == nil || probed <= 0 {
		return
	}
	if minWorkers <= 0 {
		minWorkers = 1
	}
	st.SoftCap = profile.ClampInt(probed, minWorkers, maxWorkers)
	nextCeiling := probed + 1
	if maxWorkers > 0 && nextCeiling > maxWorkers {
		nextCeiling = maxWorkers
	}
	if st.CeilingSafe <= 0 {
		st.CeilingSafe = nextCeiling
	} else {
		st.CeilingSafe = int(math.Round(softCapEMAAlpha*float64(nextCeiling) + (1-softCapEMAAlpha)*float64(st.CeilingSafe)))
		if st.CeilingSafe < nextCeiling {
			st.CeilingSafe = nextCeiling
		}
		if maxWorkers > 0 && st.CeilingSafe > maxWorkers {
			st.CeilingSafe = maxWorkers
		}
	}
	// Keep SoftCap at proven probed level (not ceiling-1) so we rest where we sustained.
	st.SoftCap = profile.ClampInt(probed, minWorkers, maxWorkers)
	st.Ssthresh = st.SoftCap
}

func (st *State) AbortSoftCapProbe() {
	if st == nil {
		return
	}
	st.ProbeArmedAt = time.Time{}
	st.ProbeLevel = 0
}

func (st *State) ArmSoftCapProbe(level int, now time.Time) {
	if st == nil || level <= 0 {
		return
	}
	st.ProbeLevel = level
	st.ProbeArmedAt = now
}

func (st *State) SoftCapProbeActive() bool {
	return st != nil && !st.ProbeArmedAt.IsZero() && st.ProbeLevel > 0
}

// ProbeBounceElapsed reports whether the FS soft-cap probe wait has cleared.
func (st *State) ProbeBounceElapsed(now time.Time) bool {
	if st == nil || st.ProbeBounce <= 0 {
		return true
	}
	if st.LastProbeFail.IsZero() {
		return true
	}
	return now.Sub(st.LastProbeFail) >= st.ProbeBounce
}

// SustainDurationForProbe is the wall time the probed level must hold without RL.
func (st *State) SustainDurationForProbe() time.Duration {
	if st == nil {
		return 0
	}
	if st.LastProbeBounce > 0 {
		return st.LastProbeBounce
	}
	return st.ProbeBounce
}

func (st *State) ClearProbeBounceOnSuccess() {
	if st == nil {
		return
	}
	st.ProbeBounce = 0
	st.LastProbeBounce = 0
	st.LastProbeFail = time.Time{}
}

// ClampSoftCapToBounds shrinks ceiling/SoftCap when profile max drops.
func (st *State) ClampSoftCapToBounds(minWorkers, maxWorkers int) {
	if st == nil || st.CeilingSafe <= 0 {
		return
	}
	if minWorkers <= 0 {
		minWorkers = 1
	}
	if maxWorkers > 0 && st.CeilingSafe > maxWorkers {
		st.CeilingSafe = maxWorkers
	}
	if st.SoftCap > 0 {
		st.SoftCap = profile.ClampInt(st.SoftCap, minWorkers, maxWorkers)
	} else {
		st.RecomputeSoftCap(minWorkers, maxWorkers)
	}
	if st.Ssthresh > maxWorkers && maxWorkers > 0 {
		st.Ssthresh = maxWorkers
	}
}
