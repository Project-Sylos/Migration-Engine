// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
)

func MaxRateLimitedUntilForQueues(internal map[string]observe.InternalMetricsSnapshot, queues []string) time.Time {
	var latest time.Time
	for _, name := range queues {
		if u := internal[name].RateLimitedUntil; u.After(latest) {
			latest = u
		}
	}
	return latest
}

func MaxRateLimitedUntil(internal map[string]observe.InternalMetricsSnapshot, name string) time.Time {
	if internal == nil {
		return time.Time{}
	}
	return internal[name].RateLimitedUntil
}

// FsThrottleActuationBlocked is true after an FS throttle step-down until retry-after (and min probe cooldown) elapse.
// The first decrease in an episode is always allowed (FSBackoffUntil not yet set).
func FSThrottleActuationBlocked(st *State, now, rateLimitedUntil time.Time) bool {
	if st == nil || st.FSBackoffUntil.IsZero() {
		return false
	}
	until := st.FSBackoffUntil
	if rateLimitedUntil.After(until) {
		until = rateLimitedUntil
	}
	return now.Before(until)
}

func FSBackoffRemaining(st *State, now, rateLimitedUntil time.Time) time.Duration {
	if st == nil || st.FSBackoffUntil.IsZero() {
		return 0
	}
	until := st.FSBackoffUntil
	if rateLimitedUntil.After(until) {
		until = rateLimitedUntil
	}
	if !now.Before(until) {
		return 0
	}
	return until.Sub(now)
}

func (st *State) NoteFSBackoff(now, rateLimitedUntil time.Time, probeCooldown time.Duration) {
	if st == nil {
		return
	}
	until := rateLimitedUntil
	if until.Before(now) {
		until = now
	}
	if probeCooldown > 0 {
		minUntil := now.Add(probeCooldown)
		if minUntil.After(until) {
			until = minUntil
		}
	}
	st.FSBackoffUntil = until
}

func (st *State) ClearFSBackoff() {
	if st == nil {
		return
	}
	st.FSBackoffUntil = time.Time{}
}

func InterOpIncreaseAllowed(st *State, now, rateLimitedUntil time.Time) bool {
	return !FSThrottleActuationBlocked(st, now, rateLimitedUntil)
}

// InterOpDecreaseAllowed gates AIMD delay recovery on calm ticks (same retry-after / probe cooldown as worker step-down).
func InterOpDecreaseAllowed(st *State, now, rateLimitedUntil time.Time, baseCooldown time.Duration) bool {
	if FSThrottleActuationBlocked(st, now, rateLimitedUntil) {
		return false
	}
	if st == nil || st.DelayLastIncrease.IsZero() {
		return true
	}
	cooldown := st.ProbeCooldownDuration(baseCooldown)
	return !now.Before(st.DelayLastIncrease.Add(cooldown))
}
