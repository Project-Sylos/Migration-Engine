// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func maxRateLimitedUntilForQueues(internal map[string]queue.InternalMetricsSnapshot, queues []string) time.Time {
	var latest time.Time
	for _, name := range queues {
		if u := internal[name].RateLimitedUntil; u.After(latest) {
			latest = u
		}
	}
	return latest
}

func maxRateLimitedUntil(internal map[string]queue.InternalMetricsSnapshot, name string) time.Time {
	if internal == nil {
		return time.Time{}
	}
	return internal[name].RateLimitedUntil
}

// fsThrottleActuationBlocked is true after an FS throttle step-down until retry-after (and min probe cooldown) elapse.
// The first decrease in an episode is always allowed (fsBackoffUntil not yet set).
func fsThrottleActuationBlocked(st *queueAIMDState, now, rateLimitedUntil time.Time) bool {
	if st == nil || st.fsBackoffUntil.IsZero() {
		return false
	}
	until := st.fsBackoffUntil
	if rateLimitedUntil.After(until) {
		until = rateLimitedUntil
	}
	return now.Before(until)
}

func fsBackoffRemaining(st *queueAIMDState, now, rateLimitedUntil time.Time) time.Duration {
	if st == nil || st.fsBackoffUntil.IsZero() {
		return 0
	}
	until := st.fsBackoffUntil
	if rateLimitedUntil.After(until) {
		until = rateLimitedUntil
	}
	if !now.Before(until) {
		return 0
	}
	return until.Sub(now)
}

func (st *queueAIMDState) noteFSBackoff(now, rateLimitedUntil time.Time, probeCooldown time.Duration) {
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
	st.fsBackoffUntil = until
}

func (st *queueAIMDState) clearFSBackoff() {
	if st == nil {
		return
	}
	st.fsBackoffUntil = time.Time{}
}

func interOpIncreaseAllowed(st *queueAIMDState, now, rateLimitedUntil time.Time) bool {
	return !fsThrottleActuationBlocked(st, now, rateLimitedUntil)
}

// interOpDecreaseAllowed gates AIMD delay recovery on calm ticks (same retry-after / probe cooldown as worker step-down).
func interOpDecreaseAllowed(st *queueAIMDState, now, rateLimitedUntil time.Time, baseCooldown time.Duration) bool {
	if fsThrottleActuationBlocked(st, now, rateLimitedUntil) {
		return false
	}
	if st == nil || st.delayLastIncrease.IsZero() {
		return true
	}
	cooldown := st.probeCooldownDuration(baseCooldown)
	return !now.Before(st.delayLastIncrease.Add(cooldown))
}
