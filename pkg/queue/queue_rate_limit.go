// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
)

// rateLimitReleaseJitterFrac is the max extra wait fraction added per worker so
// concurrent WaitRateLimited calls do not wake in lockstep (thundering herd).
const rateLimitReleaseJitterFrac = 0.25

// rateLimitReleaseJitterCap bounds absolute jitter so huge Retry-After windows
// do not add multi-minute random skew.
const rateLimitReleaseJitterCap = 2 * time.Second


// RateLimitTelemetry supplies FS throttle signals for workers, observer, and autoscaler.
type RateLimitTelemetry interface {
	TakeRecentHits() int64
	RateLimitedUntil() time.Time
}

// SetRateLimitTelemetry attaches FS degradation telemetry used by workers to idle during throttle windows.
// Convention: first source is SRC, second (if present) is DST. Traversal/delete attach one side only.
func (q *Queue) SetRateLimitTelemetry(sources ...RateLimitTelemetry) {
	if q == nil {
		return
	}
	q.pool.mu.Lock()
	q.pool.rateLimitSources = sources
	q.pool.mu.Unlock()
}

// RateLimitedUntilSides returns active rate-limit deadlines for SRC and/or DST.
// Mapping: src/delete queues → sources[0] as SRC; dst → sources[0] as DST; copy → sources[0]=SRC, sources[1]=DST.
func (q *Queue) RateLimitedUntilSides() (srcUntil, dstUntil time.Time) {
	if q == nil {
		return time.Time{}, time.Time{}
	}
	q.pool.mu.Lock()
	sources := q.pool.rateLimitSources
	name := q.name
	q.pool.mu.Unlock()
	switch name {
	case "dst":
		if len(sources) > 0 && sources[0] != nil {
			dstUntil = sources[0].RateLimitedUntil()
		}
	case "copy":
		if len(sources) > 0 && sources[0] != nil {
			srcUntil = sources[0].RateLimitedUntil()
		}
		if len(sources) > 1 && sources[1] != nil {
			dstUntil = sources[1].RateLimitedUntil()
		}
	default: // "src", "delete", and unknown: treat first as SRC
		if len(sources) > 0 && sources[0] != nil {
			srcUntil = sources[0].RateLimitedUntil()
		}
	}
	return srcUntil, dstUntil
}

// RateLimitedWaitDuration returns remaining time workers should wait before leasing (0 if clear).
func (q *Queue) RateLimitedWaitDuration() time.Duration {
	if q == nil {
		return 0
	}
	q.pool.mu.Lock()
	sources := q.pool.rateLimitSources
	q.pool.mu.Unlock()
	now := time.Now()
	var latest time.Time
	for _, src := range sources {
		if src == nil {
			continue
		}
		if until := src.RateLimitedUntil(); until.After(latest) {
			latest = until
		}
	}
	if latest.IsZero() || !latest.After(now) {
		return 0
	}
	return latest.Sub(now)
}

// WaitRateLimited blocks until d elapses or ctx is canceled.
// Adds per-call jitter (0–25% of d, capped) so workers released from the same
// RateLimitedUntil do not all retry at once.
// Beats the queue watchdog periodically so rate-limit idle time is not treated as a stall.
func (q *Queue) WaitRateLimited(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	d = d + rateLimitReleaseJitter(d)
	if q != nil && q.watchdog != nil {
		q.watchdog.Beat()
	}
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		chunk := remaining
		if chunk > time.Second {
			chunk = time.Second
		}
		if ctx == nil {
			time.Sleep(chunk)
		} else {
			timer := time.NewTimer(chunk)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if q != nil && q.watchdog != nil {
			q.watchdog.Beat()
		}
	}
}

func rateLimitReleaseJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	maxJitter := time.Duration(float64(base) * rateLimitReleaseJitterFrac)
	if maxJitter > rateLimitReleaseJitterCap {
		maxJitter = rateLimitReleaseJitterCap
	}
	if maxJitter <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(maxJitter) + 1))
}

// IsRateLimitActive reports whether any attached FS telemetry has an open retry-after window.
func (q *Queue) IsRateLimitActive() bool {
	return q.RateLimitedWaitDuration() > 0
}

// IsThrottleError reports whether err is an explicit FS rate limit (worker should yield the task).
func IsThrottleError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := credentials.IsRateLimitedDefault(err); ok {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "rate limit") || strings.Contains(msg, "too many requests") {
		return true
	}
	if strings.Contains(msg, "429") {
		return true
	}
	return false
}

// IsNonRetryableCopyError reports permanent FS failures that should not consume copy retry budget.
func IsNonRetryableCopyError(errMsg string) bool {
	msg := strings.ToLower(strings.TrimSpace(errMsg))
	if msg == "" {
		return false
	}
	for _, frag := range []string{
		"no_write_permission",
		"path/not_found",
		"malformed_path",
		"insufficient_space",
		"disallowed_name",
		"access_restricted",
		"cannot create files or folders at team space root",
		"missing scope",
		"invalid_part_size",
		"bytes but declared size",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

// IsNonRetryableTraversalError reports permanent list/traverse failures that should not
// consume traversal retry budget (permission denied, blocked pseudo-paths, stalls).
func IsNonRetryableTraversalError(errMsg string) bool {
	msg := strings.ToLower(strings.TrimSpace(errMsg))
	if msg == "" {
		return false
	}
	for _, frag := range []string{
		"path blocked from migration",
		"permission denied",
		"operation not permitted",
		"eacces",
		"eperm",
		"traversal stalled",
		"not a directory",
		"no such file or directory",
		"enoent",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

// yieldTaskOnRateLimit returns a leased task to the pending buffer without incrementing attempts.
func (q *Queue) yieldTaskOnRateLimit(task *TaskBase, executionDelta time.Duration) {
	q.RecordExecutionTime(executionDelta)
	nodeID := task.ID
	task.Locked = false
	q.RemoveInProgress(nodeID)
	if !q.Add(task) {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("rate-limit yield re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
				"queue", q.name, q.name)
		}
	}
}
