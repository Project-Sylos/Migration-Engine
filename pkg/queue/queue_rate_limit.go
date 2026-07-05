// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
)

// SetRateLimitTelemetry attaches FS degradation telemetry used by workers to idle during throttle windows.
func (q *Queue) SetRateLimitTelemetry(sources ...RateLimitTelemetry) {
	if q == nil {
		return
	}
	q.pool.mu.Lock()
	q.pool.rateLimitSources = sources
	q.pool.mu.Unlock()
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
func (q *Queue) WaitRateLimited(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if ctx == nil {
		time.Sleep(d)
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

// yieldTaskOnRateLimit returns a leased task to the pending buffer without incrementing attempts.
func (q *Queue) yieldTaskOnRateLimit(task *TaskBase, executionDelta time.Duration) {
	q.recordExecutionTime(executionDelta)
	nodeID := task.ID
	task.Locked = false
	q.removeInProgress(nodeID)
	if !q.Add(task) {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("rate-limit yield re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
				"queue", q.name, q.name)
		}
	}
}
