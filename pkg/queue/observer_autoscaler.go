// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "time"

// RateLimitTelemetry supplies FS throttle signals for observer/autoscaler polling.
type RateLimitTelemetry interface {
	TakeRecentHits() int64
	RateLimitedUntil() time.Time
}

// InternalMetricsSnapshot is a copy of internal queue metrics for autoscaler decisions.
type InternalMetricsSnapshot struct {
	TimeProcessing          time.Duration
	TimeWaitingOnQueue      time.Duration
	TimeWaitingOnFS         time.Duration
	TimeRateLimited         time.Duration
	TimePausedRoundBoundary time.Duration
	TimeIdleNoWork          time.Duration
	TasksCompletedWhileActive int64
	RateLimitHitsSinceLastPoll int64
	RateLimitedUntil        time.Time // shared FS adapter retry-after window (if any)
}

// RegisterRateLimitTelemetry attaches FS degradation telemetry for a queue name.
func (o *QueueObserver) RegisterRateLimitTelemetry(queueName string, src RateLimitTelemetry) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.rateLimitSources == nil {
		o.rateLimitSources = make(map[string]RateLimitTelemetry)
	}
	o.rateLimitSources[queueName] = src
}

// SnapshotThroughputRate returns the best available items/sec EMA for a queue (traversal discovery or copy).
func (o *QueueObserver) SnapshotThroughputRate(queueName string) float64 {
	if o == nil {
		return 0
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if rate, ok := o.prevEMARates[queueName]; ok && rate > 0 {
		return rate
	}
	if rate, ok := o.prevEMARates[queueName+"-items"]; ok {
		return rate
	}
	return 0
}

// SnapshotTaskCompletionRate returns EMA task completions/sec (≈ FS list/copy ops per second).
func (o *QueueObserver) SnapshotTaskCompletionRate(queueName string) float64 {
	if o == nil {
		return 0
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.prevEMARates[queueName+"-tasks"]
}

// SnapshotInternalMetrics returns per-queue internal metrics (does not reset time buckets).
func (o *QueueObserver) SnapshotInternalMetrics() map[string]InternalMetricsSnapshot {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make(map[string]InternalMetricsSnapshot, len(o.internalMetrics))
	for name, m := range o.internalMetrics {
		if m == nil {
			continue
		}
		snap := InternalMetricsSnapshot{
			TimeProcessing:          m.TimeProcessing,
			TimeWaitingOnQueue:      m.TimeWaitingOnQueue,
			TimeWaitingOnFS:         m.TimeWaitingOnFS,
			TimeRateLimited:         m.TimeRateLimited,
			TimePausedRoundBoundary: m.TimePausedRoundBoundary,
			TimeIdleNoWork:          m.TimeIdleNoWork,
			TasksCompletedWhileActive: m.TasksCompletedWhileActive,
		}
		if src, ok := o.rateLimitSources[name]; ok && src != nil {
			snap.RateLimitHitsSinceLastPoll = src.TakeRecentHits()
			until := src.RateLimitedUntil()
			snap.RateLimitedUntil = until
			if until.After(time.Now()) {
				snap.TimeRateLimited += time.Until(until)
			}
		}
		out[name] = snap
	}
	return out
}
