// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// PressureClass identifies autoscaler pressure bucket.
type PressureClass string

const (
	PressureNone       PressureClass = "NONE"
	PressureFSThrottle PressureClass = "FS_THROTTLE"
	PressureUnderfeed  PressureClass = "UNDERFEED"
	PressureMemory     PressureClass = "MEMORY_PRESSURE"
	PressureSeal       PressureClass = "SEAL_BACKPRESSURE"
)

// ClassifierInput is one autoscaler tick snapshot.
type ClassifierInput struct {
	Internal      map[string]queue.InternalMetricsSnapshot
	SealTelemetry db.SealBufferTelemetry
	MemoryLevel   MemoryLevel
	InProgress    map[string]int
	Pending       map[string]int
}

// Classify returns the highest-priority pressure class for this tick.
// Host memory red (≥90% system use) is MEMORY_PRESSURE. Seal backpressure is handled
// separately in the autoscaler tick (batch/seal step-down only, not mislabeled as host OOM).
func Classify(in ClassifierInput) PressureClass {
	if in.MemoryLevel == MemoryRed {
		return PressureMemory
	}
	now := time.Now()
	for _, snap := range in.Internal {
		if snap.RateLimitHitsSinceLastPoll > 0 {
			return PressureFSThrottle
		}
		if !snap.RateLimitedUntil.IsZero() && snap.RateLimitedUntil.After(now) {
			return PressureFSThrottle
		}
	}
	for name, snap := range in.Internal {
		if snap.TimeWaitingOnQueue > 500*time.Millisecond && in.Pending[name] > 0 {
			return PressureUnderfeed
		}
	}
	return PressureNone
}
