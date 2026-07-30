// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
)

func TestClassifyFSThrottle(t *testing.T) {
	p := Classify(ClassifierInput{
		Internal: map[string]observe.InternalMetricsSnapshot{
			"src": {RateLimitHitsSinceLastPoll: 3},
		},
	})
	if p != PressureFSThrottle {
		t.Fatalf("got %s want FS_THROTTLE", p)
	}
}

func TestClassifyFSThrottleFromRetryAfterWindow(t *testing.T) {
	p := Classify(ClassifierInput{
		Internal: map[string]observe.InternalMetricsSnapshot{
			"dst": {RateLimitedUntil: time.Now().Add(2 * time.Second)},
		},
	})
	if p != PressureFSThrottle {
		t.Fatalf("got %s want FS_THROTTLE from RateLimitedUntil", p)
	}
}

func TestClassifyUnderfeed(t *testing.T) {
	p := Classify(ClassifierInput{
		Internal: map[string]observe.InternalMetricsSnapshot{
			"src": {TimeWaitingOnQueue: time.Second},
		},
		InProgress: map[string]int{"src": 0},
		Pending:    map[string]int{"src": 10},
		MemoryLevel: memory.MemoryGreen,
	})
	if p != PressureUnderfeed {
		t.Fatalf("got %s want UNDERFEED", p)
	}
}

func TestClassifyUnderfeedWithBusyWorkers(t *testing.T) {
	p := Classify(ClassifierInput{
		Internal: map[string]observe.InternalMetricsSnapshot{
			"copy": {TimeWaitingOnQueue: time.Second},
		},
		InProgress: map[string]int{"copy": 4},
		Pending:    map[string]int{"copy": 100},
		MemoryLevel: memory.MemoryGreen,
	})
	if p != PressureUnderfeed {
		t.Fatalf("got %s want UNDERFEED with busy workers", p)
	}
}

func TestClassifyMemoryFromHostRed(t *testing.T) {
	p := Classify(ClassifierInput{
		MemoryLevel: memory.MemoryRed,
	})
	if p != PressureMemory {
		t.Fatalf("got %s want MEMORY_PRESSURE", p)
	}
}

func TestClassifySealBackpressureNotMemoryPressure(t *testing.T) {
	p := Classify(ClassifierInput{
		SealTelemetry: db.SealBufferTelemetry{HardCapHitsSinceLastPoll: 1},
		MemoryLevel:   memory.MemoryGreen,
	})
	if p != PressureNone {
		t.Fatalf("seal hard-cap alone should not classify as memory pressure, got %s", p)
	}
}
