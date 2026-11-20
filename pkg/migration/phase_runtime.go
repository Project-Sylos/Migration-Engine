// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/loop"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PhaseRuntime holds the knobs and hooks shared by every phase config
// (MigrationConfig, SweepConfig, CopyPhaseConfig, DeletePhaseConfig).
type PhaseRuntime struct {
	WorkerCount     int
	MaxRetries      int
	LogAddress      string
	LogLevel        string
	PersistLogLevel string
	SkipListener    bool
	StartupDelay    time.Duration
	ProgressTick    time.Duration
	ShutdownContext context.Context
	// SoftSuspendRequested is polled in the run loop; when true, queues drain and state is flushed.
	SoftSuspendRequested func() bool
	// ReportStopProgress updates live stop checklist (step id + in-progress count).
	ReportStopProgress   func(step string, inProgress int)
	ObserverPollInterval time.Duration
	// OnQueueObserver is called with the live observer after queues register, and with nil when the run exits (before observer.Stop).
	OnQueueObserver func(*observe.QueueObserver)
	// OnAutoscaler is called with the live autoscaler after start, and with nil when the run exits.
	OnAutoscaler func(*loop.Autoscaler)
	Autoscaler   AutoscalerConfig
	SrcService   Service
	DstService   Service
	// PathCheckTarget selects destination-name rules ("none", "auto", or provider id).
	PathCheckTarget string
	// WindowsCompat enables Windows desktop-sync overlays on soft cloud destinations.
	WindowsCompat bool
	// OverwritePolicy selects exists-on-both file pairing (newest_mtime | decide | source_wins).
	OverwritePolicy string
	// HashVerify enables dual-stream SHA-256 compare for name-matched files during DST traversal.
	HashVerify bool
}

// Begin validates the phase inputs and starts the log service. Every phase entry point calls it first.
// When SkipListener: bind port and discard UDP packets so sender writes don't fail; no terminal, no display.
// When !SkipListener: spawn listener in a new terminal that displays logs.
func (rt *PhaseRuntime) Begin(database *db.DB, adapters ...types.FSAdapter) error {
	if database == nil {
		return fmt.Errorf("migration DB is required")
	}
	for _, a := range adapters {
		if a == nil {
			return fmt.Errorf("phase adapters must be provided")
		}
	}
	if rt.LogAddress == "" {
		return nil
	}
	startupDelay := rt.StartupDelay
	if startupDelay <= 0 {
		startupDelay = 500 * time.Millisecond
	}
	if rt.SkipListener {
		logservice.StartListenerDiscard(rt.LogAddress)
	} else if err := logservice.StartListener(rt.LogAddress); err != nil {
		return fmt.Errorf("start log listener: %w", err)
	}
	time.Sleep(startupDelay)
	persist := rt.PersistLogLevel
	if persist == "" {
		persist = "trace"
	}
	if err := logservice.InitGlobalLoggerPersist(database.LogsDBForWrite(), rt.LogAddress, rt.LogLevel, persist); err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	return nil
}

// ShutdownOrBackground returns ShutdownContext, or a background context when unset.
func (rt *PhaseRuntime) ShutdownOrBackground() context.Context {
	if rt.ShutdownContext != nil {
		return rt.ShutdownContext
	}
	return context.Background()
}

// suspendTuning returns the observer poll and progress tick (ms) recorded in a suspend payload.
func (rt *PhaseRuntime) suspendTuning(defaultProgressMs int) (obsMs, progMs int) {
	obsMs = int(rt.ObserverPollInterval / time.Millisecond)
	if obsMs <= 0 {
		obsMs = 200
	}
	progMs = int(rt.ProgressTick / time.Millisecond)
	if progMs <= 0 {
		progMs = defaultProgressMs
	}
	return obsMs, progMs
}
