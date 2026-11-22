// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
)

// shedForMemoryPressure pauses queues, drops pendingBuff, drains in-flight work,
// flushes the seal buffer, rewinds keyset cursors, then resumes. Returns true when
// a shed ran (caller should skip worker scaling this tick).
// Logs only when a shed actually runs (or a shed step fails) — not on cooldown skips.
func (a *Autoscaler) shedForMemoryPressure(now time.Time, sample memory.MemorySample) bool {
	if a == nil {
		return false
	}
	if !a.memoryShedAllowed(now) {
		return false
	}

	type shedTarget struct {
		name   string
		shed   scaling.MemoryShedding
		resume queue.QueueState
	}
	targets := make([]shedTarget, 0, len(a.queues))
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		shed, ok := q.(scaling.MemoryShedding)
		if !ok {
			continue
		}
		targets = append(targets, shedTarget{name: name, shed: shed})
	}
	if len(targets) == 0 {
		return false
	}

	a.logMemoryAction(fmt.Sprintf("MEMORY_PRESSURE shed begin %s", memory.FormatMemoryBudgetLine(sample)))
	for i := range targets {
		pendingBefore := 0
		if q := a.queues[targets[i].name]; q != nil {
			pendingBefore = q.GetPendingCount()
		}
		targets[i].resume = targets[i].shed.BeginMemoryShed()
		targets[i].shed.ClearPendingBufferForSuspend()
		a.emit(scaling.ScalingEvent{
			Queue:    targets[i].name,
			Knob:     "MemoryShed",
			OldValue: pendingBefore,
			NewValue: 0,
			Pressure: scaling.PressureMemory,
			At:       now,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, t := range targets {
		if err := t.shed.WaitInProgressZeroFunc(ctx, 50*time.Millisecond, nil); err != nil {
			a.logMemoryAction(fmt.Sprintf("MEMORY_PRESSURE shed drain %s failed: %v", t.name, err))
		}
	}

	if a.database != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err := a.database.Flush(flushCtx)
		flushCancel()
		if err != nil {
			a.logMemoryAction(fmt.Sprintf("MEMORY_PRESSURE shed flush failed: %v", err))
		}
	}

	for _, t := range targets {
		t.shed.ResetKeysetCursorForShed()
		t.shed.SetLastPullWasPartial(false)
		t.shed.FinishMemoryShed(t.resume)
	}

	a.noteMemoryShedCooldown(now)
	a.logMemoryAction(fmt.Sprintf("MEMORY_PRESSURE shed end %s", memory.FormatMemoryBudgetLine(a.sampleMemory())))
	return true
}

func (a *Autoscaler) memoryShedAllowed(now time.Time) bool {
	if a == nil || a.memoryShedCooldownUntil.IsZero() {
		return true
	}
	return !now.Before(a.memoryShedCooldownUntil)
}

func (a *Autoscaler) defaultMemoryShedCooldown() time.Duration {
	cooldown := 5 * a.interval
	if cooldown < 30*time.Second {
		cooldown = 30 * time.Second
	}
	return cooldown
}

func (a *Autoscaler) effectiveMemoryShedCooldown() time.Duration {
	if a != nil && a.memoryShedCooldownOverride > 0 {
		return a.memoryShedCooldownOverride
	}
	return a.defaultMemoryShedCooldown()
}

func (a *Autoscaler) noteMemoryShedCooldown(now time.Time) {
	if a == nil {
		return
	}
	until := now.Add(a.effectiveMemoryShedCooldown())
	if until.After(a.memoryShedCooldownUntil) {
		a.memoryShedCooldownUntil = until
	}
}

// logMemoryAction writes only actionable shed outcomes (not per-tick classification).
func (a *Autoscaler) logMemoryAction(msg string) {
	if logservice.LS != nil {
		_ = logservice.LS.Log("warn", msg, "autoscaler", "memory", "memory")
	}
	if a != nil && a.debugAutoscaler {
		a.debugAutoscalerPrint("  " + msg)
	}
}
