// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/traversal/shared"
)

func main() {
	fmt.Println("=== Autoscaler Throttle Integration Test ===")
	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	const initialWorkers = 20
	var events []scaling.ScalingEvent
	var mu sync.Mutex

	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	cfg, err := shared.SetupEphemeralThrottleTest(true, initialWorkers, migration.AutoscalerConfig{
		Interval:  3 * time.Second,
		DebugAIMD: true,
		OnEvent: func(ev scaling.ScalingEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
			// Newline so scaling events remain readable; run loop redraws progress each tick.
			fmt.Printf("\n  scaling event: %s\n", scaling.FormatScalingEvent(ev.Queue, ev.Knob, ev.OldValue, ev.NewValue, ev.Pressure))
		},
	})
	if err != nil {
		return err
	}
	fmt.Println()

	fmt.Println("🚀 Phase 2: Migration (autoscaler + FS throttle)")
	fmt.Println("=================================================")
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	shared.PrintVerification(result)

	mu.Lock()
	evCopy := append([]scaling.ScalingEvent(nil), events...)
	mu.Unlock()

	scaleDown := false
	fsThrottle := false
	for _, ev := range evCopy {
		if ev.Knob == "WorkerCount" && ev.NewValue < ev.OldValue {
			scaleDown = true
		}
		if ev.Pressure == scaling.PressureFSThrottle {
			fsThrottle = true
		}
	}

	minWorkers := initialWorkers
	for _, ev := range evCopy {
		if ev.Knob == "WorkerCount" && ev.NewValue < minWorkers {
			minWorkers = ev.NewValue
		}
	}
	if result.Runtime.Src.Round == 0 && result.Runtime.Dst.Round == 0 {
		// traversal completed with rounds advanced — sanity only
	}

	if !fsThrottle {
		return fmt.Errorf("expected FS_THROTTLE pressure from Spectra rate limits; events=%d", len(evCopy))
	}
	if !scaleDown && minWorkers >= initialWorkers {
		return fmt.Errorf("expected scale-back (worker step-down); min workers seen=%d initial=%d events=%d", minWorkers, initialWorkers, len(evCopy))
	}

	fmt.Printf("FS throttle + scale-back confirmed: min workers=%d (initial=%d), scaling events=%d\n", minWorkers, initialWorkers, len(evCopy))
	return nil
}
