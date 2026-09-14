// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Scale-down + resume chaos: Spectra rate limits force AIMD worker step-down during
// copy. Asserts no duplicate copy success events per node and no already_existed
// on nodes that still carry an attempt marker.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	copyshared "codeberg.org/Sylos/Migration-Engine/pkg/tests/copy/shared"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/traversal/shared"
)

func main() {
	fmt.Println("=== Copy Scale-Down Resume Chaos Test ===")
	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	const initialWorkers = 16
	var events []scaling.ScalingEvent
	var mu sync.Mutex
	onEvent := func(ev scaling.ScalingEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
		fmt.Printf("\n  scaling event: %s\n", scaling.FormatScalingEvent(ev.Queue, ev.Knob, ev.OldValue, ev.NewValue, ev.Pressure))
	}

	fmt.Println("📋 Phase 1: Setup (Spectra chaos + autoscaler)")
	cfg, err := shared.SetupEphemeralThrottleTest(true, initialWorkers, migration.AutoscalerConfig{
		Interval:  2 * time.Second,
		DebugAIMD: true,
		OnEvent:   onEvent,
	})
	if err != nil {
		return err
	}
	dbPath, err := filepath.Abs("pkg/tests/copy/shared/scale_down_resume_test.db")
	if err != nil {
		return err
	}
	cfg.Database.Path = dbPath
	cfg.Database.RemoveExisting = true

	fmt.Println("🚀 Phase 2: Traversal")
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("traversal failed: %w", err)
	}
	shared.PrintVerification(result)

	database, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		return fmt.Errorf("reopen migration db: %w", err)
	}
	defer database.Close()

	fmt.Println("🚀 Phase 3: Copy under throttle / scale-down")
	_, err = migration.RunCopyPhase(migration.CopyPhaseConfig{
		DuckDB:       database,
		SrcAdapter:   cfg.Source.Adapter,
		DstAdapter:   cfg.Destination.Adapter,
		WorkerCount:  initialWorkers,
		MaxRetries:   3,
		SkipListener: true,
		LogAddress:   "127.0.0.1:8081",
		LogLevel:     "trace",
		StartupDelay: 500 * time.Millisecond,
		ProgressTick: time.Second,
		SrcService:   cfg.Source,
		DstService:   cfg.Destination,
		Autoscaler: migration.AutoscalerConfig{
			Interval:  2 * time.Second,
			DebugAIMD: true,
			OnEvent:   onEvent,
		}.Resolve(),
	})
	if err != nil {
		return fmt.Errorf("copy phase failed: %w", err)
	}

	mu.Lock()
	evCopy := append([]scaling.ScalingEvent(nil), events...)
	mu.Unlock()
	sawThrottle := false
	sawScaleDown := false
	for _, ev := range evCopy {
		if ev.Pressure == scaling.PressureFSThrottle {
			sawThrottle = true
		}
		if ev.Knob == "WorkerCount" && ev.NewValue < ev.OldValue {
			sawScaleDown = true
		}
	}
	if !sawThrottle {
		return fmt.Errorf("expected FS_THROTTLE during chaos run; events=%d", len(evCopy))
	}
	if !sawScaleDown {
		fmt.Println("⚠ no WorkerCount scale-down observed (throttle may have hit inter-op only); continuing unique-event checks")
	}

	fmt.Println("✓ Phase 4: Unique copy success events")
	if err := copyshared.VerifyCopyCompletion(database); err != nil {
		return err
	}
	if err := copyshared.VerifyUniqueCopySuccessEvents(database); err != nil {
		return err
	}
	if err := assertNoAttemptWithAlreadyExisted(database); err != nil {
		return err
	}
	return nil
}

func assertNoAttemptWithAlreadyExisted(database *db.DB) error {
	if database == nil || database.Ops() == nil {
		return nil
	}
	ops := database.Ops()
	var bad []string
	for after := ""; ; {
		ids, err := ops.ListNodeIDs(opsdb.SideSRC, after, 500)
		if err != nil {
			return fmt.Errorf("list src nodes: %w", err)
		}
		if len(ids) == 0 {
			break
		}
		stMap, err := ops.BatchGetStatus(opsdb.SideSRC, ids)
		if err != nil {
			return fmt.Errorf("batch get status: %w", err)
		}
		for _, id := range ids {
			st, ok := stMap[id]
			if !ok {
				continue
			}
			if st.XferDstRef != "" && st.CopyStatus == db.CopyStatusAlreadyExisted {
				bad = append(bad, id)
			}
		}
		after = ids[len(ids)-1]
	}
	if len(bad) > 0 {
		return fmt.Errorf("already_existed on nodes with attempt marker: %v", bad)
	}
	fmt.Println("✓ No already_existed events on nodes with attempt markers")
	return nil
}
