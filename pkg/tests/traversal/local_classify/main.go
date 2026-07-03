// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/traversal/shared"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
)

func main() {
	fmt.Println("=== Local Autoscaler Classification Smoke Test ===")
	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	srcRoot, dstRoot, cleanup, err := seedLocalTree()
	if err != nil {
		return err
	}
	defer cleanup()

	const workers = 16
	var events []scaling.ScalingEvent
	var mu sync.Mutex

	fmt.Println("📋 Phase 1: Setup")
	fmt.Println("================")
	cfg, err := shared.SetupLocalTest(srcRoot, dstRoot, true)
	if err != nil {
		return err
	}
	cfg.WorkerCount = workers
	cfg.Autoscaler = migration.AutoscalerConfig{
		Enabled:  true,
		Interval: 500 * time.Millisecond,
		OnEvent: func(ev scaling.ScalingEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
			fmt.Printf("\n  scaling event: %s\n", scaling.FormatScalingEvent(ev.Queue, ev.Knob, ev.OldValue, ev.NewValue, ev.Pressure))
		},
	}

	if src, ok := cfg.Source.Adapter.(*local.LocalFS); ok {
		src.SetActiveWorkers(workers)
		now := time.Now()
		tr := src.GetDegradationState().AmbiguousTracker()
		for i := 0; i < 4; i++ {
			tr.Record("ListChildren", "EIO", workers, now.Add(time.Duration(i)*5*time.Millisecond))
		}
		src.InjectBeforeOp(func(operation string, attempt int) error {
			if operation == "ListChildren" && attempt == 1 {
				return fmt.Errorf("read dir: %w", syscall.EIO)
			}
			return nil
		})
	}
	if dst, ok := cfg.Destination.Adapter.(*local.LocalFS); ok {
		dst.SetActiveWorkers(workers)
	}

	fmt.Println()
	fmt.Println("🚀 Phase 2: Migration (local + autoscaler + injected EIO)")
	fmt.Println("=========================================================")
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	shared.PrintVerification(result)

	mu.Lock()
	evCopy := append([]scaling.ScalingEvent(nil), events...)
	mu.Unlock()

	fsThrottle := false
	for _, ev := range evCopy {
		if ev.Pressure == scaling.PressureFSThrottle {
			fsThrottle = true
			break
		}
	}
	if !fsThrottle {
		return fmt.Errorf("expected FS_THROTTLE from local ambiguous promotion; events=%d", len(evCopy))
	}
	fmt.Printf("FS_THROTTLE confirmed from local degradation signals (%d scaling events)\n", len(evCopy))
	return nil
}

func seedLocalTree() (src, dst string, cleanup func(), err error) {
	base, err := os.MkdirTemp("", "me-local-classify-*")
	if err != nil {
		return "", "", nil, err
	}
	src = filepath.Join(base, "src")
	dst = filepath.Join(base, "dst")
	for _, p := range []string{src, dst} {
		if err := os.MkdirAll(p, 0755); err != nil {
			os.RemoveAll(base)
			return "", "", nil, err
		}
	}
	for i := 0; i < 20; i++ {
		sub := filepath.Join(src, fmt.Sprintf("dir%d", i))
		if err := os.MkdirAll(sub, 0755); err != nil {
			os.RemoveAll(base)
			return "", "", nil, err
		}
		for j := 0; j < 10; j++ {
			p := filepath.Join(sub, fmt.Sprintf("file%d.txt", j))
			if err := os.WriteFile(p, []byte("payload"), 0644); err != nil {
				os.RemoveAll(base)
				return "", "", nil, err
			}
		}
	}
	return src, dst, func() { _ = os.RemoveAll(base) }, nil
}
