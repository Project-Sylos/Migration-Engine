// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/tests/traversal/shared"
)

func main() {
	fmt.Println("=== Spectra FUSE Mount Migration Test ===")
	if err := runTest(); err != nil {
		fmt.Printf("\n❌ TEST FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n✅ TEST PASSED!")
}

func runTest() error {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return fmt.Errorf("skipped: FUSE mount test requires linux or darwin (got %s)", runtime.GOOS)
	}

	configPath, err := resolveConfigPath()
	if err != nil {
		return err
	}
	fmt.Printf("Spectra config: %s\n", configPath)

	chaosEnabled, err := shared.IsChaosEnabled(configPath)
	if err != nil {
		return fmt.Errorf("read chaos config: %w", err)
	}
	if chaosEnabled {
		fmt.Println("  Spectra chaos: enabled (rate limits may apply)")
	} else {
		fmt.Println("  Spectra chaos: disabled")
	}

	srcMount, err := os.MkdirTemp("", "spectra-fuse-src-")
	if err != nil {
		return fmt.Errorf("create src mount dir: %w", err)
	}
	dstMount, err := os.MkdirTemp("", "spectra-fuse-dst-")
	if err != nil {
		_ = os.RemoveAll(srcMount)
		return fmt.Errorf("create dst mount dir: %w", err)
	}
	defer os.RemoveAll(srcMount)
	defer os.RemoveAll(dstMount)

	srcMount, err = filepath.Abs(srcMount)
	if err != nil {
		return err
	}
	dstMount, err = filepath.Abs(dstMount)
	if err != nil {
		return err
	}

	fmt.Println("📋 Phase 1: Spectra FUSE mounts")
	fmt.Println("================================")
	fmt.Printf("  primary (src): %s\n", srcMount)
	fmt.Printf("  s1 (dst):      %s\n", dstMount)

	mountProc, err := StartSpectraMounts(srcMount, dstMount, configPath)
	if err != nil {
		return err
	}
	defer mountProc.Stop()

	if err := mountProc.WaitReady(60 * time.Second); err != nil {
		return fmt.Errorf("mount readiness: %w", err)
	}
	fmt.Println("  ✓ both worlds mounted and accessible")
	fmt.Println()

	fmt.Println("📋 Phase 2: Sylos setup (local adapters → FUSE paths)")
	fmt.Println("======================================================")
	localOpts := shared.LocalTestOptions{
		RemoveMigrationDB: true,
		WorkerCount:       20,
		ProgressTick:      time.Second,
	}
	autoscaler := migration.DefaultAutoscalerConfig()
	autoscaler.DebugAIMD = true
	autoscaler.OnEvent = logScalingEvent
	localOpts.Autoscaler = autoscaler
	fmt.Println("  autoscaler: enabled (default)")

	cfg, err := shared.SetupLocalTestWithOptions(srcMount, dstMount, localOpts)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}
	fmt.Println()

	fmt.Println("🚀 Phase 3: Migration through FUSE mount points")
	fmt.Println("===============================================")
	result, err := migration.LetsMigrate(cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	fmt.Println()

	fmt.Println("✓ Phase 4: Verification")
	fmt.Println("=========================")
	shared.PrintVerification(result)

	scalingEventLog.mu.Lock()
	evCount := len(scalingEventLog.events)
	scalingEventLog.mu.Unlock()
	if evCount == 0 {
		fmt.Println("\nAutoscaler: no scaling events recorded")
	} else {
		fmt.Printf("\nAutoscaler: %d scaling event(s) recorded\n", evCount)
	}
	return nil
}

var scalingEventLog struct {
	mu     sync.Mutex
	events []scaling.ScalingEvent
}

func logScalingEvent(ev scaling.ScalingEvent) {
	scalingEventLog.mu.Lock()
	scalingEventLog.events = append(scalingEventLog.events, ev)
	scalingEventLog.mu.Unlock()
	fmt.Printf("\n  scaling event: %s\n", scaling.FormatScalingEvent(ev.Queue, ev.Knob, ev.OldValue, ev.NewValue, ev.Pressure))
}
