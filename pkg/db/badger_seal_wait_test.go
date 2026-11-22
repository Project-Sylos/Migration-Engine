// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestHardCapWaitNsAccumulatesAndResets(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "wait.db"),
		OpsDir: filepath.Join(dir, "wait.ops"),
		SealBuffer: &SealBufferOptions{
			RowThreshold:  100,
			HardCap:       2,
			FlushInterval: time.Hour, // avoid timer flush during wait
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	seal.rowThreshold = 100
	seal.hardCap = 2

	// Fill to hard cap without flushing.
	for i := 0; i < 2; i++ {
		if err := seal.AddDiscoveryNodes([]InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{ID: "pre-" + string(rune('a'+i)), Depth: 0, Type: NodeTypeFile, TraversalStatus: StatusSuccessful},
		}}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = seal.AddDiscoveryNodes([]InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{ID: "blocked", Depth: 0, Type: NodeTypeFile, TraversalStatus: StatusSuccessful},
		}})
	}()

	// Wait until a waiter is blocked, then flush to release.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if seal.IOWaitActive() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // ensure measurable wait
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	tel := seal.TelemetrySnapshot()
	if tel.HardCapWaitNsSinceLastPoll <= 0 {
		t.Fatalf("expected HardCapWaitNsSinceLastPoll > 0, got %d (hits=%d)",
			tel.HardCapWaitNsSinceLastPoll, tel.HardCapHitsSinceLastPoll)
	}
	tel2 := seal.TelemetrySnapshot()
	if tel2.HardCapWaitNsSinceLastPoll != 0 {
		t.Fatalf("expected swap-on-read reset, got %d", tel2.HardCapWaitNsSinceLastPoll)
	}
}
