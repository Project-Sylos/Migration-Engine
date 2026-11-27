// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSealAtHardCapAllowsAppendWithoutWait(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "wait.db"),
		OpsDir: filepath.Join(dir, "wait.ops"),
		SealBuffer: &SealBufferOptions{
			RowThreshold:  100,
			HardCap:       2,
			FlushInterval: time.Hour, // avoid timer flush during test
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	seal.rowThreshold = 100
	seal.hardCap = 2

	for i := 0; i < 2; i++ {
		if err := seal.AddDiscoveryNodes([]InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{ID: "pre-" + string(rune('a'+i)), Depth: 0, Type: NodeTypeFile, TraversalStatus: StatusSuccessful},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if !seal.AtHardCap() {
		t.Fatal("expected AtHardCap after filling to hardCap")
	}
	if !database.SealAtHardCap() {
		t.Fatal("expected DB.SealAtHardCap")
	}

	// Append must not block when at hard cap (lease/pull gate owns backpressure).
	done := make(chan struct{})
	go func() {
		_ = seal.AddDiscoveryNodes([]InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{ID: "over", Depth: 0, Type: NodeTypeFile, TraversalStatus: StatusSuccessful},
		}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("append blocked on hard cap; expected non-blocking handoff")
	}

	tel := seal.TelemetrySnapshot()
	if tel.HardCapHitsSinceLastPoll <= 0 {
		t.Fatalf("expected HardCapHitsSinceLastPoll > 0, got %d", tel.HardCapHitsSinceLastPoll)
	}
	if tel.HardCapWaitNsSinceLastPoll != 0 {
		t.Fatalf("expected HardCapWaitNsSinceLastPoll == 0 (no producer wait), got %d", tel.HardCapWaitNsSinceLastPoll)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && seal.AtHardCap() {
		time.Sleep(5 * time.Millisecond)
	}
	if seal.AtHardCap() {
		t.Fatal("expected overflow flush to drain below the hard cap without the producer waiting")
	}
}
