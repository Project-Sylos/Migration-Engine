// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestOrganicFlushTelemetryExcludesForced(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Path:   filepath.Join(dir, "org.db"),
		OpsDir: filepath.Join(dir, "org.ops"),
		SealBuffer: &SealBufferOptions{
			RowThreshold:  1000,
			HardCap:       10_000,
			FlushInterval: time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seal := database.sealBuffer.(*badgerSeal)
	for i := 0; i < 5; i++ {
		if err := seal.AddDiscoveryNodes([]InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{
				ID: fmt.Sprintf("f-%d", i), Depth: 0, Type: NodeTypeFile,
				TraversalStatus: StatusSuccessful,
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}

	// Explicit Flush is forced (round-advance / stop style).
	if err := seal.Flush(); err != nil {
		t.Fatal(err)
	}
	tel := seal.TelemetrySnapshot()
	if tel.FlushCountSinceLastPoll < 1 {
		t.Fatalf("expected total flush count, got %d", tel.FlushCountSinceLastPoll)
	}
	if tel.OrganicFlushCountSinceLastPoll != 0 {
		t.Fatalf("forced Flush must not count as organic, got %d", tel.OrganicFlushCountSinceLastPoll)
	}
	if tel.ForcedFlushCountSinceLastPoll < 1 || tel.ForcedFlushDurationNsSinceLastPoll <= 0 {
		t.Fatalf("forced flush stats missing: count=%d dur=%d",
			tel.ForcedFlushCountSinceLastPoll, tel.ForcedFlushDurationNsSinceLastPoll)
	}

	for i := 0; i < 5; i++ {
		if err := seal.AddDiscoveryNodes([]InsertOperation{{
			QueueType: "SRC", Level: 0, Status: StatusSuccessful,
			State: &NodeState{
				ID: fmt.Sprintf("g-%d", i), Depth: 0, Type: NodeTypeFile,
				TraversalStatus: StatusSuccessful,
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := seal.flush(true); err != nil {
		t.Fatal(err)
	}
	tel = seal.TelemetrySnapshot()
	if tel.OrganicFlushCountSinceLastPoll < 1 {
		t.Fatalf("organic flush should count, got %d", tel.OrganicFlushCountSinceLastPoll)
	}
	if tel.OrganicFlushDurationNsSinceLastPoll <= 0 {
		t.Fatalf("organic flush duration sum missing: %d", tel.OrganicFlushDurationNsSinceLastPoll)
	}
	if tel.ForcedFlushCountSinceLastPoll != 0 {
		t.Fatalf("organic path must not count as forced, got %d", tel.ForcedFlushCountSinceLastPoll)
	}
	if tel.LastOrganicFlushRows <= 0 || tel.LastOrganicFlushDurationNs <= 0 {
		t.Fatalf("organic last flush stats missing: rows=%d dur=%d",
			tel.LastOrganicFlushRows, tel.LastOrganicFlushDurationNs)
	}
}
