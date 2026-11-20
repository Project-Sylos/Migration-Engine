// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package loop

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/memory"
)

func TestSealConsistentlyAtMax(t *testing.T) {
	sc := &Autoscaler{}
	now := time.Now()
	for i := 0; i < 5; i++ {
		sc.sealRing = append(sc.sealRing, SealSamplePoint{
			At: now.Add(time.Duration(i) * time.Second), Rows: 50_000, Threshold: 50_000, AtMax: true,
		})
	}
	if !sc.sealConsistentlyAtMax() {
		t.Fatal("expected at-max")
	}
	// Only 2 of last 5 at max → below need (3).
	sc.sealRing[0].AtMax = false
	sc.sealRing[1].AtMax = false
	sc.sealRing[2].AtMax = false
	if sc.sealConsistentlyAtMax() {
		t.Fatal("expected not at-max with 2 of 5")
	}
	sc.sealRing[2].AtMax = true
	if !sc.sealConsistentlyAtMax() {
		t.Fatal("expected at-max with 3 of 5")
	}
}

func TestMaybeProbeSealRowThresholdRaisesWhenAtMax(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/probe.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, nil, nil, Config{Enabled: true, Interval: time.Second})
	sc.sealRows = 50_000
	now := time.Now()
	for i := 0; i < 5; i++ {
		sc.sealRing = append(sc.sealRing, SealSamplePoint{
			At: now, Rows: 50_000, Threshold: 50_000, AtMax: true,
		})
	}
	sc.sealFlushRing = []SealFlushPoint{{At: now, Rows: 10_000, DurationMs: 100}}

	sample := memory.MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 16 * 1024 * 1024}
	if !sc.maybeProbeSealRowThreshold(now, sample) {
		t.Fatal("expected probe raise")
	}
	if sc.sealRows <= 50_000 {
		t.Fatalf("sealRows=%d want > 50000", sc.sealRows)
	}
	if !sc.sealProbe.pending {
		t.Fatal("expected pending probe")
	}
	// Second raise blocked while probe pending.
	if sc.maybeProbeSealRowThreshold(now, sample) {
		t.Fatal("should not raise again while probe pending")
	}
}

func TestMaybeProbeSealRowThresholdBlockedWhenNotAtMax(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/probe2.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, nil, nil, Config{Enabled: true})
	sc.sealRows = 50_000
	now := time.Now()
	for i := 0; i < 5; i++ {
		sc.sealRing = append(sc.sealRing, SealSamplePoint{
			At: now, Rows: 1_000, Threshold: 50_000, AtMax: false,
		})
	}
	sample := memory.MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 16 * 1024 * 1024}
	if sc.maybeProbeSealRowThreshold(now, sample) {
		t.Fatal("should not raise when not at max")
	}
}

func TestSealEfficiencyProbeAcceptsLowerFlushCost(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/eff.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, nil, nil, Config{Enabled: true})
	sc.sealRows = 62_500
	sc.sealProbe = sealEfficiencyProbe{
		pending:              true,
		windowStart:          time.Now().Add(-sealProbeMinWindow - time.Second),
		rowsBefore:           50_000,
		baselineCostNsPerRow: 10_000, // 10µs/row
		baselineStallPerSec:  0.1,
		hasBaseline:          true,
		flushNs:              8_000 * 10_000, // 8µs/row over 10k rows
		flushRows:            10_000,
		stallWaitNs:          0,
	}
	sc.evaluateSealEfficiencyProbe(time.Now())
	if sc.sealProbe.pending {
		t.Fatal("probe should clear")
	}
	if sc.sealRows != 62_500 {
		t.Fatalf("accepted probe should keep raised size, got %d", sc.sealRows)
	}
	if sc.sealProbeLastResult != "accepted" {
		t.Fatalf("result=%q", sc.sealProbeLastResult)
	}
}

func TestSealEfficiencyProbeRollsBackWorseFlushCost(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/eff2.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	sc := NewAutoscaler(database, nil, nil, nil, nil, Config{Enabled: true})
	sc.sealRows = 62_500
	sc.sealProbe = sealEfficiencyProbe{
		pending:              true,
		windowStart:          time.Now().Add(-sealProbeMinWindow - time.Second),
		rowsBefore:           50_000,
		baselineCostNsPerRow: 10_000,
		baselineStallPerSec:  0,
		hasBaseline:          true,
		flushNs:              20_000 * 10_000, // 2x worse
		flushRows:            10_000,
		stallWaitNs:          int64(0.2 * float64(sealProbeMinWindow)), // worse stall
	}
	sc.evaluateSealEfficiencyProbe(time.Now())
	if sc.sealRows != 50_000 {
		t.Fatalf("rolled back sealRows=%d want 50000", sc.sealRows)
	}
	if sc.sealProbeLastResult != "rolled_back" {
		t.Fatalf("result=%q", sc.sealProbeLastResult)
	}
	if sc.sealProbeCooldownUntil.IsZero() {
		t.Fatal("expected probe cooldown after rollback")
	}
}
