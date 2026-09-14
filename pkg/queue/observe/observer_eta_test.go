// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"math"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestChooseEtaBasis_folderOnlyForcesItems(t *testing.T) {
	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: 100,
		BytesRemaining: 1e12,
		ItemsRate:      2,
		BytesRate:      1e9,
		ItemsCV:        2,
		BytesCV:        0.01,
		FolderOnly:     true,
		Now:            time.Now(),
	})
	if !out.OK || out.Basis != EtaBasisItems {
		t.Fatalf("got %+v want items", out)
	}
	if math.Abs(out.Seconds-50) > 0.01 {
		t.Fatalf("seconds=%v want 50", out.Seconds)
	}
}

func TestChooseEtaBasis_stableItemsBeatsNoisyBytes(t *testing.T) {
	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: 900,
		BytesRemaining: 50e6,
		ItemsRate:      3,
		BytesRate:      1e4,
		ItemsCV:        0.1,
		BytesCV:        2.5,
		Now:            time.Now(),
	})
	if !out.OK || out.Basis != EtaBasisItems {
		t.Fatalf("got %+v want items", out)
	}
}

func TestChooseEtaBasis_fatFileOverride(t *testing.T) {
	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: 1,
		BytesRemaining: 5e9,
		ItemsRate:      0.01,
		BytesRate:      20e6,
		ItemsCV:        0.01,
		BytesCV:        0.5,
		Now:            time.Now(),
	})
	if !out.OK || out.Basis != EtaBasisBytes {
		t.Fatalf("got %+v want bytes", out)
	}
}

func TestChooseEtaBasis_hysteresisHoldsBriefly(t *testing.T) {
	now := time.Now()
	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: 500,
		BytesRemaining: 50e6,
		ItemsRate:      3,
		BytesRate:      1e5,
		ItemsCV:        0.2,
		BytesCV:        0.25, // items slightly better, but not <= 0.7 * bytes
		PrevBasis:      EtaBasisBytes,
		PrevSince:      now.Add(-5 * time.Second),
		Now:            now,
	})
	if !out.OK || out.Basis != EtaBasisBytes {
		t.Fatalf("got %+v want sticky bytes", out)
	}
}

func TestChooseEtaBasis_hysteresisYieldsOnClearCVAdvantage(t *testing.T) {
	now := time.Now()
	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: 500,
		BytesRemaining: 50e6,
		ItemsRate:      3,
		BytesRate:      1e5,
		ItemsCV:        0.1,
		BytesCV:        1.0, // items CV <= 0.7 * bytes CV
		PrevBasis:      EtaBasisBytes,
		PrevSince:      now.Add(-5 * time.Second),
		Now:            now,
	})
	if !out.OK || out.Basis != EtaBasisItems {
		t.Fatalf("got %+v want items after clear advantage", out)
	}
}

func TestChooseEtaBasis_done(t *testing.T) {
	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: 0,
		BytesRemaining: 0,
		PrevBasis:      EtaBasisBytes,
		Now:            time.Now(),
	})
	if !out.OK || out.Basis != EtaBasisBytes || out.Seconds != 0 {
		t.Fatalf("got %+v", out)
	}
}

func TestCoefficientOfVariation(t *testing.T) {
	stable := coefficientOfVariation([]float64{3, 3.1, 2.9, 3.05})
	noisy := coefficientOfVariation([]float64{1, 100, 2, 80})
	if !(stable < noisy) {
		t.Fatalf("stable CV=%v should be < noisy CV=%v", stable, noisy)
	}
	if !math.IsInf(coefficientOfVariation([]float64{1, 2}), 1) {
		t.Fatal("too few samples should be +Inf")
	}
}

func TestApplyCopyDeleteETA_endToEnd(t *testing.T) {
	o := NewQueueObserver(nil, time.Second)
	now := time.Now()
	// Seed stable item interval rates, noisy byte rates.
	for i := 0; i < 10; i++ {
		at := now.Add(time.Duration(i) * time.Second)
		metric := &ExternalQueueMetrics{
			ItemsCompleted: int64(i * 3),
			ItemsTotal:     300,
			Bytes:          int64(i * 1000 * (1 + i%5)), // jumpy
			BytesTotal:     1e9,
			ItemsPerSecond: 3,
			BytesPerSecond: float64(1000 * (1 + i%5)),
			CopyPass:       2,
		}
		o.recordETAIntervalRates("copy", metric.ItemsCompleted, metric.Bytes, at)
		o.applyCopyDeleteETA("copy", metric, at)
		if i < etaMinCVSamples {
			continue
		}
		if metric.EtaBasis != EtaBasisItems {
			t.Fatalf("tick %d basis=%q want items (eta=%v)", i, metric.EtaBasis, metric.EtaSeconds)
		}
		if metric.EtaSeconds == nil {
			t.Fatalf("tick %d missing eta_seconds", i)
		}
	}
}

func TestFolderOnlyPass(t *testing.T) {
	if !folderOnlyPass("copy", 1) || folderOnlyPass("copy", 2) {
		t.Fatal("copy pass checks")
	}
	if folderOnlyPass("delete", 1) || !folderOnlyPass("delete", 2) {
		t.Fatal("delete pass checks")
	}
}

func TestEtaSaturated(t *testing.T) {
	if etaSaturated(16, 16) != true {
		t.Fatal("16/16 should be saturated")
	}
	if etaSaturated(16, 12) != true { // 0.75
		t.Fatal("12/16 should be saturated")
	}
	if etaSaturated(16, 2) {
		t.Fatal("2/16 should be underfed")
	}
	if etaSaturated(0, 0) {
		t.Fatal("zero workers should not be saturated")
	}
}

func TestEtaSecondsWithCruise_underfeedSplit(t *testing.T) {
	// Phase: 1002 items left; round drain: 2; observed collapsed to 2/s; cruise 16/s.
	// Naive remaining/observed = 1002/2 = 501s. Split: 2/2 + 1000/16 = 1 + 62.5 = 63.5s.
	sec := etaSecondsWithCruise(
		EtaBasisItems,
		1002, 0,
		2, true,
		2, 0,
		16, 0,
		true,
		1002.0/2,
	)
	want := 1.0 + 1000.0/16.0
	if math.Abs(sec-want) > 0.01 {
		t.Fatalf("sec=%v want %v", sec, want)
	}
}

func TestEtaSecondsWithCruise_saturatedUsesFallback(t *testing.T) {
	sec := etaSecondsWithCruise(
		EtaBasisItems,
		800, 0,
		50, true,
		16, 0,
		16, 0,
		false,
		50,
	)
	if sec != 50 {
		t.Fatalf("sec=%v want fallback 50", sec)
	}
}

func TestEtaSecondsWithCruise_noRoundStatsUsesCruise(t *testing.T) {
	sec := etaSecondsWithCruise(
		EtaBasisItems,
		800, 0,
		0, false,
		2, 0,
		16, 0,
		true,
		400,
	)
	want := 800.0 / 16.0
	if math.Abs(sec-want) > 0.01 {
		t.Fatalf("sec=%v want %v", sec, want)
	}
}

func TestApplyCopyDeleteETA_underfeedUsesCruiseForRest(t *testing.T) {
	o := NewQueueObserver(nil, time.Second)
	now := time.Now()

	// Saturate: refresh cruise at 16 items/s.
	for i := 0; i < 5; i++ {
		at := now.Add(time.Duration(i) * time.Second)
		metric := &ExternalQueueMetrics{
			ItemsCompleted: int64(i * 16),
			ItemsTotal:     5000,
			BytesTotal:     1,
			ItemsPerSecond: 16,
			BytesPerSecond: 1,
			CopyPass:       2,
			RoundExpected:  200,
			RoundCompleted: 50 + i*16,
			QueueStats:     queue.QueueStats{Workers: 16, InProgress: 16},
		}
		o.applyCopyDeleteETA("copy", metric, at)
	}

	// Underfeed at round end: 2 in progress, observed 2/s, still lots of phase work.
	under := &ExternalQueueMetrics{
		ItemsCompleted: 2000,
		ItemsTotal:     5000,
		BytesTotal:     1,
		ItemsPerSecond: 2,
		BytesPerSecond: 1,
		CopyPass:       2,
		RoundExpected:  200,
		RoundCompleted: 198,
		QueueStats:     queue.QueueStats{Workers: 16, InProgress: 2},
	}
	o.applyCopyDeleteETA("copy", under, now.Add(10*time.Second))
	if under.EtaSeconds == nil {
		t.Fatal("missing eta")
	}
	// drain 2 @ 2/s + rest 2998 @ 16/s ≈ 1 + 187.375
	want := 1.0 + 2998.0/16.0
	naive := 3000.0 / 2.0
	if math.Abs(*under.EtaSeconds-want) > 0.5 {
		t.Fatalf("eta=%v want ~%v (naive would be %v)", *under.EtaSeconds, want, naive)
	}
	if *under.EtaSeconds >= naive/2 {
		t.Fatalf("eta=%v still close to naive underfeed spike %v", *under.EtaSeconds, naive)
	}
}

func TestApplyTraversalBatchETA(t *testing.T) {
	t.Run("remaining over rate", func(t *testing.T) {
		m := &ExternalQueueMetrics{
			RoundExpected:  100,
			RoundCompleted: 40,
		}
		applyTraversalBatchETA(m, 10)
		if m.EtaBasis != EtaBasisItems || m.EtaSeconds == nil {
			t.Fatalf("got basis=%q eta=%v", m.EtaBasis, m.EtaSeconds)
		}
		if math.Abs(*m.EtaSeconds-6) > 0.01 {
			t.Fatalf("eta=%v want 6", *m.EtaSeconds)
		}
	})
	t.Run("batch complete", func(t *testing.T) {
		m := &ExternalQueueMetrics{
			RoundExpected:  50,
			RoundCompleted: 50,
		}
		applyTraversalBatchETA(m, 5)
		if m.EtaBasis != EtaBasisItems || m.EtaSeconds == nil || *m.EtaSeconds != 0 {
			t.Fatalf("got basis=%q eta=%v want 0", m.EtaBasis, m.EtaSeconds)
		}
	})
	t.Run("no expected leaves unset", func(t *testing.T) {
		m := &ExternalQueueMetrics{
			RoundExpected:  0,
			RoundCompleted: 0,
		}
		applyTraversalBatchETA(m, 10)
		if m.EtaBasis != "" || m.EtaSeconds != nil {
			t.Fatalf("want unset, got basis=%q eta=%v", m.EtaBasis, m.EtaSeconds)
		}
	})
	t.Run("zero rate leaves unset", func(t *testing.T) {
		m := &ExternalQueueMetrics{
			RoundExpected:  20,
			RoundCompleted: 5,
		}
		applyTraversalBatchETA(m, 0)
		if m.EtaBasis != "" || m.EtaSeconds != nil {
			t.Fatalf("want unset, got basis=%q eta=%v", m.EtaBasis, m.EtaSeconds)
		}
	})
}
