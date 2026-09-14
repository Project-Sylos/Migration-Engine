// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"math"
	"time"
)

const (
	EtaBasisItems = "items"
	EtaBasisBytes = "bytes"

	etaCVWindow         = 90 * time.Second
	etaHysteresis       = 20 * time.Second
	etaCVAdvantage      = 0.7
	etaIdleItemsRate    = 0.05 // items/sec below this looks idle for fat-file override
	etaMinCVSamples     = 3
	// etaSaturateUtil: in_progress/workers at or above this updates cruise throughput.
	// Below it, end-of-round underfeed freezes cruise so drain dips do not inflate phase ETA.
	etaSaturateUtil = 0.7
)

// etaState tracks per-queue interval rates (for CV), sticky basis choice, and cruise rates.
type etaState struct {
	basis           string
	basisSince      time.Time
	itemRates       []ratePoint
	byteRates       []ratePoint
	lastItems       int64
	lastBytes       int64
	lastAt          time.Time
	cruiseItemsRate float64 // aggregate items/sec while saturated; frozen during underfeed
	cruiseBytesRate float64 // aggregate bytes/sec while saturated; frozen during underfeed
}

type ratePoint struct {
	at   time.Time
	rate float64
}

type etaChooseInput struct {
	ItemsRemaining int64
	BytesRemaining int64
	ItemsRate      float64
	BytesRate      float64
	ItemsCV        float64
	BytesCV        float64
	FolderOnly     bool
	PrevBasis      string
	PrevSince      time.Time
	Now            time.Time
}

type etaChooseResult struct {
	Basis   string
	Seconds float64
	OK      bool
}

func appendRatePoint(hist []ratePoint, now time.Time, rate float64, window time.Duration) []ratePoint {
	hist = append(hist, ratePoint{at: now, rate: rate})
	cutoff := now.Add(-window)
	firstKeep := 0
	for i := 0; i < len(hist); i++ {
		if hist[i].at.Before(cutoff) {
			firstKeep = i + 1
			continue
		}
		break
	}
	if firstKeep > 0 {
		hist = hist[firstKeep:]
	}
	const maxSamples = 512
	if len(hist) > maxSamples {
		hist = hist[len(hist)-maxSamples:]
	}
	return hist
}

func coefficientOfVariation(rates []float64) float64 {
	if len(rates) < etaMinCVSamples {
		return math.Inf(1)
	}
	var sum float64
	for _, r := range rates {
		sum += r
	}
	mean := sum / float64(len(rates))
	if mean <= 1e-12 {
		// All near-zero: treat as unstable for that clock.
		return math.Inf(1)
	}
	var varSum float64
	for _, r := range rates {
		d := r - mean
		varSum += d * d
	}
	stddev := math.Sqrt(varSum / float64(len(rates)))
	return stddev / mean
}

func cvFromPoints(pts []ratePoint) float64 {
	rates := make([]float64, len(pts))
	for i, p := range pts {
		rates[i] = p.rate
	}
	return coefficientOfVariation(rates)
}

func etaFromRemaining(remaining int64, rate float64) (seconds float64, ok bool) {
	if remaining <= 0 {
		return 0, true
	}
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0, false
	}
	return float64(remaining) / rate, true
}

func etaUtilization(workers, inProgress int) float64 {
	if workers <= 0 {
		return 0
	}
	u := float64(inProgress) / float64(workers)
	if u > 1 {
		return 1
	}
	return u
}

func etaSaturated(workers, inProgress int) bool {
	return etaUtilization(workers, inProgress) >= etaSaturateUtil
}

// etaSecondsWithCruise splits current-round drain (observed rate) from the rest of the
// phase (cruise rate). Underfeed at round end depresses aggregate throughput; cruise is
// only updated while saturated so later rounds are not priced at the drain rate.
func etaSecondsWithCruise(
	basis string,
	itemsRemaining, bytesRemaining int64,
	roundItemsRemaining int64,
	hasRoundStats bool,
	observedItems, observedBytes, cruiseItems, cruiseBytes float64,
	underfed bool,
	fallbackSeconds float64,
) float64 {
	if !underfed {
		return fallbackSeconds
	}
	var observed, cruise float64
	var phaseRemaining int64
	switch basis {
	case EtaBasisBytes:
		observed, cruise = observedBytes, cruiseBytes
		phaseRemaining = bytesRemaining
	default:
		observed, cruise = observedItems, cruiseItems
		phaseRemaining = itemsRemaining
	}
	if cruise <= 0 || math.IsNaN(cruise) || math.IsInf(cruise, 0) {
		return fallbackSeconds
	}
	if phaseRemaining <= 0 {
		return 0
	}
	// No round stats: price the whole remainder at cruise (avoid underfeed spike).
	if !hasRoundStats {
		sec, ok := etaFromRemaining(phaseRemaining, cruise)
		if ok {
			return sec
		}
		return fallbackSeconds
	}
	if observed <= 0 || math.IsNaN(observed) || math.IsInf(observed, 0) {
		sec, ok := etaFromRemaining(phaseRemaining, cruise)
		if ok {
			return sec
		}
		return fallbackSeconds
	}

	drainItems := roundItemsRemaining
	if drainItems < 0 {
		drainItems = 0
	}
	if drainItems > itemsRemaining {
		drainItems = itemsRemaining
	}

	var drainAmt, restAmt int64
	if basis == EtaBasisBytes {
		if itemsRemaining > 0 && drainItems < itemsRemaining {
			drainAmt = int64(float64(bytesRemaining) * float64(drainItems) / float64(itemsRemaining))
			if drainAmt > bytesRemaining {
				drainAmt = bytesRemaining
			}
			restAmt = bytesRemaining - drainAmt
		} else {
			drainAmt = bytesRemaining
		}
	} else {
		drainAmt = drainItems
		restAmt = itemsRemaining - drainItems
	}

	drainSec, drainOK := etaFromRemaining(drainAmt, observed)
	restSec, restOK := etaFromRemaining(restAmt, cruise)
	if !drainOK && !restOK {
		return fallbackSeconds
	}
	if !drainOK {
		sec, ok := etaFromRemaining(phaseRemaining, cruise)
		if ok {
			return sec
		}
		return fallbackSeconds
	}
	if !restOK {
		return drainSec
	}
	return drainSec + restSec
}

func folderOnlyPass(queueName string, copyPass int) bool {
	// Copy pass 1 = folders; delete pass 2 = folders (files first on delete pass 1).
	if queueName == "copy" {
		return copyPass == 1
	}
	if queueName == "delete" {
		return copyPass == 2
	}
	return false
}

// chooseEtaBasis picks items vs bytes using CV, folder-only force, fat-file override, and hysteresis.
func chooseEtaBasis(in etaChooseInput) etaChooseResult {
	itemSec, itemOK := etaFromRemaining(in.ItemsRemaining, in.ItemsRate)
	byteSec, byteOK := etaFromRemaining(in.BytesRemaining, in.BytesRate)

	if in.ItemsRemaining <= 0 && in.BytesRemaining <= 0 {
		basis := in.PrevBasis
		if basis == "" {
			basis = EtaBasisItems
		}
		return etaChooseResult{Basis: basis, Seconds: 0, OK: true}
	}

	if in.FolderOnly {
		if itemOK {
			return etaChooseResult{Basis: EtaBasisItems, Seconds: itemSec, OK: true}
		}
		return etaChooseResult{}
	}

	// Fat-file: bytes moving, items nearly idle.
	if byteOK && in.BytesRate > 0 && in.ItemsRate < etaIdleItemsRate {
		return etaChooseResult{Basis: EtaBasisBytes, Seconds: byteSec, OK: true}
	}

	pick := ""
	switch {
	case itemOK && byteOK:
		if in.ItemsCV < in.BytesCV {
			pick = EtaBasisItems
		} else if in.BytesCV < in.ItemsCV {
			pick = EtaBasisBytes
		} else if in.PrevBasis == EtaBasisBytes || in.PrevBasis == EtaBasisItems {
			pick = in.PrevBasis
		} else {
			pick = EtaBasisItems
		}
	case itemOK:
		pick = EtaBasisItems
	case byteOK:
		pick = EtaBasisBytes
	default:
		return etaChooseResult{}
	}

	// Hysteresis: keep previous basis unless the other is clearly stabler.
	if in.PrevBasis != "" && in.PrevBasis != pick && !in.PrevSince.IsZero() {
		held := in.Now.Sub(in.PrevSince)
		otherBetter := false
		if pick == EtaBasisItems && in.PrevBasis == EtaBasisBytes {
			otherBetter = !math.IsInf(in.ItemsCV, 1) && in.ItemsCV <= etaCVAdvantage*in.BytesCV
		} else if pick == EtaBasisBytes && in.PrevBasis == EtaBasisItems {
			otherBetter = !math.IsInf(in.BytesCV, 1) && in.BytesCV <= etaCVAdvantage*in.ItemsCV
		}
		if held < etaHysteresis && !otherBetter {
			pick = in.PrevBasis
		}
	}

	if pick == EtaBasisItems && itemOK {
		return etaChooseResult{Basis: EtaBasisItems, Seconds: itemSec, OK: true}
	}
	if pick == EtaBasisBytes && byteOK {
		return etaChooseResult{Basis: EtaBasisBytes, Seconds: byteSec, OK: true}
	}
	if itemOK {
		return etaChooseResult{Basis: EtaBasisItems, Seconds: itemSec, OK: true}
	}
	if byteOK {
		return etaChooseResult{Basis: EtaBasisBytes, Seconds: byteSec, OK: true}
	}
	return etaChooseResult{}
}

// applyTraversalBatchETA fills eta_seconds for the current BFS batch only.
// remaining = round_expected - round_completed; rate = list-task completions/sec
// (not discovery_rate_items_per_sec, which counts newly found children).
func applyTraversalBatchETA(metric *ExternalQueueMetrics, taskCompletionsPerSec float64) {
	if metric == nil {
		return
	}
	expected := metric.RoundExpected
	if expected <= 0 {
		return
	}
	remaining := expected - metric.RoundCompleted
	if remaining < 0 {
		remaining = 0
	}
	if remaining == 0 {
		metric.EtaBasis = EtaBasisItems
		metric.EtaSeconds = float64Ptr(0)
		return
	}
	rate := taskCompletionsPerSec
	if rate <= 0 {
		return
	}
	metric.EtaBasis = EtaBasisItems
	metric.EtaSeconds = float64Ptr(float64(remaining) / rate)
}

func float64Ptr(v float64) *float64 {
	return &v
}
