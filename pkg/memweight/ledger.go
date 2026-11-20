// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package memweight

import (
	"sync"
)

// Bucket identifies an in-process memory-weight consumer.
type Bucket int

const (
	Pending Bucket = iota
	InFlight
	SealBuffer
	SealFlush
	BucketCount
)

const weightEMAAlpha = 0.2

// Default soft ceilings (same units as task/seal row weights).
const (
	DefaultPendingCeiling    int64 = 10_000
	DefaultInFlightCeiling   int64 = 10_000
	DefaultSealBufferCeiling int64 = 50_000
	DefaultSealFlushCeiling  int64 = 50_000
)

// Snapshot is a point-in-time view of one ledger bucket.
type Snapshot struct {
	Bucket  Bucket
	Level   int64
	Ceiling int64
	// Over is Level/Ceiling (0 when Ceiling <= 0).
	Over float64
	// Climb is inflow EMA minus outflow EMA (positive = filling).
	Climb float64
}

// Ledger tracks exact per-item memory weight across pending, in-flight, and seal buckets.
// Units are arbitrary but ratio-honest (not bytes). Every Add has a matching Move or Release.
type Ledger struct {
	mu      sync.Mutex
	level   [BucketCount]int64
	ceiling [BucketCount]int64
	inflow  [BucketCount]float64
	outflow [BucketCount]float64
}

// New builds a ledger with default soft ceilings.
func New() *Ledger {
	l := &Ledger{}
	l.ceiling[Pending] = DefaultPendingCeiling
	l.ceiling[InFlight] = DefaultInFlightCeiling
	l.ceiling[SealBuffer] = DefaultSealBufferCeiling
	l.ceiling[SealFlush] = DefaultSealFlushCeiling
	return l
}

// SetCeiling sets the soft ceiling for b (ignored when c <= 0 or b out of range).
func (l *Ledger) SetCeiling(b Bucket, c int64) {
	if l == nil || b < 0 || b >= BucketCount || c <= 0 {
		return
	}
	l.mu.Lock()
	l.ceiling[b] = c
	l.mu.Unlock()
}

// DecayCeiling gently lowers the soft ceiling toward toward when toward is below the current ceiling.
func (l *Ledger) DecayCeiling(b Bucket, toward int64) {
	if l == nil || b < 0 || b >= BucketCount || toward <= 0 {
		return
	}
	l.mu.Lock()
	cur := l.ceiling[b]
	if toward < cur {
		next := (cur + toward) / 2
		if next < toward {
			next = toward
		}
		if next < cur {
			l.ceiling[b] = next
		}
	}
	l.mu.Unlock()
}

// Add increases bucket b by w (no-op when w <= 0).
func (l *Ledger) Add(b Bucket, w int64) {
	if l == nil || w <= 0 || b < 0 || b >= BucketCount {
		return
	}
	l.mu.Lock()
	l.level[b] += w
	l.inflow[b] = weightEMAAlpha*float64(w) + (1-weightEMAAlpha)*l.inflow[b]
	l.mu.Unlock()
}

// Release decreases bucket b by w, clamped so level never goes negative.
func (l *Ledger) Release(b Bucket, w int64) {
	if l == nil || w <= 0 || b < 0 || b >= BucketCount {
		return
	}
	l.mu.Lock()
	if w > l.level[b] {
		w = l.level[b]
	}
	l.level[b] -= w
	l.outflow[b] = weightEMAAlpha*float64(w) + (1-weightEMAAlpha)*l.outflow[b]
	l.mu.Unlock()
}

// Move transfers w from one bucket to another atomically (clamped to source level).
func (l *Ledger) Move(from, to Bucket, w int64) {
	if l == nil || w <= 0 || from < 0 || from >= BucketCount || to < 0 || to >= BucketCount {
		return
	}
	if from == to {
		return
	}
	l.mu.Lock()
	if w > l.level[from] {
		w = l.level[from]
	}
	if w <= 0 {
		l.mu.Unlock()
		return
	}
	l.level[from] -= w
	l.level[to] += w
	l.outflow[from] = weightEMAAlpha*float64(w) + (1-weightEMAAlpha)*l.outflow[from]
	l.inflow[to] = weightEMAAlpha*float64(w) + (1-weightEMAAlpha)*l.inflow[to]
	l.mu.Unlock()
}

// Level returns the current weight in b.
func (l *Ledger) Level(b Bucket) int64 {
	if l == nil || b < 0 || b >= BucketCount {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level[b]
}

// Snapshots returns all bucket views under one lock.
func (l *Ledger) Snapshots() [BucketCount]Snapshot {
	var out [BucketCount]Snapshot
	if l == nil {
		return out
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for b := Bucket(0); b < BucketCount; b++ {
		ceil := l.ceiling[b]
		level := l.level[b]
		over := 0.0
		if ceil > 0 {
			over = float64(level) / float64(ceil)
		}
		out[b] = Snapshot{
			Bucket:  b,
			Level:   level,
			Ceiling: ceil,
			Over:    over,
			Climb:   l.inflow[b] - l.outflow[b],
		}
	}
	return out
}

// FurthestOver picks the bucket furthest over its soft ceiling.
// Climb breaks ties / prefers a filling bucket over a draining one at similar Over.
// When every level is zero, returns Pending (default consumer under host pressure).
func (l *Ledger) FurthestOver() Snapshot {
	snaps := l.Snapshots()
	best := snaps[Pending]
	any := false
	for i := range snaps {
		s := snaps[i]
		if s.Level > 0 {
			any = true
		}
		if bucketScore(s) > bucketScore(best) {
			best = s
		}
	}
	if !any {
		return snaps[Pending]
	}
	return best
}

func bucketScore(s Snapshot) float64 {
	bonus := 0.0
	if s.Climb > 0 {
		bonus = 0.05
	} else if s.Climb < 0 {
		bonus = -0.02
	}
	return s.Over + bonus
}

// BucketName returns a stable string for logs/events.
func BucketName(b Bucket) string {
	switch b {
	case Pending:
		return "pending"
	case InFlight:
		return "inFlight"
	case SealBuffer:
		return "sealBuffer"
	case SealFlush:
		return "sealFlush"
	default:
		return "unknown"
	}
}
