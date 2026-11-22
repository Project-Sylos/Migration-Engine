// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package memweight

import "testing"

func TestLedgerMoveReleaseInvariants(t *testing.T) {
	l := New()
	l.Add(Pending, 10)
	l.Move(Pending, InFlight, 4)
	if got := l.Level(Pending); got != 6 {
		t.Fatalf("pending=%d want 6", got)
	}
	if got := l.Level(InFlight); got != 4 {
		t.Fatalf("inFlight=%d want 4", got)
	}
	l.Move(InFlight, SealBuffer, 4)
	l.Move(SealBuffer, SealFlush, 4)
	l.Release(SealFlush, 4)
	if got := l.Level(SealFlush); got != 0 {
		t.Fatalf("sealFlush=%d want 0", got)
	}
	l.Add(Pending, 2)
	l.Release(Pending, 99)
	if got := l.Level(Pending); got != 0 {
		t.Fatalf("pending after clamp=%d want 0", got)
	}
}

func TestLedgerFlushFailRestores(t *testing.T) {
	l := New()
	l.Add(SealBuffer, 100)
	l.Move(SealBuffer, SealFlush, 100)
	l.Move(SealFlush, SealBuffer, 100)
	if got := l.Level(SealBuffer); got != 100 {
		t.Fatalf("sealBuffer=%d want 100", got)
	}
	if got := l.Level(SealFlush); got != 0 {
		t.Fatalf("sealFlush=%d want 0", got)
	}
}

func TestLedgerFurthestOver(t *testing.T) {
	l := New()
	l.SetCeiling(Pending, 100)
	l.SetCeiling(InFlight, 100)
	l.SetCeiling(SealBuffer, 100)
	l.SetCeiling(SealFlush, 100)

	l.Add(Pending, 50)
	l.Add(SealBuffer, 120)
	best := l.FurthestOver()
	if best.Bucket != SealBuffer {
		t.Fatalf("got %s want sealBuffer", BucketName(best.Bucket))
	}

	empty := New()
	if got := empty.FurthestOver().Bucket; got != Pending {
		t.Fatalf("empty furthest=%s want pending", BucketName(got))
	}
}

func TestLedgerDecayCeiling(t *testing.T) {
	l := New()
	l.SetCeiling(Pending, 10_000)
	l.DecayCeiling(Pending, 2_000)
	snap := l.Snapshots()[Pending]
	if snap.Ceiling >= 10_000 || snap.Ceiling < 2_000 {
		t.Fatalf("ceiling=%d want between 2000 and 10000", snap.Ceiling)
	}
}
