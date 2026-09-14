// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import "testing"

func TestDepthStatsBuildAsWeGo(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.ApplyReviewAndDepth(
		[]string{"copy/pending"},
		[]int64{1},
		[]DepthCounterDelta{
			{Side: SideSRC, Depth: 2, Key: "traversal/pending", Delta: 3},
			{Side: SideSRC, Depth: 3, Key: "traversal/pending", Delta: 2},
			{Side: SideSRC, Depth: 2, Key: "traversal/pending", Delta: -1},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.GetDepthStat(SideSRC, 2, "traversal/pending")
	if err != nil || n != 2 {
		t.Fatalf("depth 2 pending=%d err=%v", n, err)
	}
	n, err = s.GetDepthStatTotal(SideSRC, "traversal/pending")
	if err != nil || n != 4 {
		t.Fatalf("total pending=%d err=%v", n, err)
	}
	max, err := s.GetDepthMax(SideSRC)
	if err != nil || max != 3 {
		t.Fatalf("max=%d err=%v", max, err)
	}
	rev, err := s.GetStat("copy/pending")
	if err != nil || rev != 1 {
		t.Fatalf("review=%d err=%v", rev, err)
	}
	rows, err := s.ListDepthStats()
	if err != nil || len(rows) != 2 {
		t.Fatalf("list=%d err=%v", len(rows), err)
	}
}

// Large uncoalesced discovery flushes must SUM to a few keys, not one Badger write per row.
func TestApplyReviewAndDepthCoalescesLargeBatch(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 50_000
	reviewKeys := make([]string, 0, n)
	reviewDeltas := make([]int64, 0, n)
	depth := make([]DepthCounterDelta, 0, n*2)
	for i := 0; i < n; i++ {
		reviewKeys = append(reviewKeys, "traversal/pending")
		reviewDeltas = append(reviewDeltas, 1)
		depth = append(depth,
			DepthCounterDelta{Side: SideSRC, Depth: 4, Key: "traversal/pending", Delta: 1},
			DepthCounterDelta{Side: SideSRC, Depth: 4, Key: "traversal/folder", Delta: 1},
		)
	}
	if err := s.ApplyReviewAndDepth(reviewKeys, reviewDeltas, depth); err != nil {
		t.Fatal(err)
	}
	rev, err := s.GetStat("traversal/pending")
	if err != nil || rev != n {
		t.Fatalf("review=%d err=%v", rev, err)
	}
	pending, err := s.GetDepthStat(SideSRC, 4, "traversal/pending")
	if err != nil || pending != n {
		t.Fatalf("depth pending=%d err=%v", pending, err)
	}
	folder, err := s.GetDepthStat(SideSRC, 4, "traversal/folder")
	if err != nil || folder != n {
		t.Fatalf("depth folder=%d err=%v", folder, err)
	}
}
