// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"testing"
	"time"
)

func TestRecentRing_newestFirstAndCap(t *testing.T) {
	r := newRecentRing(3)
	r.add(RecentLog{ID: "1", Message: "a", At: time.Unix(1, 0)})
	r.add(RecentLog{ID: "2", Message: "b", At: time.Unix(2, 0)})
	r.add(RecentLog{ID: "3", Message: "c", At: time.Unix(3, 0)})
	r.add(RecentLog{ID: "4", Message: "d", At: time.Unix(4, 0)})
	got := r.recent(10)
	if len(got) != 3 || got[0].ID != "4" || got[1].ID != "3" || got[2].ID != "2" {
		t.Fatalf("got %+v", got)
	}
	got = r.recent(1)
	if len(got) != 1 || got[0].ID != "4" {
		t.Fatalf("limit 1 got %+v", got)
	}
}

func TestSender_RecentDoesNotNeedDB(t *testing.T) {
	s, err := NewSender(nil, "127.0.0.1:1997", "info")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Log("info", "hello", "test", "id"); err != nil {
		t.Fatal(err)
	}
	got := s.Recent(10)
	if len(got) != 1 || got[0].Message != "hello" || got[0].Level != "info" || got[0].ID == "" {
		t.Fatalf("got %+v", got)
	}
}
