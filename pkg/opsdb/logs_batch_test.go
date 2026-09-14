// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
	"testing"
)

func TestAppendLogsBatch(t *testing.T) {
	s, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	recs := make([]LogRecord, 100)
	ids := make([]string, 100)
	for i := range recs {
		id := fmt.Sprintf("log-%d", i)
		ids[i] = id
		recs[i] = LogRecord{ID: id, Level: "info", Message: "m", Entity: "e", EntityID: "x"}
	}
	if err := s.AppendLogs(recs); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetLogsByIDs(ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("got %d logs, want %d", len(got), len(ids))
	}
	for _, id := range ids {
		if got[id].Message != "m" {
			t.Fatalf("missing or wrong log %s: %+v", id, got[id])
		}
	}
}
