// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"
)

func TestMinimalSecondaryIndexNames(t *testing.T) {
	nodes := nodeSecondaryIndexes(TableSrcNodes)
	if len(nodes) != 2 {
		t.Fatalf("node indexes=%d want 2", len(nodes))
	}
	wantNode := map[string]bool{
		TableSrcNodes + "_parent_id_idx": true,
		TableSrcNodes + "_depth_idx":     true,
	}
	for _, idx := range nodes {
		if !wantNode[idx.name] {
			t.Fatalf("unexpected node index %s", idx.name)
		}
	}
	for _, name := range obsoleteNodeIndexNames(TableSrcNodes) {
		if wantNode[name] {
			t.Fatalf("obsolete %s still in active set", name)
		}
	}

	ev := statusEventSecondaryIndexes(TableDstStatusEvents)
	if len(ev) != 1 || ev[0].name != TableDstStatusEvents+"_id_event_time_idx" {
		t.Fatalf("status indexes=%v want only id_event_time", ev)
	}
	for _, name := range obsoleteStatusEventIndexNames(TableDstStatusEvents) {
		if name == ev[0].name {
			t.Fatal("active status index marked obsolete")
		}
	}
}
