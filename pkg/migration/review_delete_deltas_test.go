// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestDeleteSkippedStatusTransitionDeltas(t *testing.T) {
	t.Parallel()

	deltas := make(map[string]int64)
	addReviewDeltaForDeleteStatusTransition(
		deltas,
		db.DeleteStatusPending,
		db.DeleteStatusSkipped,
	)
	if deltas[DeltaDeletePending] != -1 || deltas[DeltaDeleteSkipped] != 1 {
		t.Fatalf("pending -> skipped deltas = %#v", deltas)
	}

	deltas = make(map[string]int64)
	addReviewDeltaForDeleteStatusTransition(
		deltas,
		db.DeleteStatusSkipped,
		db.DeleteStatusPending,
	)
	if deltas[DeltaDeleteSkipped] != -1 || deltas[DeltaDeletePending] != 1 {
		t.Fatalf("skipped -> pending deltas = %#v", deltas)
	}
}

func TestAddDeleteSelectedSizeDelta(t *testing.T) {
	t.Parallel()
	file := &db.NodeState{
		Type: db.NodeTypeFile, Size: 250,
		CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	deltas := make(map[string]int64)
	addDeleteSelectedSizeDelta(deltas, file, db.DeleteStatusPending, db.DeleteStatusSkipped)
	if deltas[DeltaSizeSelected] != -250 || deltas[DeltaSizeDeleteSelected] != -250 {
		t.Fatalf("pending->skipped size deltas = %#v", deltas)
	}
	if deltas[DeltaFiles] != -1 {
		t.Fatalf("pending->skipped files delta = %#v", deltas)
	}

	deltas = make(map[string]int64)
	addDeleteSelectedSizeDelta(deltas, file, db.DeleteStatusSkipped, db.DeleteStatusPending)
	if deltas[DeltaSizeSelected] != 250 || deltas[DeltaSizeDeleteSelected] != 250 {
		t.Fatalf("skipped->pending size deltas = %#v", deltas)
	}
	if deltas[DeltaFiles] != 1 {
		t.Fatalf("skipped->pending files delta = %#v", deltas)
	}

	deltas = make(map[string]int64)
	folder := &db.NodeState{
		Type: db.NodeTypeFolder, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	addDeleteSelectedSizeDelta(deltas, folder, db.DeleteStatusPending, db.DeleteStatusSkipped)
	if deltas[DeltaFolders] != -1 || deltas[DeltaSizeSelected] != 0 {
		t.Fatalf("folder skip deltas = %#v", deltas)
	}

	// Not copy-complete: no size delta.
	deltas = make(map[string]int64)
	pendingCopy := &db.NodeState{Type: db.NodeTypeFile, Size: 250, CopyStatus: db.CopyStatusPending}
	addDeleteSelectedSizeDelta(deltas, pendingCopy, db.DeleteStatusPending, db.DeleteStatusSkipped)
	if len(deltas) != 0 {
		t.Fatalf("expected no deltas for non-complete copy, got %#v", deltas)
	}
}
