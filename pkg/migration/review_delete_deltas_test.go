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
