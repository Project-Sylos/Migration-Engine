// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestGetRemainingSourceSizeAfterDelete(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/remaining-src-size.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const want = int64(200 + 300 + 400 + 500 + 600)
	if err := database.WriteReviewStatsSnapshot(db.ReviewStatsSnapshot{SizeSrc: want}); err != nil {
		t.Fatal(err)
	}

	got, err := GetRemainingSourceSizeAfterDelete(database)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("remaining source size = %d, want %d", got, want)
	}
}
