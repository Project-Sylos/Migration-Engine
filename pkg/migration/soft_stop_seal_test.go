// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"strings"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestFinishSoftStopBulkPhaseSkipsIndexRebuild(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/soft-stop-seal.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.BeginTraversalPhase(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Soft stop must not require CREATE INDEX; seal stop + checkpoint is enough.
	if err := finishSoftStopBulkPhase(context.Background(), database, nil, nil); err != nil {
		t.Fatal(err)
	}
	if database.HardAborted() {
		t.Fatal("soft stop must not hard-abort")
	}
}

func TestFinishSoftStopBulkPhaseHonorsHardAbort(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/soft-stop-abort.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.BeginTraversalPhase(context.Background()); err != nil {
		t.Fatal(err)
	}
	database.AbortTraversalPhase()
	err = finishSoftStopBulkPhase(context.Background(), database, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "force stop") {
		t.Fatalf("want force-stop teardown error, got %v", err)
	}
}
