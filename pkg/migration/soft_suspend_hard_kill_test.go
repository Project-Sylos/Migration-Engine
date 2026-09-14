// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestSoftSuspendHardKilledHelpers(t *testing.T) {
	var unset context.Context // shutdown unset (same as cfg.ShutdownContext == nil)
	if softSuspendHardKilled(unset) {
		t.Fatal("nil shutdown should not be hard-killed")
	}
	if softSuspendHardKilled(context.Background()) {
		t.Fatal("live context should not be hard-killed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !softSuspendHardKilled(ctx) {
		t.Fatal("canceled shutdown should be hard-killed")
	}
	err := errIfSoftSuspendHardKilled(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if errIfSoftSuspendHardKilled(context.Background()) != nil {
		t.Fatal("live context should not error")
	}
}

func TestForceStopOverridesSoftSuspend(t *testing.T) {
	var unset context.Context
	if forceStopOverridesSoftSuspend(unset, nil) {
		t.Fatal("nil args should not override")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !forceStopOverridesSoftSuspend(ctx, nil) {
		t.Fatal("canceled shutdown should override")
	}
}

func TestAwaitSoftSuspendDBBailsOnCancel(t *testing.T) {
	waitCtx, cancelWait := context.WithCancel(context.Background())
	shutdown, cancelShutdown := context.WithCancel(context.Background())
	started := make(chan struct{})
	blocked := make(chan struct{})

	go func() {
		<-started
		cancelShutdown()
		cancelWait()
	}()

	err := awaitSoftSuspendDB(waitCtx, shutdown, func() error {
		close(started)
		<-blocked // never closed: simulates long Flush
		return nil
	})
	if err == nil {
		t.Fatal("expected cancel error")
	}
	if !softSuspendHardKilled(shutdown) {
		t.Fatal("shutdown should be canceled")
	}
	close(blocked)
}

func TestHardAbortSkipsDurableSoftStopTeardown(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/hard-abort.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.BeginTraversalPhase(context.Background()); err != nil {
		t.Fatal(err)
	}
	database.AbortTraversalPhase()
	if !database.HardAborted() {
		t.Fatal("expected HardAborted after AbortTraversalPhase")
	}
	if !forceStopOverridesSoftSuspend(context.Background(), database) {
		t.Fatal("HardAborted should override soft suspend")
	}
	err = finishSoftStopBulkPhase(context.Background(), database, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "force stop") {
		t.Fatalf("want force-stop teardown error, got %v", err)
	}
	if err := database.CheckpointWithRetry(context.Background(), 3); err != nil {
		t.Fatalf("CheckpointWithRetry after hard abort must no-op: %v", err)
	}
}
