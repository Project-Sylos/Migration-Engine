// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"errors"
	"strings"
	"testing"
)

func TestRetryFinalizeRequiresFinalizeFailed(t *testing.T) {
	m := &Migration{ID: "rf", phase: PhaseTraversing}
	err := m.RetryFinalize(FinalizeOverrides{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "finalize-failed") {
		t.Fatalf("err=%v", err)
	}
}

func TestNormalizeDeadFinalizingToFinalizeFailed(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()

	dir := t.TempDir()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "finalize-dead",
		Name:         "finalize-dead",
		MigrationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{PhaseFiltersSet, PhaseTraversing, PhaseTraversalFinalizing} {
		if err := mig.transitionTo(p); err != nil {
			t.Fatalf("transition to %s: %v", p, err)
		}
	}
	changed, err := mig.NormalizeDeadInProgressToSuspended()
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected phase change")
	}
	if mig.Phase() != PhaseTraversalFinalizeFailed {
		t.Fatalf("phase=%s", mig.Phase())
	}
	if mig.FinalizeError() == "" {
		t.Fatal("expected finalize error message")
	}
}

func TestCompleteDurableFinalizeSuccess(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()

	dir := t.TempDir()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "finalize-ok",
		Name:         "finalize-ok",
		MigrationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{PhaseFiltersSet, PhaseTraversing} {
		if err := mig.transitionTo(p); err != nil {
			t.Fatalf("transition to %s: %v", p, err)
		}
	}
	if err := mig.completeDurableFinalize(
		PhaseTraversalFinalizing,
		PhaseTraversalFinalizeFailed,
		PhaseTraversalReview,
		FinalizeOverrides{},
	); err != nil {
		t.Fatal(err)
	}
	if mig.Phase() != PhaseTraversalReview {
		t.Fatalf("phase=%s want review", mig.Phase())
	}
}

func TestRetryFinalizeAfterForcedFailure(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()

	dir := t.TempDir()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "finalize-retry",
		Name:         "finalize-retry",
		MigrationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{PhaseFiltersSet, PhaseTraversing, PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed} {
		if err := mig.transitionTo(p); err != nil {
			t.Fatalf("transition to %s: %v", p, err)
		}
	}
	mig.setFinalizeError(errors.New("forced ensure failure"))
	if err := mig.RetryFinalize(FinalizeOverrides{Threads: 2, MemoryLimitGB: 2}); err != nil {
		t.Fatal(err)
	}
	if mig.Phase() != PhaseTraversalReview {
		t.Fatalf("phase=%s want review", mig.Phase())
	}
	if mig.FinalizeError() != "" {
		t.Fatalf("finalize error still set: %s", mig.FinalizeError())
	}
}
