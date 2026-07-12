// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"
	"time"
)

func TestSyncRecordRejectsStalePhaseSnapshot(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 7, 12, 18, 0, 0, 0, time.UTC)
	newer := older.Add(time.Second)

	m := &Migration{
		ID:    "migration-test",
		Name:  "live",
		phase: PhaseTraversing,
	}
	m.persistRecord.Store(migrationRecord{
		ID:        m.ID,
		Name:      "live",
		Phase:     PhaseTraversing,
		UpdatedAt: newer,
	})

	m.syncRecord(migrationRecord{
		ID:        m.ID,
		Name:      "stale",
		Phase:     PhaseFiltersSet,
		UpdatedAt: older,
	})

	if m.Phase() != PhaseTraversing {
		t.Fatalf("phase = %q, want %q (stale sync must not roll back)", m.Phase(), PhaseTraversing)
	}
	if m.GetName() != "live" {
		t.Fatalf("name = %q, want live", m.GetName())
	}
}

func TestSyncRecordAppliesNewerSnapshot(t *testing.T) {
	t.Parallel()

	older := time.Date(2026, 7, 12, 18, 0, 0, 0, time.UTC)
	newer := older.Add(time.Second)

	m := &Migration{
		ID:    "migration-test",
		Name:  "old",
		phase: PhaseFiltersSet,
	}
	m.persistRecord.Store(migrationRecord{
		ID:        m.ID,
		Name:      "old",
		Phase:     PhaseFiltersSet,
		UpdatedAt: older,
	})

	m.syncRecord(migrationRecord{
		ID:        m.ID,
		Name:      "fresh",
		Phase:     PhaseTraversing,
		UpdatedAt: newer,
	})

	if m.Phase() != PhaseTraversing {
		t.Fatalf("phase = %q, want %q", m.Phase(), PhaseTraversing)
	}
	if m.GetName() != "fresh" {
		t.Fatalf("name = %q, want fresh", m.GetName())
	}
}
