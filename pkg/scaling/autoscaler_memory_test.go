// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

type memActuator struct {
	lease, refill int
}

func (m *memActuator) GetWorkerCount() int            { return 1 }
func (m *memActuator) SetTargetWorkerCount(int) error { return nil }
func (m *memActuator) GetInterOpDelay() time.Duration { return 0 }
func (m *memActuator) SetInterOpDelay(time.Duration)  {}
func (m *memActuator) EffectiveLeaseBatchSize() int   { return m.lease }
func (m *memActuator) SetLeaseBatchSize(n int)        { m.lease = n }
func (m *memActuator) EffectiveRefillBatchSize() int  { return m.refill }
func (m *memActuator) SetRefillBatchSize(n int)       { m.refill = n }
func (m *memActuator) GetListPageSize() int           { return 100 }
func (m *memActuator) SetListPageSize(int)            {}
func (m *memActuator) ListItemsP95() int              { return 0 }
func (m *memActuator) GetPendingCount() int           { return 0 }
func (m *memActuator) InProgressCount() int           { return 0 }

func TestStepDownMemoryKnobs(t *testing.T) {
	act := &memActuator{lease: 1000, refill: 8000}
	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	var events []ScalingEvent
	sc := NewAutoscaler(database, nil, nil, map[string]FSPerformanceProfile{
		"src": LookupProfile("local", ""),
	}, map[string]QueueActuator{"src": act}, Config{
		Enabled: true,
		OnEvent: func(ev ScalingEvent) { events = append(events, ev) },
	})

	sc.stepDownMemoryKnobs(time.Now(), PressureMemory)

	if act.lease != 500 {
		t.Fatalf("lease=%d want 500", act.lease)
	}
	if act.refill != 4000 {
		t.Fatalf("refill=%d want 4000", act.refill)
	}
	if len(events) < 2 {
		t.Fatalf("expected batch scaling events, got %d", len(events))
	}
}
