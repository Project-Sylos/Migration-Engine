// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
)

func TestStopPausesQueuesAndSetsSoftProgress(t *testing.T) {
	phases := []string{PhaseTraversing, PhaseCopying, PhaseDeleting}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			m := &Migration{
				ID:      "stop-test",
				phase:   phase,
				running: true,
				logRing: newLogRing(8),
			}
			obs := observe.NewQueueObserver(nil, time.Hour)
			q := queue.NewQueue("src", 3, 1, nil, nil)
			q.SetState(queue.QueueStateRunning)
			if !q.Add(&queue.TaskBase{ID: "pending-1", Round: 1, Type: queue.TaskTypeSrcTraversal}) {
				t.Fatal("failed to enqueue pending task")
			}
			obs.RegisterQueue("src", q)
			m.activeQueueObs.Store(obs)

			res, err := m.Stop()
			if err != nil {
				t.Fatalf("Stop: %v", err)
			}
			m.disarmStopGraceTimer()

			if !res.SoftSuspendRequested {
				t.Fatal("expected SoftSuspendRequested")
			}
			if !m.softSuspendRequested.Load() {
				t.Fatal("expected softSuspendRequested flag")
			}
			if q.State() != queue.QueueStatePaused {
				t.Fatalf("queue state = %s, want paused", q.State())
			}
			if q.GetPendingCount() != 0 {
				t.Fatalf("pending after pause clear = %d, want 0", q.GetPendingCount())
			}

			sp := m.GetStopProgress()
			if !sp.Active || sp.Mode != "soft" {
				t.Fatalf("stop progress active=%v mode=%q", sp.Active, sp.Mode)
			}
			if sp.Step != StopStepDraining {
				t.Fatalf("step = %q, want %s", sp.Step, StopStepDraining)
			}
			if len(sp.Steps) != 3 {
				t.Fatalf("expected soft checklist steps, got %d", len(sp.Steps))
			}
			if phase == PhaseDeleting {
				if sp.Steps[0].Label != "Finishing current removals" {
					t.Fatalf("delete drain label = %q", sp.Steps[0].Label)
				}
			}
		})
	}
}

func TestSoftStopChecklistLabels(t *testing.T) {
	copySteps := softStopChecklist(PhaseCopying)
	if copySteps[0].Label != "Finishing current copies" {
		t.Fatalf("copy drain label = %q", copySteps[0].Label)
	}
	travSteps := softStopChecklist(PhaseTraversing)
	if travSteps[0].Label != "Finishing current folders and files" {
		t.Fatalf("traversal drain label = %q", travSteps[0].Label)
	}
	if len(travSteps) != 3 {
		t.Fatalf("checklist len = %d, want 3", len(travSteps))
	}
	if travSteps[1].ID != StopStepSaving {
		t.Fatalf("expected saving step, got %q", travSteps[1].ID)
	}
}

func TestAbortTransitionsToAborted(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()

	dir := t.TempDir()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "abort-test",
		Name:         "abort-test",
		MigrationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseFiltersSet); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseTraversing); err != nil {
		t.Fatal(err)
	}
	mig.mu.Lock()
	mig.running = true
	mig.mu.Unlock()

	obs := observe.NewQueueObserver(nil, time.Hour)
	q := queue.NewQueue("src", 3, 1, nil, nil)
	q.SetState(queue.QueueStateRunning)
	if !q.Add(&queue.TaskBase{ID: "pending-1", Round: 1, Type: queue.TaskTypeSrcTraversal}) {
		t.Fatal("enqueue failed")
	}
	leased := &queue.TaskBase{ID: "leased", Round: 1, Type: queue.TaskTypeSrcTraversal}
	q.AddInProgress(leased.ID, leased)
	obs.RegisterQueue("src", q)
	mig.activeQueueObs.Store(obs)

	res, err := mig.Abort()
	if err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if res.Phase != PhaseAborted {
		t.Fatalf("phase = %q, want %s", res.Phase, PhaseAborted)
	}
	if mig.Phase() != PhaseAborted {
		t.Fatalf("mig.Phase() = %q, want %s", mig.Phase(), PhaseAborted)
	}
	if q.State() != queue.QueueStatePaused {
		t.Fatalf("queue state = %s, want paused", q.State())
	}
	if q.InProgressCount() != 0 {
		t.Fatalf("inProgress = %d, want 0", q.InProgressCount())
	}
	if q.GetPendingCount() != 0 {
		t.Fatalf("pending after hard abandon = %d, want 0", q.GetPendingCount())
	}
	sp := mig.GetStopProgress()
	if sp.Step != StopStepAborted {
		t.Fatalf("stop step = %q, want %s", sp.Step, StopStepAborted)
	}
	if canTransition(PhaseAborted, PhaseTraversing) {
		t.Fatal("aborted must be terminal")
	}
}
