// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestApplyWorkerCapOverride(t *testing.T) {
	t.Parallel()
	base := FSPerformanceProfile{MinWorkers: 1, MaxWorkers: 8, DefaultWorkers: 4}
	got := ApplyWorkerCapOverride(base, WorkerCapOverrides{}, WorkerCapCopyFolders)
	if got.MaxWorkers != 8 {
		t.Fatalf("empty override: MaxWorkers=%d want 8", got.MaxWorkers)
	}
	got = ApplyWorkerCapOverride(base, WorkerCapOverrides{Caps: map[WorkerCapMode]int{
		WorkerCapCopyFolders: 64,
	}}, WorkerCapCopyFolders)
	if got.MaxWorkers != 64 {
		t.Fatalf("raise: MaxWorkers=%d want 64", got.MaxWorkers)
	}
	got = ApplyWorkerCapOverride(base, WorkerCapOverrides{Caps: map[WorkerCapMode]int{
		WorkerCapCopyFolders: 500,
	}}, WorkerCapCopyFolders)
	if got.MaxWorkers != AbsoluteMaxWorkers {
		t.Fatalf("absolute clamp: MaxWorkers=%d want %d", got.MaxWorkers, AbsoluteMaxWorkers)
	}
	got = ApplyWorkerCapOverride(FSPerformanceProfile{MinWorkers: 4, MaxWorkers: 32}, WorkerCapOverrides{Caps: map[WorkerCapMode]int{
		WorkerCapCopyFiles: 2,
	}}, WorkerCapCopyFiles)
	if got.MaxWorkers != 4 {
		t.Fatalf("min clamp: MaxWorkers=%d want 4", got.MaxWorkers)
	}
}

func TestWorkerCapModeFromContext(t *testing.T) {
	t.Parallel()
	if m := WorkerCapModeFromContext(queue.ScalingContext{Mode: queue.ScalingModeCopy, CopyPass: 1}); m != WorkerCapCopyFolders {
		t.Fatalf("pass1=%s", m)
	}
	if m := WorkerCapModeFromContext(queue.ScalingContext{Mode: queue.ScalingModeCopy, CopyPass: 2}); m != WorkerCapCopyFiles {
		t.Fatalf("pass2=%s", m)
	}
	if m := WorkerCapModeFromContext(queue.ScalingContext{Mode: queue.ScalingModeDelete}); m != WorkerCapDelete {
		t.Fatalf("delete=%s", m)
	}
	if m := WorkerCapModeFromContext(queue.ScalingContext{Mode: queue.ScalingModeTraversal}); m != WorkerCapTraversal {
		t.Fatalf("traversal=%s", m)
	}
}

func TestDefaultWorkerCapsBox(t *testing.T) {
	t.Parallel()
	caps := DefaultWorkerCaps("box")
	if caps.Modes[WorkerCapCopyFolders].MaxWorkers != 12 {
		t.Fatalf("box copy_folders max=%d want 12", caps.Modes[WorkerCapCopyFolders].MaxWorkers)
	}
	if caps.Modes[WorkerCapCopyFiles].MaxWorkers != 32 {
		t.Fatalf("box copy_files max=%d want 32", caps.Modes[WorkerCapCopyFiles].MaxWorkers)
	}
}
