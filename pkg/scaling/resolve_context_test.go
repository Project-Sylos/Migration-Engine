// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestResolveOperationProfileCopyPass1(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "copy",
		Mode:        queue.ScalingModeCopy,
		CopyPass:    1,
		SrcProvider: "google_drive",
		DstProvider: "local",
	}
	op := ResolveOperationProfile(ctx)
	if op.MaxWorkers != 64 {
		t.Fatalf("pass1 uses dst create_folder MaxWorkers=%d want 64", op.MaxWorkers)
	}
}

func TestResolveOperationProfileCopyPass2GDriveToLocal(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "copy",
		Mode:        queue.ScalingModeCopy,
		CopyPass:    2,
		SrcProvider: "google_drive",
		DstProvider: "local",
	}
	op := ResolveOperationProfile(ctx)
	if op.MaxWorkers != 16 {
		t.Fatalf("pass2 min(gdrive download 16, local upload 64)=%d want 16", op.MaxWorkers)
	}
}

func TestResolveOperationProfileCopyPass2LocalToGDrive(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "copy",
		Mode:        queue.ScalingModeCopy,
		CopyPass:    2,
		SrcProvider: "local",
		DstProvider: "google_drive",
	}
	op := ResolveOperationProfile(ctx)
	if op.MaxWorkers != 16 {
		t.Fatalf("pass2 MaxWorkers=%d want 16 (gdrive upload cap)", op.MaxWorkers)
	}
}

func TestResolveOperationProfileCopyPass2GDriveSpectra(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "copy",
		Mode:        queue.ScalingModeCopy,
		CopyPass:    2,
		SrcProvider: "google_drive",
		DstProvider: "spectra",
	}
	op := ResolveOperationProfile(ctx)
	if op.MaxWorkers != 16 {
		t.Fatalf("spectra uncapped upload; gdrive download cap=%d want 16", op.MaxWorkers)
	}
}

func TestResolveOperationProfileTraversalSrc(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "src",
		Mode:        queue.ScalingModeTraversal,
		SrcProvider: "google_drive",
		DstProvider: "local",
	}
	op := ResolveOperationProfile(ctx)
	if op.DefaultListPageSize != 100 {
		t.Fatalf("src list profile DefaultListPageSize=%d want 100", op.DefaultListPageSize)
	}
}

func TestActiveOperations(t *testing.T) {
	if ops := ActiveOperations(queue.ScalingContext{Mode: queue.ScalingModeCopy, CopyPass: 1}); len(ops) != 1 || ops[0] != OpCreateFolder {
		t.Fatalf("pass1 ops=%v", ops)
	}
	if ops := ActiveOperations(queue.ScalingContext{Mode: queue.ScalingModeCopy, CopyPass: 2}); len(ops) != 2 {
		t.Fatalf("pass2 ops=%v", ops)
	}
	if ops := ActiveOperations(queue.ScalingContext{Mode: queue.ScalingModeTraversal}); len(ops) != 1 || ops[0] != OpListChildren {
		t.Fatalf("traversal ops=%v", ops)
	}
}

func TestResolveInitialWorkersCopyPass1(t *testing.T) {
	ctx := queue.ScalingContext{
		Mode:        queue.ScalingModeCopy,
		CopyPass:    1,
		DstProvider: "google_drive",
		SrcProvider: "local",
	}
	if got := ResolveInitialWorkers(ctx, 0); got != 4 {
		t.Fatalf("got %d want 4 (gdrive create_folder default)", got)
	}
}
