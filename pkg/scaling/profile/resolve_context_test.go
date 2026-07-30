// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

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
	if ops := ActiveOperations(queue.ScalingContext{Mode: queue.ScalingModeDelete}); len(ops) != 1 || ops[0] != OpDelete {
		t.Fatalf("delete ops=%v", ops)
	}
	if ops := ActiveOperations(queue.ScalingContext{Mode: queue.ScalingModeDeleteRetry}); len(ops) != 1 || ops[0] != OpDelete {
		t.Fatalf("delete-retry ops=%v", ops)
	}
}

func TestResolveOperationProfileDeleteOneDriveToGDrive(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "delete",
		Mode:        queue.ScalingModeDelete,
		SrcProvider: "onedrive",
		DstProvider: "google_drive",
	}
	op := ResolveOperationProfile(ctx)
	// Min of onedrive list (6/12) and gdrive list (6/16).
	if op.DefaultWorkers != 6 || op.MaxWorkers != 12 {
		t.Fatalf("delete profile: %+v want DefaultWorkers=6 MaxWorkers=12 (min list)", op)
	}
}

func TestResolveOperationProfileDeleteDropboxLocal(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "delete",
		Mode:        queue.ScalingModeDelete,
		SrcProvider: "local",
		DstProvider: "dropbox",
	}
	op := ResolveOperationProfile(ctx)
	// Min of local list (8/64) and dropbox list (4/5).
	if op.DefaultWorkers != 4 || op.MaxWorkers != 5 {
		t.Fatalf("delete profile: %+v want DefaultWorkers=4 MaxWorkers=5 (min list)", op)
	}
}

func TestResolveInitialWorkersDeleteZeroUsesProfile(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "delete",
		Mode:        queue.ScalingModeDelete,
		SrcProvider: "onedrive",
		DstProvider: "google_drive",
	}
	if got := ResolveInitialWorkers(ctx, 0); got != 6 {
		t.Fatalf("got %d want 6 (onedrive∩gdrive list default)", got)
	}
}

func TestResolveOperationProfileDropboxListCap(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "dst",
		Mode:        queue.ScalingModeTraversal,
		DstProvider: "dropbox",
	}
	op := ResolveOperationProfile(ctx)
	if op.MaxWorkers != 5 || op.DefaultWorkers != 4 {
		t.Fatalf("dropbox list: %+v want DefaultWorkers=4 MaxWorkers=5", op)
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
