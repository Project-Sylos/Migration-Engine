// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestComposePipelineMinListProfiles(t *testing.T) {
	gdrive := LookupOperationProfile("google_drive", "", OpListChildren)
	local := LookupOperationProfile("local", "", OpListChildren)
	merged := ComposePipelineMin(
		OperationProfile{DefaultWorkers: gdrive.DefaultWorkers, MaxWorkers: gdrive.MaxWorkers},
		OperationProfile{DefaultWorkers: local.DefaultWorkers, MaxWorkers: local.MaxWorkers},
	)
	if merged.DefaultWorkers != 6 {
		t.Fatalf("DefaultWorkers=%d want 6", merged.DefaultWorkers)
	}
	if merged.MaxWorkers != 16 {
		t.Fatalf("MaxWorkers=%d want 16", merged.MaxWorkers)
	}
}

func TestComposePipelineMinCopyPass2(t *testing.T) {
	gdrive := LookupOperationProfile("google_drive", "", OpDownload)
	generic := LookupOperationProfile("generic", "", OpUpload)
	merged := ComposePipelineMin(gdrive, generic)
	if merged.MaxWorkers != 16 {
		t.Fatalf("MaxWorkers=%d want 16", merged.MaxWorkers)
	}
}

func TestResolveEffectiveProfileTraversalSrc(t *testing.T) {
	ctx := queue.ScalingContext{
		QueueName:   "src",
		Mode:        queue.ScalingModeTraversal,
		SrcProvider: "google_drive",
		DstProvider: "local",
	}
	prof := ResolveEffectiveProfile(ctx, nil, nil)
	if prof.DefaultListPageSize != 100 {
		t.Fatalf("DefaultListPageSize=%d want 100", prof.DefaultListPageSize)
	}
	if prof.MaxInterOpDelay != 5*time.Second {
		t.Fatalf("MaxInterOpDelay=%v", prof.MaxInterOpDelay)
	}
}
