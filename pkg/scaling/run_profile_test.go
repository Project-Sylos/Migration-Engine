// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"
)

func TestMergeRunProfilesConservative(t *testing.T) {
	gdrive := LookupProfile("google_drive", "")
	local := LookupProfile("local", "")
	merged := MergeRunProfiles(gdrive, local)
	if merged.DefaultWorkers != 2 {
		t.Fatalf("DefaultWorkers=%d want 2", merged.DefaultWorkers)
	}
	if merged.MaxWorkers != 8 {
		t.Fatalf("MaxWorkers=%d want 8", merged.MaxWorkers)
	}
	if merged.DefaultListPageSize != 50 {
		t.Fatalf("DefaultListPageSize=%d want 50", merged.DefaultListPageSize)
	}
	if merged.DefaultLeaseBatch != 50 {
		t.Fatalf("DefaultLeaseBatch=%d want 50", merged.DefaultLeaseBatch)
	}
	if merged.MaxInterOpDelay != 10*time.Second {
		t.Fatalf("MaxInterOpDelay=%v", merged.MaxInterOpDelay)
	}
	if merged.PreferLargePages {
		t.Fatal("expected PreferLargePages false")
	}
}

func TestMergeRunProfilesGenericDst(t *testing.T) {
	gdrive := LookupProfile("google_drive", "")
	generic := LookupProfile("generic", "")
	merged := MergeRunProfiles(gdrive, generic)
	if merged.DefaultWorkers != 2 {
		t.Fatalf("DefaultWorkers=%d want 2", merged.DefaultWorkers)
	}
	if merged.MaxWorkers != 8 {
		t.Fatalf("MaxWorkers=%d want 8", merged.MaxWorkers)
	}
}
