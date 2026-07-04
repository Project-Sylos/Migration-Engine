// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"
)

func TestComposePipelineMinZeroCap(t *testing.T) {
	gdrive := LookupOperationProfile("google_drive", "", OpDownload)
	spectra := spectraUncappedOp()
	got := ComposePipelineMin(gdrive, spectra)
	if got.MaxWorkers != 16 {
		t.Fatalf("MaxWorkers=%d want 16 (gdrive cap wins)", got.MaxWorkers)
	}
	got2 := ComposePipelineMin(spectra, spectra)
	if got2.MaxWorkers != 0 {
		t.Fatalf("both uncapped MaxWorkers=%d want 0", got2.MaxWorkers)
	}
}

func TestToActuatorProfileZeroCap(t *testing.T) {
	prof := ToActuatorProfile(spectraUncappedOp())
	if prof.DefaultWorkers != DefaultWorkersForUnbounded {
		t.Fatalf("DefaultWorkers=%d", prof.DefaultWorkers)
	}
	if prof.MaxWorkers != UnboundedMaxWorkers {
		t.Fatalf("MaxWorkers=%d want %d", prof.MaxWorkers, UnboundedMaxWorkers)
	}
	if prof.MaxInterOpDelay != DefaultMaxInterOpDelay {
		t.Fatalf("MaxInterOpDelay=%v", prof.MaxInterOpDelay)
	}
}

func TestLookupOperationProfileGoogleDrive(t *testing.T) {
	dl := LookupOperationProfile("google_drive", "", OpDownload)
	if dl.MaxWorkers != 16 || dl.DefaultWorkers != 8 {
		t.Fatalf("download profile: %+v", dl)
	}
	cf := LookupOperationProfile("google_drive", "", OpCreateFolder)
	if cf.MaxWorkers != 12 {
		t.Fatalf("create_folder MaxWorkers=%d want 12", cf.MaxWorkers)
	}
}

func TestSpectraAllOpsUncapped(t *testing.T) {
	pop := LookupProviderOperations("spectra", "")
	for _, op := range []FSOperation{OpListChildren, OpCreateFolder, OpDownload, OpUpload} {
		p := pop.Ops[op]
		if p.MaxWorkers != 0 {
			t.Fatalf("%s MaxWorkers=%d want 0", op, p.MaxWorkers)
		}
		if p.DefaultWorkers != DefaultWorkersForUnbounded {
			t.Fatalf("%s DefaultWorkers=%d", op, p.DefaultWorkers)
		}
	}
}

func TestComposePipelineMinInterOpDelay(t *testing.T) {
	a := OperationProfile{MaxInterOpDelay: 2 * time.Second}
	b := OperationProfile{MaxInterOpDelay: 5 * time.Second}
	got := ComposePipelineMin(a, b)
	if got.MaxInterOpDelay != 5*time.Second {
		t.Fatalf("got %v", got.MaxInterOpDelay)
	}
}
