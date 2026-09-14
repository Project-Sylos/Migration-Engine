// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	"testing"
	"time"
)

func TestComposePipelineMinZeroCap(t *testing.T) {
	gdrive := LookupOperationProfile("google_drive", "", OpDownload)
	uncapped := OperationProfile{MaxWorkers: 0, DefaultWorkers: 0}
	got := ComposePipelineMin(gdrive, uncapped)
	if got.MaxWorkers != 16 {
		t.Fatalf("MaxWorkers=%d want 16 (gdrive cap wins)", got.MaxWorkers)
	}
	got2 := ComposePipelineMin(uncapped, uncapped)
	if got2.MaxWorkers != 0 {
		t.Fatalf("both uncapped MaxWorkers=%d want 0", got2.MaxWorkers)
	}
	spectra := LookupOperationProfile("spectra", "", OpUpload)
	got3 := ComposePipelineMin(gdrive, spectra)
	if got3.MaxWorkers != 16 {
		t.Fatalf("gdrive∩spectra MaxWorkers=%d want 16", got3.MaxWorkers)
	}
}

func TestToActuatorProfileZeroCap(t *testing.T) {
	prof := ToActuatorProfile(OperationProfile{})
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

func TestSpectraHighThroughputDefaults(t *testing.T) {
	for _, op := range []FSOperation{OpListChildren, OpCreateFolder, OpDelete, OpDownload, OpUpload} {
		p := LookupOperationProfile("spectra", "", op)
		if p.MaxWorkers != 64 {
			t.Fatalf("%s MaxWorkers=%d want 64", op, p.MaxWorkers)
		}
		if p.DefaultWorkers != 16 {
			t.Fatalf("%s DefaultWorkers=%d want 16", op, p.DefaultWorkers)
		}
	}
	prof := ToActuatorProfile(LookupOperationProfile("spectra", "", OpListChildren))
	if prof.DefaultWorkers != 16 || prof.MaxWorkers != 64 {
		t.Fatalf("actuator profile: %+v", prof)
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
	del := LookupOperationProfile("google_drive", "", OpDelete)
	if del.DefaultWorkers != 4 || del.MaxWorkers != 8 {
		t.Fatalf("delete profile: %+v", del)
	}
}

func TestLookupOperationProfileDropbox(t *testing.T) {
	dl := LookupOperationProfile("dropbox", "", OpDownload)
	if dl.MaxWorkers != 4 || dl.DefaultWorkers != 4 {
		t.Fatalf("download profile: %+v", dl)
	}
	up := LookupOperationProfile("dropbox", "", OpUpload)
	if up.MaxWorkers != 4 || up.DefaultWorkers != 4 {
		t.Fatalf("upload profile: %+v", up)
	}
	list := LookupOperationProfile("dropbox", "", OpListChildren)
	if list.MaxListPageSize != 500 || list.DefaultWorkers != 4 || list.MaxWorkers != 5 {
		t.Fatalf("list_children profile: %+v", list)
	}
	cf := LookupOperationProfile("dropbox", "", OpCreateFolder)
	if cf.DefaultWorkers != 4 || cf.MaxWorkers != 8 {
		t.Fatalf("create_folder profile: %+v", cf)
	}
	del := LookupOperationProfile("dropbox", "", OpDelete)
	if del.DefaultWorkers != 4 || del.MaxWorkers != 8 {
		t.Fatalf("delete profile: %+v", del)
	}
}

func TestLookupOperationProfileOneDrive(t *testing.T) {
	dl := LookupOperationProfile("onedrive", "", OpDownload)
	if dl.MaxWorkers != 4 || dl.DefaultWorkers != 4 {
		t.Fatalf("download profile: %+v", dl)
	}
	up := LookupOperationProfile("onedrive", "", OpUpload)
	if up.MaxWorkers != 4 || up.DefaultWorkers != 4 {
		t.Fatalf("upload profile: %+v", up)
	}
	list := LookupOperationProfile("onedrive", "", OpListChildren)
	if list.DefaultWorkers != 6 || list.MaxWorkers != 12 {
		t.Fatalf("list_children profile: %+v", list)
	}
}

func TestLookupOperationProfileSharePoint(t *testing.T) {
	list := LookupOperationProfile("sharepoint", "", OpListChildren)
	if list.DefaultWorkers != 4 || list.MaxWorkers != 8 {
		t.Fatalf("list_children profile: %+v", list)
	}
	del := LookupOperationProfile("sharepoint", "", OpDelete)
	if del.DefaultWorkers != 4 || del.MaxWorkers != 6 {
		t.Fatalf("delete profile: %+v", del)
	}
}

func TestLookupOperationProfileBox(t *testing.T) {
	up := LookupOperationProfile("box", "", OpUpload)
	if up.DefaultWorkers != 16 || up.MaxWorkers != 32 {
		t.Fatalf("upload profile: %+v", up)
	}
	cf := LookupOperationProfile("box", "", OpCreateFolder)
	if cf.DefaultWorkers != 6 || cf.MaxWorkers != 12 {
		t.Fatalf("create_folder profile: %+v", cf)
	}
	list := LookupOperationProfile("box", "", OpListChildren)
	if list.DefaultListPageSize != 1000 || list.MaxListPageSize != 1000 {
		t.Fatalf("list_children profile: %+v", list)
	}
}

func TestLookupFallsBackToProviderDefault(t *testing.T) {
	// local has no OpDelete override — should use provider Default (8/64).
	del := LookupOperationProfile("local", "", OpDelete)
	if del.DefaultWorkers != 8 || del.MaxWorkers != 64 {
		t.Fatalf("local delete via Default: %+v", del)
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
