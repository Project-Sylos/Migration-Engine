// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import "testing"

func TestMapFSOperation(t *testing.T) {
	cases := map[string]FSOperation{
		"ListChildren":     OpListChildren,
		"CreateFolder":     OpCreateFolder,
		"DeleteNode":       OpDelete,
		"DeleteBatch":      OpDelete,
		"DeleteFile":       OpDelete,
		"DeleteFolder":     OpDelete,
		"OpenRead":         OpDownload,
		"CreateFileUpload": OpUpload,
		"UploadFile":       OpUpload,
		"unknown":          "",
	}
	for in, want := range cases {
		if got := MapFSOperation(in); got != want {
			t.Fatalf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestDegradationAppliesToOperation(t *testing.T) {
	active := []FSOperation{OpDownload, OpUpload}
	if !DegradationAppliesToOperation("OpenRead", active) {
		t.Fatal("OpenRead should apply to download op")
	}
	if DegradationAppliesToOperation("ListChildren", active) {
		t.Fatal("ListChildren should not apply during file copy")
	}
}
