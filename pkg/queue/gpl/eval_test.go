// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package gpl

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	pathgpl "codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

func TestApplyGPLToSRCChildren_setsStateAndCollision(t *testing.T) {
	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	a := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFile, "a*b.txt"), Name: "a*b.txt", Type: db.NodeTypeFile, Depth: 1,
	}
	b := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFile, "a|b.txt"), Name: "a|b.txt", Type: db.NodeTypeFile, Depth: 1,
	}
	ApplyGPLToSRCChildren(nil, pathgpl.Windows, nil, []*db.NodeState{a, b}, false, false)
	if a.GPLState == "" || b.GPLState == "" {
		t.Fatal("expected gpl_state on both children")
	}
	var pa, pb db.GPLStatePayload
	if err := json.Unmarshal([]byte(a.GPLState), &pa); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(b.GPLState), &pb); err != nil {
		t.Fatal(err)
	}
	if pa.EffectiveProposedClean() == "" && pb.EffectiveProposedClean() == "" {
		t.Fatal("expected at least one proposed clean")
	}
	if !pa.Part.Collision && !pb.Part.Collision && pa.EffectiveProposedClean() == pb.EffectiveProposedClean() && pa.EffectiveProposedClean() != "" {
		t.Fatal("expected collision when cleans collide")
	}
	if len(pa.Parts) == 0 {
		t.Fatal("expected parts on gpl_state")
	}
}

func TestPathChecksRequired(t *testing.T) {
	if PathChecksRequired("dropbox", "dropbox", "auto") {
		t.Fatal("same provider auto should skip path checks")
	}
	if PathChecksRequired("Dropbox", "dropbox", "") {
		t.Fatal("case-insensitive same provider should skip")
	}
	if PathChecksRequired("google-drive", "google_drive", "auto") {
		t.Fatal("normalized same provider should skip")
	}
	if !PathChecksRequired("google_drive", "dropbox", "auto") {
		t.Fatal("cross-provider auto should require checks")
	}
	if !PathChecksRequired("", "dropbox", "auto") {
		t.Fatal("missing src should still target dst")
	}
	if !PathChecksRequired("dropbox", "", "auto") {
		t.Fatal("missing dst should still check")
	}
	if PathChecksRequired("google_drive", "dropbox", "none") {
		t.Fatal("none profile should skip")
	}
	if !PathChecksRequired("local", "local", "windows") {
		t.Fatal("explicit windows profile should check")
	}
	if got := ResolvePathCheckTarget("local", "local", "windows"); got != "windows" {
		t.Fatalf("got %q want windows", got)
	}
}

func TestApplyGPLToSRCChildren_skipChecks_passthroughNoEvents(t *testing.T) {
	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	child := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFile, "ok.txt"), Name: "ok.txt", Type: db.NodeTypeFile, Depth: 1,
	}
	ApplyGPLToSRCChildren(nil, pathgpl.Windows, []string{"folder"}, []*db.NodeState{child}, true, false)
	if child.GPLState == "" {
		t.Fatal("expected passthrough gpl_state")
	}
}

func TestApplyGPLToSRCChildren_windowsTrailingSpace(t *testing.T) {
	root := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	child := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFolder, "name "), Name: "name ", Type: db.NodeTypeFolder, Depth: 1,
	}
	ApplyGPLToSRCChildren(nil, pathgpl.Windows, nil, []*db.NodeState{child}, false, false)
	if child.GPLState == "" {
		t.Fatal("expected gpl_state")
	}
	var payload db.GPLStatePayload
	if err := json.Unmarshal([]byte(child.GPLState), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Valid {
		t.Fatalf("trailing space should be invalid on windows: %#v", payload)
	}
}
