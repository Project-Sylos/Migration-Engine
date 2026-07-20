// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

func TestApplyGPLToSRCChildren_setsStateAndCollision(t *testing.T) {
	root := db.RootNodeID("SRC")
	a := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFile, "a*b.txt"), Name: "a*b.txt", Type: db.NodeTypeFile, Depth: 1,
	}
	b := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFile, "a|b.txt"), Name: "a|b.txt", Type: db.NodeTypeFile, Depth: 1,
	}
	applyGPLToSRCChildren(nil, gpl.Windows, nil, []*db.NodeState{a, b}, false)
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
	// Both clean toward similar basenames on Windows → sibling collision likely on one/both
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
		t.Fatal("cross-provider should require path checks")
	}
	if !PathChecksRequired("", "dropbox", "auto") {
		t.Fatal("empty src should require path checks")
	}
	if !PathChecksRequired("dropbox", "", "auto") {
		t.Fatal("empty dst should require path checks")
	}
	if PathChecksRequired("google_drive", "dropbox", "none") {
		t.Fatal("none profile should skip even when providers differ")
	}
	if !PathChecksRequired("local", "local", "windows") {
		t.Fatal("explicit windows profile should require checks for local→local")
	}
	if got := ResolvePathCheckTarget("local", "local", "windows"); got != "windows" {
		t.Fatalf("explicit target=%q want windows", got)
	}
}

func TestApplyGPLToSRCChildren_skipChecks_passthroughNoEvents(t *testing.T) {
	root := db.RootNodeID("SRC")
	child := &db.NodeState{
		ID: db.MintNodeID("SRC", root, db.NodeTypeFile, "a*b.txt"), Name: "a*b.txt", Type: db.NodeTypeFile, Depth: 1,
	}
	applyGPLToSRCChildren(nil, gpl.Windows, []string{"folder"}, []*db.NodeState{child}, true)
	if child.GPLState == "" {
		t.Fatal("expected passthrough gpl_state")
	}
	var p db.GPLStatePayload
	if err := json.Unmarshal([]byte(child.GPLState), &p); err != nil {
		t.Fatal(err)
	}
	if !p.Valid || len(p.Parts) != 2 || p.Parts[1] != "a*b.txt" {
		t.Fatalf("passthrough parts=%v valid=%v", p.Parts, p.Valid)
	}
	if p.EffectiveProposedClean() != "" {
		t.Fatalf("skip checks should not propose cleans, got %q", p.EffectiveProposedClean())
	}
}

func TestApplyGPLToSRCChildren_windowsTrailingSpace(t *testing.T) {
	root := db.RootNodeID("SRC")
	child := &db.NodeState{
		ID:    db.MintNodeID("SRC", root, db.NodeTypeFolder, "Extra Space "),
		Name:  "Extra Space ",
		Type:  db.NodeTypeFolder,
		Depth: 1,
	}
	applyGPLToSRCChildren(nil, gpl.Windows, nil, []*db.NodeState{child}, false)
	if child.GPLState == "" {
		t.Fatal("expected gpl_state")
	}
	var p db.GPLStatePayload
	if err := json.Unmarshal([]byte(child.GPLState), &p); err != nil {
		t.Fatal(err)
	}
	if p.Valid || p.Part.Valid {
		t.Fatalf("expected invalid trailing-space name, got %+v", p)
	}
	foundTrailing := false
	for _, cat := range p.Part.Categories {
		if cat == "TrailingSpace" {
			foundTrailing = true
			break
		}
	}
	if !foundTrailing {
		t.Fatalf("expected TrailingSpace category, got %v", p.Part.Categories)
	}
	// parts store the effective cleaned segment; original lives on node path/name.
	if len(p.Parts) != 1 || p.Parts[0] != "Extra Space" {
		t.Fatalf("effective parts=%#v want [\"Extra Space\"]", p.Parts)
	}
	if clean := p.EffectiveProposedClean(); clean != "Extra Space" {
		t.Fatalf("proposed clean=%q want %q", clean, "Extra Space")
	}
}
