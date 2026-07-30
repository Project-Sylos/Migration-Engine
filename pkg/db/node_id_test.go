// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "testing"

func TestMintNodeID_stableAndParentScoped(t *testing.T) {
	root := MintNodeID("SRC", "", NodeTypeFolder, "/")
	a1 := MintNodeID("SRC", root, NodeTypeFolder, "alpha")
	a2 := MintNodeID("SRC", root, NodeTypeFolder, "alpha")
	if a1 != a2 {
		t.Fatalf("expected stable mint, got %q vs %q", a1, a2)
	}
	b := MintNodeID("SRC", root, NodeTypeFolder, "beta")
	if a1 == b {
		t.Fatal("different basenames must mint different ids")
	}
	// Same basename under different parents → different ids
	otherParent := MintNodeID("SRC", root, NodeTypeFolder, "other")
	aUnderOther := MintNodeID("SRC", otherParent, NodeTypeFolder, "alpha")
	if aUnderOther == a1 {
		t.Fatal("same basename under different parents must differ")
	}
	// SRC vs DST roots differ
	if MintNodeID("SRC", "", NodeTypeFolder, "/") == MintNodeID("DST", "", NodeTypeFolder, "/") {
		t.Fatal("SRC and DST roots must differ")
	}
}

func TestNormalizeNodeBasename(t *testing.T) {
	if got := NormalizeNodeBasename("/a/b.txt"); got != "b.txt" {
		t.Fatalf("got %q", got)
	}
	if got := NormalizeNodeBasename("plain"); got != "plain" {
		t.Fatalf("got %q", got)
	}
	// Trailing/leading spaces are significant for destination OS rules (e.g. Windows).
	if got := NormalizeNodeBasename("Extra Space "); got != "Extra Space " {
		t.Fatalf("trailing space stripped: got %q", got)
	}
	if got := NormalizeNodeBasename("/Testing-GPL/Extra Space "); got != "Extra Space " {
		t.Fatalf("path trailing space stripped: got %q", got)
	}
	if got := NormalizeNodeBasename(" leading"); got != " leading" {
		t.Fatalf("leading space stripped: got %q", got)
	}
}
