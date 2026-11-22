// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import "testing"

func TestRenameDisplayPathLeaf(t *testing.T) {
	if got := RenameDisplayPathLeaf("/Reports", "Docs", 1); got != "/Docs" {
		t.Fatalf("depth1=%q", got)
	}
	if got := RenameDisplayPathLeaf("/Reports/Q1", "Q2", 2); got != "/Reports/Q2" {
		t.Fatalf("depth2=%q", got)
	}
	if got := RenameDisplayPathLeaf("", "A", 1); got != "/A" {
		t.Fatalf("empty=%q", got)
	}
}

func TestReplaceDisplayPathPrefix(t *testing.T) {
	if got := ReplaceDisplayPathPrefix("/Reports/Q1/a.txt", "/Reports", "/Docs"); got != "/Docs/Q1/a.txt" {
		t.Fatalf("child=%q", got)
	}
	if got := ReplaceDisplayPathPrefix("/Reports", "/Reports", "/Docs"); got != "/Docs" {
		t.Fatalf("exact=%q", got)
	}
	if got := ReplaceDisplayPathPrefix("/Other/x", "/Reports", "/Docs"); got != "/Other/x" {
		t.Fatalf("unrelated=%q", got)
	}
}
