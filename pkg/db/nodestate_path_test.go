// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "testing"

func TestNodeInsertPathFields_depth1EmptyParentMatchesRoot(t *testing.T) {
	_, normParent := NodeInsertPathFields("/alpha", "", 1)
	if normParent != "/" {
		t.Fatalf("normParent=%q want /", normParent)
	}
}

func TestNodeInsertPathFields_rootKeepsEmptyParent(t *testing.T) {
	normPath, normParent := NodeInsertPathFields("/", "", 0)
	if normParent != "" {
		t.Fatalf("root normParent=%q want empty", normParent)
	}
	if normPath != "/" {
		t.Fatalf("root normPath=%q want /", normPath)
	}
}

func TestNormalizeQueueNodeType(t *testing.T) {
	if got := NormalizeQueueNodeType(NodeTypeFile); got != NodeTypeFile {
		t.Fatalf("file: got %q", got)
	}
	if got := NormalizeQueueNodeType(NodeTypeFolder); got != NodeTypeFolder {
		t.Fatalf("folder: got %q", got)
	}
	if got := NormalizeQueueNodeType("team_folder"); got != NodeTypeFolder {
		t.Fatalf("team_folder: got %q want folder", got)
	}
	if got := NormalizeQueueNodeType("shared_folder"); got != NodeTypeFolder {
		t.Fatalf("shared_folder: got %q want folder", got)
	}
}
