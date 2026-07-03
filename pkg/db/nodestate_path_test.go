// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "testing"

func TestNodeInsertPathFields_depth1EmptyParentMatchesRoot(t *testing.T) {
	_, normParent, _, parentHash := NodeInsertPathFields("/DMT", "", 1)
	if normParent != "/" {
		t.Fatalf("normParent=%q want /", normParent)
	}
	rootHash := PathHash("/")
	if parentHash != rootHash {
		t.Fatalf("parentPathHash=%q want %q (root join key)", parentHash, rootHash)
	}
}

func TestNodeInsertPathFields_rootKeepsEmptyParent(t *testing.T) {
	_, normParent, pathHash, parentHash := NodeInsertPathFields("/", "", 0)
	if normParent != "" {
		t.Fatalf("root normParent=%q want empty", normParent)
	}
	if pathHash != PathHash("/") {
		t.Fatalf("root pathHash mismatch")
	}
	if parentHash != PathHash("") {
		t.Fatalf("root parentPathHash=%q want PathHash(\"\")", parentHash)
	}
}
