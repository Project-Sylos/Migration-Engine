// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestDstChildMatchKey_rootFilePathDisplayName(t *testing.T) {
	const want = "file:file_1.txt"
	if got := dstChildMatchKey(types.NodeTypeFile, "/file_1.txt", "/file_1.txt"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := dstChildMatchKey(types.NodeTypeFile, "file_1.txt", "/file_1.txt"); got != want {
		t.Fatalf("basename displayName got %q want %q", got, want)
	}
}

func TestDstChildMatchKey_nestedPath(t *testing.T) {
	const want = "file:readme.md"
	if got := dstChildMatchKey(types.NodeTypeFile, "readme.md", "/alpha/beta/readme.md"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
