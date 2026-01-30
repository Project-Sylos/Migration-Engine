// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"hash/fnv"
)

// DeterministicNodeID generates a stable node ID from logical identity.
// This eliminates duplicate logical nodes and traversal races by making
// node identity stable, global, and race-safe across workers.
//
// Inputs:
//   - queueType: "SRC" or "DST"
//   - nodeType: "folder" or "file" (from types.NodeTypeFolder/types.NodeTypeFile)
//   - path: normalized, root-relative path (e.g., "/", "/items", "/items/subfolder")
//
// Output format: "node:<16-char-hex>" (FNV-1a 64-bit hash)
//
// The same logical node (same queueType + nodeType + path) will always produce
// the same ID, regardless of which worker discovers it or when. This makes
// deduplication automatic at all layers.
func DeterministicNodeID(queueType, nodeType, path string) string {
	canonical := fmt.Sprintf("%s|%s|%s", queueType, nodeType, path)
	h := fnv.New64a()
	h.Write([]byte(canonical))
	return fmt.Sprintf("node:%016x", h.Sum64())
}
