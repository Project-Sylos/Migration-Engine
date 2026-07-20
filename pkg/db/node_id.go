// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"path"
	"strings"

	"github.com/google/uuid"
)

// nodeIDNamespace is a fixed Sylos namespace for UUID v5 node ids.
// Do not change — race-safe dedupe depends on it.
var nodeIDNamespace = uuid.MustParse("a7c3e5f1-9b2d-4e8a-8f6c-1d0e2b3a4c5d")

// MintNodeID returns a race-safe UUID v5 for a node from (side, parentID, type, basename).
// side is "SRC" or "DST". basename is the single path segment (not a full path).
// Roots use parentID="" and basename="/".
func MintNodeID(side, parentID, nodeType, basename string) string {
	side = strings.ToUpper(side)
	nodeType = NormalizeQueueNodeType(nodeType)
	basename = NormalizeNodeBasename(basename)
	name := side + "\x00" + parentID + "\x00" + nodeType + "\x00" + basename
	return uuid.NewSHA1(nodeIDNamespace, []byte(name)).String()
}

// RootNodeID returns the fixed UUID v5 for the SRC or DST root folder.
func RootNodeID(side string) string {
	return MintNodeID(side, "", NodeTypeFolder, "/")
}

// NormalizeNodeBasename returns the final path segment used for minting, sibling checks, and GPL.
// Full paths (e.g. "/a/b.txt" or adapters that put LocationPath in DisplayName) collapse to "b.txt".
//
// Leading/trailing spaces and dots are preserved: Windows (and some other targets) forbid them, and
// TrimSpace here would hide those issues from destination-name checks.
func NormalizeNodeBasename(name string) string {
	if name == "" || name == "/" {
		return "/"
	}
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimSuffix(name, "/")
	if name == "" {
		return "/"
	}
	base := path.Base(name)
	if base == "." || base == "/" {
		return name
	}
	return base
}
