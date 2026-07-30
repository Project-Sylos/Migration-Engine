// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"strings"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// DstChildMatchName returns the bare child name used when matching DST listed children to SRC expected children.
// Adapters may set DisplayName to a root-relative path (e.g. "/file_1.txt"); DB rows use path with basename names.
func DstChildMatchName(displayName, locationPath string) string {
	if locationPath != "" {
		return rootRelativeBaseName(locationPath)
	}
	return rootRelativeBaseName(displayName)
}

func rootRelativeBaseName(p string) string {
	p = types.NormalizeLocationPath(p)
	p = strings.TrimSuffix(p, "/")
	if p == "" || p == "/" {
		return ""
	}
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// DstChildMatchKey returns the Type:Name lookup key for DST traversal child comparison.
func DstChildMatchKey(typ, displayName, locationPath string) string {
	return typ + ":" + DstChildMatchName(displayName, locationPath)
}
