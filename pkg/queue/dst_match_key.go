// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"strings"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// DstChildMatchName returns the bare child name used when matching DST listed children to SRC expected children.
// Prefer DisplayName/Name (mutable basename). LocationPath may be an id_path ancestry key and is only a fallback.
func DstChildMatchName(displayName, locationPath string) string {
	if base := rootRelativeBaseName(displayName); base != "" {
		return base
	}
	return rootRelativeBaseName(locationPath)
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
