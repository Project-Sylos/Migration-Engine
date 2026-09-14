// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import "strings"

// DisplayPathForChild builds the write-once discovery display_path for a child.
// Root (depth 0) is always "/". Depth 1 is "/{name}". Depth > 1 appends to the
// parent's frozen display_path.
func DisplayPathForChild(parentDisplayPath string, parentDepth int, childName string) string {
	name := strings.Trim(childName, "/")
	if parentDepth < 0 {
		parentDepth = 0
	}
	childDepth := parentDepth + 1
	switch {
	case childDepth <= 0:
		return "/"
	case childDepth == 1:
		if name == "" {
			return "/"
		}
		return "/" + name
	default:
		base := parentDisplayPath
		if base == "" || base == "/" {
			if name == "" {
				return "/"
			}
			return "/" + name
		}
		if name == "" {
			return base
		}
		return strings.TrimRight(base, "/") + "/" + name
	}
}

// RootDisplayPath is the frozen display path for the chosen level-0 folder.
func RootDisplayPath() string {
	return "/"
}
