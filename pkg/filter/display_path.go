// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import "strings"

// DisplayPathForChild builds the root-relative display_path for a child.
// Root (depth 0) is always "/". Depth 1 is "/{name}". Depth > 1 appends to the
// parent's display_path. Maintained on GPL rename (not write-once).
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

// RootDisplayPath is the display path for the chosen level-0 folder.
func RootDisplayPath() string {
	return "/"
}

// RenameDisplayPathLeaf replaces the last segment of displayPath with newName.
// depth is the node's Depth (0 = root "/").
func RenameDisplayPathLeaf(displayPath, newName string, depth int) string {
	newName = strings.Trim(newName, "/")
	if depth <= 0 {
		return "/"
	}
	if depth == 1 {
		if newName == "" {
			return "/"
		}
		return "/" + newName
	}
	base := strings.TrimRight(strings.TrimSpace(displayPath), "/")
	if base == "" || base == "/" {
		if newName == "" {
			return "/"
		}
		return "/" + newName
	}
	i := strings.LastIndexByte(base, '/')
	if i < 0 {
		if newName == "" {
			return "/"
		}
		return "/" + newName
	}
	if newName == "" {
		return base[:i]
	}
	return base[:i+1] + newName
}

// ReplaceDisplayPathPrefix rewrites display paths under a renamed folder.
// oldPrefix and newPrefix are full display paths of the renamed node (no trailing slash except root).
func ReplaceDisplayPathPrefix(displayPath, oldPrefix, newPrefix string) string {
	dp := strings.TrimSpace(displayPath)
	oldPrefix = strings.TrimRight(strings.TrimSpace(oldPrefix), "/")
	newPrefix = strings.TrimRight(strings.TrimSpace(newPrefix), "/")
	if oldPrefix == "" {
		return dp
	}
	if dp == oldPrefix || dp == oldPrefix+"/" {
		if newPrefix == "" {
			return "/"
		}
		return newPrefix
	}
	if strings.HasPrefix(dp, oldPrefix+"/") {
		return newPrefix + dp[len(oldPrefix):]
	}
	return dp
}
