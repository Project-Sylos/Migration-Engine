// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// testMintFromPath mints a stable node id for fixtures from side, type, and full path.
// Depth-0 roots and depth-1 children under "/" are supported.
func testMintFromPath(side, nodeType, fullPath string) string {
	if fullPath == "/" {
		return MintNodeID(side, "", NodeTypeFolder, "/")
	}
	return MintNodeID(side, MintNodeID(side, "", NodeTypeFolder, "/"), nodeType, NormalizeNodeBasename(fullPath))
}

// DeterministicNodeID is an alias for testMintFromPath used by existing db package fixtures.
func DeterministicNodeID(side, nodeType, fullPath string) string {
	return testMintFromPath(side, nodeType, fullPath)
}
