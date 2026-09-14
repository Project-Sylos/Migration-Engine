// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// ComposeDisplayPath builds a user-visible path from ancestor name columns.
// id_path values must not be shown as display paths.
func ComposeDisplayPath(d *DB, queueType, nodeID string) (string, error) {
	m, err := ComposeDisplayPaths(d, queueType, []string{nodeID})
	if err != nil {
		return "", err
	}
	return m[nodeID], nil
}

// ComposeDisplayPaths batch-builds display paths for many node IDs (one queue side).
// Walks parent_id chains with a shared name cache so a page of results is not N+1.
func ComposeDisplayPaths(d *DB, queueType string, nodeIDs []string) (map[string]string, error) {
	out := make(map[string]string, len(nodeIDs))
	if d == nil || d.Ops() == nil || len(nodeIDs) == 0 {
		return out, nil
	}
	side := opsdb.SideSRC
	if queueType == "DST" {
		side = opsdb.SideDST
	}
	type nodeRow struct {
		name     string
		parentID string
	}
	cache := make(map[string]nodeRow, len(nodeIDs)*4)
	var frontier []string
	seen := make(map[string]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		frontier = append(frontier, id)
	}
	roots := append([]string(nil), frontier...)
	ops := d.Ops()
	for len(frontier) > 0 {
		var missing []string
		for _, id := range frontier {
			if _, ok := cache[id]; !ok {
				missing = append(missing, id)
			}
		}
		frontier = nil
		if len(missing) == 0 {
			break
		}
		nodes, err := ops.BatchGetNode(side, missing)
		if err != nil {
			return nil, err
		}
		for id, n := range nodes {
			cache[id] = nodeRow{name: n.Name, parentID: n.ParentID}
			if n.ParentID != "" {
				if _, ok := cache[n.ParentID]; !ok {
					frontier = append(frontier, n.ParentID)
				}
			}
		}
	}
	for _, id := range roots {
		parts := make([]string, 0, 8)
		cur := id
		for cur != "" && len(parts) < 512 {
			row, ok := cache[cur]
			if !ok {
				break
			}
			base := NormalizeNodeBasename(row.name)
			if base != "" && base != "/" {
				parts = append(parts, base)
			}
			cur = row.parentID
		}
		if len(parts) == 0 {
			out[id] = "/"
			continue
		}
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
		out[id] = "/" + strings.Join(parts, "/")
	}
	return out, nil
}
