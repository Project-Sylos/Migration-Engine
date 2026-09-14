// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"strings"
)

const defaultSubtreeChunkSize = 2000

// SubtreeChunk is one hydrated page of ids under a path prefix scan.
type SubtreeChunk struct {
	IDs    []string
	Paths  map[string]string
	Nodes  map[string]NodeRecord
	Status map[string]StatusRecord
}

// ApplySubtreeScan walks idx:path under root in chunks, hydrates each page, and calls fn.
func (s *Store) ApplySubtreeScan(side, root string, opts SubtreeScanOpts, chunkSize int, fn func(SubtreeChunk) error) error {
	if s == nil || fn == nil {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = defaultSubtreeChunkSize
	}
	after := ""
	for {
		ids, next, done, err := s.ScanSubtreeIDs(side, root, after, chunkSize, opts)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			if done {
				return nil
			}
			after = next
			continue
		}
		nodes, err := s.BatchGetNode(side, ids)
		if err != nil {
			return err
		}
		stMap, err := s.BatchGetStatus(side, ids)
		if err != nil {
			return err
		}
		paths := make(map[string]string, len(ids))
		for _, id := range ids {
			if n, ok := nodes[id]; ok {
				paths[id] = n.Path
			}
		}
		if err := fn(SubtreeChunk{IDs: ids, Paths: paths, Nodes: nodes, Status: stMap}); err != nil {
			return err
		}
		if done {
			return nil
		}
		after = next
	}
}

// StrictAncestorPaths returns parent paths from child up to (but not including) root "/".
// Example: /a/b/c -> [/a/b, /a]
func StrictAncestorPaths(childPath string) []string {
	childPath = NormalizeIndexPath(childPath)
	if childPath == "" || childPath == "/" {
		return nil
	}
	var out []string
	path := childPath
	for {
		idx := strings.LastIndex(path, "/")
		if idx <= 0 {
			break
		}
		path = path[:idx]
		if path == "" {
			path = "/"
		}
		if path == "/" {
			break
		}
		out = append(out, path)
	}
	return out
}

// LookupAncestorIDs resolves strict ancestor paths to node ids (missing omitted).
func (s *Store) LookupAncestorIDs(side, childPath string) (ids []string, pathByID map[string]string, err error) {
	pathByID = make(map[string]string)
	for _, p := range StrictAncestorPaths(childPath) {
		id, err := s.GetNodeIDByPath(side, p)
		if err != nil {
			return nil, nil, err
		}
		if id == "" {
			continue
		}
		ids = append(ids, id)
		pathByID[id] = p
	}
	return ids, pathByID, nil
}
