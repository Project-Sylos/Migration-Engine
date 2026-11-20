// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
	"strings"
)

// RetryScope is what a retry chunk function is applied to: the side, the
// normalized root path of the scan (the node's own path for a single node), and
// whether the pass marks (true) or unmarks (false).
type RetryScope struct {
	Side string
	Root string
	Mark bool
}

// RetryChunkFn applies one phase-specific retry/mark mutation to a hydrated chunk
// and accumulates its result into out.
type RetryChunkFn[R any] func(sc RetryScope, chunk SubtreeChunk, out *R) error

// ApplySubtreeRetry scans rootPath on side and applies chunkFn to every chunk.
func ApplySubtreeRetry[R any](s *Store, side, rootPath string, mark bool, chunkFn RetryChunkFn[R]) (R, error) {
	var out R
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	rootPath = NormalizeIndexPath(rootPath)
	if rootPath == "" {
		return out, nil
	}
	sc := RetryScope{Side: side, Root: rootPath, Mark: mark}
	err := s.ApplySubtreeScan(side, rootPath, SubtreeScanOpts{}, 0, func(chunk SubtreeChunk) error {
		return chunkFn(sc, chunk, &out)
	})
	return out, err
}

// ApplyNodeRetry applies chunkFn to the single node id on side (no descendants).
func ApplyNodeRetry[R any](s *Store, side, id string, mark bool, chunkFn RetryChunkFn[R]) (R, error) {
	var out R
	if s == nil {
		return out, fmt.Errorf("opsdb: nil store")
	}
	ids := []string{id}
	nodes, err := s.BatchGetNode(side, ids)
	if err != nil {
		return out, err
	}
	n, ok := nodes[id]
	if !ok {
		return out, fmt.Errorf("opsdb: %s node %s not found", side, id)
	}
	sts, err := s.BatchGetStatus(side, ids)
	if err != nil {
		return out, err
	}
	chunk := SubtreeChunk{
		IDs:    ids,
		Nodes:  nodes,
		Status: sts,
		Paths:  map[string]string{id: n.Path},
	}
	sc := RetryScope{Side: side, Root: NormalizeIndexPath(n.Path), Mark: mark}
	err = chunkFn(sc, chunk, &out)
	return out, err
}

// applyRetryAt applies chunkFn to the subtree under a folder node or to a file node alone.
func applyRetryAt[R any](s *Store, side string, n NodeRecord, mark bool, chunkFn RetryChunkFn[R]) (R, error) {
	if n.Type != NodeTypeFile {
		return ApplySubtreeRetry(s, side, n.Path, mark, chunkFn)
	}
	return ApplyNodeRetry(s, side, n.ID, mark, chunkFn)
}

// ApplyRetryAtID loads node id on side and applies chunkFn to its subtree (folder) or to the
// node alone (file).
func ApplyRetryAtID[R any](s *Store, side, id string, mark bool, chunkFn RetryChunkFn[R]) (R, error) {
	var out R
	if s == nil || id == "" {
		return out, fmt.Errorf("opsdb: %s id required", strings.ToLower(side))
	}
	n, ok, err := s.GetNode(side, id)
	if err != nil {
		return out, err
	}
	if !ok {
		return out, fmt.Errorf("opsdb: %s node %s not found", side, id)
	}
	return applyRetryAt(s, side, n, mark, chunkFn)
}
