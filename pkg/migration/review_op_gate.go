// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
)

// ErrReviewOpBusy is returned when a path-review mutation overlaps an in-flight
// path or bulk writer. Callers should map this to HTTP 409.
var ErrReviewOpBusy = errors.New("review operation busy")

func errReviewPathBusy() error {
	return fmt.Errorf("%w: an operation is already running on this path (or a parent/child); try again once it finishes", ErrReviewOpBusy)
}

func errReviewBulkBusy() error {
	return fmt.Errorf("%w: a bulk exclude/unexclude is already running; try again once it finishes", ErrReviewOpBusy)
}

// reviewOpGate serializes overlapping path-review writers and supersedes interactive reads.
type reviewOpGate struct {
	mu           sync.Mutex
	bulkHeld     bool
	activePaths  map[string]struct{}
	searchCancel context.CancelFunc
	searchGen    uint64
}

func newReviewOpGate() reviewOpGate {
	return reviewOpGate{activePaths: make(map[string]struct{})}
}

func normalizeReviewOpPath(path string) string {
	return db.NormalizeSubtreeRootPathForPropagation(path)
}

func reviewPathsOverlap(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	if a == "/" || b == "/" {
		// Root overlaps everything under it.
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (g *reviewOpGate) TryBeginPathMutation(path string) error {
	p := normalizeReviewOpPath(path)
	if p == "" {
		return fmt.Errorf("path required for review mutation gate")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.bulkHeld {
		return errReviewBulkBusy()
	}
	for active := range g.activePaths {
		if reviewPathsOverlap(p, active) {
			return errReviewPathBusy()
		}
	}
	g.activePaths[p] = struct{}{}
	return nil
}

func (g *reviewOpGate) EndPathMutation(path string) {
	p := normalizeReviewOpPath(path)
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.activePaths, p)
}

func (g *reviewOpGate) TryBeginBulkMutation() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.bulkHeld || len(g.activePaths) > 0 {
		return errReviewBulkBusy()
	}
	g.bulkHeld = true
	return nil
}

func (g *reviewOpGate) EndBulkMutation() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bulkHeld = false
}

func (g *reviewOpGate) BulkMutationInProgress() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.bulkHeld
}

func (g *reviewOpGate) beginInteractiveRead(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.searchCancel != nil {
		g.searchCancel()
		g.searchCancel = nil
	}
	g.searchGen++
	gen := g.searchGen
	ctx, cancel := context.WithCancel(parent)
	g.searchCancel = cancel
	return ctx, func() {
		cancel()
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.searchGen == gen {
			g.searchCancel = nil
		}
	}
}

// BeginInteractiveRead cancels any prior search/count for this migration and returns
// a context that also honors ReviewQueryContext (2m unless disabled).
func (m *Migration) BeginInteractiveRead(parent context.Context) (context.Context, context.CancelFunc) {
	if m == nil {
		if parent == nil {
			parent = context.Background()
		}
		return context.WithCancel(parent)
	}
	base, cancelBase := m.reviewOps.beginInteractiveRead(parent)
	if m.DB == nil {
		return base, cancelBase
	}
	ctx, cancelTO := m.DB.ReviewQueryContext(base)
	return ctx, func() {
		cancelTO()
		cancelBase()
	}
}

// BulkMutationInProgress reports whether a bulk search exclude/unexclude is running.
func (m *Migration) BulkMutationInProgress() bool {
	if m == nil {
		return false
	}
	return m.reviewOps.BulkMutationInProgress()
}

func (m *Migration) srcPathForNode(nodeID string) (string, error) {
	if m == nil || m.DB == nil {
		return "", fmt.Errorf("migration db required")
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return "", err
	}
	if node == nil {
		return "", fmt.Errorf("node %s not found in SRC", nodeID)
	}
	return normalizeReviewOpPath(node.Path), nil
}

func (m *Migration) withPathMutation(nodeID string, fn func() (PathReviewActionResult, error)) (PathReviewActionResult, error) {
	path, err := m.srcPathForNode(nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	if err := m.reviewOps.TryBeginPathMutation(path); err != nil {
		return PathReviewActionResult{}, err
	}
	defer m.reviewOps.EndPathMutation(path)
	return fn()
}

func (m *Migration) withBulkMutation(fn func() (PathReviewActionResult, error)) (PathReviewActionResult, error) {
	if err := m.reviewOps.TryBeginBulkMutation(); err != nil {
		return PathReviewActionResult{}, err
	}
	defer m.reviewOps.EndBulkMutation()
	return fn()
}
