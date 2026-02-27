// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sort"
	"sync"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// LevelCache holds exactly one BFS level in memory (nodes keyed by id).
// Only two active levels exist at a time: N and N+1; sealed levels are flushed to DB and dropped.
type LevelCache struct {
	mu    sync.RWMutex
	Nodes map[string]*db.NodeState // id -> node state
}

// NewLevelCache creates an empty level cache.
func NewLevelCache() *LevelCache {
	return &LevelCache{
		Nodes: make(map[string]*db.NodeState),
	}
}

// Get returns a copy of the node state for id, or nil if not present.
func (lc *LevelCache) Get(id string) *db.NodeState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	n := lc.Nodes[id]
	if n == nil {
		return nil
	}
	return copyNodeState(n)
}

// GetRef returns the internal pointer for in-place read (caller must not mutate if shared). Prefer Get for safety.
func (lc *LevelCache) GetRef(id string) *db.NodeState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return lc.Nodes[id]
}

// Put inserts or overwrites the node for id. Caller may pass the same pointer that will be stored.
func (lc *LevelCache) Put(id string, n *db.NodeState) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.Nodes == nil {
		lc.Nodes = make(map[string]*db.NodeState)
	}
	lc.Nodes[id] = n
}

// PutIfAbsent inserts n only if id is not already present. Returns true if inserted.
func (lc *LevelCache) PutIfAbsent(id string, n *db.NodeState) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.Nodes == nil {
		lc.Nodes = make(map[string]*db.NodeState)
	}
	if _, ok := lc.Nodes[id]; ok {
		return false
	}
	lc.Nodes[id] = n
	return true
}

// UpdateStatus updates traversal_status and optionally copy_status for the node at id. No-op if not present.
func (lc *LevelCache) UpdateStatus(id string, traversalStatus, copyStatus string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	n := lc.Nodes[id]
	if n == nil {
		return
	}
	if traversalStatus != "" {
		n.TraversalStatus = traversalStatus
		n.Status = traversalStatus
	}
	if copyStatus != "" {
		n.CopyStatus = copyStatus
	}
}

// Count returns the number of nodes in this level.
func (lc *LevelCache) Count() int {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.Nodes)
}

// Snapshot returns a copy of all node states in this level (for seal flush). Caller must not mutate the map.
func (lc *LevelCache) Snapshot() map[string]*db.NodeState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	out := make(map[string]*db.NodeState, len(lc.Nodes))
	for k, v := range lc.Nodes {
		out[k] = copyNodeState(v)
	}
	return out
}

// Clear removes all nodes. Call after sealing this level.
func (lc *LevelCache) Clear() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.Nodes = make(map[string]*db.NodeState)
}

// ListPending returns up to limit nodes with traversal_status == pending and id > afterID, in id order.
// Used to fill the pull buffer from cache (keyset pagination).
func (lc *LevelCache) ListPending(afterID string, limit int) []*db.NodeState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	var out []*db.NodeState
	for _, n := range lc.Nodes {
		if n == nil {
			continue
		}
		s := n.TraversalStatus
		if s == "" {
			s = n.Status
		}
		if s != "" && s != "pending" {
			continue
		}
		if n.ID <= afterID {
			continue
		}
		out = append(out, copyNodeState(n))
	}
	// Sort by id (simple slice sort)
	sortSliceByID(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// ListPendingCopy returns up to limit nodes with copy_status == pending and id > afterID, in id order. If nodeType != "" filter by type (e.g. "folder" or "file" for copy pass).
func (lc *LevelCache) ListPendingCopy(afterID string, limit int, nodeType string) []*db.NodeState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	var out []*db.NodeState
	for _, n := range lc.Nodes {
		if n == nil {
			continue
		}
		c := n.CopyStatus
		if c == "" {
			c = "pending"
		}
		if c != "pending" {
			continue
		}
		if nodeType != "" && n.Type != nodeType {
			continue
		}
		if n.ID <= afterID {
			continue
		}
		out = append(out, copyNodeState(n))
	}
	sortSliceByID(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// ListChildrenByParentPath returns nodes at this level whose parent_path equals parentPath (for building expected SRC children when DST pulls from cache).
func (lc *LevelCache) ListChildrenByParentPath(parentPath string) []*db.NodeState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	var out []*db.NodeState
	for _, n := range lc.Nodes {
		if n != nil && n.ParentPath == parentPath {
			out = append(out, copyNodeState(n))
		}
	}
	return out
}

func sortSliceByID(nodes []*db.NodeState) {
	if len(nodes) <= 1 {
		return
	}
	// insertion sort by ID (stable for small batches)
	for i := 1; i < len(nodes); i++ {
		j := i
		for j > 0 && nodes[j].ID < nodes[j-1].ID {
			nodes[j], nodes[j-1] = nodes[j-1], nodes[j]
			j--
		}
	}
}

func copyNodeState(n *db.NodeState) *db.NodeState {
	if n == nil {
		return nil
	}
	cp := *n
	return &cp
}

// LevelStats holds per-level counters for completion detection (in-memory; persisted at seal).
// For traversal: Pending/Successful/Failed are traversal status counts. For copy phase (SRC): use Copy* for copy status.
type LevelStats struct {
	Pending    int64
	Successful int64
	Failed     int64
	Completed  int64 // tasks completed this round (success + final failure)
	CopyPending    int64
	CopySuccessful int64
	CopyFailed     int64
}

// NodeCache holds per-level caches. Only levels N and N+1 are active; sealed levels are flushed and removed.
type NodeCache struct {
	mu     sync.RWMutex
	Levels map[int]*LevelCache // depth -> level cache
	Stats  map[int]*LevelStats // depth -> stats for that level
}

// NewNodeCache creates an empty node cache.
func NewNodeCache() *NodeCache {
	return &NodeCache{
		Levels: make(map[int]*LevelCache),
		Stats:  make(map[int]*LevelStats),
	}
}

// EnsureLevel returns the LevelCache for depth, creating it if needed.
func (nc *NodeCache) EnsureLevel(depth int) *LevelCache {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Levels[depth] == nil {
		nc.Levels[depth] = NewLevelCache()
	}
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	return nc.Levels[depth]
}

// GetLevel returns the LevelCache for depth, or nil if not present.
func (nc *NodeCache) GetLevel(depth int) *LevelCache {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return nc.Levels[depth]
}

// LevelDepths returns sorted level numbers present in the cache (for iteration over levels).
func (nc *NodeCache) LevelDepths() []int {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	if len(nc.Levels) == 0 {
		return nil
	}
	deps := make([]int, 0, len(nc.Levels))
	for d := range nc.Levels {
		deps = append(deps, d)
	}
	sort.Ints(deps)
	return deps
}

// GetLevelStats returns the LevelStats for depth, or nil.
func (nc *NodeCache) GetLevelStats(depth int) *LevelStats {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return nc.Stats[depth]
}

// EnsureLevelStats returns the LevelStats for depth, creating if needed (call when syncing from DB or before updating).
func (nc *NodeCache) EnsureLevelStats(depth int) *LevelStats {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	return nc.Stats[depth]
}

// SetLevelStats overwrites the stats for depth (e.g. when bootstrapping from DB).
func (nc *NodeCache) SetLevelStats(depth int, pending, successful, failed, completed int64) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	nc.Stats[depth].Pending = pending
	nc.Stats[depth].Successful = successful
	nc.Stats[depth].Failed = failed
	nc.Stats[depth].Completed = completed
}

// RecordTraversalTransition updates level stats for one status change (e.g. pending -> successful).
func (nc *NodeCache) RecordTraversalTransition(depth int, oldStatus, newStatus string) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	s := nc.Stats[depth]
	if oldStatus == "pending" {
		s.Pending--
	}
	switch newStatus {
	case "successful":
		s.Successful++
	case "failed":
		s.Failed++
	}
}

// IncrementCompleted increments the completed count for the level (task finished: success or final failure).
func (nc *NodeCache) IncrementCompleted(depth int) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	nc.Stats[depth].Completed++
}

// IncrementPending adds one to the pending count for the level (e.g. when adding a new node to the level).
func (nc *NodeCache) IncrementPending(depth int) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	nc.Stats[depth].Pending++
}

// RecordCopyTransition updates level copy stats for SRC (pending->in_progress on pull; in_progress->successful/failed on complete/fail).
func (nc *NodeCache) RecordCopyTransition(depth int, oldStatus, newStatus string) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.Stats[depth] == nil {
		nc.Stats[depth] = &LevelStats{}
	}
	s := nc.Stats[depth]
	if oldStatus == "pending" {
		s.CopyPending--
	}
	switch newStatus {
	case "in_progress":
		// no increment; in_progress is transient
	case "successful":
		s.CopySuccessful++
	case "failed":
		s.CopyFailed++
	}
}

// DropLevel removes the level and its stats from memory. Call after sealing.
func (nc *NodeCache) DropLevel(depth int) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	delete(nc.Levels, depth)
	delete(nc.Stats, depth)
}

// PromoteLevel makes the cache at fromDepth (e.g. N+1) become the cache at toDepth (e.g. N), and creates a fresh empty cache at fromDepth.
// Call after sealing N: flush and drop N, then PromoteLevel(N+1, N) so the former N+1 becomes the new current level.
func (nc *NodeCache) PromoteLevel(fromDepth, toDepth int) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	from := nc.Levels[fromDepth]
	delete(nc.Levels, toDepth)
	delete(nc.Stats, toDepth)
	if from != nil {
		nc.Levels[toDepth] = from
		nc.Stats[toDepth] = nc.Stats[fromDepth]
	} else {
		nc.Levels[toDepth] = NewLevelCache()
		nc.Stats[toDepth] = &LevelStats{}
	}
	delete(nc.Levels, fromDepth)
	delete(nc.Stats, fromDepth)
	nc.Levels[fromDepth] = NewLevelCache()
	nc.Stats[fromDepth] = &LevelStats{}
}

// TotalNodeCount returns the total number of nodes across all levels (for memory pressure checks).
func (nc *NodeCache) TotalNodeCount() int {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	var n int
	for _, lc := range nc.Levels {
		n += len(lc.Nodes)
	}
	return n
}
