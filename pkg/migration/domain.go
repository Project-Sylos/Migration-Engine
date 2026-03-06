// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// RuntimeState is the in-memory live status projection for a migration.
type RuntimeState struct {
	NodesDiscovered int64
	TasksPending    int64
	TasksCompleted  int64
	BytesCopied     int64
	Errors          int64
}

// LogEntry is a user-facing runtime log projection.
type LogEntry struct {
	Timestamp time.Time
	Level     string
	Message   string
}

type logRing struct {
	entries []LogEntry
	next    int
	size    int
}

func newLogRing(capacity int) *logRing {
	return &logRing{
		entries: make([]LogEntry, capacity),
	}
}

func (r *logRing) add(entry LogEntry) {
	if len(r.entries) == 0 {
		return
	}
	r.entries[r.next] = entry
	r.next = (r.next + 1) % len(r.entries)
	if r.size < len(r.entries) {
		r.size++
	}
}

func (r *logRing) recent(limit int) []LogEntry {
	if limit <= 0 || limit > r.size {
		limit = r.size
	}
	out := make([]LogEntry, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (r.next - 1 - i + len(r.entries)) % len(r.entries)
		out = append(out, r.entries[idx])
	}
	return out
}

// NodeQueryFilter controls review-phase node query behavior.
type NodeQueryFilter struct {
	Queue       string
	Depth       *int
	Status      string
	Excluded    *bool
	PathLike    string
	Limit       int
	Offset      int
	OrderByPath bool
}

// TraversalSummary is the review projection returned by GetTraversalSummary.
type TraversalSummary struct {
	SrcTotal    int
	DstTotal    int
	SrcPending  int
	DstPending  int
	SrcFailed   int
	DstFailed   int
	SrcExcluded int
	DstExcluded int
}

// Migration is the first-class domain object for a single migration lifecycle.
type Migration struct {
	ID      string
	Name    string
	manager *MigrationManager

	mu            sync.RWMutex
	phase         Phase
	runtimeState  RuntimeState
	logRing       *logRing
	lastRunConfig *Config
	runCancel     context.CancelFunc
	running       bool
}

func newMigration(manager *MigrationManager, record migrationRecord) *Migration {
	m := &Migration{
		ID:      record.ID,
		Name:    record.Name,
		manager: manager,
		phase:   record.Phase,
		logRing: newLogRing(256),
	}
	return m
}

func (m *Migration) Phase() Phase {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.phase
}

func (m *Migration) transitionTo(next Phase) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !canTransition(m.phase, next) {
		return fmt.Errorf("invalid migration phase transition %s -> %s", m.phase.String(), next.String())
	}
	if err := m.manager.store.updatePhase(m.ID, next); err != nil {
		return err
	}
	m.phase = next
	m.logRing.add(LogEntry{
		Timestamp: time.Now().UTC(),
		Level:     "info",
		Message:   fmt.Sprintf("phase transitioned to %s", next.String()),
	})
	return nil
}

func (m *Migration) beginRun(shutdownCtx context.Context) context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	runCtx, cancel := context.WithCancel(shutdownCtx)
	m.runCancel = cancel
	m.running = true
	return runCtx
}

func (m *Migration) endRun() {
	m.mu.Lock()
	m.runCancel = nil
	m.running = false
	m.mu.Unlock()
}

// AddRoots seeds source and destination root tasks into the migration DB.
// This is the explicit root insert step before starting traversal.
func (m *Migration) AddRoots(srcRoot, dstRoot types.Folder) (RootSeedSummary, error) {
	if m.Phase() != PhaseCreated {
		return RootSeedSummary{}, fmt.Errorf("add roots requires created phase")
	}
	normalizedSrc, err := normalizeRootFolder(srcRoot)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("source root: %w", err)
	}
	normalizedDst, err := normalizeRootFolder(dstRoot)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("destination root: %w", err)
	}
	summary, err := SeedRootTasks(normalizedSrc, normalizedDst, m.manager.db)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("seed roots: %w", err)
	}
	err = m.manager.store.updateUpdatedAt(m.ID)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("update updated at: %w", err)
	}
	return summary, nil
}

// StartTraversal begins traversal lifecycle and transitions to review on success.
func (m *Migration) StartTraversal(cfg Config) (RuntimeStats, error) {
	if err := m.transitionTo(PhaseTraversing); err != nil {
		return RuntimeStats{}, err
	}

	srcRoot, err := normalizeRootFolder(cfg.Source.Root)
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("source root: %w", err)
	}
	dstRoot, err := normalizeRootFolder(cfg.Destination.Root)
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("destination root: %w", err)
	}

	runCtx := m.beginRun(cfg.ShutdownContext)
	defer func() {
		m.endRun()
		err = m.manager.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	stats, err := RunMigration(MigrationConfig{
		DB:              m.manager.db,
		DBPath:          cfg.Database.Path,
		SrcAdapter:      cfg.Source.Adapter,
		DstAdapter:      cfg.Destination.Adapter,
		SrcRoot:         srcRoot,
		DstRoot:         dstRoot,
		SrcServiceName:  cfg.Source.Name,
		WorkerCount:     cfg.WorkerCount,
		MaxRetries:      cfg.MaxRetries,
		CoordinatorLead: cfg.CoordinatorLead,
		LogAddress:      cfg.LogAddress,
		LogLevel:        cfg.LogLevel,
		SkipListener:    cfg.SkipListener,
		StartupDelay:    cfg.StartupDelay,
		ProgressTick:    cfg.ProgressTick,
		ShutdownContext: runCtx,
	})
	if err != nil {
		return RuntimeStats{}, err
	}
	if err := m.transitionTo(PhaseReview); err != nil {
		return RuntimeStats{}, err
	}
	m.mu.Lock()
	cfgCopy := cfg
	cfgCopy.ShutdownContext = runCtx
	m.lastRunConfig = &cfgCopy
	m.mu.Unlock()
	m.refreshRuntimeState()
	return stats, nil
}

// StartCopy transitions review->copying and runs copy phase.
func (m *Migration) StartCopy() (queue.QueueStats, error) {
	if err := m.transitionTo(PhaseCopying); err != nil {
		return queue.QueueStats{}, err
	}
	m.mu.RLock()
	lastCfg := m.lastRunConfig
	m.mu.RUnlock()
	if lastCfg == nil {
		return queue.QueueStats{}, fmt.Errorf("copy requires a prior traversal run")
	}

	runCtx := m.beginRun(lastCfg.ShutdownContext)
	defer func() {
		m.endRun()
		err := m.manager.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	stats, err := RunCopyPhase(CopyPhaseConfig{
		DuckDB:          m.manager.db,
		SrcAdapter:      lastCfg.Source.Adapter,
		DstAdapter:      lastCfg.Destination.Adapter,
		WorkerCount:     lastCfg.WorkerCount,
		MaxRetries:      lastCfg.MaxRetries,
		LogAddress:      lastCfg.LogAddress,
		LogLevel:        lastCfg.LogLevel,
		SkipListener:    lastCfg.SkipListener,
		StartupDelay:    lastCfg.StartupDelay,
		ProgressTick:    lastCfg.ProgressTick,
		ShutdownContext: runCtx,
	})
	if err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseCompleted); err != nil {
		return queue.QueueStats{}, err
	}
	m.refreshRuntimeState()
	return stats, nil
}

func (m *Migration) RunRetrySweep(opts RetrySweepOptions) (RuntimeStats, error) {
	if m.Phase() != PhaseReview {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires review phase")
	}
	m.mu.RLock()
	lastCfg := m.lastRunConfig
	m.mu.RUnlock()
	if lastCfg == nil {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires prior traversal config")
	}
	if err := m.transitionTo(PhaseTraversing); err != nil {
		return RuntimeStats{}, err
	}
	runCtx := m.beginRun(lastCfg.ShutdownContext)
	defer func() {
		m.endRun()
		err := m.manager.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = lastCfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = lastCfg.MaxRetries
	}
	logAddress := opts.LogAddress
	if logAddress == "" {
		logAddress = lastCfg.LogAddress
	}
	logLevel := opts.LogLevel
	if logLevel == "" {
		logLevel = lastCfg.LogLevel
	}

	stats, err := RunRetrySweep(SweepConfig{
		DuckDB:       m.manager.db,
		SrcAdapter:   lastCfg.Source.Adapter,
		DstAdapter:   lastCfg.Destination.Adapter,
		WorkerCount:  workerCount,
		MaxRetries:   maxRetries,
		LogAddress:   logAddress,
		LogLevel:     logLevel,
		SkipListener: opts.SkipListener || lastCfg.SkipListener,
		ProgressTick: lastCfg.ProgressTick,
		StartupDelay: lastCfg.StartupDelay,
		MaxKnownDepth: func() int {
			if opts.MaxKnownDepth != 0 {
				return opts.MaxKnownDepth
			}
			return -1
		}(),
		ShutdownContext: runCtx,
	})
	if err != nil {
		return RuntimeStats{}, err
	}
	if err := m.transitionTo(PhaseReview); err != nil {
		return RuntimeStats{}, err
	}
	m.refreshRuntimeState()
	return stats, nil
}

func (m *Migration) Stop() (StopResult, error) {
	m.mu.RLock()
	cancel := m.runCancel
	running := m.running
	m.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
	return StopResult{
		MigrationID:   m.ID,
		Phase:         m.Phase(),
		RuntimeStatus: m.GetRuntimeStatus(),
		Stopped:       running,
	}, nil
}

// QueryNodes provides review-phase node search/filter without exposing SQL to API.
func (m *Migration) QueryNodes(filter NodeQueryFilter) ([]db.NodeState, error) {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying && m.Phase() != PhaseCompleted {
		return nil, fmt.Errorf("query nodes is only available after traversal reaches review phase")
	}
	return m.manager.store.queryNodes(filter)
}

// SetNodeExcluded mutates review exclusions in engine-owned store.
func (m *Migration) SetNodeExcluded(queueType, nodeID string, excluded bool) error {
	if m.Phase() != PhaseReview {
		return fmt.Errorf("set node excluded requires review phase")
	}
	err := m.manager.store.setNodeExcluded(queueType, nodeID, excluded)
	if err != nil {
		return fmt.Errorf("set node excluded: %w", err)
	}
	m.refreshRuntimeState()
	return nil
}

// BulkExclude applies exclusion over a query slice.
func (m *Migration) BulkExclude(filter NodeQueryFilter, excluded bool) (int, error) {
	nodes, err := m.QueryNodes(filter)
	if err != nil {
		return 0, err
	}
	updated := 0
	for i := range nodes {
		if nodes[i].Excluded == excluded {
			continue
		}
		if err := m.manager.store.setNodeExcluded(filter.Queue, nodes[i].ID, excluded); err != nil {
			return updated, fmt.Errorf("bulk exclude %s: %w", nodes[i].ID, err)
		}
		updated++
	}
	m.refreshRuntimeState()
	return updated, nil
}

func (m *Migration) GetTraversalSummary() (TraversalSummary, error) {
	status, err := InspectMigrationStatus(m.manager.db)
	if err != nil {
		return TraversalSummary{}, err
	}
	srcExcluded, err := db.CountExcluded(m.manager.db, "SRC")
	if err != nil {
		return TraversalSummary{}, err
	}
	dstExcluded, err := db.CountExcluded(m.manager.db, "DST")
	if err != nil {
		return TraversalSummary{}, err
	}
	return TraversalSummary{
		SrcTotal:    status.SrcTotal,
		DstTotal:    status.DstTotal,
		SrcPending:  status.SrcPending,
		DstPending:  status.DstPending,
		SrcFailed:   status.SrcFailed,
		DstFailed:   status.DstFailed,
		SrcExcluded: srcExcluded,
		DstExcluded: dstExcluded,
	}, nil
}

func (m *Migration) MarkNodeForRetryDiscovery(nodeID string) error {
	if m.Phase() != PhaseReview {
		return fmt.Errorf("retry discovery mutation requires review phase")
	}
	if err := m.manager.store.markNodeForRetryDiscovery(nodeID); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

func (m *Migration) UnmarkNodeForRetryDiscovery(nodeID string) error {
	if m.Phase() != PhaseReview {
		return fmt.Errorf("retry discovery mutation requires review phase")
	}
	if err := m.manager.store.unmarkNodeForRetryDiscovery(nodeID); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

func (m *Migration) MarkNodeForRetryCopy(nodeID string) error {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying {
		return fmt.Errorf("retry copy mutation requires review or copying phase")
	}
	if err := m.manager.store.setNodeCopyStatus(nodeID, db.CopyStatusPending); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

func (m *Migration) UnmarkNodeForRetryCopy(nodeID string) error {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying {
		return fmt.Errorf("retry copy mutation requires review or copying phase")
	}
	if err := m.manager.store.setNodeCopyStatus(nodeID, db.CopyStatusFailed); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

func (m *Migration) RetryAllFailed() error {
	if m.Phase() != PhaseReview {
		return fmt.Errorf("retry all failed requires review phase")
	}
	srcFailed, err := m.manager.store.queryNodes(NodeQueryFilter{Queue: "SRC", Status: db.StatusFailed, Limit: 100000})
	if err != nil {
		return err
	}
	for i := range srcFailed {
		if err := m.manager.store.setNodeTraversalStatus(srcFailed[i].ID, db.StatusPending); err != nil {
			return err
		}
	}
	dstFailed, err := m.manager.store.queryNodes(NodeQueryFilter{Queue: "DST", Status: db.StatusFailed, Limit: 100000})
	if err != nil {
		return err
	}
	for i := range dstFailed {
		if err := m.manager.store.setNodeTraversalStatus(dstFailed[i].ID, db.StatusPending); err != nil {
			return err
		}
	}
	m.refreshRuntimeState()
	return nil
}

func (m *Migration) SetNodeExcludedWithPropagation(queueType, nodeID string, excluded bool) error {
	if m.Phase() != PhaseReview {
		return fmt.Errorf("exclusion propagation requires review phase")
	}
	if err := m.manager.store.setNodeExcludedWithPropagation(queueType, nodeID, excluded); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

func (m *Migration) BulkExcludeWithPropagation(filter NodeQueryFilter, excluded bool) (int, error) {
	nodes, err := m.QueryNodes(filter)
	if err != nil {
		return 0, err
	}
	updated := 0
	for i := range nodes {
		if err := m.manager.store.setNodeExcludedWithPropagation(filter.Queue, nodes[i].ID, excluded); err != nil {
			return updated, err
		}
		updated++
	}
	m.refreshRuntimeState()
	return updated, nil
}

func (m *Migration) ListChildrenDiffs(req ListChildrenDiffsRequest) (ListChildrenDiffsResult, error) {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying && m.Phase() != PhaseCompleted {
		return ListChildrenDiffsResult{}, fmt.Errorf("diff listing requires review or later phase")
	}
	return m.manager.store.listChildrenDiffs(req)
}

func (m *Migration) SearchPathReviewItems(req SearchRequest) (SearchResult, error) {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying && m.Phase() != PhaseCompleted {
		return SearchResult{}, fmt.Errorf("search requires review or later phase")
	}
	return m.manager.store.searchPathReviewItems(req)
}

func (m *Migration) GetChildrenDiffsStats(path string, foldersOnly bool) (DiffsStats, error) {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying && m.Phase() != PhaseCompleted {
		return DiffsStats{}, fmt.Errorf("diff stats requires review or later phase")
	}
	return m.manager.store.getChildrenDiffsStats(path, foldersOnly)
}

// GetSearchStats returns aggregate counts for the same filter as SearchPathReviewItems (query, path, status, foldersOnly).
func (m *Migration) GetSearchStats(req SearchRequest) (DiffsStats, error) {
	if m.Phase() != PhaseReview && m.Phase() != PhaseCopying && m.Phase() != PhaseCompleted {
		return DiffsStats{}, fmt.Errorf("search stats requires review or later phase")
	}
	return m.manager.store.getSearchStats(req)
}

func (m *Migration) GetQueueMetrics() (QueueMetricsSnapshot, error) {
	return m.manager.store.getQueueMetrics()
}

func (m *Migration) GetLogs(limit int, groupByLevel bool) (LogsProjection, error) {
	entries, err := m.GetRecentLogs(limit)
	if err != nil {
		return LogsProjection{}, err
	}
	out := LogsProjection{Entries: entries}
	if groupByLevel {
		out.ByLevel = make(map[string][]LogEntry)
		for i := range entries {
			level := entries[i].Level
			out.ByLevel[level] = append(out.ByLevel[level], entries[i])
		}
	}
	return out, nil
}

func (m *Migration) refreshRuntimeState() {
	status, err := InspectMigrationStatus(m.manager.db)
	if err != nil {
		return
	}
	m.mu.Lock()
	m.runtimeState.NodesDiscovered = int64(status.SrcTotal + status.DstTotal)
	m.runtimeState.TasksPending = int64(status.SrcPending + status.DstPending)
	m.runtimeState.TasksCompleted = int64((status.SrcTotal + status.DstTotal) - (status.SrcPending + status.DstPending))
	m.runtimeState.Errors = int64(status.SrcFailed + status.DstFailed)
	m.mu.Unlock()
}

func (m *Migration) GetRuntimeStatus() RuntimeState {
	m.refreshRuntimeState()
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runtimeState
}

func (m *Migration) GetRecentLogs(limit int) ([]LogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	out, err := m.manager.store.listRecentLogs(limit)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	inMemory := m.logRing.recent(limit)
	m.mu.RUnlock()
	if len(inMemory) == 0 {
		return out, nil
	}
	merged := make([]LogEntry, 0, len(inMemory)+len(out))
	merged = append(merged, inMemory...)
	merged = append(merged, out...)
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}
