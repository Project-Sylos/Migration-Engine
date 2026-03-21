// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
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
	// CopyStatusCounts are SRC-only counts by copy_status (pending, successful, failed, skipped) from status_events.
	CopyStatusCounts CopyStatusCounts
	// Merged review totals (one row per path in merged view; use for API foldersCount, filesCount, excludedCount).
	FoldersCount   int
	FilesCount     int
	ExcludedCount  int
	TotalFileSizeSrc  int64
	TotalFileSizeDst  int64
	FoldersRatio   float64 // FoldersCount / total, rounded to 2 decimals
	FilesRatio     float64 // FilesCount / total, rounded to 2 decimals
}

// CopyStatusCounts holds copy status counts for the API (e.g. copyStatusCounts response).
type CopyStatusCounts struct {
	Pending    int
	Successful int
	Failed     int
	Skipped    int
}

// Migration is the first-class domain object for a single migration lifecycle.
type Migration struct {
	ID      string
	Name    string
	DB      *db.DB // this migration's DB (per-migration or shared in legacy mode)
	store   *migrationStore // store bound to this migration's DB
	manager *MigrationManager

	mu            sync.RWMutex
	phase         string
	runtimeState  RuntimeState
	logRing       *logRing
	lastRunConfig *Config
	runCancel     context.CancelFunc
	running       bool
}

func newMigration(manager *MigrationManager, record migrationRecord, database *db.DB) *Migration {
	m := &Migration{
		ID:      record.ID,
		Name:    record.Name,
		DB:      database,
		store:   newMigrationStore(database),
		manager: manager,
		phase:   record.Phase,
		logRing: newLogRing(256),
	}
	return m
}

func (m *Migration) syncRecord(record migrationRecord) {
	m.mu.Lock()
	m.Name = record.Name
	m.phase = record.Phase
	m.mu.Unlock()
}

// UpdateConfig writes a JSON snapshot of cfg (roots, worker knobs, verification; not FS adapters) to migrations.root_config_json.
func (m *Migration) UpdateConfig(cfg Config) error {
	raw, err := marshalPersistedRunConfigJSON(cfg)
	if err != nil {
		return fmt.Errorf("marshal persisted run config: %w", err)
	}
	return m.store.updateRootConfig(m.ID, string(raw))
}

func (m *Migration) setLastRunConfig(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := cfg
	c.ShutdownContext = nil
	m.lastRunConfig = &c
}

// bindDB attaches the database to a migration that was created without one (pending). Called by the manager when the API passes the migration folder path.
func (m *Migration) bindDB(database *db.DB) {
	m.DB = database
	m.store = newMigrationStore(database)
}

// EnsureEnvelopeMasterKey returns the 32-byte Sylos-FS envelope master key, generating and persisting it if absent.
func (m *Migration) EnsureEnvelopeMasterKey() ([]byte, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("migration has no database")
	}
	return m.store.ensureEnvelopeMasterKey()
}

// GetEnvelopeMasterKey returns the persisted envelope master key.
func (m *Migration) GetEnvelopeMasterKey() ([]byte, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("migration has no database")
	}
	return m.store.getEnvelopeMasterKey()
}

// UpsertFSCredentialBinding persists one side's FS credential binding (connection id, optional creds path, service id, serialized root folder).
func (m *Migration) UpsertFSCredentialBinding(binding FSCredentialBinding) error {
	if m.DB == nil {
		return fmt.Errorf("migration has no database")
	}
	return m.store.upsertFSCredentialBinding(binding)
}

// GetFSCredentialBinding returns a binding by role ("source" or "destination"), or an error if missing.
func (m *Migration) GetFSCredentialBinding(role string) (*FSCredentialBinding, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("migration has no database")
	}
	return m.store.getFSCredentialBinding(role)
}

// ListFSCredentialBindings returns all persisted FS credential bindings for this migration DB.
func (m *Migration) ListFSCredentialBindings() ([]FSCredentialBinding, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("migration has no database")
	}
	return m.store.listFSCredentialBindings()
}

func (m *Migration) Phase() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.phase
}

// IsLive returns true when a run is active (traversal, copy, or retry). Distinct from lifecycle phase; use for "in progress / paused" indicator.
func (m *Migration) IsLive() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

func (m *Migration) transitionTo(next string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !canTransition(m.phase, next) {
		return fmt.Errorf("invalid migration phase transition %s -> %s", m.phase, next)
	}
	if err := m.store.updatePhase(m.ID, next); err != nil {
		return err
	}
	m.phase = next
	m.logRing.add(LogEntry{
		Timestamp: time.Now().UTC(),
		Level:     "info",
		Message:   fmt.Sprintf("phase transitioned to %s", next),
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
	summary, err := SeedRootTasks(normalizedSrc, normalizedDst, m.DB)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("seed roots: %w", err)
	}
	err = m.store.updateUpdatedAt(m.ID)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("update updated at: %w", err)
	}
	if err := m.transitionTo(PhaseFiltersSet); err != nil {
		return RootSeedSummary{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after AddRoots:", err)
	}
	return summary, nil
}

// StartTraversal begins traversal lifecycle and transitions to awaiting-traversal-review on success. Requires filters-set.
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

	cfgForRun := cfg
	cfgForRun.Source.Root = srcRoot
	cfgForRun.Destination.Root = dstRoot
	if err := m.UpdateConfig(cfgForRun); err != nil {
		return RuntimeStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfgForRun)

	runCtx := m.beginRun(cfg.ShutdownContext)
	defer func() {
		m.endRun()
		err = m.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	stats, err := RunMigration(MigrationConfig{
		DB:              m.DB,
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
	if err := m.transitionTo(PhaseTraversalReview); err != nil {
		return RuntimeStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after StartTraversal:", err)
	}
	if js, err := json.Marshal(map[string]int{"last_round_src": stats.Src.Round, "last_round_dst": stats.Dst.Round}); err == nil {
		_ = m.store.updateRuntimeState(m.ID, string(js))
	}
	m.refreshRuntimeState()
	return stats, nil
}

// StartCopy transitions review->copying and runs copy phase. cfg must include live source/destination adapters (same as StartTraversal).
func (m *Migration) StartCopy(cfg Config) (queue.QueueStats, error) {
	if err := m.transitionTo(PhaseCopying); err != nil {
		return queue.QueueStats{}, err
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)

	runCtx := m.beginRun(cfg.ShutdownContext)
	defer func() {
		m.endRun()
		err := m.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	stats, err := RunCopyPhase(CopyPhaseConfig{
		DuckDB:          m.DB,
		SrcAdapter:      cfg.Source.Adapter,
		DstAdapter:      cfg.Destination.Adapter,
		WorkerCount:     cfg.WorkerCount,
		MaxRetries:      cfg.MaxRetries,
		LogAddress:      cfg.LogAddress,
		LogLevel:        cfg.LogLevel,
		SkipListener:    cfg.SkipListener,
		StartupDelay:    cfg.StartupDelay,
		ProgressTick:    cfg.ProgressTick,
		ShutdownContext: runCtx,
	})
	if err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseCopyReview); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after StartCopy:", err)
	}
	if js, err := json.Marshal(map[string]int{"last_copy_round": stats.Round}); err == nil {
		_ = m.store.updateRuntimeState(m.ID, string(js))
	}
	m.refreshRuntimeState()
	return stats, nil
}

// PrepareRetrySweep transitions to traversal-in-progress and persists phase immediately.
// Call this synchronously in the HTTP handler before returning 202 and starting RunRetrySweep in a background task,
// so clients that poll GET migration see traversal-in-progress before the sweep goroutine runs.
func (m *Migration) PrepareRetrySweep() error {
	if m.Phase() != PhaseTraversalReview {
		return fmt.Errorf("prepare retry sweep requires awaiting-traversal-review phase")
	}
	return m.transitionTo(PhaseTraversing)
}

func (m *Migration) RunRetrySweep(cfg Config, opts RetrySweepOptions) (RuntimeStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseTraversing {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires awaiting-traversal-review phase (or prepared traversal-in-progress)")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return RuntimeStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseTraversalReview {
		if err := m.transitionTo(PhaseTraversing); err != nil {
			return RuntimeStats{}, err
		}
	}
	runCtx := m.beginRun(cfg.ShutdownContext)
	defer func() {
		m.endRun()
		err := m.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = cfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = cfg.MaxRetries
	}
	logAddress := opts.LogAddress
	if logAddress == "" {
		logAddress = cfg.LogAddress
	}
	logLevel := opts.LogLevel
	if logLevel == "" {
		logLevel = cfg.LogLevel
	}

	stats, err := RunRetrySweep(SweepConfig{
		DuckDB:       m.DB,
		SrcAdapter:   cfg.Source.Adapter,
		DstAdapter:   cfg.Destination.Adapter,
		WorkerCount:  workerCount,
		MaxRetries:   maxRetries,
		LogAddress:   logAddress,
		LogLevel:     logLevel,
		SkipListener: opts.SkipListener || cfg.SkipListener,
		ProgressTick: cfg.ProgressTick,
		StartupDelay: cfg.StartupDelay,
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
	if err := m.transitionTo(PhaseTraversalReview); err != nil {
		return RuntimeStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after RunRetrySweep:", err)
	}
	if js, err := json.Marshal(map[string]int{"last_round_src": stats.Src.Round, "last_round_dst": stats.Dst.Round}); err == nil {
		_ = m.store.updateRuntimeState(m.ID, string(js))
	}
	m.refreshRuntimeState()
	return stats, nil
}

// PrepareCopyRetry transitions to copy-in-progress and persists phase immediately.
// Call synchronously before returning 202 and starting RunCopyRetry in a background task, same pattern as PrepareRetrySweep.
func (m *Migration) PrepareCopyRetry() error {
	if m.Phase() != PhaseCopyReview {
		return fmt.Errorf("prepare copy retry requires awaiting-copy-review phase")
	}
	return m.transitionTo(PhaseCopying)
}

// RunCopyRetry runs the copy phase in retry mode (only copy_status = failed). Requires awaiting-copy-review. On success transitions back to awaiting-copy-review.
func (m *Migration) RunCopyRetry(cfg Config, opts CopyPhaseOptions) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseCopyReview && phase != PhaseCopying {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires awaiting-copy-review phase (or prepared copy-in-progress)")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseCopyReview {
		if err := m.transitionTo(PhaseCopying); err != nil {
			return queue.QueueStats{}, err
		}
	}
	runCtx := m.beginRun(cfg.ShutdownContext)
	defer func() {
		m.endRun()
		if err := m.store.updateUpdatedAt(m.ID); err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = cfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = cfg.MaxRetries
	}
	logAddress := opts.LogAddress
	if logAddress == "" {
		logAddress = cfg.LogAddress
	}
	logLevel := opts.LogLevel
	if logLevel == "" {
		logLevel = cfg.LogLevel
	}
	stats, err := RunCopyRetryPhase(CopyPhaseConfig{
		DuckDB:          m.DB,
		SrcAdapter:      cfg.Source.Adapter,
		DstAdapter:      cfg.Destination.Adapter,
		WorkerCount:     workerCount,
		MaxRetries:      maxRetries,
		LogAddress:      logAddress,
		LogLevel:        logLevel,
		SkipListener:    opts.SkipListener || cfg.SkipListener,
		StartupDelay:    cfg.StartupDelay,
		ProgressTick:    cfg.ProgressTick,
		ShutdownContext: runCtx,
	})
	if err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseCopyReview); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after RunCopyRetry:", err)
	}
	if js, err := json.Marshal(map[string]int{"last_copy_round": stats.Round}); err == nil {
		_ = m.store.updateRuntimeState(m.ID, string(js))
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
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return nil, fmt.Errorf("query nodes is only available after traversal reaches review phase")
	}
	return m.store.queryNodes(filter)
}

func pathReviewResult(affected int64, deltas map[string]int64) PathReviewActionResult {
	if deltas == nil {
		deltas = make(map[string]int64)
	}
	return PathReviewActionResult{AffectedCount: affected, Deltas: deltas}
}

// SetNodeExcluded mutates review exclusions in engine-owned store.
func (m *Migration) SetNodeExcluded(queueType, nodeID string, excluded bool) (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("set node excluded requires awaiting-traversal-review phase")
	}
	n, deltas, err := m.store.setNodeExcluded(nodeID, excluded)
	if err != nil {
		return PathReviewActionResult{}, fmt.Errorf("set node excluded: %w", err)
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// BulkExclude applies exclusion over a query slice.
func (m *Migration) BulkExclude(filter NodeQueryFilter, excluded bool) (PathReviewActionResult, error) {
	nodes, err := m.QueryNodes(filter)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	var total int64
	merged := make(map[string]int64)
	for i := range nodes {
		if nodes[i].Excluded == excluded {
			continue
		}
		n, deltas, err := m.store.setNodeExcluded(nodes[i].ID, excluded)
		if err != nil {
			return pathReviewResult(total, merged), fmt.Errorf("bulk exclude %s: %w", nodes[i].ID, err)
		}
		total += n
		for k, d := range deltas {
			merged[k] += d
		}
	}
	m.refreshRuntimeState()
	return pathReviewResult(total, merged), nil
}

// GetPathReviewStats returns phase-aware review stats for API passthrough. Reads the canonical stats table; falls back to live recomputation only when the table is empty.
func (m *Migration) GetPathReviewStats() PathReviewStats {
	if m.DB == nil {
		return PathReviewStats{}
	}
	snap, err := m.DB.GetReviewStatsSnapshot()
	if err != nil {
		return PathReviewStats{}
	}
	raw := ReviewStatsRawFromSnapshot(snap)
	return raw.ToPathReviewStats(m.Phase())
}

func (m *Migration) GetTraversalSummary() (TraversalSummary, error) {
	status, err := InspectMigrationStatus(m.DB)
	if err != nil {
		return TraversalSummary{}, err
	}
	srcExcluded, err := db.CountExcluded(m.DB, "SRC")
	if err != nil {
		return TraversalSummary{}, err
	}
	dstExcluded, err := db.CountExcluded(m.DB, "DST")
	if err != nil {
		return TraversalSummary{}, err
	}
	copyCounts, err := m.DB.GetCopyStatusCountsFromEvents()
	if err != nil {
		return TraversalSummary{}, err
	}
	merged, err := db.GetMergedReviewStats(m.DB, db.ReviewFilter{})
	if err != nil {
		return TraversalSummary{}, err
	}
	total := merged.Folders + merged.Files
	var foldersRatio, filesRatio float64
	if total > 0 {
		foldersRatio = roundRatio(float64(merged.Folders)/float64(total), 2)
		filesRatio = roundRatio(float64(merged.Files)/float64(total), 2)
	}
	return TraversalSummary{
		SrcTotal:         status.SrcTotal,
		DstTotal:         status.DstTotal,
		SrcPending:       status.SrcPending,
		DstPending:       status.DstPending,
		SrcFailed:        status.SrcFailed,
		DstFailed:        status.DstFailed,
		SrcExcluded:      srcExcluded,
		DstExcluded:      dstExcluded,
		CopyStatusCounts: CopyStatusCounts{
			Pending:    int(copyCounts.Pending),
			Successful: int(copyCounts.Successful),
			Failed:     int(copyCounts.Failed),
			Skipped:    int(copyCounts.Skipped),
		},
		FoldersCount:      merged.Folders,
		FilesCount:        merged.Files,
		ExcludedCount:     merged.Excluded,
		TotalFileSizeSrc:  merged.SizeSrc,
		TotalFileSizeDst:  merged.SizeDst,
		FoldersRatio:      foldersRatio,
		FilesRatio:        filesRatio,
	}, nil
}

// roundRatio rounds v to n decimal places (e.g. 2 for 0.00).
func roundRatio(v float64, n int) float64 {
	if n <= 0 {
		return v
	}
	pow := 1.0
	for i := 0; i < n; i++ {
		pow *= 10
	}
	return float64(int64(v*pow+0.5)) / pow
}

func (m *Migration) MarkNodeForRetryDiscovery(nodeID string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("retry discovery mutation requires awaiting-traversal-review phase")
	}
	n, deltas, err := m.store.markNodeForRetryDiscovery(nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) UnmarkNodeForRetryDiscovery(nodeID string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("retry discovery mutation requires awaiting-traversal-review phase")
	}
	n, deltas, err := m.store.unmarkNodeForRetryDiscovery(nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) MarkNodeForRetryCopy(nodeID string) (PathReviewActionResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("retry copy mutation requires review or copying phase")
	}
	n, deltas, err := m.store.setNodeCopyStatus(nodeID, db.CopyStatusPending)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) UnmarkNodeForRetryCopy(nodeID string) (PathReviewActionResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("retry copy mutation requires review or copying phase")
	}
	n, deltas, err := m.store.setNodeCopyStatus(nodeID, db.CopyStatusFailed)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) RetryAllFailed() (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("retry all failed requires awaiting-traversal-review phase")
	}
	srcFailed, err := m.store.queryNodes(NodeQueryFilter{Queue: "SRC", Status: db.StatusFailed, Limit: 100000})
	if err != nil {
		return PathReviewActionResult{}, err
	}
	for i := range srcFailed {
		if err := m.store.setNodeTraversalStatus(srcFailed[i].ID, db.StatusPending); err != nil {
			return pathReviewResult(int64(i), nil), err
		}
	}
	dstFailed, err := m.store.queryNodes(NodeQueryFilter{Queue: "DST", Status: db.StatusFailed, Limit: 100000})
	if err != nil {
		return PathReviewActionResult{}, err
	}
	for i := range dstFailed {
		if err := m.store.setNodeTraversalStatus(dstFailed[i].ID, db.StatusPending); err != nil {
			return pathReviewResult(int64(len(srcFailed)+i), nil), err
		}
	}
	total := int64(len(srcFailed) + len(dstFailed))
	deltas := make(map[string]int64)
	addReviewDelta(deltas, DeltaTraversalFailed, -total)
	addReviewDelta(deltas, DeltaTraversalPending, total)
	m.refreshRuntimeState()
	return pathReviewResult(total, deltas), nil
}

func (m *Migration) SetNodeExcludedWithPropagation(queueType, nodeID string, excluded bool) (PathReviewActionResult, error) {
	if m.Phase() != PhaseTraversalReview {
		return PathReviewActionResult{}, fmt.Errorf("exclusion propagation requires awaiting-traversal-review phase")
	}
	n, deltas, err := m.store.setNodeExcludedWithPropagation(nodeID, excluded)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) BulkExcludeWithPropagation(filter NodeQueryFilter, excluded bool) (PathReviewActionResult, error) {
	nodes, err := m.QueryNodes(filter)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	var total int64
	merged := make(map[string]int64)
	for i := range nodes {
		n, deltas, err := m.store.setNodeExcludedWithPropagation(nodes[i].ID, excluded)
		if err != nil {
			return pathReviewResult(total, merged), err
		}
		total += n
		for k, d := range deltas {
			merged[k] += d
		}
	}
	m.refreshRuntimeState()
	return pathReviewResult(total, merged), nil
}

func (m *Migration) ListChildrenDiffs(req ListChildrenDiffsRequest) (ListChildrenDiffsResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return ListChildrenDiffsResult{}, fmt.Errorf("diff listing requires review or later phase")
	}
	return m.store.listChildrenDiffs(req)
}

func (m *Migration) SearchPathReviewItems(req SearchRequest) (SearchResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return SearchResult{}, fmt.Errorf("search requires review or later phase")
	}
	return m.store.searchPathReviewItems(req)
}

func (m *Migration) GetChildrenDiffsStats(path string, foldersOnly bool) (DiffsStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return DiffsStats{}, fmt.Errorf("diff stats requires review or later phase")
	}
	return m.store.getChildrenDiffsStats(path, foldersOnly)
}

// GetSearchStats returns aggregate counts for the same filter as SearchPathReviewItems (query, path, status, foldersOnly).
func (m *Migration) GetSearchStats(req SearchRequest) (DiffsStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview {
		return DiffsStats{}, fmt.Errorf("search stats requires review or later phase")
	}
	return m.store.getSearchStats(req)
}

func (m *Migration) GetQueueMetrics() (QueueMetricsSnapshot, error) {
	return m.store.getQueueMetrics()
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
	status, err := InspectMigrationStatus(m.DB)
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
	out, err := m.store.listRecentLogs(limit)
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
