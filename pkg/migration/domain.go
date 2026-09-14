// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/loop"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
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
	ID        string
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
	FoldersCount     int
	FilesCount       int
	ExcludedCount    int
	TotalFileSizeSrc int64
	TotalFileSizeDst int64
	FoldersRatio     float64 // FoldersCount / total, rounded to 2 decimals
	FilesRatio       float64 // FilesCount / total, rounded to 2 decimals
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
	DB      *db.DB          // this migration's DB (per-migration file or shared manager DB)
	store   *migrationStore // store bound to this migration's DB
	manager *MigrationManager

	tokenEncryptionKey []byte // nil = plaintext oauth_credentials rows (tests)

	mu                   sync.RWMutex
	phase                string
	runtimeState         RuntimeState
	logRing              *logRing
	lastRunConfig        *Config
	pathCheckSrcProvider string
	pathCheckDstProvider string
	pathCheckProfile     string
	windowsCompatFlag    bool
	filterRuleset        *filter.CompiledRuleset
	runCancel            context.CancelFunc
	running              bool
	softSuspendRequested atomic.Bool
	stopGraceMu          sync.Mutex
	stopGraceTimer       *time.Timer
	stopProgressMu       sync.Mutex
	stopProgress         StopProgress
	// persistRecord holds the last known migrations row (refreshed on DB sync); used to avoid DuckDB reads on hot API paths while Live.
	persistRecord atomic.Value // migrationRecord
	// activeQueueObs is set for the duration of traversal/copy/retry runs so queue metrics APIs can read memory instead of queue_stats.
	activeQueueObs atomic.Pointer[observe.QueueObserver]
	// activeAutoscaler is set while a phase autoscaler is running (for live MaxWorkers overrides).
	activeAutoscaler   atomic.Pointer[loop.Autoscaler]
	workerCapOverrides atomic.Value // profile.WorkerCapOverrides
	finalizeError      string       // last durable-teardown failure (also in runtime_state JSON)
	reviewOps          reviewOpGate // path-review mutation / interactive-read gate
}

func newMigration(manager *MigrationManager, record migrationRecord, database *db.DB, tokenKey []byte) *Migration {
	m := &Migration{
		ID:                 record.ID,
		Name:               record.Name,
		DB:                 database,
		store:              newMigrationStore(database, tokenKey),
		manager:            manager,
		tokenEncryptionKey: tokenKey,
		phase:              record.Phase,
		logRing:            newLogRing(1000),
		finalizeError:      finalizeErrorFromRuntimeJSON(record.RuntimeStateJSON),
		reviewOps:          newReviewOpGate(),
	}
	m.persistRecord.Store(record)
	return m
}

func finalizeErrorFromRuntimeJSON(runtimeJSON string) string {
	if runtimeJSON == "" || runtimeJSON == "{}" {
		return ""
	}
	var top finalizeErrorRuntime
	if err := json.Unmarshal([]byte(runtimeJSON), &top); err != nil {
		return ""
	}
	return top.FinalizeError
}

// SetWorkerCapOverrides updates MaxWorkers overlays for this migration and pushes them
// to the active autoscaler when a phase is running.
func (m *Migration) SetWorkerCapOverrides(o profile.WorkerCapOverrides) {
	if m == nil {
		return
	}
	cloned := o.Clone()
	m.workerCapOverrides.Store(cloned)
	if a := m.activeAutoscaler.Load(); a != nil {
		a.SetWorkerCapOverrides(cloned)
	}
}

// WorkerCapOverrides returns the current MaxWorkers overlays (session + last Set).
func (m *Migration) WorkerCapOverrides() profile.WorkerCapOverrides {
	if m == nil {
		return profile.WorkerCapOverrides{}
	}
	if v := m.workerCapOverrides.Load(); v != nil {
		if o, ok := v.(profile.WorkerCapOverrides); ok {
			return o.Clone()
		}
	}
	return profile.WorkerCapOverrides{}
}

func (m *Migration) bindAutoscaler(a *loop.Autoscaler) {
	if m == nil {
		return
	}
	m.activeAutoscaler.Store(a)
	if a == nil {
		return
	}
	a.SetWorkerCapOverrides(m.WorkerCapOverrides())
}

// resolveAutoscalerConfig merges Config.Autoscaler with any session WorkerCapOverrides on m.
func (m *Migration) resolveAutoscalerConfig(cfg AutoscalerConfig) AutoscalerConfig {
	out := cfg.Resolve()
	session := m.WorkerCapOverrides()
	if len(session.Caps) > 0 {
		out.WorkerCapOverrides = session
		return out
	}
	if len(out.WorkerCapOverrides.Caps) > 0 {
		m.workerCapOverrides.Store(out.WorkerCapOverrides.Clone())
	}
	return out
}

func (m *Migration) syncRecord(record migrationRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Concurrent GetMigration can SELECT a row, then lose a race to transitionTo's UPDATE,
	// and finally apply that stale snapshot here — rolling phase backward (e.g. filters-set
	// over traversal-in-progress). Reject records older than the last applied snapshot.
	if v := m.persistRecord.Load(); v != nil {
		if cur, ok := v.(migrationRecord); ok {
			if !record.UpdatedAt.IsZero() && !cur.UpdatedAt.IsZero() && record.UpdatedAt.Before(cur.UpdatedAt) {
				return
			}
		}
	}
	m.Name = record.Name
	m.phase = record.Phase
	m.persistRecord.Store(record)
}

// cachedMigrationDetailsForLiveAPI returns details from the last persisted migration row without querying DuckDB.
func (m *Migration) cachedMigrationDetailsForLiveAPI() (*MigrationDetails, bool) {
	v := m.persistRecord.Load()
	if v == nil {
		return nil, false
	}
	rec, ok := v.(migrationRecord)
	if !ok || rec.ID == "" {
		return nil, false
	}
	d := recordToDetails(&rec)
	return d, true
}

// UpdateConfig writes a JSON snapshot of cfg (roots, worker knobs, verification; not FS adapters) to migrations.root_config_json.
func (m *Migration) UpdateConfig(cfg Config) error {
	raw, err := json.Marshal(persistedRunConfigFrom(cfg))
	if err != nil {
		return fmt.Errorf("marshal persisted run config: %w", err)
	}
	return m.store.updateMigrationField(m.ID, "root_config_json", string(raw), "updateRootConfig")
}

func (m *Migration) setLastRunConfig(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := cfg
	c.ShutdownContext = nil
	m.lastRunConfig = &c
	m.pathCheckSrcProvider = c.Source.ProviderID
	m.pathCheckDstProvider = c.Destination.ProviderID
	m.pathCheckProfile = c.PathCheckTarget
	m.windowsCompatFlag = c.WindowsCompat
	if c.FilterRuleset != nil {
		m.filterRuleset = c.FilterRuleset
	}
}

// SetFilterRuleset retains a compiled ruleset for compatibility with existing callers.
// Allowed in roots-set / filters-set / review / suspended; rejected while traversal is in progress.
func (m *Migration) SetFilterRuleset(rs *filter.CompiledRuleset) error {
	if m == nil {
		return fmt.Errorf("nil migration")
	}
	phase := m.Phase()
	if phase == PhaseTraversing {
		return fmt.Errorf("cannot change filter rules while discovery is running")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.filterRuleset = rs
	return nil
}

// FilterRuleset returns the compiled filter ruleset (may be nil).
func (m *Migration) FilterRuleset() *filter.CompiledRuleset {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.filterRuleset
}

// SetPathCheckProviders records source/destination provider IDs and optional path-check profile
// used to decide whether destination-name checks apply.
func (m *Migration) SetPathCheckProviders(srcProvider, dstProvider, profile string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pathCheckSrcProvider = srcProvider
	m.pathCheckDstProvider = dstProvider
	if profile != "" {
		m.pathCheckProfile = profile
	}
}

// SetWindowsCompat records whether Windows desktop-sync overlays apply to soft cloud destinations.
func (m *Migration) SetWindowsCompat(enabled bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.windowsCompatFlag = enabled
}

// WindowsCompat reports the stored Windows-compat opt-in flag.
func (m *Migration) WindowsCompat() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.windowsCompatFlag
}

// bindDB attaches the database to a migration that was created without one (pending). Called by the manager when the API passes the migration folder path.
func (m *Migration) bindDB(database *db.DB) {
	m.DB = database
	m.store = newMigrationStore(database, m.tokenEncryptionKey)
}

func (m *Migration) setTokenEncryptionKey(tokenKey []byte) {
	m.tokenEncryptionKey = tokenKey
	if m.store != nil {
		m.store.setTokenKey(tokenKey)
	}
}
func (m *Migration) Phase() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.phase
}

// GetName returns the migration's current display name.
func (m *Migration) GetName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Name
}

// SetName persists a new display name and updates the in-memory value.
func (m *Migration) SetName(name string) error {
	if m.DB == nil {
		return fmt.Errorf("migration has no database")
	}
	if err := m.store.updateMigrationField(m.ID, "name", name, "updateName"); err != nil {
		return err
	}
	m.mu.Lock()
	m.Name = name
	m.mu.Unlock()
	return nil
}

// IsLive returns true when a run is active (traversal, copy, or retry). Distinct from lifecycle phase; use for "in progress / paused" indicator.
func (m *Migration) IsLive() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// NormalizeDeadInProgressToSuspended moves a non-live *-in-progress phase to the matching
// *-suspended phase so resume can restart. No-op when live or when not in an in-progress phase.
// Dead *-finalizing becomes *-finalize-failed so the UI can retry durable teardown.
func (m *Migration) NormalizeDeadInProgressToSuspended() (bool, error) {
	if m.IsLive() {
		return false, nil
	}
	var target string
	switch m.Phase() {
	case PhaseTraversing:
		target = PhaseTraversalSuspended
	case PhaseCopying:
		target = PhaseCopySuspended
	case PhaseDeleting:
		target = PhaseDeleteSuspended
	case PhaseTraversalFinalizing:
		target = PhaseTraversalFinalizeFailed
	case PhaseCopyFinalizing:
		target = PhaseCopyFinalizeFailed
	case PhaseDeleteFinalizing:
		target = PhaseDeleteFinalizeFailed
	default:
		return false, nil
	}
	if err := m.transitionTo(target); err != nil {
		return false, err
	}
	if IsFinalizeFailedPhase(target) && m.FinalizeError() == "" {
		m.setFinalizeError(fmt.Errorf("durable teardown interrupted (process stopped during finalizing)"))
	}
	return true, nil
}

func (m *Migration) transitionTo(next string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !canTransition(m.phase, next) {
		return fmt.Errorf("invalid migration phase transition %s -> %s", m.phase, next)
	}
	if err := m.store.updateMigrationField(m.ID, "phase", next, "updatePhase"); err != nil {
		return err
	}
	m.phase = next
	if v := m.persistRecord.Load(); v != nil {
		if rec, ok := v.(migrationRecord); ok {
			rec.Phase = next
			rec.UpdatedAt = time.Now().UTC()
			m.persistRecord.Store(rec)
		}
	}
	m.logRing.add(LogEntry{
		ID:        fmt.Sprintf("phase-%s-%d", next, time.Now().UnixNano()),
		Timestamp: time.Now().UTC(),
		Level:     "info",
		Message:   fmt.Sprintf("phase transitioned to %s", next),
	})
	return nil
}

// beginRun returns the per-run context and its cancel function. Call cancelRun when the run is finished
// (success, error, or soft suspend) so queue Run loops and workers exit; otherwise they keep polling while paused.
func (m *Migration) beginRun(shutdownCtx context.Context) (runCtx context.Context, cancelRun context.CancelFunc) {
	m.disarmStopGraceTimer()
	m.clearStopProgress()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.softSuspendRequested.Store(false)
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	runCtx, cancel := context.WithCancel(shutdownCtx)
	m.runCancel = cancel
	m.running = true
	return runCtx, cancel
}

func (m *Migration) runtimeStateJSON() string {
	if m.DB == nil {
		return ""
	}
	rec, err := m.store.getMigration(m.DB, m.ID)
	if err != nil || rec == nil {
		return ""
	}
	return rec.RuntimeStateJSON
}

func (m *Migration) endRun() {
	m.mu.Lock()
	m.runCancel = nil
	m.running = false
	m.mu.Unlock()
	m.disarmStopGraceTimer()
}

func (m *Migration) refreshRuntimeState() {
	if m.DB == nil {
		return
	}
	snap, err := stats.GetReviewStatsSnapshot(m.DB)
	if err != nil {
		return
	}
	discovered := snap.TraversalPending + snap.TraversalSuccessful + snap.TraversalFailed
	m.mu.Lock()
	m.runtimeState.NodesDiscovered = discovered
	m.runtimeState.TasksPending = snap.TraversalPending
	m.runtimeState.TasksCompleted = snap.TraversalSuccessful
	m.runtimeState.Errors = snap.TraversalFailed
	m.mu.Unlock()
}
