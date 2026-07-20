// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
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
	runCancel            context.CancelFunc
	running              bool
	softSuspendRequested atomic.Bool
	stopGraceMu          sync.Mutex
	stopGraceTimer       *time.Timer
	// persistRecord holds the last known migrations row (refreshed on DB sync); used to avoid DuckDB reads on hot API paths while Live.
	persistRecord atomic.Value // migrationRecord
	// activeQueueObs is set for the duration of traversal/copy/retry runs so queue metrics APIs can read memory instead of queue_stats.
	activeQueueObs atomic.Pointer[queue.QueueObserver]
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
		logRing:            newLogRing(256),
	}
	m.persistRecord.Store(record)
	return m
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
	m.pathCheckSrcProvider = c.Source.ProviderID
	m.pathCheckDstProvider = c.Destination.ProviderID
	m.pathCheckProfile = c.PathCheckTarget
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

// UpsertOAuthCredentials stores OAuth refresh credentials JSON for a connection, encrypted when a token key is configured.
func (m *Migration) UpsertOAuthCredentials(connectionID string, credsJSON []byte) error {
	if m.DB == nil {
		return fmt.Errorf("migration has no database")
	}
	return m.store.upsertOAuthCredentials(connectionID, credsJSON)
}

// GetOAuthCredentials returns stored OAuth credentials JSON for a connection, decrypting when encrypted at rest.
func (m *Migration) GetOAuthCredentials(connectionID string) ([]byte, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("migration has no database")
	}
	return m.store.getOAuthCredentials(connectionID)
}

// DeleteOAuthCredentials removes stored OAuth credentials for a connection.
func (m *Migration) DeleteOAuthCredentials(connectionID string) error {
	if m.DB == nil {
		return fmt.Errorf("migration has no database")
	}
	return m.store.deleteOAuthCredentials(connectionID)
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
	if err := m.store.updateName(m.ID, name); err != nil {
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
	default:
		return false, nil
	}
	if err := m.transitionTo(target); err != nil {
		return false, err
	}
	return true, nil
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
	if v := m.persistRecord.Load(); v != nil {
		if rec, ok := v.(migrationRecord); ok {
			rec.Phase = next
			rec.UpdatedAt = time.Now().UTC()
			m.persistRecord.Store(rec)
		}
	}
	m.logRing.add(LogEntry{
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

// StartTraversal begins traversal lifecycle and transitions to awaiting-traversal-review on success.
// Requires filters-set, or traversal-suspended to resume after soft suspend.
func (m *Migration) StartTraversal(cfg Config) (RuntimeStats, error) {
	prevPhase := m.Phase()
	if prevPhase != PhaseFiltersSet && prevPhase != PhaseTraversalSuspended && prevPhase != PhaseCreated {
		return RuntimeStats{}, fmt.Errorf("start traversal requires filters-set or traversal-suspended phase, got %s", prevPhase)
	}
	if err := m.transitionTo(PhaseTraversing); err != nil {
		return RuntimeStats{}, err
	}

	// Mark live immediately so concurrent GetMigration skips syncRecord and cannot race
	// a stale filters-set snapshot over this phase before RunMigration starts.
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		if updErr := m.store.updateUpdatedAt(m.ID); updErr != nil {
			fmt.Println("error updating updated at", updErr)
		}
	}()

	var resume *RuntimeSuspendV1
	if prevPhase == PhaseTraversalSuspended {
		if s, ok := parseRuntimeSuspendV1(m.runtimeStateJSON()); ok && s.Kind == "traversal" {
			resume = &s
		}
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

	stats, err := RunMigration(MigrationConfig{
		DB:                   m.DB,
		DBPath:               cfg.Database.Path,
		SrcAdapter:           cfg.Source.Adapter,
		DstAdapter:           cfg.Destination.Adapter,
		SrcRoot:              srcRoot,
		DstRoot:              dstRoot,
		SrcServiceName:       cfg.Source.Name,
		WorkerCount:          cfg.WorkerCount,
		MaxRetries:           cfg.MaxRetries,
		CoordinatorLead:      cfg.CoordinatorLead,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		ResumeTraversal:      resume,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		OnQueueObserver:      func(o *queue.QueueObserver) { m.activeQueueObs.Store(o) },
		Autoscaler:           cfg.Autoscaler,
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
	})
	if err != nil {
		if errors.Is(err, ErrTraversalSoftSuspended) {
			rstats, sus, ok := AsTraversalSuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseTraversalSuspended); e3 != nil {
					return rstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return rstats, nil
			}
		}
		return RuntimeStats{}, err
	}
	if err := m.transitionTo(PhaseTraversalReview); err != nil {
		return RuntimeStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after StartTraversal:", err)
	}
	m.refreshRuntimeState()
	return stats, nil
}

// StartCopy transitions review->copying and runs copy phase. cfg must include live source/destination adapters (same as StartTraversal).
// Requires awaiting-traversal-review or copy-suspended (resume after soft suspend).
func (m *Migration) StartCopy(cfg Config) (queue.QueueStats, error) {
	prevPhase := m.Phase()
	validCopyStartPhases := []string{PhaseTraversalReview, PhaseCopySuspended, PhaseCopyReview}
	if !slices.Contains(validCopyStartPhases, prevPhase) {
		return queue.QueueStats{}, fmt.Errorf("start copy requires awaiting-traversal-review or copy-suspended phase, got %s", prevPhase)
	}
	if err := m.transitionTo(PhaseCopying); err != nil {
		return queue.QueueStats{}, err
	}

	var resumeCopy *RuntimeSuspendV1
	if prevPhase == PhaseCopySuspended {
		if s, ok := parseRuntimeSuspendV1(m.runtimeStateJSON()); ok && s.Kind == "copy" {
			resumeCopy = &s
		}
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)

	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		err := m.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	stats, err := RunCopyPhase(CopyPhaseConfig{
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		DstAdapter:           cfg.Destination.Adapter,
		WorkerCount:          cfg.WorkerCount,
		MaxRetries:           cfg.MaxRetries,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		ResumeCopy:           resumeCopy,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		OnQueueObserver:      func(o *queue.QueueObserver) { m.activeQueueObs.Store(o) },
		Autoscaler:           cfg.Autoscaler,
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
	})
	if err != nil {
		if errors.Is(err, ErrCopySoftSuspended) {
			cstats, sus, ok := AsCopySuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseCopySuspended); e3 != nil {
					return cstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return cstats, nil
			}
		}
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseCopyReview); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after StartCopy:", err)
	}
	m.refreshRuntimeState()
	return stats, nil
}

// DeleteSummary holds counts and source info for the delete confirmation modal.
type DeleteSummary struct {
	SourceRootPath string
	Pending        int64
	Failed         int64
	Deleted        int64
}

// GetDeleteSummary returns delete status counts for confirmation UI.
func (m *Migration) GetDeleteSummary(sourceRootPath string) (DeleteSummary, error) {
	if m.DB == nil {
		return DeleteSummary{}, fmt.Errorf("database not available")
	}
	counts, err := m.DB.GetDeleteStatusCountsFromEvents()
	if err != nil {
		return DeleteSummary{}, err
	}
	return DeleteSummary{
		SourceRootPath: sourceRootPath,
		Pending:        counts.Pending,
		Failed:         counts.Failed,
		Deleted:        counts.Deleted,
	}, nil
}

// StartDelete transitions copy-review->deleting and runs delete phase. Requires awaiting-copy-review or delete-suspended.
func (m *Migration) StartDelete(cfg Config) (queue.QueueStats, error) {
	prevPhase := m.Phase()
	validDeleteStartPhases := []string{PhaseCopyReview, PhaseDeleteSuspended}
	if !slices.Contains(validDeleteStartPhases, prevPhase) {
		return queue.QueueStats{}, fmt.Errorf("start delete requires awaiting-copy-review or delete-suspended phase, got %s", prevPhase)
	}
	if err := m.transitionTo(PhaseDeleting); err != nil {
		return queue.QueueStats{}, err
	}
	if cfg.Source.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("delete requires source adapter in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)

	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		_ = m.store.updateUpdatedAt(m.ID)
	}()
	stats, err := RunDeletePhase(DeletePhaseConfig{
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		WorkerCount:          cfg.WorkerCount,
		MaxRetries:           cfg.MaxRetries,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		OnQueueObserver:      func(o *queue.QueueObserver) { m.activeQueueObs.Store(o) },
		Autoscaler:           cfg.Autoscaler,
		SrcService:           cfg.Source,
	})
	if err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseDeleteReview); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after StartDelete:", err)
	}
	m.refreshRuntimeState()
	return stats, nil
}

// PrepareDeleteRetry transitions to delete-in-progress before async retry.
func (m *Migration) PrepareDeleteRetry() error {
	phase := m.Phase()
	if phase == PhaseDeleting {
		// Already prepared (or left mid-run); allow idempotent re-prepare.
		return nil
	}
	if phase != PhaseDeleteReview && phase != PhaseDeleteSuspended {
		return fmt.Errorf("prepare delete retry requires awaiting-delete-review or delete-suspended phase")
	}
	return m.transitionTo(PhaseDeleting)
}

// RunDeleteRetry runs delete retry for failed items only.
func (m *Migration) RunDeleteRetry(cfg Config, opts CopyPhaseOptions) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseDeleteReview && phase != PhaseDeleting && phase != PhaseDeleteSuspended {
		return queue.QueueStats{}, fmt.Errorf("delete retry requires awaiting-delete-review, delete-suspended, or prepared delete-in-progress")
	}
	if cfg.Source.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("delete retry requires source adapter in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseDeleteReview || phase == PhaseDeleteSuspended {
		if err := m.transitionTo(PhaseDeleting); err != nil {
			return queue.QueueStats{}, err
		}
	}
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		_ = m.store.updateUpdatedAt(m.ID)
	}()
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = cfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = cfg.MaxRetries
	}
	stats, err := RunDeleteRetryPhase(DeletePhaseConfig{
		DuckDB:          m.DB,
		SrcAdapter:      cfg.Source.Adapter,
		WorkerCount:     workerCount,
		MaxRetries:      maxRetries,
		LogAddress:      cfg.LogAddress,
		LogLevel:        cfg.LogLevel,
		SkipListener:    opts.SkipListener || cfg.SkipListener,
		StartupDelay:    cfg.StartupDelay,
		ShutdownContext: runCtx,
		OnQueueObserver: func(o *queue.QueueObserver) { m.activeQueueObs.Store(o) },
		SrcService:      cfg.Source,
	})
	if err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseDeleteReview); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after delete retry:", err)
	}
	m.refreshRuntimeState()
	return stats, nil
}

// PrepareRetrySweep transitions to traversal-in-progress and persists phase immediately.
// Call this synchronously in the HTTP handler before returning 202 and starting RunRetrySweep in a background task,
// so clients that poll GET migration see traversal-in-progress before the sweep goroutine runs.
// Allowed from awaiting-traversal-review or traversal-suspended (resume after soft suspend).
func (m *Migration) PrepareRetrySweep() error {
	phase := m.Phase()
	if phase == PhaseTraversing {
		// Already prepared (or left mid-run); allow idempotent re-prepare.
		return nil
	}
	if phase != PhaseTraversalReview && phase != PhaseTraversalSuspended {
		return fmt.Errorf("prepare retry sweep requires awaiting-traversal-review or traversal-suspended phase")
	}
	return m.transitionTo(PhaseTraversing)
}

func (m *Migration) RunRetrySweep(cfg Config, opts RetrySweepOptions) (RuntimeStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseTraversing && phase != PhaseTraversalSuspended {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires awaiting-traversal-review, traversal-suspended, or prepared traversal-in-progress")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return RuntimeStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseTraversalReview || phase == PhaseTraversalSuspended {
		if err := m.transitionTo(PhaseTraversing); err != nil {
			return RuntimeStats{}, err
		}
	}
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
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
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		OnQueueObserver:      func(o *queue.QueueObserver) { m.activeQueueObs.Store(o) },
		Autoscaler:           cfg.Autoscaler,
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
	})
	if err != nil {
		if errors.Is(err, ErrTraversalSoftSuspended) {
			rstats, sus, ok := AsTraversalSuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseTraversalSuspended); e3 != nil {
					return rstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return rstats, nil
			}
		}
		return RuntimeStats{}, err
	}
	if err := m.transitionTo(PhaseTraversalReview); err != nil {
		return RuntimeStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after RunRetrySweep:", err)
	}
	m.refreshRuntimeState()
	return stats, nil
}

// PrepareCopyRetry transitions to copy-in-progress and persists phase immediately.
// Call synchronously before returning 202 and starting RunCopyRetry in a background task, same pattern as PrepareRetrySweep.
// Allowed from awaiting-copy-review or copy-suspended.
func (m *Migration) PrepareCopyRetry() error {
	phase := m.Phase()
	if phase == PhaseCopying {
		// Already prepared (or left mid-run); allow idempotent re-prepare.
		return nil
	}
	if phase != PhaseCopyReview && phase != PhaseCopySuspended {
		return fmt.Errorf("prepare copy retry requires awaiting-copy-review or copy-suspended phase")
	}
	return m.transitionTo(PhaseCopying)
}

// RunCopyRetry runs the copy phase in retry mode (only copy_status = failed). On success transitions back to awaiting-copy-review.
func (m *Migration) RunCopyRetry(cfg Config, opts CopyPhaseOptions) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseCopyReview && phase != PhaseCopying && phase != PhaseCopySuspended {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires awaiting-copy-review, copy-suspended, or prepared copy-in-progress")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseCopyReview || phase == PhaseCopySuspended {
		if err := m.transitionTo(PhaseCopying); err != nil {
			return queue.QueueStats{}, err
		}
	}
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
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
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		DstAdapter:           cfg.Destination.Adapter,
		WorkerCount:          workerCount,
		MaxRetries:           maxRetries,
		LogAddress:           logAddress,
		LogLevel:             logLevel,
		SkipListener:         opts.SkipListener || cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		OnQueueObserver:      func(o *queue.QueueObserver) { m.activeQueueObs.Store(o) },
		Autoscaler:           cfg.Autoscaler,
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
	})
	if err != nil {
		if errors.Is(err, ErrCopySoftSuspended) {
			cstats, sus, ok := AsCopySuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseCopySuspended); e3 != nil {
					return cstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return cstats, nil
			}
		}
		return queue.QueueStats{}, err
	}
	if err := m.transitionTo(PhaseCopyReview); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.ResyncReviewStats(); err != nil {
		fmt.Println("warning: resync review stats after RunCopyRetry:", err)
	}
	m.refreshRuntimeState()
	return stats, nil
}

func (m *Migration) Stop() (StopResult, error) {
	m.mu.RLock()
	cancel := m.runCancel
	running := m.running
	phase := m.phase
	m.mu.RUnlock()

	result := StopResult{
		MigrationID:   m.ID,
		Phase:         m.Phase(),
		RuntimeStatus: m.GetRuntimeStatus(),
		Stopped:       running,
	}

	if !running {
		return result, nil
	}

	switch phase {
	case PhaseTraversing, PhaseCopying:
		m.softSuspendRequested.Store(true)
		result.SoftSuspendRequested = true
		m.armStopGraceTimer(DefaultStopGracePeriod)
		return result, nil
	default:
		if cancel != nil {
			cancel()
		}
		return result, nil
	}
}

// ForceStop cancels the active run context and abandons in-flight queue work.
// Use after a soft-suspend grace period when workers are stuck (e.g. FS retry loops).
func (m *Migration) ForceStop() (StopResult, error) {
	m.mu.RLock()
	cancel := m.runCancel
	running := m.running
	m.mu.RUnlock()

	result := StopResult{
		MigrationID:   m.ID,
		Phase:         m.Phase(),
		RuntimeStatus: m.GetRuntimeStatus(),
		Stopped:       running,
		ForceStopped:  running,
	}

	if !running {
		return result, nil
	}

	m.disarmStopGraceTimer()
	m.softSuspendRequested.Store(false)
	if cancel != nil {
		cancel()
	}
	return result, nil
}

// QueryNodes provides review-phase node search/filter without exposing SQL to API.
func (m *Migration) QueryNodes(filter NodeQueryFilter) ([]db.NodeState, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseTraversalSuspended &&
		phase != PhaseCopying && phase != PhaseCopySuspended && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
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

// GetPathReviewStats returns phase-aware review stats for API passthrough.
func (m *Migration) GetPathReviewStats() PathReviewStats {
	return m.GetPathReviewStatsForView("")
}

// GetPathReviewStatsForView projects stats for a specific review UI. The
// source-cleanup view uses eligible delete statuses even while the migration is
// technically still in copy review.
func (m *Migration) GetPathReviewStatsForView(view string) PathReviewStats {
	if m.DB == nil {
		return PathReviewStats{}
	}
	snap, err := m.DB.GetReviewStatsSnapshot()
	if err != nil {
		return PathReviewStats{}
	}
	raw := ReviewStatsRawFromSnapshot(snap)
	phase := m.Phase()
	if phase == PhaseDeleteReview {
		if remainingSize, sizeErr := m.DB.GetRemainingSourceSizeAfterDelete(); sizeErr == nil {
			raw.SizeSrc = remainingSize
		}
	}
	stats := raw.ToPathReviewStats(phase)
	if view == "source-cleanup" {
		if counts, countsErr := m.DB.GetEligibleDeleteStatusCounts(); countsErr == nil {
			stats.PendingCount = int(counts.Pending)
			stats.FailedCount = int(counts.Failed)
			stats.ExcludedCount = int(counts.Skipped)
			stats.SuccessfulCount = int(counts.Deleted)
			stats.PendingRetriesCount = 0
			if phase == PhaseDeleteReview {
				stats.PendingRetriesCount = int(counts.Pending)
			}
		}
	}
	return stats
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
		SrcTotal:    status.SrcTotal,
		DstTotal:    status.DstTotal,
		SrcPending:  status.SrcPending,
		DstPending:  status.DstPending,
		SrcFailed:   status.SrcFailed,
		DstFailed:   status.DstFailed,
		SrcExcluded: srcExcluded,
		DstExcluded: dstExcluded,
		CopyStatusCounts: CopyStatusCounts{
			Pending:    int(copyCounts.Pending),
			Successful: int(copyCounts.Complete()),
			Failed:     int(copyCounts.Failed),
			Skipped:    int(copyCounts.Skipped),
		},
		FoldersCount:     merged.Folders,
		FilesCount:       merged.Files,
		ExcludedCount:    merged.Excluded,
		TotalFileSizeSrc: merged.SizeSrc,
		TotalFileSizeDst: merged.SizeDst,
		FoldersRatio:     foldersRatio,
		FilesRatio:       filesRatio,
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

func (m *Migration) MarkNodeForRetryDelete(nodeID string) (PathReviewActionResult, error) {
	phase := m.Phase()
	if phase != PhaseCopyReview && phase != PhaseDeleting && phase != PhaseDeleteReview {
		return PathReviewActionResult{}, fmt.Errorf("retry delete mutation requires delete review or deleting phase")
	}
	n, deltas, err := m.store.setNodeDeleteStatus(nodeID, db.DeleteStatusPending)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

func (m *Migration) UnmarkNodeForRetryDelete(nodeID string) (PathReviewActionResult, error) {
	phase := m.Phase()
	if phase != PhaseCopyReview && phase != PhaseDeleting && phase != PhaseDeleteReview {
		return PathReviewActionResult{}, fmt.Errorf("retry delete mutation requires delete review or deleting phase")
	}
	n, deltas, err := m.store.setNodeDeleteStatus(nodeID, db.DeleteStatusFailed)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// SkipNodeDelete opts a successfully copied SRC node out of source removal during cleanup planning.
func (m *Migration) SkipNodeDelete(nodeID string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("skip delete requires awaiting-copy-review phase")
	}
	node, err := db.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	if node == nil {
		return PathReviewActionResult{}, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if !db.CopyStatusIsComplete(node.CopyStatus) {
		return PathReviewActionResult{}, fmt.Errorf("skip delete requires copy complete (successful or already_existed)")
	}
	n, deltas, err := m.store.setNodeDeleteStatusWithPropagation(nodeID, db.DeleteStatusSkipped)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// UnskipNodeDelete re-includes a SRC node in source removal during cleanup planning.
func (m *Migration) UnskipNodeDelete(nodeID string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("unskip delete requires awaiting-copy-review phase")
	}
	node, err := db.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	if node == nil {
		return PathReviewActionResult{}, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if !db.CopyStatusIsComplete(node.CopyStatus) {
		return PathReviewActionResult{}, fmt.Errorf("unskip delete requires copy complete (successful or already_existed)")
	}
	if node.DeleteStatus == db.DeleteStatusDeleted {
		return PathReviewActionResult{}, fmt.Errorf("cannot unskip already deleted node")
	}
	n, deltas, err := m.store.setNodeDeleteStatusWithPropagation(nodeID, db.DeleteStatusPending)
	if err != nil {
		return PathReviewActionResult{}, err
	}
	m.refreshRuntimeState()
	return pathReviewResult(n, deltas), nil
}

// PrepareSourceCleanup aligns delete_status with the user's selected SRC nodes for source removal.
// When keepNodeIDs is non-empty, only those nodes are pending and others are skipped.
// When deselectedNodeIDs is non-empty (and keepNodeIDs is empty), those nodes are skipped and others pending.
// When both are empty, all successful copies are pending.
func (m *Migration) PrepareSourceCleanup(keepNodeIDs, deselectedNodeIDs []string) (PathReviewActionResult, error) {
	if m.Phase() != PhaseCopyReview {
		return PathReviewActionResult{}, fmt.Errorf("prepare source cleanup requires awaiting-copy-review phase")
	}
	useKeepList := len(keepNodeIDs) > 0
	keep := make(map[string]bool, len(keepNodeIDs))
	for _, id := range keepNodeIDs {
		if id != "" {
			keep[id] = true
		}
	}
	deselected := make(map[string]bool, len(deselectedNodeIDs))
	for _, id := range deselectedNodeIDs {
		if id != "" {
			deselected[id] = true
		}
	}
	var total int64
	merged := make(map[string]int64)
	offset := 0
	const pageSize = 1000
	for {
		nodes, err := db.ListSrcNodesByCopyStatus(m.DB, db.CopyStatusSuccessful, pageSize, offset)
		if err != nil {
			return PathReviewActionResult{}, err
		}
		if len(nodes) == 0 {
			break
		}
		for _, node := range nodes {
			if node.Depth == 0 {
				continue // root is metadata-only; no delete_status event (null)
			}
			var selected bool
			switch {
			case useKeepList:
				selected = keep[node.ID]
			case len(deselected) > 0:
				selected = !deselected[node.ID]
			default:
				selected = true
			}
			var target string
			if selected {
				if node.DeleteStatus == db.DeleteStatusDeleted || node.DeleteStatus == db.DeleteStatusFailed {
					continue
				}
				target = db.DeleteStatusPending
			} else {
				if node.DeleteStatus == db.DeleteStatusDeleted {
					continue
				}
				target = db.DeleteStatusSkipped
			}
			if node.DeleteStatus == target {
				continue
			}
			n, deltas, err := m.store.setNodeDeleteStatus(node.ID, target)
			if err != nil {
				return pathReviewResult(total, merged), err
			}
			total += n
			for k, v := range deltas {
				merged[k] += v
			}
		}
		if len(nodes) < pageSize {
			break
		}
		offset += pageSize
	}
	m.refreshRuntimeState()
	return pathReviewResult(total, merged), nil
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
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return ListChildrenDiffsResult{}, fmt.Errorf("diff listing requires review or later phase")
	}
	return m.store.listChildrenDiffs(req)
}

func (m *Migration) SearchPathReviewItems(req SearchRequest) (SearchResult, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return SearchResult{}, fmt.Errorf("search requires review or later phase")
	}
	return m.store.searchPathReviewItems(req)
}

func (m *Migration) GetChildrenDiffsStats(path string, foldersOnly bool, includeDestinationOnly *bool) (DiffsStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return DiffsStats{}, fmt.Errorf("diff stats requires review or later phase")
	}
	return m.store.getChildrenDiffsStats(path, foldersOnly, includeDestinationOnly)
}

// GetSearchStats returns aggregate counts for the same filter as SearchPathReviewItems (query, path, status, foldersOnly).
func (m *Migration) GetSearchStats(req SearchRequest) (DiffsStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseCopying && phase != PhaseCopyReview &&
		phase != PhaseDeleting && phase != PhaseDeleteSuspended && phase != PhaseDeleteReview {
		return DiffsStats{}, fmt.Errorf("search stats requires review or later phase")
	}
	return m.store.getSearchStats(req)
}

func (m *Migration) GetQueueMetrics() (QueueMetricsSnapshot, error) {
	if o := m.activeQueueObs.Load(); o != nil {
		if raw, ok := o.LastQueueMetricsForAPI(); ok && len(raw) > 0 {
			return queueMetricsSnapshotFromRawJSON(raw), nil
		}
	}
	return m.store.getQueueMetrics()
}

// PossibleStall reports whether any active queue watchdog recently detected a stall.
func (m *Migration) PossibleStall() bool {
	if o := m.activeQueueObs.Load(); o != nil {
		return o.AnyPossibleStall()
	}
	return false
}

func queueMetricsSnapshotFromRawJSON(raw map[string][]byte) QueueMetricsSnapshot {
	out := QueueMetricsSnapshot{Queues: make(map[string]map[string]any, len(raw))}
	for key, blob := range raw {
		var parsed map[string]any
		if err := json.Unmarshal(blob, &parsed); err != nil {
			parsed = map[string]any{"raw": string(blob)}
		}
		out.Queues[key] = parsed
	}
	return out
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
