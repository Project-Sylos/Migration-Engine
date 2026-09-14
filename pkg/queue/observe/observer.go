// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Package observe provides QueueObserver and stall watchdogs for pkg/queue.
//
// The QueueObserver polls queues directly at regular intervals (default: 200ms) and keeps an
// in-memory metrics snapshot for live API reads. Durable queue_stats rows are written
// asynchronously so DuckDB write locks cannot stall progress-monitor freshness.
//
// Usage:
//   observer := observe.NewQueueObserver(database, 200*time.Millisecond)
//   observer.RegisterQueue("src", srcQueue)
//   observer.RegisterQueue("dst", dstQueue)
//   observer.Start()
//   // Stats are published to /STATS/queue-stats bucket with keys like "src-traversal", "dst-traversal"
//
// Stats can be retrieved from DuckDB using:
//   statsJSON, err := database.GetLatestQueueStats("src-traversal", db.QueueStatsPhaseTraversal)
//   allStats, err := database.GetAllQueueStats()
//
// For low-latency APIs while a run is active, use LastQueueMetricsForAPI(): same JSON shape as
// queue_stats audit rows, refreshed every observer tick from memory. Durable queue_stats writes
// run on a separate goroutine so DuckDB writeMu contention cannot stall live API metrics.

package observe

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// ExternalQueueMetrics contains user-facing metrics published to DuckDB for API access.
type ExternalQueueMetrics struct {
	// Monotonic counters (traversal phase)
	FilesDiscoveredTotal   int64 `json:"files_discovered_total"`
	FoldersDiscoveredTotal int64 `json:"folders_discovered_total"`

	// EMA-smoothed rates (2-5 second window) - traversal phase.
	// Newly discovered children (files + folders) per second.
	DiscoveryRateItemsPerSec float64 `json:"discovery_rate_items_per_sec"`

	// Verification counts (for O(1) stats bucket lookups)
	TotalDiscovered int64 `json:"total_discovered"` // files + folders
	TotalPending    int   `json:"total_pending"`    // pending across all rounds (from DB)
	TotalFailed     int   `json:"total_failed"`     // failed across all rounds

	// Copy phase metrics (monotonic counters)
	Folders int64 `json:"folders"` // Total folders created
	Files   int64 `json:"files"`   // Total files created
	Total   int64 `json:"total"`   // Total items (folders + files)
	Bytes   int64 `json:"bytes"`   // Total bytes transferred (done)

	// Already-on-destination (copy) or not-deleting (delete) completions.
	FoldersAlreadyExists int64 `json:"folders_already_exists,omitempty"`
	FilesAlreadyExists   int64 `json:"files_already_exists,omitempty"`
	BytesAlreadyExists   int64 `json:"bytes_already_exists,omitempty"`

	// Permanent failures counted after seal-buffer accept (folder/file split for UI grid).
	FoldersFailed int64 `json:"folders_failed,omitempty"`
	FilesFailed   int64 `json:"files_failed,omitempty"`

	// Migration-wide expected denominators for Files/Folders/Total (copy/delete).
	FoldersExpected int64 `json:"folders_expected,omitempty"`
	FilesExpected   int64 `json:"files_expected,omitempty"`
	TotalExpected   int64 `json:"total_expected,omitempty"`

	// Items progress (copy/delete): completed/eligible, retry-aware.
	ItemsCompleted       int64   `json:"items_completed,omitempty"`
	ItemsTotal           int64   `json:"items_total,omitempty"`
	ItemsProgressPercent float64 `json:"items_progress_percent,omitempty"`
	// Segment shares of ItemsTotal (0–100) for stacked bars: ok → already_exists → failed.
	ItemsOkPercent            float64 `json:"items_ok_percent,omitempty"`
	ItemsAlreadyExistsPercent float64 `json:"items_already_exists_percent,omitempty"`

	// Bytes progress (copy/delete): done vs fixed migration-wide eligible file size total.
	// Bytes is transferred (and in-flight); BytesFailed is permanent-failure file sizes.
	// BytesProgressPercent uses touched bytes (Bytes+BytesAlreadyExists+BytesFailed) in normal mode.
	BytesTotal           int64   `json:"bytes_total,omitempty"`
	BytesFailed          int64   `json:"bytes_failed,omitempty"`
	BytesProgressPercent float64 `json:"bytes_progress_percent,omitempty"`
	// Segment shares of BytesTotal (0–100) for stacked bars.
	BytesOkPercent            float64 `json:"bytes_ok_percent,omitempty"`
	BytesAlreadyExistsPercent float64 `json:"bytes_already_exists_percent,omitempty"`
	// Failed share of the bytes bar (0–100 of BytesTotal); UI paints this red.
	BytesFailedPercent float64 `json:"bytes_failed_percent,omitempty"`
	// Failed share of the items bar (0–100 of ItemsTotal); UI paints this red.
	ItemsFailedPercent float64 `json:"items_failed_percent,omitempty"`

	// Copy phase rates (EMA-smoothed)
	ItemsPerSecond float64 `json:"items_per_second"` // Items/sec (folders + files)
	BytesPerSecond float64 `json:"bytes_per_second"` // Bytes/sec

	// Deterministic copy/delete progress (0–100). Alias of items_progress_percent. Omitted for traversal.
	ProgressPercent float64 `json:"progress_percent,omitempty"`

	// Current round Expected/Completed (same counters as console progress lines).
	RoundExpected  int `json:"round_expected,omitempty"`
	RoundCompleted int `json:"round_completed,omitempty"`
	// Copy pass (1=folders, 2=files); omit for non-copy queues.
	CopyPass int `json:"copy_pass,omitempty"`

	// Rate-limit windows from attached FS telemetry (RFC3339 UTC when active).
	// Traversal src → until_src; traversal dst → until_dst; copy → both; delete → until_src.
	RateLimitedUntilSrc string `json:"rate_limited_until_src,omitempty"`
	RateLimitedUntilDst string `json:"rate_limited_until_dst,omitempty"`
	// Remaining ms at poll time (0 omitted). UI may subtract ~1s for poll staleness.
	RateLimitedRemainingMsSrc int64 `json:"rate_limited_remaining_ms_src,omitempty"`
	RateLimitedRemainingMsDst int64 `json:"rate_limited_remaining_ms_dst,omitempty"`

	// AIMD inter-op pacing delay (ns→ms for API). Non-zero means workers are artificially slowed.
	InterOpDelayMs int64 `json:"inter_op_delay_ms,omitempty"`

	// Last DuckDB frontier pull (this queue). Surfaced via observer → queue_stats.
	DBPullDurationMs float64 `json:"db_pull_duration_ms,omitempty"`
	DBPullRows       int64   `json:"db_pull_rows,omitempty"`
	DBPullRowsPerSec float64 `json:"db_pull_rows_per_sec,omitempty"`

	// Last successful seal buffer flush (migration-wide; same gauges on each queue snapshot).
	SealFlushDurationMs float64 `json:"seal_flush_duration_ms,omitempty"`
	SealFlushRows       int64   `json:"seal_flush_rows,omitempty"`
	SealFlushRowsPerSec float64 `json:"seal_flush_rows_per_sec,omitempty"`

	// Engine-owned remaining-time estimate (UI renders only).
	// Copy/delete: overall phase. Traversal: current batch only (round_expected - round_completed).
	// EtaSeconds is set when EtaBasis is non-empty; 0 means complete.
	EtaSeconds *float64 `json:"eta_seconds,omitempty"`
	EtaBasis   string   `json:"eta_basis,omitempty"` // "items" | "bytes"

	// Queue lifecycle: running | paused | stopped | waiting | completed.
	State string `json:"state,omitempty"`

	// Current state (for API)
	queue.QueueStats
	Round         int  `json:"round"`
	PossibleStall bool `json:"possible_stall"`
}

// InternalQueueMetrics contains control system metrics stored in memory for autoscaling decisions.
type InternalQueueMetrics struct {
	// State-based time tracking (additive counters)
	TimeProcessing          time.Duration
	TimeWaitingOnQueue      time.Duration
	TimeWaitingOnFS         time.Duration
	TimeRateLimited         time.Duration
	TimePausedRoundBoundary time.Duration
	TimeIdleNoWork          time.Duration

	// Capacity metrics
	TasksCompletedWhileActive int64
	ActiveProcessingTime      time.Duration

	// Utilization metrics
	WallClockTime       time.Duration
	LastState           queue.QueueState
	LastStateChangeTime time.Time
}

// QueueObserver collects live statistics from queue memory and periodically appends audit snapshots.
// Similar to QueueCoordinator, but focused on observability rather than coordination.
type QueueObserver struct {
	mu             sync.RWMutex
	database       *db.DB
	queues         map[string]*queue.Queue // Map of queue name -> queue reference
	stopChan       chan struct{}
	updateTicker   *time.Ticker
	updateInterval time.Duration
	running        bool // Whether the observe loop is running
	// Internal metrics (in-memory only, for autoscaling)
	internalMetrics  map[string]*InternalQueueMetrics // Per-queue internal metrics
	rateLimitSources map[string]RateLimitTelemetry
	// EMA rate tracking
	prevEMARates map[string]float64 // Previous EMA values for rate smoothing (key: queueName)
	// Discovery totals tracking for delta calculation
	prevDiscoveryTotals map[string]struct {
		files   int64
		folders int64
		time    time.Time
	} // Previous discovery totals and time for each queue
	// Copy/delete item rates: sliding window on completed folder+file counters.
	copyRateHistory map[string][]rateSample
	taskRateHistory map[string][]rateSample
	// Live byte snapshot + time for EMA bytes/sec (key: queueName).
	prevByteTotals map[string]struct {
		bytes int64
		time  time.Time
	}
	// etaState holds CV interval rates and sticky eta_basis per copy/delete queue.
	etaState map[string]*etaState
	// lastAPIMetrics: marshaled ExternalQueueMetrics per queue_stats key (e.g. src-traversal), for O(1) API reads.
	lastAPIMetricsMu sync.RWMutex
	lastAPIMetrics   map[string][]byte
	// lastDBPersist tracks when an audit snapshot was last successfully written to DuckDB.
	lastDBPersistMu sync.Mutex
	lastDBPersist   time.Time
	// Async queue_stats audit: observe loop never blocks on writeMu.
	auditCh   chan auditSnapshot // buffer 1; latest-wins when the writer is busy
	auditStop chan struct{}
	auditWG   sync.WaitGroup
	// waitReason is a human-readable soft-stop / DB wait label for status polling.
	waitReason atomic.Value // string
}

// auditSnapshot is a pre-marshaled queue_stats write batch (built on the observe tick).
type auditSnapshot struct {
	rows []auditRow
}

type auditRow struct {
	key   string
	phase string
	json  string
}

// rateSample is one monotonic counter observation for sliding-window rate math.
type rateSample struct {
	at    time.Time
	value int64
}

const (
	// emaAlpha is the smoothing factor for exponential moving average (0.2 ≈ several seconds).
	emaAlpha = 0.2
	// rateWindow is the lookback for copy/delete items/sec and task-completion rate.
	// Bytes/sec uses EMA on the live snapshot instead. Item completions stay windowed
	// so Dropbox/Graph batch finishes remain visible for the full window.
	rateWindow = 5 * time.Second
	// Audit snapshots are durable history, not part of live metrics. Keep writes
	// infrequent so observability cannot contend with migration I/O.
	dbPersistInterval = 30 * time.Second
)

// PhaseFamilyForMode maps internal queue modes to persisted queue_stats phase families.
func PhaseFamilyForMode(mode queue.QueueMode) string {
	switch mode {
	case queue.QueueModeCopy, queue.QueueModeCopyRetry:
		return db.QueueStatsPhaseCopy
	case queue.QueueModeDelete, queue.QueueModeDeleteRetry:
		return db.QueueStatsPhaseDelete
	default:
		return db.QueueStatsPhaseTraversal
	}
}

// NewQueueObserver creates an in-memory observer. updateInterval controls memory
// snapshot refresh (default: 200ms); durable audit writes have a separate interval.
func NewQueueObserver(database *db.DB, updateInterval time.Duration) *QueueObserver {
	if updateInterval <= 0 {
		updateInterval = 200 * time.Millisecond
	}

	return &QueueObserver{
		database:        database,
		queues:          make(map[string]*queue.Queue),
		stopChan:        make(chan struct{}),
		updateTicker:    time.NewTicker(updateInterval),
		updateInterval:  updateInterval,
		internalMetrics: make(map[string]*InternalQueueMetrics),
		prevEMARates:    make(map[string]float64),
		prevDiscoveryTotals: make(map[string]struct {
			files   int64
			folders int64
			time    time.Time
		}),
		copyRateHistory: make(map[string][]rateSample),
		taskRateHistory: make(map[string][]rateSample),
		prevByteTotals: make(map[string]struct {
			bytes int64
			time  time.Time
		}),
		etaState: make(map[string]*etaState),
		auditCh:  make(chan auditSnapshot, 1),
	}
}

// RegisterQueue registers a queue with the observer.
// The observer will poll this queue directly for statistics.
func (o *QueueObserver) RegisterQueue(queueName string, q *queue.Queue) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.queues[queueName] = q
	o.startLoopsLocked()
}

// PauseRegisteredQueues pauses every registered queue, clears pending buffers, and stops
// watchdogs so soft Stop refuses new leases immediately. Returns total in-progress tasks.
func (o *QueueObserver) PauseRegisteredQueues() int {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	queues := make([]*queue.Queue, 0, len(o.queues))
	for _, q := range o.queues {
		if q != nil {
			queues = append(queues, q)
		}
	}
	o.mu.Unlock()
	inProgress := 0
	for _, q := range queues {
		q.SetState(queue.QueueStatePaused)
		q.StopWatchdog()
		q.ClearPendingBufferForSuspend()
		inProgress += q.InProgressCount()
	}
	return inProgress
}

// InProgressTotal returns the sum of in-progress tasks across registered queues.
func (o *QueueObserver) InProgressTotal() int {
	if o == nil {
		return 0
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	n := 0
	for _, q := range o.queues {
		if q != nil {
			n += q.InProgressCount()
		}
	}
	return n
}

// SetWaitReason records a human-readable wait label for soft-stop UI (empty clears).
func (o *QueueObserver) SetWaitReason(reason string) {
	if o == nil {
		return
	}
	o.waitReason.Store(reason)
}

// WaitReason returns the current soft-stop / DB wait label.
func (o *QueueObserver) WaitReason() string {
	if o == nil {
		return ""
	}
	v := o.waitReason.Load()
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

// AbandonRegisteredQueues abandons in-flight tasks on every registered queue (force stop).
// Uses DB-only abandon (no requeue into pending) and cancels busy worker contexts so
// mid-flight ListChildren / FS work aborts instead of draining like a soft suspend.
func (o *QueueObserver) AbandonRegisteredQueues() {
	if o == nil {
		return
	}
	o.mu.Lock()
	queues := make([]*queue.Queue, 0, len(o.queues))
	for _, q := range o.queues {
		if q != nil {
			queues = append(queues, q)
		}
	}
	o.mu.Unlock()
	for _, q := range queues {
		q.SetState(queue.QueueStatePaused)
		q.StopWatchdog()
		q.Spin.AbandonDBOnly.Store(true)
		q.ClearPendingBufferForSuspend()
		q.RequestForceCheckoutAllWorkersForStop()
		q.CancelBusyWorkerContexts()
		q.AbandonInProgressTasks()
		q.ClearPendingBufferForSuspend()
	}
}

// UnregisterQueue removes a queue from the observer.
func (o *QueueObserver) UnregisterQueue(queueName string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.queues, queueName)
	delete(o.internalMetrics, queueName)
	delete(o.prevEMARates, queueName)
	delete(o.prevEMARates, queueName+"-tasks")
	delete(o.prevEMARates, queueName+"-items")
	delete(o.prevEMARates, queueName+"-bytes")
	delete(o.prevDiscoveryTotals, queueName)
	delete(o.prevByteTotals, queueName)
	delete(o.copyRateHistory, queueName)
	delete(o.taskRateHistory, queueName)
	delete(o.etaState, queueName)
}

// Start begins the observer loop (and async audit writer).
// This is called automatically when the first queue is registered, but can be called manually.
func (o *QueueObserver) Start() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.updateTicker == nil {
		o.updateTicker = time.NewTicker(o.updateInterval)
	}
	o.startLoopsLocked()
}

// startLoopsLocked starts observe + audit goroutines once. Caller must hold o.mu.
func (o *QueueObserver) startLoopsLocked() {
	if o.running {
		return
	}
	if o.updateTicker == nil {
		return
	}
	// After Stop, stopChan is closed; reopen so the new observe loop does not exit immediately.
	select {
	case <-o.stopChan:
		o.stopChan = make(chan struct{})
	default:
	}
	o.running = true
	o.auditStop = make(chan struct{})
	o.auditCh = make(chan auditSnapshot, 1)
	o.auditWG.Add(1)
	go o.auditLoop()
	go o.observeLoop()
}

// Stop stops the observer loop and cleans up resources.
// This should only be called once. Calling it multiple times is safe but has no effect.
func (o *QueueObserver) Stop() {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return
	}

	// Final live snapshot while queues are still registered.
	queues := make(map[string]*queue.Queue, len(o.queues))
	for name, queue := range o.queues {
		queues[name] = queue
	}
	auditStop := o.auditStop
	o.mu.Unlock()

	if len(queues) > 0 {
		metrics := make(map[string]ExternalQueueMetrics, len(queues))
		for queueName, queue := range queues {
			if metric := o.pollQueue(queueName, queue); metric != nil {
				metrics[queueName] = *metric
			}
		}
		if len(metrics) > 0 {
			o.storeLastAPIMetrics(metrics)
		}
	}

	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return
	}
	o.running = false

	select {
	case <-o.stopChan:
		o.stopChan = make(chan struct{})
		close(o.stopChan)
	default:
		close(o.stopChan)
	}
	if o.updateTicker != nil {
		o.updateTicker.Stop()
		o.updateTicker = nil
	}
	o.mu.Unlock()

	// Drain async audit writer before a synchronous final persist.
	if auditStop != nil {
		close(auditStop)
	}
	o.auditWG.Wait()

	if len(queues) > 0 && o.database != nil {
		metrics := make(map[string]ExternalQueueMetrics, len(queues))
		for queueName, queue := range queues {
			if metric := o.pollQueue(queueName, queue); metric != nil {
				metrics[queueName] = *metric
			}
		}
		if len(metrics) > 0 {
			o.publishAuditSnapshot(buildAuditSnapshot(metrics, queues))
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.queues = make(map[string]*queue.Queue)
	o.internalMetrics = make(map[string]*InternalQueueMetrics)
	o.prevEMARates = make(map[string]float64)
	o.prevDiscoveryTotals = make(map[string]struct {
		files   int64
		folders int64
		time    time.Time
	})
	o.copyRateHistory = make(map[string][]rateSample)
	o.taskRateHistory = make(map[string][]rateSample)
	o.prevByteTotals = make(map[string]struct {
		bytes int64
		time  time.Time
	})
	o.etaState = make(map[string]*etaState)
	o.auditStop = nil
	o.lastAPIMetricsMu.Lock()
	o.lastAPIMetrics = nil
	o.lastAPIMetricsMu.Unlock()
}

// observeLoop polls queue memory directly and occasionally appends an audit snapshot.
func (o *QueueObserver) observeLoop() {
	defer func() {
		o.mu.Lock()
		o.running = false
		o.mu.Unlock()
	}()

	for {
		// Get ticker and stop channel references (must check on each iteration)
		o.mu.RLock()
		ticker := o.updateTicker
		running := o.running
		stopChan := o.stopChan
		o.mu.RUnlock()

		// If we're not running or ticker is nil, exit
		if !running || ticker == nil {
			return
		}

		// Use select with nil channel trick: if ticker is nil, this case won't be selected
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Get queue references
			o.mu.RLock()
			queues := make(map[string]*queue.Queue)
			for name, queue := range o.queues {
				queues[name] = queue
			}
			o.mu.RUnlock()

			// Poll each queue and collect metrics
			metrics := make(map[string]ExternalQueueMetrics)
			for queueName, queue := range queues {
				metric := o.pollQueue(queueName, queue)
				if metric != nil {
					metrics[queueName] = *metric
				}
			}

			if len(metrics) > 0 {
				o.storeLastAPIMetrics(metrics)
				if o.database != nil && o.shouldPersistToDB() {
					o.enqueueAuditSnapshot(metrics, queues)
				}
			}
		}
	}
}

// auditLoop writes queued queue_stats snapshots without blocking the observe tick.
func (o *QueueObserver) auditLoop() {
	defer o.auditWG.Done()
	for {
		o.mu.RLock()
		stop := o.auditStop
		o.mu.RUnlock()
		if stop == nil {
			return
		}
		select {
		case <-stop:
			o.drainAuditChannel()
			return
		case snap := <-o.auditCh:
			o.publishAuditSnapshot(snap)
		}
	}
}

func (o *QueueObserver) enqueueAuditSnapshot(metrics map[string]ExternalQueueMetrics, queues map[string]*queue.Queue) {
	snap := buildAuditSnapshot(metrics, queues)
	if len(snap.rows) == 0 || o.auditCh == nil {
		return
	}
	select {
	case o.auditCh <- snap:
	default:
		// Writer busy: keep only the newest snapshot.
		select {
		case <-o.auditCh:
		default:
		}
		select {
		case o.auditCh <- snap:
		default:
		}
	}
}

func (o *QueueObserver) drainAuditChannel() {
	for {
		select {
		case snap := <-o.auditCh:
			o.publishAuditSnapshot(snap)
		default:
			return
		}
	}
}

func buildAuditSnapshot(metrics map[string]ExternalQueueMetrics, queues map[string]*queue.Queue) auditSnapshot {
	rows := make([]auditRow, 0, len(metrics))
	for queueName, met := range metrics {
		phase := db.QueueStatsPhaseTraversal
		if q := queues[queueName]; q != nil {
			phase = PhaseFamilyForMode(q.GetMode())
		}
		b, err := json.Marshal(met)
		if err != nil {
			continue
		}
		rows = append(rows, auditRow{
			key:   queueStatsKeyForAPI(queueName),
			phase: phase,
			json:  string(b),
		})
	}
	return auditSnapshot{rows: rows}
}

func (o *QueueObserver) publishAuditSnapshot(snap auditSnapshot) {
	if o.database == nil || len(snap.rows) == 0 {
		return
	}
	var err error
	for _, row := range snap.rows {
		if e := o.database.AppendQueueStats(row.key, row.phase, row.json); e != nil {
			err = fmt.Errorf("failed to append metrics for %s: %w", row.key, e)
			break
		}
	}
	o.lastDBPersistMu.Lock()
	o.lastDBPersist = time.Now()
	o.lastDBPersistMu.Unlock()
	if err != nil {
		if logservice.LS != nil {
			err := logservice.LS.Log("error",
				fmt.Sprintf("Failed to publish queue metrics: %v", err),
				"observer", "publish", "")
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
	}
}

func queueStatsKeyForAPI(queueName string) string {
	switch queueName {
	case "copy", "delete":
		return queueName
	default:
		return queueName + "-traversal"
	}
}

func (o *QueueObserver) storeLastAPIMetrics(metrics map[string]ExternalQueueMetrics) {
	cache := make(map[string][]byte, len(metrics))
	for queueName, met := range metrics {
		key := queueStatsKeyForAPI(queueName)
		b, err := json.Marshal(met)
		if err != nil {
			continue
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		cache[key] = cp
	}
	if len(cache) == 0 {
		return
	}
	o.lastAPIMetricsMu.Lock()
	o.lastAPIMetrics = cache
	o.lastAPIMetricsMu.Unlock()
}

// LastQueueMetricsForAPI returns a copy of the latest marshaled metrics per queue_stats key.
// Keys match DuckDB queue_stats.queue_key (e.g. "src-traversal"). False if no tick has run yet.
func (o *QueueObserver) LastQueueMetricsForAPI() (map[string][]byte, bool) {
	o.lastAPIMetricsMu.RLock()
	defer o.lastAPIMetricsMu.RUnlock()
	if len(o.lastAPIMetrics) == 0 {
		return nil, false
	}
	out := make(map[string][]byte, len(o.lastAPIMetrics))
	for k, v := range o.lastAPIMetrics {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out, true
}

// pollQueue polls a queue directly and calculates both external and internal metrics.
func (o *QueueObserver) pollQueue(queueName string, q *queue.Queue) *ExternalQueueMetrics {
	if q == nil {
		return nil
	}

	now := time.Now()

	// Get current queue state and stats
	stats := q.Stats()
	currentState := q.State()

	// Get discovery totals (traversal phase)
	filesTotal := q.GetFilesDiscoveredTotal()
	foldersTotal := q.GetFoldersDiscoveredTotal()
	totalDiscovered := q.GetTotalDiscovered()

	// Live byte snapshot (streamed chunks; not completed+inflight overlay).
	bytesTransferredTotal := q.GetLiveBytesTransferredTotal()
	foldersCreatedTotal := q.GetFoldersCreatedTotal()
	filesCreatedTotal := q.GetFilesCreatedTotal()
	foldersAlreadyExists := q.GetFoldersAlreadyExistsTotal()
	filesAlreadyExists := q.GetFilesAlreadyExistsTotal()
	bytesAlreadyExists := q.GetBytesAlreadyExistsTotal()
	foldersFailed := q.GetFoldersFailedTotal()
	filesFailed := q.GetFilesFailedTotal()

	// Live observability is memory-only. DB status aggregation here used to replay
	// event tables every 200ms and starved the two-connection migration database.
	totalPending, totalFailed := q.MemoryStatusTotals()

	// Update internal metrics (state tracking)
	o.updateInternalMetrics(queueName, q, currentState, now)
	o.updateRateLimitMetrics(queueName, now)

	// Calculate EMA-smoothed rates.
	// Discovery rate = newly discovered children (files+folders)/sec.
	// Task completion rate drives batch ETA (remaining work is list tasks, not children).
	discoveryRate := o.calculateDiscoveryRate(queueName, filesTotal, foldersTotal, now)
	taskCompletionRate := o.calculateTaskCompletionRate(queueName, q.GetTasksCompletedTotal(), now)

	// Items/sec: completed folder+file counts over a sliding window.
	// Bytes/sec: EMA of the live byte snapshot (chunk progress during long copies).
	itemsPerSecond := o.calculateCopyItemsRate(queueName, foldersCreatedTotal, filesCreatedTotal, now)
	bytesPerSecond := o.calculateBytesEMA(queueName, bytesTransferredTotal, now)
	if discoveryRate > itemsPerSecond {
		itemsPerSecond = discoveryRate
	}
	// Prefer task-completion rate when it is higher so list/copy/delete work still
	// shows items/sec while children-discovered or create counters are quiet
	// (leaf folders, fat-file copy, Dropbox batch finishes, permanent failures).
	if taskCompletionRate > itemsPerSecond {
		itemsPerSecond = taskCompletionRate
	}

	// Calculate total items (folders + files)
	totalItems := foldersCreatedTotal + filesCreatedTotal

	// Build external metrics
	metric := ExternalQueueMetrics{
		FilesDiscoveredTotal:     filesTotal,
		FoldersDiscoveredTotal:   foldersTotal,
		DiscoveryRateItemsPerSec: discoveryRate,
		TotalDiscovered:          totalDiscovered,
		TotalPending:             totalPending,
		TotalFailed:              totalFailed,
		Folders:                  foldersCreatedTotal,
		Files:                    filesCreatedTotal,
		Total:                    totalItems,
		Bytes:                    bytesTransferredTotal,
		FoldersAlreadyExists:     foldersAlreadyExists,
		FilesAlreadyExists:       filesAlreadyExists,
		BytesAlreadyExists:       bytesAlreadyExists,
		FoldersFailed:            foldersFailed,
		FilesFailed:              filesFailed,
		ItemsPerSecond:           itemsPerSecond,
		BytesPerSecond:           bytesPerSecond,
		QueueStats:               stats,
		Round:                    stats.Round,
		State:                    string(q.State()),
		PossibleStall:            q.PossibleStall(),
	}

	if queueName == "copy" || queueName == "delete" {
		enrichCopyDeleteProgressFromMemory(&metric, q, q.GetMode())
	}
	if roundStats := q.GetRoundStats(stats.Round); roundStats != nil {
		metric.RoundExpected = roundStats.Expected
		metric.RoundCompleted = roundStats.Completed
	}
	if queueName == "copy" || queueName == "delete" {
		metric.CopyPass = q.GetCopyPass()
	}

	srcUntil, dstUntil := q.RateLimitedUntilSides()
	metric.RateLimitedUntilSrc, metric.RateLimitedRemainingMsSrc = formatActiveRateLimit(srcUntil, now)
	metric.RateLimitedUntilDst, metric.RateLimitedRemainingMsDst = formatActiveRateLimit(dstUntil, now)
	if d := q.GetInterOpDelay(); d > 0 {
		metric.InterOpDelayMs = d.Milliseconds()
		if metric.InterOpDelayMs < 1 {
			metric.InterOpDelayMs = 1
		}
	}

	pullRows, pullMs, pullRPS := q.DBPullStats()
	metric.DBPullRows = pullRows
	metric.DBPullDurationMs = pullMs
	metric.DBPullRowsPerSec = pullRPS
	if o.database != nil {
		flush := o.database.LastSealFlushStats()
		metric.SealFlushRows = flush.Rows
		if flush.DurationNs > 0 {
			metric.SealFlushDurationMs = float64(flush.DurationNs) / 1e6
			sec := float64(flush.DurationNs) / 1e9
			if sec > 0 {
				metric.SealFlushRowsPerSec = float64(flush.Rows) / sec
			}
		}
	}

	if queueName == "copy" || queueName == "delete" {
		o.applyCopyDeleteETA(queueName, &metric, now)
	} else {
		applyTraversalBatchETA(&metric, taskCompletionRate)
	}

	return &metric
}

// formatActiveRateLimit returns RFC3339 until + remaining ms when the window is still open.
func formatActiveRateLimit(until time.Time, now time.Time) (untilStr string, remainingMs int64) {
	if until.IsZero() || !until.After(now) {
		return "", 0
	}
	return until.UTC().Format(time.RFC3339Nano), until.Sub(now).Milliseconds()
}

// enrichCopyDeleteProgressFromMemory fills progress from phase totals loaded once
// before workers start and live queue counters. It never touches DuckDB.
func enrichCopyDeleteProgressFromMemory(metric *ExternalQueueMetrics, q *queue.Queue, mode queue.QueueMode) {
	if metric == nil || q == nil {
		return
	}
	retryMode := mode == queue.QueueModeCopyRetry || mode == queue.QueueModeDeleteRetry
	totals := q.GetWorkTotals()
	_, failedMem := q.MemoryStatusTotals()
	failedItems := metric.FoldersFailed + metric.FilesFailed
	if failedItems == 0 && failedMem > 0 {
		// Older persisted metrics / mid-run before seal-coupled failed item counters.
		failedItems = int64(failedMem)
	}
	bytesFailed := q.GetBytesFailedTotal()
	alreadyItems := metric.FoldersAlreadyExists + metric.FilesAlreadyExists
	bytesAlready := metric.BytesAlreadyExists

	successful := metric.Folders + metric.Files
	itemsCompleted := successful + alreadyItems
	if !retryMode {
		itemsCompleted = successful + alreadyItems + failedItems
	}
	itemsTotal := totals.Items()
	itemsPct := sealedProgressPercent(itemsCompleted, itemsTotal)

	metric.ItemsCompleted = itemsCompleted
	metric.ItemsTotal = itemsTotal
	metric.ItemsProgressPercent = itemsPct
	metric.ProgressPercent = itemsPct
	metric.ItemsOkPercent = db.BytesProgressPercent(successful, itemsTotal)
	metric.ItemsAlreadyExistsPercent = db.BytesProgressPercent(alreadyItems, itemsTotal)
	if !retryMode {
		metric.ItemsFailedPercent = db.BytesProgressPercent(failedItems, itemsTotal)
	}

	metric.FoldersExpected = totals.Folders
	metric.FilesExpected = totals.Files
	metric.TotalExpected = itemsTotal

	metric.BytesTotal = totals.Bytes
	metric.BytesFailed = bytesFailed
	metric.BytesOkPercent = db.BytesProgressPercent(metric.Bytes, totals.Bytes)
	metric.BytesAlreadyExistsPercent = db.BytesProgressPercent(bytesAlready, totals.Bytes)
	bytesDone := metric.Bytes + bytesAlready
	if !retryMode {
		bytesDone = metric.Bytes + bytesAlready + bytesFailed
		metric.BytesFailedPercent = db.BytesProgressPercent(bytesFailed, totals.Bytes)
	}
	metric.BytesProgressPercent = db.BytesProgressPercent(bytesDone, totals.Bytes)
}

// applyCopyDeleteETA fills eta_seconds / eta_basis using dual-metric CV selection.
// While workers are saturated, observed throughput refreshes a cruise rate. Near round
// end (underfeed), cruise is frozen and only the current-round drain uses the depressed
// observed rate; the rest of the phase is priced at cruise.
func (o *QueueObserver) applyCopyDeleteETA(queueName string, metric *ExternalQueueMetrics, now time.Time) {
	if o == nil || metric == nil {
		return
	}
	itemsRemaining := metric.ItemsTotal - metric.ItemsCompleted
	if itemsRemaining < 0 {
		itemsRemaining = 0
	}
	bytesRemaining := metric.BytesTotal - metric.Bytes - metric.BytesAlreadyExists - metric.BytesFailed
	if bytesRemaining < 0 {
		bytesRemaining = 0
	}

	itemsCV, bytesCV := o.recordETAIntervalRates(queueName, metric.ItemsCompleted, metric.Bytes, now)

	saturated := etaSaturated(metric.Workers, metric.InProgress)
	o.mu.Lock()
	st := o.etaState[queueName]
	if st == nil {
		st = &etaState{}
		o.etaState[queueName] = st
	}
	if saturated {
		if metric.ItemsPerSecond > 0 {
			st.cruiseItemsRate = metric.ItemsPerSecond
		}
		if metric.BytesPerSecond > 0 {
			st.cruiseBytesRate = metric.BytesPerSecond
		}
	}
	prevBasis := st.basis
	prevSince := st.basisSince
	cruiseItems := st.cruiseItemsRate
	cruiseBytes := st.cruiseBytesRate
	o.mu.Unlock()

	out := chooseEtaBasis(etaChooseInput{
		ItemsRemaining: itemsRemaining,
		BytesRemaining: bytesRemaining,
		ItemsRate:      metric.ItemsPerSecond,
		BytesRate:      metric.BytesPerSecond,
		ItemsCV:        itemsCV,
		BytesCV:        bytesCV,
		FolderOnly:     folderOnlyPass(queueName, metric.CopyPass),
		PrevBasis:      prevBasis,
		PrevSince:      prevSince,
		Now:            now,
	})
	if !out.OK {
		return
	}

	hasRoundStats := metric.RoundExpected > 0
	roundItemsRemaining := int64(0)
	if hasRoundStats {
		roundItemsRemaining = int64(metric.RoundExpected - metric.RoundCompleted)
		if roundItemsRemaining < 0 {
			roundItemsRemaining = 0
		}
	}
	underfed := !saturated && (cruiseItems > 0 || cruiseBytes > 0)
	seconds := etaSecondsWithCruise(
		out.Basis,
		itemsRemaining,
		bytesRemaining,
		roundItemsRemaining,
		hasRoundStats,
		metric.ItemsPerSecond,
		metric.BytesPerSecond,
		cruiseItems,
		cruiseBytes,
		underfed,
		out.Seconds,
	)

	metric.EtaBasis = out.Basis
	metric.EtaSeconds = float64Ptr(seconds)

	o.mu.Lock()
	st = o.etaState[queueName]
	if st == nil {
		st = &etaState{}
		o.etaState[queueName] = st
	}
	if st.basis != out.Basis {
		st.basis = out.Basis
		st.basisSince = now
	} else if st.basisSince.IsZero() {
		st.basisSince = now
	}
	o.mu.Unlock()
}

// recordETAIntervalRates appends per-tick interval rates for CV over etaCVWindow.
func (o *QueueObserver) recordETAIntervalRates(queueName string, itemsCompleted, bytesDone int64, now time.Time) (itemsCV, bytesCV float64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	st := o.etaState[queueName]
	if st == nil {
		st = &etaState{}
		o.etaState[queueName] = st
	}
	if !st.lastAt.IsZero() {
		dt := now.Sub(st.lastAt).Seconds()
		if dt > 0 {
			itemRate := float64(itemsCompleted-st.lastItems) / dt
			if itemRate < 0 {
				itemRate = 0
			}
			byteRate := float64(bytesDone-st.lastBytes) / dt
			if byteRate < 0 {
				byteRate = 0
			}
			st.itemRates = appendRatePoint(st.itemRates, now, itemRate, etaCVWindow)
			st.byteRates = appendRatePoint(st.byteRates, now, byteRate, etaCVWindow)
		}
	}
	st.lastItems = itemsCompleted
	st.lastBytes = bytesDone
	st.lastAt = now
	return cvFromPoints(st.itemRates), cvFromPoints(st.byteRates)
}

// sealedProgressPercent is completed/grand_total for sealed denominators (retry and normal).
func sealedProgressPercent(completed, total int64) float64 {
	if completed < 0 {
		completed = 0
	}
	if total <= 0 {
		return 0
	}
	pct := 100.0 * float64(completed) / float64(total)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// AnyPossibleStall reports whether any registered queue recently triggered the watchdog.
func (o *QueueObserver) AnyPossibleStall() bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, queue := range o.queues {
		if queue != nil && queue.PossibleStall() {
			return true
		}
	}
	return false
}

// calculateDiscoveryRate calculates EMA-smoothed discovery rate (items/sec).
func (o *QueueObserver) calculateDiscoveryRate(queueName string, filesTotal, foldersTotal int64, now time.Time) float64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	prev, hasPrev := o.prevDiscoveryTotals[queueName]

	if !hasPrev {
		// First poll - initialize tracking
		o.prevDiscoveryTotals[queueName] = struct {
			files   int64
			folders int64
			time    time.Time
		}{
			files:   filesTotal,
			folders: foldersTotal,
			time:    now,
		}
		o.prevEMARates[queueName] = 0.0
		return 0.0
	}

	timeDelta := now.Sub(prev.time).Seconds()
	if timeDelta <= 0 {
		return o.prevEMARates[queueName]
	}

	itemsDelta := (filesTotal - prev.files) + (foldersTotal - prev.folders)
	// Hold EMA only when a seal flush produced no new discoveries. Live discovery
	// (and copy bytes below) must keep updating even while flush is active, or
	// the progress page sticks at 0 items/sec / 0 B/s through the whole run.
	if itemsDelta == 0 && o.database != nil && o.database.SealIOWaitActive() {
		return o.prevEMARates[queueName]
	}

	currentRate := float64(itemsDelta) / timeDelta

	// Update EMA: newEMA = alpha * currentRate + (1-alpha) * previousEMA
	prevEMA := o.prevEMARates[queueName]
	newEMA := emaAlpha*currentRate + (1-emaAlpha)*prevEMA

	// Store for next calculation
	o.prevEMARates[queueName] = newEMA
	o.prevDiscoveryTotals[queueName] = struct {
		files   int64
		folders int64
		time    time.Time
	}{
		files:   filesTotal,
		folders: foldersTotal,
		time:    now,
	}

	return newEMA
}

// calculateTaskCompletionRate returns completions/sec over rateWindow (matches Comp growth).
func (o *QueueObserver) calculateTaskCompletionRate(queueName string, tasksTotal int64, now time.Time) float64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	rate, hist := appendSlidingRate(o.taskRateHistory[queueName], now, tasksTotal)
	o.taskRateHistory[queueName] = hist
	return rate
}

// calculateCopyItemsRate returns completed items/sec (folders+files) over rateWindow.
// Bursty Dropbox batch completions stay visible for the full window instead of
// EMA-decaying to 0 within ~2s.
func (o *QueueObserver) calculateCopyItemsRate(queueName string, foldersTotal, filesTotal int64, now time.Time) float64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	rate, hist := appendSlidingRate(o.copyRateHistory[queueName], now, foldersTotal+filesTotal)
	o.copyRateHistory[queueName] = hist
	return rate
}

// calculateBytesEMA returns rolling EMA bytes/sec from the live byte snapshot.
func (o *QueueObserver) calculateBytesEMA(queueName string, bytesTotal int64, now time.Time) float64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	emaKey := queueName + "-bytes"
	prev, hasPrev := o.prevByteTotals[queueName]
	if !hasPrev {
		o.prevByteTotals[queueName] = struct {
			bytes int64
			time  time.Time
		}{
			bytes: bytesTotal,
			time:  now,
		}
		o.prevEMARates[emaKey] = 0
		return 0
	}

	timeDelta := now.Sub(prev.time).Seconds()
	if timeDelta <= 0 {
		return o.prevEMARates[emaKey]
	}

	delta := bytesTotal - prev.bytes
	if delta < 0 {
		delta = 0
	}
	if delta == 0 && o.database != nil && o.database.SealIOWaitActive() {
		return o.prevEMARates[emaKey]
	}

	newEMA := emaAlpha*(float64(delta)/timeDelta) + (1-emaAlpha)*o.prevEMARates[emaKey]
	o.prevEMARates[emaKey] = newEMA
	o.prevByteTotals[queueName] = struct {
		bytes int64
		time  time.Time
	}{
		bytes: bytesTotal,
		time:  now,
	}
	return newEMA
}

// appendSlidingRate records value at now and returns (value-oldest)/dt over rateWindow.
func appendSlidingRate(hist []rateSample, now time.Time, value int64) (float64, []rateSample) {
	hist = append(hist, rateSample{at: now, value: value})
	cutoff := now.Add(-rateWindow)
	// Keep one sample at/before cutoff as the baseline, drop older.
	firstKeep := 0
	for i := 0; i < len(hist)-1; i++ {
		if hist[i].at.Before(cutoff) {
			firstKeep = i
			continue
		}
		break
	}
	if firstKeep > 0 {
		hist = hist[firstKeep:]
	}
	// Cap history length to avoid unbounded growth if clock stalls.
	const maxSamples = 256
	if len(hist) > maxSamples {
		hist = hist[len(hist)-maxSamples:]
	}
	return slidingWindowRate(hist, now, value), hist
}

func slidingWindowRate(hist []rateSample, now time.Time, value int64) float64 {
	if len(hist) == 0 {
		return 0
	}
	oldest := hist[0]
	dt := now.Sub(oldest.at).Seconds()
	if dt <= 0 {
		return 0
	}
	delta := value - oldest.value
	if delta < 0 {
		delta = 0
	}
	return float64(delta) / dt
}

// updateInternalMetrics updates internal metrics by attributing time deltas to state buckets.
func (o *QueueObserver) updateInternalMetrics(queueName string, q *queue.Queue, currentState queue.QueueState, now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Get or create internal metrics for this queue
	internal, exists := o.internalMetrics[queueName]
	if !exists {
		internal = &InternalQueueMetrics{
			LastState:           currentState,
			LastStateChangeTime: now,
		}
		o.internalMetrics[queueName] = internal
		return // First poll, just initialize
	}

	if o.database != nil && o.database.SealIOWaitActive() {
		internal.LastStateChangeTime = now
		return
	}

	// Calculate time delta since last poll
	delta := now.Sub(internal.LastStateChangeTime)
	if delta <= 0 {
		return
	}

	// Update wall clock time
	internal.WallClockTime += delta

	// Attribute delta to appropriate state bucket
	stats := q.Stats()
	inProgressCount := stats.InProgress
	pendingCount := stats.Pending

	switch currentState {
	case queue.QueueStateRunning:
		if inProgressCount > 0 {
			// Actively processing tasks
			internal.TimeProcessing += delta
			internal.ActiveProcessingTime += delta
		} else if pendingCount > 0 {
			// Waiting for tasks to be leased
			internal.TimeWaitingOnQueue += delta
		} else {
			// No work available
			internal.TimeIdleNoWork += delta
		}
	case queue.QueueStateWaiting:
		// Waiting for coordinator (DST gating)
		internal.TimePausedRoundBoundary += delta
	case queue.QueueStatePaused:
		// Manually paused
		internal.TimePausedRoundBoundary += delta
	case queue.QueueStateStopped, queue.QueueStateCompleted:
		// Queue is stopped or completed - don't attribute time
		// (these states are terminal)
	}

	// Update state tracking
	if internal.LastState != currentState {
		// State changed - reset last state change time
		internal.LastState = currentState
		internal.LastStateChangeTime = now
	} else {
		// Same state - update last state change time for next delta calculation
		internal.LastStateChangeTime = now
	}
}

func (o *QueueObserver) updateRateLimitMetrics(queueName string, now time.Time) {
	o.mu.Lock()
	src, ok := o.rateLimitSources[queueName]
	internal := o.internalMetrics[queueName]
	o.mu.Unlock()
	if !ok || src == nil || internal == nil {
		return
	}
	// Do NOT TakeRecentHits here — that is reserved for SnapshotInternalMetrics
	// (autoscaler tick). Draining on every observer poll hid FS_THROTTLE from AIMD.
	until := src.RateLimitedUntil()
	if until.After(now) {
		o.mu.Lock()
		if m := o.internalMetrics[queueName]; m != nil {
			delta := until.Sub(now)
			if delta > o.updateInterval {
				delta = o.updateInterval
			}
			m.TimeRateLimited += delta
		}
		o.mu.Unlock()
	}
}

func (o *QueueObserver) shouldPersistToDB() bool {
	o.lastDBPersistMu.Lock()
	defer o.lastDBPersistMu.Unlock()
	if o.lastDBPersist.IsZero() {
		return true
	}
	return time.Since(o.lastDBPersist) >= dbPersistInterval
}

// RehydrateCountersFromMetricsJSON seeds monotonic discovery/copy counters from persisted metrics JSON.
func RehydrateCountersFromMetricsJSON(q *queue.Queue, metricsJSON []byte) error {
	if q == nil || len(metricsJSON) == 0 {
		return nil
	}
	var metrics ExternalQueueMetrics
	if err := json.Unmarshal(metricsJSON, &metrics); err != nil {
		return err
	}
	switch q.Name() {
	case "src", "dst":
		q.SeedDiscoveryCounters(metrics.FilesDiscoveredTotal, metrics.FoldersDiscoveredTotal)
	case "copy", "delete":
		q.SeedCopyCounters(metrics.Folders, metrics.Files, metrics.Bytes, metrics.BytesFailed)
		q.SeedAlreadyExistsCounters(metrics.FoldersAlreadyExists, metrics.FilesAlreadyExists, metrics.BytesAlreadyExists)
		q.SeedFailedItemCounters(metrics.FoldersFailed, metrics.FilesFailed)
	}
	return nil
}

// RateLimitTelemetry is queue.RateLimitTelemetry (FS throttle signals for observer polling).
type RateLimitTelemetry = queue.RateLimitTelemetry

// InternalMetricsSnapshot is a copy of internal queue metrics for autoscaler decisions.
type InternalMetricsSnapshot struct {
	TimeProcessing             time.Duration
	TimeWaitingOnQueue         time.Duration
	TimeWaitingOnFS            time.Duration
	TimeRateLimited            time.Duration
	TimePausedRoundBoundary    time.Duration
	TimeIdleNoWork             time.Duration
	TasksCompletedWhileActive  int64
	RateLimitHitsSinceLastPoll int64
	RateLimitedUntil           time.Time // shared FS adapter retry-after window (if any)
}

// RegisterRateLimitTelemetry attaches FS degradation telemetry for a queue name.
func (o *QueueObserver) RegisterRateLimitTelemetry(queueName string, src RateLimitTelemetry) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.rateLimitSources == nil {
		o.rateLimitSources = make(map[string]RateLimitTelemetry)
	}
	o.rateLimitSources[queueName] = src
}

// SnapshotThroughputRate returns the best available items/sec EMA for a queue (traversal discovery or copy).
func (o *QueueObserver) SnapshotThroughputRate(queueName string) float64 {
	if o == nil {
		return 0
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	// Prefer task completions for traversal (matches Comp / FS list ops); children-discovered
	// EMA goes to 0 on leaf folders even while work continues.
	if rate, ok := o.prevEMARates[queueName+"-tasks"]; ok && rate > 0 {
		return rate
	}
	if rate, ok := o.prevEMARates[queueName]; ok && rate > 0 {
		return rate
	}
	if rate, ok := o.prevEMARates[queueName+"-items"]; ok {
		return rate
	}
	return 0
}

// SnapshotEMARate returns EMA rate for queueName+suffix (e.g. "-bytes", "-tasks").
func (o *QueueObserver) SnapshotEMARate(queueName, suffix string) float64 {
	if o == nil {
		return 0
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.prevEMARates[queueName+suffix]
}

// SnapshotInternalMetrics returns per-queue internal metrics (does not reset time buckets).
func (o *QueueObserver) SnapshotInternalMetrics() map[string]InternalMetricsSnapshot {
	if o == nil {
		return nil
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make(map[string]InternalMetricsSnapshot, len(o.internalMetrics))
	for name, m := range o.internalMetrics {
		if m == nil {
			continue
		}
		snap := InternalMetricsSnapshot{
			TimeProcessing:            m.TimeProcessing,
			TimeWaitingOnQueue:        m.TimeWaitingOnQueue,
			TimeWaitingOnFS:           m.TimeWaitingOnFS,
			TimeRateLimited:           m.TimeRateLimited,
			TimePausedRoundBoundary:   m.TimePausedRoundBoundary,
			TimeIdleNoWork:            m.TimeIdleNoWork,
			TasksCompletedWhileActive: m.TasksCompletedWhileActive,
		}
		if src, ok := o.rateLimitSources[name]; ok && src != nil {
			snap.RateLimitHitsSinceLastPoll = src.TakeRecentHits()
			until := src.RateLimitedUntil()
			snap.RateLimitedUntil = until
			if until.After(time.Now()) {
				snap.TimeRateLimited += time.Until(until)
			}
		}
		out[name] = snap
	}
	return out
}
