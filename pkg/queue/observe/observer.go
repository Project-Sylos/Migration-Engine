// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Package observe provides QueueObserver and stall watchdogs for pkg/queue.
//
// The QueueObserver polls queues directly at regular intervals (default: 200ms) and publishes
// metrics to DuckDB. This allows external APIs to poll DuckDB for real-time queue statistics
// without disrupting queue operations.
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
// For low-latency APIs while a run is active, use LastQueueMetricsForAPI(): same JSON as written to
// queue_stats, updated every observer tick without requiring a DB read.

package observe

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// ExternalQueueMetrics contains user-facing metrics published to DuckDB for API access.
type ExternalQueueMetrics struct {
	// Monotonic counters (traversal phase)
	FilesDiscoveredTotal   int64 `json:"files_discovered_total"`
	FoldersDiscoveredTotal int64 `json:"folders_discovered_total"`

	// EMA-smoothed rates (2-5 second window) - traversal phase.
	// Published as list-task completions/sec (not newly discovered children).
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

	// Migration-wide expected denominators for Files/Folders/Total (copy/delete).
	FoldersExpected int64 `json:"folders_expected,omitempty"`
	FilesExpected   int64 `json:"files_expected,omitempty"`
	TotalExpected   int64 `json:"total_expected,omitempty"`

	// Items progress (copy/delete): completed/eligible, retry-aware.
	ItemsCompleted       int64   `json:"items_completed,omitempty"`
	ItemsTotal           int64   `json:"items_total,omitempty"`
	ItemsProgressPercent float64 `json:"items_progress_percent,omitempty"`

	// Bytes progress (copy/delete): done vs fixed migration-wide eligible file size total.
	// Bytes is transferred (and in-flight); BytesFailed is permanent-failure file sizes.
	// BytesProgressPercent uses touched bytes (Bytes+BytesFailed) in normal mode, Bytes in retry mode.
	BytesTotal           int64   `json:"bytes_total,omitempty"`
	BytesFailed          int64   `json:"bytes_failed,omitempty"`
	BytesProgressPercent float64 `json:"bytes_progress_percent,omitempty"`
	// Failed share of the bytes bar (0–100 of BytesTotal); UI paints this red at the start.
	BytesFailedPercent float64 `json:"bytes_failed_percent,omitempty"`
	// Failed share of the items bar (0–100 of ItemsTotal); UI paints this red at the start.
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
	// Copy metrics tracking for sliding-window rates (Dropbox/Graph batch completions are bursty).
	copyRateHistory map[string][]rateSample
	taskRateHistory map[string][]rateSample
	// etaState holds CV interval rates and sticky eta_basis per copy/delete queue.
	etaState map[string]*etaState
	// lastAPIMetrics: marshaled ExternalQueueMetrics per queue_stats key (e.g. src-traversal), for O(1) API reads.
	lastAPIMetricsMu sync.RWMutex
	lastAPIMetrics   map[string][]byte
	// lastDBPersist tracks when audit metrics were last appended to DuckDB.
	lastDBPersistMu sync.Mutex
	lastDBPersist   time.Time
}

// rateSample is one monotonic counter observation for sliding-window rate math.
type rateSample struct {
	at    time.Time
	value int64
}

const (
	// emaAlpha is the smoothing factor for exponential moving average (0.2 ≈ several seconds).
	emaAlpha = 0.2
	// rateWindow is the lookback used for copy/delete throughput. EMA-per-tick decays to ~0
	// within ~2s after a Dropbox batch completes; a wall-clock window keeps burst completions
	// visible for the full window.
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
		etaState:        make(map[string]*etaState),
	}
}

// RegisterQueue registers a queue with the observer.
// The observer will poll this queue directly for statistics.
func (o *QueueObserver) RegisterQueue(queueName string, q *queue.Queue) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.queues[queueName] = q

	// Start observer loop if not already running
	if !o.running && o.updateTicker != nil {
		o.running = true
		go o.observeLoop()
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
	delete(o.copyRateHistory, queueName)
	delete(o.copyRateHistory, queueName+"-bytes")
	delete(o.taskRateHistory, queueName)
	delete(o.etaState, queueName)
}

// Start begins the observer loop that publishes stats to DuckDB.
// This is called automatically when the first queue is registered, but can be called manually.
func (o *QueueObserver) Start() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.updateTicker == nil {
		o.updateTicker = time.NewTicker(o.updateInterval)
	}
	if !o.running {
		o.running = true
		go o.observeLoop()
	}
}

// Stop stops the observer loop and cleans up resources.
// This should only be called once. Calling it multiple times is safe but has no effect.
func (o *QueueObserver) Stop() {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return
	}

	// Flush final metrics before tearing down queue references.
	queues := make(map[string]*queue.Queue, len(o.queues))
	for name, queue := range o.queues {
		queues[name] = queue
	}
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
			if o.database != nil {
				o.publishMetricsToDuckDB(metrics, queues)
			}
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.running {
		return // Already stopped by concurrent Stop
	}

	o.running = false

	// Signal the observe loop to exit first (so it can check running flag)
	// Only close if not already closed
	select {
	case <-o.stopChan:
		// Already closed, create a new one for potential future use
		o.stopChan = make(chan struct{})
		close(o.stopChan)
	default:
		close(o.stopChan)
	}

	// Stop ticker after signaling (this prevents new ticks from firing)
	// The observe loop will exit on next iteration due to running=false check
	if o.updateTicker != nil {
		o.updateTicker.Stop()
		o.updateTicker = nil
	}

	// Clear queues
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
	o.etaState = make(map[string]*etaState)
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
					o.publishMetricsToDuckDB(metrics, queues)
				}
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

	// Get copy phase totals (live bytes include in-flight leased progress).
	bytesTransferredTotal := q.GetLiveBytesTransferredTotal()
	foldersCreatedTotal := q.GetFoldersCreatedTotal()
	filesCreatedTotal := q.GetFilesCreatedTotal()

	// Live observability is memory-only. DB status aggregation here used to replay
	// event tables every 200ms and starved the two-connection migration database.
	totalPending, totalFailed := q.MemoryStatusTotals()

	// Update internal metrics (state tracking)
	o.updateInternalMetrics(queueName, q, currentState, now)
	o.updateRateLimitMetrics(queueName, now)

	// Calculate EMA-smoothed rates.
	// Discovery rate for the UI tracks list-task completions (matches Comp growth), not
	// newly discovered children — leaf/empty folders complete work without adding children.
	_ = o.calculateDiscoveryRate(queueName, filesTotal, foldersTotal, now)
	taskCompletionRate := o.calculateTaskCompletionRate(queueName, q.GetTasksCompletedTotal(), now)

	// Copy/delete phase rates over a sliding window (shared create+bytes snapshot).
	itemsPerSecond, bytesPerSecond := o.calculateCopyPhaseRates(
		queueName, foldersCreatedTotal, filesCreatedTotal, bytesTransferredTotal, now,
	)
	// Prefer task-completion rate for items/sec on copy/delete so Dropbox batch finishes
	// and permanent failures still show activity matching the Comp ticker. Create-counter
	// rate alone stays 0 between rare batch commits and undercounts failed work.
	if queueName == "copy" || queueName == "delete" {
		if taskCompletionRate > itemsPerSecond {
			itemsPerSecond = taskCompletionRate
		}
	}

	discoveryRate := taskCompletionRate

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

	if queueName == "copy" || queueName == "delete" {
		o.applyCopyDeleteETA(queueName, &metric, now)
	} else {
		applyTraversalBatchETA(&metric)
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
	_, failedInt := q.MemoryStatusTotals()
	failed := int64(failedInt)
	bytesFailed := q.GetBytesFailedTotal()

	successful := metric.Folders + metric.Files
	itemsCompleted := successful
	if !retryMode {
		itemsCompleted = successful + failed
	}
	itemsTotal := totals.Items()
	itemsPct := sealedProgressPercent(itemsCompleted, itemsTotal)

	metric.ItemsCompleted = itemsCompleted
	metric.ItemsTotal = itemsTotal
	metric.ItemsProgressPercent = itemsPct
	metric.ProgressPercent = itemsPct
	if !retryMode {
		metric.ItemsFailedPercent = db.BytesProgressPercent(failed, itemsTotal)
	}

	metric.FoldersExpected = totals.Folders
	metric.FilesExpected = totals.Files
	metric.TotalExpected = itemsTotal

	metric.BytesTotal = totals.Bytes
	metric.BytesFailed = bytesFailed
	bytesDone := metric.Bytes
	if !retryMode {
		bytesDone = metric.Bytes + bytesFailed
		metric.BytesFailedPercent = db.BytesProgressPercent(bytesFailed, totals.Bytes)
	}
	metric.BytesProgressPercent = db.BytesProgressPercent(bytesDone, totals.Bytes)
}

// applyCopyDeleteETA fills eta_seconds / eta_basis using dual-metric CV selection.
func (o *QueueObserver) applyCopyDeleteETA(queueName string, metric *ExternalQueueMetrics, now time.Time) {
	if o == nil || metric == nil {
		return
	}
	itemsRemaining := metric.ItemsTotal - metric.ItemsCompleted
	if itemsRemaining < 0 {
		itemsRemaining = 0
	}
	bytesRemaining := metric.BytesTotal - metric.Bytes - metric.BytesFailed
	if bytesRemaining < 0 {
		bytesRemaining = 0
	}

	itemsCV, bytesCV := o.recordETAIntervalRates(queueName, metric.ItemsCompleted, metric.Bytes, now)

	o.mu.Lock()
	st := o.etaState[queueName]
	var prevBasis string
	var prevSince time.Time
	if st != nil {
		prevBasis = st.basis
		prevSince = st.basisSince
	}
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
	metric.EtaBasis = out.Basis
	metric.EtaSeconds = float64Ptr(out.Seconds)

	o.mu.Lock()
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

	if o.database != nil && o.database.SealIOWaitActive() {
		return o.prevEMARates[queueName]
	}

	// Calculate current instantaneous rate
	timeDelta := now.Sub(prev.time).Seconds()
	if timeDelta <= 0 {
		return o.prevEMARates[queueName]
	}

	itemsDelta := (filesTotal - prev.files) + (foldersTotal - prev.folders)
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

	if o.database != nil && o.database.SealIOWaitActive() {
		return slidingWindowRate(o.taskRateHistory[queueName], now, tasksTotal)
	}
	rate, hist := appendSlidingRate(o.taskRateHistory[queueName], now, tasksTotal)
	o.taskRateHistory[queueName] = hist
	return rate
}

// calculateCopyPhaseRates returns items/sec and bytes/sec over a shared rateWindow.
// Items use successful create counters (folders+files). Bursty Dropbox batch completions
// stay visible for the full window instead of EMA-decaying to 0 within ~2s.
func (o *QueueObserver) calculateCopyPhaseRates(
	queueName string,
	foldersTotal, filesTotal, bytesTotal int64,
	now time.Time,
) (itemsPerSec, bytesPerSec float64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	itemsTotal := foldersTotal + filesTotal
	if o.database != nil && o.database.SealIOWaitActive() {
		return slidingWindowRate(o.copyRateHistory[queueName], now, itemsTotal),
			slidingWindowRate(o.copyRateHistory[queueName+"-bytes"], now, bytesTotal)
	}
	itemsPerSec, itemsHist := appendSlidingRate(o.copyRateHistory[queueName], now, itemsTotal)
	bytesPerSec, bytesHist := appendSlidingRate(o.copyRateHistory[queueName+"-bytes"], now, bytesTotal)
	o.copyRateHistory[queueName] = itemsHist
	o.copyRateHistory[queueName+"-bytes"] = bytesHist
	return itemsPerSec, bytesPerSec
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

// publishMetricsToDuckDB appends external queue metrics as audit history.
func (o *QueueObserver) publishMetricsToDuckDB(metricsMap map[string]ExternalQueueMetrics, queues map[string]*queue.Queue) {
	if o.database == nil {
		return
	}

	err := o.database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			for queueName, metrics := range metricsMap {
				key := queueStatsKeyForAPI(queueName)
				phase := db.QueueStatsPhaseTraversal
				if q := queues[queueName]; q != nil {
					phase = PhaseFamilyForMode(q.GetMode())
				}
				metricsJSON, err := json.Marshal(metrics)
				if err != nil {
					if logservice.LS != nil {
						err := logservice.LS.Log("error",
							fmt.Sprintf("Failed to marshal metrics for queue %s: %v", queueName, err),
							"observer", "publish", "")
						if err != nil {
							fmt.Println("error logging", err)
						}
					}
					continue
				}
				if err := w.AppendQueueStats(key, phase, string(metricsJSON)); err != nil {
					return fmt.Errorf("failed to append metrics for %s: %w", key, err)
				}
			}
			return nil
		})
	})

	o.lastDBPersistMu.Lock()
	o.lastDBPersist = time.Now()
	o.lastDBPersistMu.Unlock()

	if err != nil {
		if logservice.LS != nil {
			err := logservice.LS.Log("error",
				fmt.Sprintf("Failed to publish metrics to DuckDB: %v", err),
				"observer", "publish", "")
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
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
	}
	return nil
}

// RateLimitTelemetry is queue.RateLimitTelemetry (FS throttle signals for observer polling).
type RateLimitTelemetry = queue.RateLimitTelemetry

// InternalMetricsSnapshot is a copy of internal queue metrics for autoscaler decisions.
type InternalMetricsSnapshot struct {
	TimeProcessing          time.Duration
	TimeWaitingOnQueue      time.Duration
	TimeWaitingOnFS         time.Duration
	TimeRateLimited         time.Duration
	TimePausedRoundBoundary time.Duration
	TimeIdleNoWork          time.Duration
	TasksCompletedWhileActive int64
	RateLimitHitsSinceLastPoll int64
	RateLimitedUntil        time.Time // shared FS adapter retry-after window (if any)
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
			TimeProcessing:          m.TimeProcessing,
			TimeWaitingOnQueue:      m.TimeWaitingOnQueue,
			TimeWaitingOnFS:         m.TimeWaitingOnFS,
			TimeRateLimited:         m.TimeRateLimited,
			TimePausedRoundBoundary: m.TimePausedRoundBoundary,
			TimeIdleNoWork:          m.TimeIdleNoWork,
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
