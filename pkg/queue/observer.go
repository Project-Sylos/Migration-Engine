// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Package queue provides the QueueObserver for collecting and publishing queue statistics.
//
// The QueueObserver polls queues directly at regular intervals (default: 200ms) and publishes
// metrics to DuckDB. This allows external APIs to poll DuckDB for real-time queue statistics
// without disrupting queue operations.
//
// Usage:
//   observer := queue.NewQueueObserver(database, 200*time.Millisecond)
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

package queue

import (
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
	Bytes   int64 `json:"bytes"`   // Total bytes transferred

	// Copy phase rates (EMA-smoothed)
	ItemsPerSecond float64 `json:"items_per_second"` // Items/sec (folders + files)
	BytesPerSecond float64 `json:"bytes_per_second"` // Bytes/sec

	// Deterministic copy/delete progress (0–100). Omitted for traversal.
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

	// Current state (for API)
	QueueStats
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
	LastState           QueueState
	LastStateChangeTime time.Time
}

// QueueObserver collects statistics from queues by polling them directly and publishes them to DuckDB periodically.
// Similar to QueueCoordinator, but focused on observability rather than coordination.
type QueueObserver struct {
	mu             sync.RWMutex
	database       *db.DB
	queues         map[string]*Queue // Map of queue name -> queue reference
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
	// lastAPIMetrics: marshaled ExternalQueueMetrics per queue_stats key (e.g. src-traversal), for O(1) API reads.
	lastAPIMetricsMu sync.RWMutex
	lastAPIMetrics   map[string][]byte
	// lastDBPersist tracks when metrics were last appended to DuckDB.
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
	// dbPersistInterval is how often metrics are appended to DuckDB (in-memory cache updates every tick).
	dbPersistInterval = 1 * time.Second
)

// PhaseFamilyForMode maps internal queue modes to persisted queue_stats phase families.
func PhaseFamilyForMode(mode QueueMode) string {
	switch mode {
	case QueueModeCopy, QueueModeCopyRetry:
		return db.QueueStatsPhaseCopy
	case QueueModeDelete, QueueModeDeleteRetry:
		return db.QueueStatsPhaseDelete
	default:
		return db.QueueStatsPhaseTraversal
	}
}

// NewQueueObserver creates a new observer that will publish stats to DuckDB.
// updateInterval is how often stats are written to DuckDB (default: 200ms).
func NewQueueObserver(database *db.DB, updateInterval time.Duration) *QueueObserver {
	if updateInterval <= 0 {
		updateInterval = 200 * time.Millisecond
	}

	return &QueueObserver{
		database:        database,
		queues:          make(map[string]*Queue),
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
	}
}

// RegisterQueue registers a queue with the observer.
// The observer will poll this queue directly for statistics.
func (o *QueueObserver) RegisterQueue(queueName string, queue *Queue) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.queues[queueName] = queue

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
	queues := make(map[string]*Queue, len(o.queues))
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
	o.queues = make(map[string]*Queue)
	o.internalMetrics = make(map[string]*InternalQueueMetrics)
	o.prevEMARates = make(map[string]float64)
	o.prevDiscoveryTotals = make(map[string]struct {
		files   int64
		folders int64
		time    time.Time
	})
	o.copyRateHistory = make(map[string][]rateSample)
	o.taskRateHistory = make(map[string][]rateSample)
	o.lastAPIMetricsMu.Lock()
	o.lastAPIMetrics = nil
	o.lastAPIMetricsMu.Unlock()
}

// observeLoop is the main loop that polls queues directly and publishes metrics to DuckDB.
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
			queues := make(map[string]*Queue)
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
func (o *QueueObserver) pollQueue(queueName string, queue *Queue) *ExternalQueueMetrics {
	if queue == nil {
		return nil
	}

	now := time.Now()

	// Get current queue state and stats
	stats := queue.Stats()
	currentState := queue.State()

	// Get discovery totals (traversal phase)
	filesTotal := queue.GetFilesDiscoveredTotal()
	foldersTotal := queue.GetFoldersDiscoveredTotal()
	totalDiscovered := queue.GetTotalDiscovered()

	// Get copy phase totals (live bytes include in-flight leased progress).
	bytesTransferredTotal := queue.GetLiveBytesTransferredTotal()
	foldersCreatedTotal := queue.GetFoldersCreatedTotal()
	filesCreatedTotal := queue.GetFilesCreatedTotal()

	// Get total pending and failed counts from stats bucket (O(1) lookup)
	var totalPending, totalFailed int
	if queueName == "delete" {
		totalPending, totalFailed = o.getDeleteStatusTotals()
	} else {
		totalPending = o.getTotalStatusCount(queueName, db.StatusPending, db.CopyStatusPending)
		totalFailed = o.getTotalStatusCount(queueName, db.StatusFailed, db.CopyStatusFailed)
	}

	// Update internal metrics (state tracking)
	o.updateInternalMetrics(queueName, queue, currentState, now)
	o.updateRateLimitMetrics(queueName, now)

	// Calculate EMA-smoothed rates.
	// Discovery rate for the UI tracks list-task completions (matches Comp growth), not
	// newly discovered children — leaf/empty folders complete work without adding children.
	_ = o.calculateDiscoveryRate(queueName, filesTotal, foldersTotal, now)
	taskCompletionRate := o.calculateTaskCompletionRate(queueName, queue.GetTasksCompletedTotal(), now)

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
		PossibleStall:            queue.PossibleStall(),
	}

	if queueName == "copy" || queueName == "delete" {
		metric.ProgressPercent = o.computePhaseProgressPercent(queueName, queue.GetMode())
	}
	if roundStats := queue.GetRoundStats(stats.Round); roundStats != nil {
		metric.RoundExpected = roundStats.Expected
		metric.RoundCompleted = roundStats.Completed
	}
	if queueName == "copy" {
		metric.CopyPass = queue.GetCopyPass()
	}

	srcUntil, dstUntil := queue.RateLimitedUntilSides()
	metric.RateLimitedUntilSrc, metric.RateLimitedRemainingMsSrc = formatActiveRateLimit(srcUntil, now)
	metric.RateLimitedUntilDst, metric.RateLimitedRemainingMsDst = formatActiveRateLimit(dstUntil, now)
	if d := queue.GetInterOpDelay(); d > 0 {
		metric.InterOpDelayMs = d.Milliseconds()
		if metric.InterOpDelayMs < 1 {
			metric.InterOpDelayMs = 1
		}
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

// computePhaseProgressPercent returns durable copy/delete progress from review-stat / eligible counts.
// Copy-retry and delete-retry treat already-successful work as the baseline and failed/pending as remaining.
func (o *QueueObserver) computePhaseProgressPercent(queueName string, mode QueueMode) float64 {
	if o.database == nil {
		return 0
	}
	var counts db.PhaseProgressCounts
	var err error
	switch queueName {
	case "copy":
		counts, err = o.database.GetCopyProgressCounts()
	case "delete":
		counts, err = o.database.GetDeleteProgressCounts()
	default:
		return 0
	}
	if err != nil {
		return 0
	}
	retryMode := mode == QueueModeCopyRetry || mode == QueueModeDeleteRetry
	if retryMode {
		return counts.ProgressPercentRetry()
	}
	return counts.ProgressPercent()
}

// AnyPossibleStall reports whether any registered queue recently triggered the watchdog.
func (o *QueueObserver) AnyPossibleStall() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
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
func (o *QueueObserver) updateInternalMetrics(queueName string, queue *Queue, currentState QueueState, now time.Time) {
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
	stats := queue.Stats()
	inProgressCount := stats.InProgress
	pendingCount := stats.Pending

	switch currentState {
	case QueueStateRunning:
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
	case QueueStateWaiting:
		// Waiting for coordinator (DST gating)
		internal.TimePausedRoundBoundary += delta
	case QueueStatePaused:
		// Manually paused
		internal.TimePausedRoundBoundary += delta
	case QueueStateStopped, QueueStateCompleted:
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

// getDeleteStatusTotals returns pending and failed delete counts across all depths.
func (o *QueueObserver) getDeleteStatusTotals() (pending, failed int) {
	if o.database == nil {
		return 0, 0
	}
	levels, err := db.GetAllLevels(o.database, "SRC")
	if err != nil {
		return 0, 0
	}
	for _, level := range levels {
		for _, nt := range []string{db.NodeTypeFolder, db.NodeTypeFile} {
			if c, err := o.database.GetDeleteCountAtDepth(level, nt, db.DeleteStatusPending, false); err == nil {
				pending += int(c)
			}
			if c, err := o.database.GetDeleteCountAtDepth(level, nt, db.DeleteStatusFailed, false); err == nil {
				failed += int(c)
			}
		}
	}
	return pending, failed
}

// getTotalStatusCount reads total traversal or copy status count from stats buckets.
func (o *QueueObserver) getTotalStatusCount(queueName, traversalStatus, copyStatus string) int {
	if o.database == nil {
		return 0
	}

	if queueName == "copy" {
		levels, err := db.GetAllLevels(o.database, "SRC")
		if err != nil {
			return 0
		}

		total := 0
		for _, level := range levels {
			count, err := o.database.GetCopyCountAtDepth(level, db.NodeTypeFolder, copyStatus, false)
			if err == nil {
				total += int(count)
			}
			count, err = o.database.GetCopyCountAtDepth(level, db.NodeTypeFile, copyStatus, false)
			if err == nil {
				total += int(count)
			}
		}

		return total
	}

	queueType := getQueueType(queueName)
	if queueType == "" {
		return 0
	}

	levels, err := db.GetAllLevels(o.database, queueType)
	if err != nil {
		return 0
	}

	total := 0
	for _, level := range levels {
		count, err := o.database.GetStatsCountAtDepth(queueType, level, db.StatsKey(db.StatsKindTraversal, traversalStatus))
		if err == nil {
			total += int(count)
		}
	}

	return total
}

// publishMetricsToDuckDB appends external queue metrics to DuckDB and prunes older rows.
func (o *QueueObserver) publishMetricsToDuckDB(metricsMap map[string]ExternalQueueMetrics, queues map[string]*Queue) {
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
			if err := w.PruneQueueStats(); err != nil {
				return fmt.Errorf("failed to prune queue_stats: %w", err)
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
