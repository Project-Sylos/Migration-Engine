// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Package queue provides the QueueObserver for collecting and publishing queue statistics.
//
// The QueueObserver polls queues directly at regular intervals (default: 200ms) and publishes
// metrics to BoltDB. This allows external APIs to poll BoltDB for real-time queue statistics
// without disrupting queue operations.
//
// Usage:
//   observer := queue.NewQueueObserver(boltDB, 200*time.Millisecond)
//   observer.RegisterQueue("src", srcQueue)
//   observer.RegisterQueue("dst", dstQueue)
//   observer.Start()
//   // Stats are published to /STATS/queue-stats bucket with keys like "src-traversal", "dst-traversal"
//
// Stats can be retrieved from BoltDB using:
//   statsJSON, err := boltDB.GetQueueStats("src-traversal")
//   allStats, err := boltDB.GetAllQueueStats()

package queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	bolt "go.etcd.io/bbolt"
)

// ExternalQueueMetrics contains user-facing metrics published to BoltDB for API access.
type ExternalQueueMetrics struct {
	// Monotonic counters (traversal phase)
	FilesDiscoveredTotal   int64 `json:"files_discovered_total"`
	FoldersDiscoveredTotal int64 `json:"folders_discovered_total"`

	// EMA-smoothed rates (2-5 second window) - traversal phase
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

	// Current state (for API)
	QueueStats
	Round int `json:"round"`
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

// QueueObserver collects statistics from queues by polling them directly and publishes them to BoltDB periodically.
// Similar to QueueCoordinator, but focused on observability rather than coordination.
type QueueObserver struct {
	mu             sync.RWMutex
	boltDB         *db.DB
	queues         map[string]*Queue // Map of queue name -> queue reference
	stopChan       chan struct{}
	updateTicker   *time.Ticker
	updateInterval time.Duration
	running        bool // Whether the observe loop is running
	// Internal metrics (in-memory only, for autoscaling)
	internalMetrics map[string]*InternalQueueMetrics // Per-queue internal metrics
	// EMA rate tracking
	prevEMARates map[string]float64 // Previous EMA values for rate smoothing (key: queueName)
	// Discovery totals tracking for delta calculation
	prevDiscoveryTotals map[string]struct {
		files   int64
		folders int64
		time    time.Time
	} // Previous discovery totals and time for each queue
	// Copy metrics tracking for delta calculation
	prevCopyTotals map[string]struct {
		bytes   int64
		folders int64
		files   int64
		time    time.Time
	} // Previous copy totals and time for each queue
}

const (
	// emaAlpha is the smoothing factor for exponential moving average (0.2 = ~5 second window)
	emaAlpha = 0.2
)

// NewQueueObserver creates a new observer that will publish stats to BoltDB.
// updateInterval is how often stats are written to BoltDB (default: 200ms).
func NewQueueObserver(boltInstance *db.DB, updateInterval time.Duration) *QueueObserver {
	if updateInterval <= 0 {
		updateInterval = 200 * time.Millisecond
	}

	return &QueueObserver{
		boltDB:          boltInstance,
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
		prevCopyTotals: make(map[string]struct {
			bytes   int64
			folders int64
			files   int64
			time    time.Time
		}),
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
	delete(o.prevDiscoveryTotals, queueName)
	delete(o.prevCopyTotals, queueName)
}

// Start begins the observer loop that publishes stats to BoltDB.
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
	defer o.mu.Unlock()

	if !o.running {
		return // Already stopped
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
	o.prevCopyTotals = make(map[string]struct {
		bytes   int64
		folders int64
		files   int64
		time    time.Time
	})
}

// observeLoop is the main loop that polls queues directly and publishes metrics to BoltDB.
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

			// Publish all collected metrics to BoltDB
			if len(metrics) > 0 && o.boltDB != nil {
				o.publishMetricsToBoltDB(metrics)
			}
		}
	}
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

	// Get copy phase totals
	bytesTransferredTotal := queue.GetBytesTransferredTotal()
	foldersCreatedTotal := queue.GetFoldersCreatedTotal()
	filesCreatedTotal := queue.GetFilesCreatedTotal()

	// Get total pending and failed counts from stats bucket (O(1) lookup)
	// The stats bucket is maintained by the output buffer during flush operations
	totalPending := o.getTotalPendingCount(queueName)
	totalFailed := o.getTotalFailedCount(queueName)

	// Update internal metrics (state tracking)
	o.updateInternalMetrics(queueName, queue, currentState, now)

	// Calculate EMA-smoothed discovery rate (traversal phase)
	discoveryRate := o.calculateDiscoveryRate(queueName, filesTotal, foldersTotal, now)

	// Calculate copy phase rates (EMA-smoothed)
	itemsPerSecond := o.calculateItemsPerSecond(queueName, foldersCreatedTotal, filesCreatedTotal, now)
	bytesPerSecond := o.calculateBytesPerSecond(queueName, bytesTransferredTotal, now)

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
	}

	return &metric
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

// calculateBytesPerSecond calculates EMA-smoothed bytes/sec rate for copy phase.
func (o *QueueObserver) calculateBytesPerSecond(queueName string, bytesTotal int64, now time.Time) float64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	prev, hasPrev := o.prevCopyTotals[queueName]

	if !hasPrev {
		// First poll - initialize tracking
		o.prevCopyTotals[queueName] = struct {
			bytes   int64
			folders int64
			files   int64
			time    time.Time
		}{
			bytes:   bytesTotal,
			folders: 0,
			files:   0,
			time:    now,
		}
		return 0.0
	}

	// Calculate current instantaneous rate
	timeDelta := now.Sub(prev.time).Seconds()
	if timeDelta <= 0 {
		return 0.0
	}

	bytesDelta := bytesTotal - prev.bytes
	currentRate := float64(bytesDelta) / timeDelta

	// Update EMA: newEMA = alpha * currentRate + (1-alpha) * previousEMA
	prevEMA := o.prevEMARates[queueName+"-bytes"]
	newEMA := emaAlpha*currentRate + (1-emaAlpha)*prevEMA

	// Store for next calculation
	o.prevEMARates[queueName+"-bytes"] = newEMA
	o.prevCopyTotals[queueName] = struct {
		bytes   int64
		folders int64
		files   int64
		time    time.Time
	}{
		bytes:   bytesTotal,
		folders: prev.folders,
		files:   prev.files,
		time:    now,
	}

	return newEMA
}

// calculateItemsPerSecond calculates EMA-smoothed items/sec rate for copy phase (folders + files).
func (o *QueueObserver) calculateItemsPerSecond(queueName string, foldersTotal, filesTotal int64, now time.Time) float64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	prev, hasPrev := o.prevCopyTotals[queueName]

	if !hasPrev {
		// First poll - initialize tracking
		o.prevCopyTotals[queueName] = struct {
			bytes   int64
			folders int64
			files   int64
			time    time.Time
		}{
			bytes:   0,
			folders: foldersTotal,
			files:   filesTotal,
			time:    now,
		}
		o.prevEMARates[queueName+"-items"] = 0.0
		return 0.0
	}

	// Calculate current instantaneous rate
	timeDelta := now.Sub(prev.time).Seconds()
	if timeDelta <= 0 {
		return o.prevEMARates[queueName+"-items"]
	}

	// Calculate total items delta (folders + files)
	itemsDelta := (foldersTotal - prev.folders) + (filesTotal - prev.files)
	currentRate := float64(itemsDelta) / timeDelta

	// Update EMA: newEMA = alpha * currentRate + (1-alpha) * previousEMA
	prevEMA := o.prevEMARates[queueName+"-items"]
	newEMA := emaAlpha*currentRate + (1-emaAlpha)*prevEMA

	// Store for next calculation
	o.prevEMARates[queueName+"-items"] = newEMA
	o.prevCopyTotals[queueName] = struct {
		bytes   int64
		folders int64
		files   int64
		time    time.Time
	}{
		bytes:   prev.bytes,
		folders: foldersTotal,
		files:   filesTotal,
		time:    now,
	}

	return newEMA
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

	// Track tasks completed while active
	if currentState == QueueStateRunning && inProgressCount > 0 {
		// We're actively processing - track completed tasks
		// Note: This is tracked per-poll, actual completion tracking happens in completeTask
		// For now, we'll track this separately if needed
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

// getTotalPendingCount reads total pending count from stats bucket (O(1) lookup).
// The stats bucket is maintained by the output buffer during flush operations.
func (o *QueueObserver) getTotalPendingCount(queueName string) int {
	if o.boltDB == nil {
		return 0
	}

	// Handle copy phase separately
	if queueName == "copy" {
		// For copy phase, sum pending counts across all levels and both node types
		levels, err := o.boltDB.GetAllLevels("SRC")
		if err != nil {
			return 0
		}

		totalPending := 0
		for _, level := range levels {
			// Sum pending for folders
			bucketPath := db.GetCopyStatusBucketPath(level, db.NodeTypeFolder, db.CopyStatusPending)
			count, err := o.boltDB.GetBucketCount(bucketPath)
			if err == nil {
				totalPending += int(count)
			}

			// Sum pending for files
			bucketPath = db.GetCopyStatusBucketPath(level, db.NodeTypeFile, db.CopyStatusPending)
			count, err = o.boltDB.GetBucketCount(bucketPath)
			if err == nil {
				totalPending += int(count)
			}
		}

		return totalPending
	}

	// Traversal phase: use traversal status buckets
	queueType := getQueueType(queueName)
	if queueType == "" {
		return 0
	}

	// Get all levels for this queue type
	levels, err := o.boltDB.GetAllLevels(queueType)
	if err != nil {
		return 0
	}

	// Sum pending counts across all levels using stats bucket (O(1) per level)
	totalPending := 0
	for _, level := range levels {
		// Use GetBucketCount which reads from stats bucket for O(1) lookup
		bucketPath := db.GetTraversalStatusBucketPath(queueType, level, db.StatusPending)
		count, err := o.boltDB.GetBucketCount(bucketPath)
		if err == nil {
			totalPending += int(count)
		}
	}

	return totalPending
}

// getTotalFailedCount reads total failed count from stats bucket (O(1) lookup).
// The stats bucket is maintained by the output buffer during flush operations.
func (o *QueueObserver) getTotalFailedCount(queueName string) int {
	if o.boltDB == nil {
		return 0
	}

	// Handle copy phase separately
	if queueName == "copy" {
		// For copy phase, sum failed counts across all levels and both node types
		levels, err := o.boltDB.GetAllLevels("SRC")
		if err != nil {
			return 0
		}

		totalFailed := 0
		for _, level := range levels {
			// Sum failed for folders
			bucketPath := db.GetCopyStatusBucketPath(level, db.NodeTypeFolder, db.CopyStatusFailed)
			count, err := o.boltDB.GetBucketCount(bucketPath)
			if err == nil {
				totalFailed += int(count)
			}

			// Sum failed for files
			bucketPath = db.GetCopyStatusBucketPath(level, db.NodeTypeFile, db.CopyStatusFailed)
			count, err = o.boltDB.GetBucketCount(bucketPath)
			if err == nil {
				totalFailed += int(count)
			}
		}

		return totalFailed
	}

	// Traversal phase: use traversal status buckets
	queueType := getQueueType(queueName)
	if queueType == "" {
		return 0
	}

	// Get all levels for this queue type
	levels, err := o.boltDB.GetAllLevels(queueType)
	if err != nil {
		return 0
	}

	// Sum failed counts across all levels using stats bucket (O(1) per level)
	totalFailed := 0
	for _, level := range levels {
		// Use GetBucketCount which reads from stats bucket for O(1) lookup
		bucketPath := db.GetTraversalStatusBucketPath(queueType, level, db.StatusFailed)
		count, err := o.boltDB.GetBucketCount(bucketPath)
		if err == nil {
			totalFailed += int(count)
		}
	}

	return totalFailed
}

// publishMetricsToBoltDB writes external queue metrics to BoltDB in the queue-stats bucket.
// Only external metrics are published - internal metrics remain in memory for autoscaling decisions.
// Each queue's metrics are stored under a key like "src-traversal", "dst-traversal", etc.
func (o *QueueObserver) publishMetricsToBoltDB(metricsMap map[string]ExternalQueueMetrics) {
	if o.boltDB == nil {
		return
	}

	err := o.boltDB.Update(func(tx *bolt.Tx) error {
		// Get or create the queue-stats bucket
		statsBucket, err := getQueueStatsBucket(tx)
		if err != nil {
			return err
		}

		// Write metrics for each queue
		for queueName, metrics := range metricsMap {
			// Determine the key based on queue name
			// "src" -> "src-traversal", "dst" -> "dst-traversal", "copy" -> "copy", etc.
			key := queueName
			if queueName != "copy" {
				key = queueName + "-traversal"
			}

			// Marshal metrics to JSON
			metricsJSON, err := json.Marshal(metrics)
			if err != nil {
				if logservice.LS != nil {
					_ = logservice.LS.Log("error",
						fmt.Sprintf("Failed to marshal metrics for queue %s: %v", queueName, err),
						"observer", "publish", "")
				}
				continue
			}

			// Write to BoltDB
			if err := statsBucket.Put([]byte(key), metricsJSON); err != nil {
				return fmt.Errorf("failed to write metrics for %s: %w", key, err)
			}
		}

		return nil
	})

	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to publish metrics to BoltDB: %v", err),
				"observer", "publish", "")
		}
	}
}

// getQueueStatsBucket returns the queue-stats bucket, creating it if needed.
// This is a helper that uses the db package's bucket helpers.
func getQueueStatsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	return db.GetOrCreateQueueStatsBucket(tx)
}
