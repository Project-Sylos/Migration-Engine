// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Project-Sylos/Sylos-DB/pkg/store"
)

// QueueStatsProvider is an interface that queue implementations must implement
// to provide statistics to the observer.
type QueueStatsProvider interface {
	// Stats returns the current queue statistics
	Stats() QueueStats
	// GetAverageExecutionTime returns the average task execution time
	GetAverageExecutionTime() time.Duration
	// GetTotalCompleted returns the total number of completed tasks across all rounds
	GetTotalCompleted() int
}

// QueueObserverMetrics contains computed metrics for a queue.
type QueueObserverMetrics struct {
	QueueStats
	AverageExecutionTime time.Duration // Average task execution time
	TasksPerSecond       float64       // Tasks completed per second (calculated from last poll)
	TotalCompleted       int           // Total completed tasks across all rounds
	LastPollTime         time.Time     // When this metric was last calculated
}

// QueueObserver collects statistics from queues by polling them directly and publishes them to BoltDB periodically.
// This allows external APIs to poll BoltDB for real-time queue statistics without disrupting queue operations.
type QueueObserver struct {
	mu             sync.RWMutex
	store          *store.Store
	queues         map[string]QueueStatsProvider // Map of queue name -> queue reference
	stopChan       chan struct{}
	updateTicker   *time.Ticker
	updateInterval time.Duration
	running        bool // Whether the observe loop is running
	// Track previous state for rate calculations
	prevMetrics map[string]QueueObserverMetrics // Previous metrics for each queue
}

// NewQueueObserver creates a new observer that will publish stats to the Store.
// updateInterval is how often stats are written to the Store (default: 200ms).
func NewQueueObserver(storeInstance *store.Store, updateInterval time.Duration) *QueueObserver {
	if updateInterval <= 0 {
		updateInterval = 200 * time.Millisecond
	}

	return &QueueObserver{
		store:          storeInstance,
		queues:         make(map[string]QueueStatsProvider),
		stopChan:       make(chan struct{}),
		updateTicker:   time.NewTicker(updateInterval),
		updateInterval: updateInterval,
		prevMetrics:    make(map[string]QueueObserverMetrics),
	}
}

// RegisterQueue registers a queue with the observer.
// The observer will poll this queue directly for statistics.
func (o *QueueObserver) RegisterQueue(queueName string, queue QueueStatsProvider) {
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
	delete(o.prevMetrics, queueName)
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
	o.queues = make(map[string]QueueStatsProvider)
	o.prevMetrics = make(map[string]QueueObserverMetrics)
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
			queues := make(map[string]QueueStatsProvider)
			for name, queue := range o.queues {
				queues[name] = queue
			}
			o.mu.RUnlock()

			// Poll each queue and collect metrics
			metrics := make(map[string]QueueObserverMetrics)
			for queueName, queue := range queues {
				metric := o.pollQueue(queueName, queue)
				if metric != nil {
					metrics[queueName] = *metric
				}
			}

			// Publish all collected metrics to BoltDB
			if len(metrics) > 0 && o.store != nil {
				o.publishMetricsToBoltDB(metrics)
			}
		}
	}
}

// pollQueue polls a queue directly and calculates metrics.
func (o *QueueObserver) pollQueue(queueName string, queue QueueStatsProvider) *QueueObserverMetrics {
	if queue == nil {
		return nil
	}

	// Get current queue stats
	stats := queue.Stats()
	avgExecutionTime := queue.GetAverageExecutionTime()

	// Calculate total completed tasks across all rounds
	totalCompleted := queue.GetTotalCompleted()

	// Get previous metrics for rate calculation
	o.mu.RLock()
	prevMetric, hasPrev := o.prevMetrics[queueName]
	o.mu.RUnlock()

	now := time.Now()
	var tasksPerSecond float64

	if hasPrev {
		// Calculate tasks/sec based on total completed count change
		timeDelta := now.Sub(prevMetric.LastPollTime).Seconds()
		if timeDelta > 0 {
			completedDelta := totalCompleted - prevMetric.TotalCompleted
			if completedDelta > 0 {
				tasksPerSecond = float64(completedDelta) / timeDelta
			}
		}
	}

	metric := QueueObserverMetrics{
		QueueStats:           stats,
		AverageExecutionTime: avgExecutionTime,
		TasksPerSecond:       tasksPerSecond,
		TotalCompleted:       totalCompleted,
		LastPollTime:         now,
	}

	// Store for next calculation
	o.mu.Lock()
	o.prevMetrics[queueName] = metric
	o.mu.Unlock()

	return &metric
}

// publishMetricsToBoltDB writes queue metrics to BoltDB in the queue-stats bucket.
// Each queue's metrics are stored under a key like "src-traversal", "dst-traversal", etc.
func (o *QueueObserver) publishMetricsToBoltDB(metricsMap map[string]QueueObserverMetrics) {
	if o.store == nil {
		return
	}

	// Convert metrics to JSON map for batch write
	statsMap := make(map[string][]byte)
	for queueName, metrics := range metricsMap {
		// Determine the key based on queue name
		key := queueNameToStatsKey(queueName)

		// Marshal metrics to JSON
		metricsJSON, err := json.Marshal(metrics)
		if err != nil {
			// Log error but continue with other queues
			continue
		}

		statsMap[key] = metricsJSON
	}

	// Write all metrics in a single transaction using Store API
	if err := o.store.SetQueueStatsBatch(statsMap); err != nil {
		// Error logging should be handled by caller or logging service
		_ = err
	}
}

// queueNameToStatsKey converts a queue name to a stats key.
// "src" -> "src-traversal", "dst" -> "dst-traversal", etc.
func queueNameToStatsKey(queueName string) string {
	switch queueName {
	case "src":
		return "src-traversal"
	case "dst":
		return "dst-traversal"
	default:
		// For other queue types, use as-is or add suffix
		return queueName
	}
}
