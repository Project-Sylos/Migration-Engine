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

// QueueObserverMetrics contains computed metrics for a queue.
type QueueObserverMetrics struct {
	QueueStats
	AverageExecutionTime time.Duration // Average task execution time
	TasksPerSecond       float64       // Tasks completed per second (calculated from last poll)
	TotalCompleted       int           // Total completed tasks across all rounds
	LastPollTime         time.Time     // When this metric was last calculated
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
	// Track previous state for rate calculations
	prevMetrics map[string]QueueObserverMetrics // Previous metrics for each queue
}

// NewQueueObserver creates a new observer that will publish stats to BoltDB.
// updateInterval is how often stats are written to BoltDB (default: 200ms).
func NewQueueObserver(boltInstance *db.DB, updateInterval time.Duration) *QueueObserver {
	if updateInterval <= 0 {
		updateInterval = 200 * time.Millisecond
	}

	return &QueueObserver{
		boltDB:         boltInstance,
		queues:         make(map[string]*Queue),
		stopChan:       make(chan struct{}),
		updateTicker:   time.NewTicker(updateInterval),
		updateInterval: updateInterval,
		prevMetrics:    make(map[string]QueueObserverMetrics),
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

	if o.updateTicker != nil {
		o.updateTicker.Stop()
		o.updateTicker = nil
	}

	o.running = false
	close(o.stopChan)

	// Clear queues
	o.queues = make(map[string]*Queue)
	o.prevMetrics = make(map[string]QueueObserverMetrics)
}

// observeLoop is the main loop that polls queues directly and publishes metrics to BoltDB.
func (o *QueueObserver) observeLoop() {
	for {
		select {
		case <-o.stopChan:
			o.mu.Lock()
			o.running = false
			o.mu.Unlock()
			return
		case <-o.updateTicker.C:
			// Get queue references
			o.mu.RLock()
			queues := make(map[string]*Queue)
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
			if len(metrics) > 0 && o.boltDB != nil {
				o.publishMetricsToBoltDB(metrics)
			}
		}
	}
}

// pollQueue polls a queue directly and calculates metrics.
func (o *QueueObserver) pollQueue(queueName string, queue *Queue) *QueueObserverMetrics {
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
			// "src" -> "src-traversal", "dst" -> "dst-traversal", etc.
			key := queueNameToStatsKey(queueName)

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

// getQueueStatsBucket returns the queue-stats bucket, creating it if needed.
// This is a helper that uses the db package's bucket helpers.
func getQueueStatsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	return db.GetOrCreateQueueStatsBucket(tx)
}
