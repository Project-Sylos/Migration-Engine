// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
)

// CoordinatorState represents the current state of the coordinator.
type CoordinatorState string

const (
	StateStopped CoordinatorState = "stopped"
	StateRunning CoordinatorState = "running"
	StatePaused  CoordinatorState = "paused"
)

// Coordinator manages the lifecycle of src and dst queues, coordinates round advancement,
// generates tasks from the database, and ensures proper synchronization between the two queues.
type Coordinator struct {
	srcQueue  *Queue
	dstQueue  *Queue
	db        *db.DB
	srcCtx    fsservices.ServiceContext
	dstCtx    fsservices.ServiceContext
	state     CoordinatorState
	mu        sync.RWMutex
	stopChan  chan struct{}
	pauseChan chan struct{}
	wg        sync.WaitGroup
}

// NewCoordinator creates a new coordinator instance.
func NewCoordinator(
	db *db.DB,
	srcContext fsservices.ServiceContext,
	dstContext fsservices.ServiceContext,
	maxRetries int,
	batchSize int,
) *Coordinator {
	srcQueue := NewQueue("src", maxRetries, batchSize)
	dstQueue := NewQueue("dst", maxRetries, batchSize)

	// Initialize queues with DB and context references
	srcQueue.Initialize(db, "src_nodes", nil)
	dstQueue.Initialize(db, "dst_nodes", &srcContext)

	return &Coordinator{
		srcQueue:  srcQueue,
		dstQueue:  dstQueue,
		db:        db,
		srcCtx:    srcContext,
		dstCtx:    dstContext,
		state:     StateStopped,
		stopChan:  make(chan struct{}),
		pauseChan: make(chan struct{}),
	}
}

// Start begins the coordinator monitoring loop.
func (c *Coordinator) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateRunning {
		return fmt.Errorf("coordinator already running")
	}

	c.state = StateRunning
	c.stopChan = make(chan struct{})
	c.pauseChan = make(chan struct{})

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator starting", "coordinator", "main")
	}

	// Start the round monitoring loop
	c.wg.Add(1)
	go c.roundMonitorLoop()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator started", "coordinator", "main")
	}

	return nil
}

// Stop gracefully stops the coordinator.
// Workers will stop automatically when queues are exhausted.
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	if c.state == StateStopped {
		c.mu.Unlock()
		return fmt.Errorf("coordinator already stopped")
	}
	c.state = StateStopped
	c.mu.Unlock()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator stopping", "coordinator", "main")
	}

	// Signal coordination loop to stop
	close(c.stopChan)
	c.wg.Wait()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator stopped", "coordinator", "main")
	}

	return nil
}

// Pause temporarily halts task generation but keeps workers running.
func (c *Coordinator) Pause() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != StateRunning {
		return fmt.Errorf("coordinator not running")
	}

	c.state = StatePaused
	close(c.pauseChan)

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator paused", "coordinator", "main")
	}

	return nil
}

// Resume continues task generation after a pause.
func (c *Coordinator) Resume() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != StatePaused {
		return fmt.Errorf("coordinator not paused")
	}

	c.state = StateRunning
	c.pauseChan = make(chan struct{})

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator resumed", "coordinator", "main")
	}

	return nil
}

// State returns the current coordinator state.
func (c *Coordinator) State() CoordinatorState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// roundMonitorLoop waits for both queues to complete their traversals.
// Queues manage their own round advancement, so coordinator just waits for exhaustion.
func (c *Coordinator) roundMonitorLoop() {
	defer c.wg.Done()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Waiting for src queue to complete...", "coordinator", "main")
	}

	// Wait for src queue to exhaust
	c.waitForQueueCompletion(c.srcQueue, "src")

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Src queue complete. Waiting for dst queue to complete...", "coordinator", "main")
	}

	// Wait for dst queue to exhaust
	c.waitForQueueCompletion(c.dstQueue, "dst")

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Migration complete! Both queues finished.", "coordinator", "main")
	}
}

// waitForQueueCompletion blocks until the specified queue reaches exhausted state.
func (c *Coordinator) waitForQueueCompletion(queue *Queue, queueName string) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-c.pauseChan:
			// Wait until resumed or stopped
			select {
			case <-c.stopChan:
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		case <-ticker.C:
			if queue.IsExhausted() {
				if logservice.LS != nil {
					_ = logservice.LS.Log("info",
						fmt.Sprintf("%s queue exhausted at round %d", queueName, queue.Round()),
						"coordinator",
						queueName)
				}
				return
			}
		}
	}
}

// AddWorkers creates and starts workers for the specified queue.
// Workers run immediately and autonomously - they manage their own lifecycle.
// queueName should be "src" or "dst".
func (c *Coordinator) AddWorkers(queueName string, count int) error {
	var queue *Queue
	var ctx fsservices.ServiceContext
	var tableName string

	switch queueName {
	case "src":
		queue = c.srcQueue
		ctx = c.srcCtx
		tableName = "src_nodes"
	case "dst":
		queue = c.dstQueue
		ctx = c.dstCtx
		tableName = "dst_nodes"
	default:
		return fmt.Errorf("invalid queue name: %s (expected 'src' or 'dst')", queueName)
	}

	for i := 0; i < count; i++ {
		worker := NewTraversalWorker(
			i,
			queue,
			c.db,
			ctx.Connector,
			tableName,
			queueName,
		)

		// Register worker with queue (for reference/tracking)
		queue.AddWorker(worker)

		// Worker starts running immediately and manages itself
		go worker.Run()

		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"info",
				fmt.Sprintf("Worker %d started", i),
				"queue",
				queueName,
			)
		}
	}

	return nil
}

// QueueStats returns stats for the specified queue.
// queueName should be "src" or "dst".
func (c *Coordinator) QueueStats(queueName string) (QueueStats, error) {
	switch queueName {
	case "src":
		return c.srcQueue.Stats(), nil
	case "dst":
		return c.dstQueue.Stats(), nil
	default:
		return QueueStats{}, fmt.Errorf("invalid queue name: %s (expected 'src' or 'dst')", queueName)
	}
}

// IsComplete returns true if both queues have exhausted their traversals.
func (c *Coordinator) IsComplete() bool {
	return c.srcQueue.IsExhausted() && c.dstQueue.IsExhausted()
}
