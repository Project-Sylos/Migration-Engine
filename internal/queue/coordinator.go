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
	flushThreshold int,
	flushInterval time.Duration,
) *Coordinator {
	srcQueue := NewQueue("src", maxRetries, batchSize, flushThreshold, flushInterval)
	dstQueue := NewQueue("dst", maxRetries, batchSize, flushThreshold, flushInterval)

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

// Start begins the coordinator and all workers.
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

	// Start workers for both queues
	c.srcQueue.StartWorkers()
	c.dstQueue.StartWorkers()

	// Start the round monitoring loop
	c.wg.Add(1)
	go c.roundMonitorLoop()

	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Coordinator started", "coordinator", "main")
	}

	return nil
}

// Stop gracefully stops the coordinator and all workers.
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

	// Stop all workers
	c.srcQueue.StopWorkers()
	c.dstQueue.StopWorkers()

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

// roundMonitorLoop monitors queue states and advances rounds when queues complete a level.
// Queues pull their own tasks, so coordinator only monitors for completion.
func (c *Coordinator) roundMonitorLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
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
			c.mu.RLock()
			if c.state == StateRunning {
				c.mu.RUnlock()
				c.checkRoundAdvancement()
			} else {
				c.mu.RUnlock()
			}
		}
	}
}

// checkRoundAdvancement checks queue states and advances rounds when appropriate.
func (c *Coordinator) checkRoundAdvancement() {
	// Check src queue state
	srcState := c.srcQueue.State()

	switch srcState {
	case QueueStateRoundComplete:
		// Round finished, advance to next
		c.advanceSrcRound()
	case QueueStateExhausted:
		// Src traversal complete - log and do nothing
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "Src traversal complete", "coordinator", "src")
		}
	}

	// Check dst queue state
	dstState := c.dstQueue.State()
	srcRound := c.srcQueue.Round()
	dstRound := c.dstQueue.Round()

	// Dst can only advance if it's behind src
	if dstRound >= srcRound {
		return // Dst is caught up or ahead (shouldn't happen)
	}

	switch dstState {
	case QueueStateRoundComplete:
		// Round finished, advance to next
		c.advanceDstRound()
	case QueueStateExhausted:
		// Dst traversal complete - log and do nothing
		if logservice.LS != nil {
			_ = logservice.LS.Log("info", "Dst traversal complete", "coordinator", "dst")
		}
	}
}

// advanceSrcRound advances the src queue to the next round after flushing buffers.
func (c *Coordinator) advanceSrcRound() {
	currentRound := c.srcQueue.Round()

	// Flush database buffers to ensure round completion is durable
	c.flushDatabase()

	// Advance to next round
	c.srcQueue.SetRound(currentRound + 1)

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Src queue advanced to round %d", currentRound+1),
			"coordinator",
			"src",
		)
	}
}

// advanceDstRound advances the dst queue to the next round after flushing buffers.
func (c *Coordinator) advanceDstRound() {
	currentRound := c.dstQueue.Round()

	// Flush database buffers
	c.flushDatabase()

	// Advance to next round
	c.dstQueue.SetRound(currentRound + 1)

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Dst queue advanced to round %d", currentRound+1),
			"coordinator",
			"dst",
		)
	}
}

// flushDatabase forces all buffered DB writes to complete.
func (c *Coordinator) flushDatabase() {
	// Trigger a dummy query to force buffer flush (Query() flushes all buffers)
	_, _ = c.db.Query("SELECT 1")
}

// AddSrcWorkers adds workers to the src queue.
func (c *Coordinator) AddSrcWorkers(count int) {
	for i := 0; i < count; i++ {
		worker := NewTraversalWorker(
			i,
			c.srcQueue,
			c.db,
			c.srcCtx.Connector,
			"src_nodes",
			"src",
		)
		c.srcQueue.AddWorker(worker)
	}
}

// AddDstWorkers adds workers to the dst queue.
func (c *Coordinator) AddDstWorkers(count int) {
	for i := 0; i < count; i++ {
		worker := NewTraversalWorker(
			i,
			c.dstQueue,
			c.db,
			c.dstCtx.Connector,
			"dst_nodes",
			"dst",
		)
		c.dstQueue.AddWorker(worker)
	}
}

// SrcQueueStats returns stats for the source queue.
func (c *Coordinator) SrcQueueStats() QueueStats {
	return c.srcQueue.Stats()
}

// DstQueueStats returns stats for the destination queue.
func (c *Coordinator) DstQueueStats() QueueStats {
	return c.dstQueue.Stats()
}
