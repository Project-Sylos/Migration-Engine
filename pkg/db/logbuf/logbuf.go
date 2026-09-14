// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logbuf

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// LogEntry is a single log line for persistence.
type LogEntry struct {
	ID        string
	Timestamp string
	Level     string
	Entity    string
	EntityID  string
	Message   string
	Queue     string
}

// LogBuffer buffers log entries and flushes them to the DB logs table.
// Flush triggers: row threshold (batchSize), time-based (interval).
// Backpressure: when buffer reaches 2*batchSize, block all writers, drain the entire buffer, then unblock; writers then push freely until 2*batchSize again.
type LogBuffer struct {
	database  *db.DB
	entries   []LogEntry
	batchSize int
	mu        sync.Mutex
	cond      *sync.Cond
	flushing  bool
	draining  bool
	interval  time.Duration
	stopCh    chan struct{}
	stopped   int32
}

// NewLogBuffer creates a log buffer that flushes to the main DB's logs table.
// Flush threshold is batchSize; backpressure threshold is 2*batchSize (block all writers and drain, then resume).
func NewLogBuffer(database *db.DB, batchSize int, interval time.Duration) *LogBuffer {
	lb := &LogBuffer{
		database:  database,
		entries:   make([]LogEntry, 0, batchSize*2),
		batchSize: batchSize,
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
	lb.cond = sync.NewCond(&lb.mu)
	go lb.flushLoop()
	return lb
}

func (lb *LogBuffer) takeBatch() (batch []LogEntry, hadWork bool) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.flushing || len(lb.entries) == 0 {
		return nil, false
	}
	n := len(lb.entries)
	if n > lb.batchSize {
		n = lb.batchSize
	}
	batch = make([]LogEntry, n)
	copy(batch, lb.entries[:n])
	copy(lb.entries[:], lb.entries[n:])
	newLen := len(lb.entries) - n
	var zero LogEntry
	for i := newLen; i < len(lb.entries); i++ {
		lb.entries[i] = zero
	}
	lb.entries = lb.entries[:newLen]
	lb.flushing = true
	lb.cond.Broadcast()
	return batch, true
}

func (lb *LogBuffer) setFlushingDone() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.flushing = false
	lb.cond.Broadcast()
}

func (lb *LogBuffer) writeBatch(batch []LogEntry) error {
	if lb.database == nil || lb.database.Ops() == nil {
		return fmt.Errorf("ops store not open")
	}
	recs := make([]opsdb.LogRecord, len(batch))
	for i, e := range batch {
		recs[i] = opsdb.LogRecord{
			ID: e.ID, Level: e.Level, Message: e.Message,
			Component: e.Entity, Entity: e.Entity, EntityID: e.EntityID, Queue: e.Queue,
		}
	}
	return lb.database.Ops().AppendLogs(recs)
}

// drainAll writes the entire buffer in batches until empty. Call when at backpressure (caller must set draining so Add() blocks).
func (lb *LogBuffer) drainAll() {
	for {
		batch, hadWork := lb.takeBatch()
		if !hadWork || len(batch) == 0 {
			return
		}
		if err := lb.writeBatch(batch); err != nil {
			fmt.Printf("logbuf drain: %v\n", err)
		}
		lb.setFlushingDone()
	}
}

func (lb *LogBuffer) flushLoop() {
	ticker := time.NewTicker(lb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-lb.stopCh:
			lb.drainAll()
			return
		case <-ticker.C:
			batch, hadWork := lb.takeBatch()
			if !hadWork {
				continue
			}
			if err := lb.writeBatch(batch); err != nil {
				fmt.Printf("logbuf flush: %v\n", err)
			}
			lb.setFlushingDone()
		}
	}
}

// Add appends one log entry. Blocks when buffer is at 2*batchSize until drain completes.
func (lb *LogBuffer) Add(entry LogEntry) {
	lb.mu.Lock()
	for len(lb.entries) >= lb.batchSize*2 && !lb.draining {
		lb.draining = true
		lb.mu.Unlock()
		lb.drainAll()
		lb.mu.Lock()
		lb.draining = false
		lb.cond.Broadcast()
	}
	lb.entries = append(lb.entries, entry)
	shouldFlush := len(lb.entries) >= lb.batchSize
	lb.mu.Unlock()
	if shouldFlush {
		batch, hadWork := lb.takeBatch()
		if hadWork {
			if err := lb.writeBatch(batch); err != nil {
				fmt.Printf("logbuf add flush: %v\n", err)
			}
			lb.setFlushingDone()
		}
	}
}

// Stop drains and stops the background flush loop.
func (lb *LogBuffer) Stop() {
	if !atomic.CompareAndSwapInt32(&lb.stopped, 0, 1) {
		return
	}
	close(lb.stopCh)
}
