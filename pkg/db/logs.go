// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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

// GenerateLogID returns a unique log id (UUID).
func GenerateLogID() string {
	return uuid.New().String()
}

// LogBuffer buffers log entries and flushes them to the DB logs table.
// Flush triggers: row threshold (batchSize), time-based (interval).
// Backpressure: when buffer reaches 2*batchSize, block all writers, drain the entire buffer, then unblock; writers then push freely until 2*batchSize again.
type LogBuffer struct {
	db        *DB
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
func NewLogBuffer(db *DB, batchSize int, interval time.Duration) *LogBuffer {
	lb := &LogBuffer{
		db:        db,
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
	// Take up to batchSize, or all if we're at/over threshold so we drain faster
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
	return lb.db.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			for _, e := range batch {
				if err := w.InsertLog(e.ID, e.Level, e.Message, e.Entity, e.Entity, e.EntityID, e.Queue); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// drainAll writes the entire buffer in batches until empty. Call when at backpressure (caller must set draining so Add() blocks).
func (lb *LogBuffer) drainAll() {
	for {
		batch, hadWork := lb.takeBatch()
		if !hadWork || len(batch) == 0 {
			return
		}
		if err := lb.writeBatch(batch); err != nil {
			fmt.Println("error running write", err)
		}
		lb.setFlushingDone()
	}
}

// Add adds an entry to the buffer. When buffer reaches 2*batchSize we block all writers, drain the entire buffer, then resume.
func (lb *LogBuffer) Add(e LogEntry) {
	hardCap := lb.batchSize * 2
	lb.mu.Lock()
	for len(lb.entries) >= hardCap {
		if !lb.draining {
			lb.draining = true
			lb.mu.Unlock()
			lb.drainAll()
			lb.mu.Lock()
			lb.draining = false
			lb.cond.Broadcast()
			continue
		}
		lb.cond.Wait()
	}
	lb.entries = append(lb.entries, e)
	count := len(lb.entries)
	lb.mu.Unlock()
	if count >= lb.batchSize {
		lb.Flush()
	}
}

func (lb *LogBuffer) flushLoop() {
	ticker := time.NewTicker(lb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-lb.stopCh:
			return
		case <-ticker.C:
			lb.Flush()
		}
	}
}

// Flush writes one batch if the buffer has entries. Does not block writers.
func (lb *LogBuffer) Flush() {
	batch, hadWork := lb.takeBatch()
	if !hadWork || len(batch) == 0 {
		return
	}
	defer lb.setFlushingDone()
	if err := lb.writeBatch(batch); err != nil {
		fmt.Println("error running write", err)
	}
}

// Stop stops the flush loop and drains the entire buffer. Does not close the DB.
func (lb *LogBuffer) Stop() {
	if atomic.CompareAndSwapInt32(&lb.stopped, 0, 1) {
		close(lb.stopCh)
	}
	lb.drainAll()
}
