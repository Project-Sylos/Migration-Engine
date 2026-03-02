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
// Flush triggers: row threshold (batchSize), time-based (interval), and on backpressure (drain all).
// Backpressure: when buffer reaches 2*batchSize, Add blocks and drains the entire buffer before accepting more.
type LogBuffer struct {
	db        *DB
	entries   []LogEntry
	batchSize int
	mu        sync.Mutex
	cond      *sync.Cond
	flushing  bool
	slots     chan struct{}
	interval  time.Duration
	stopCh    chan struct{}
	stopped   int32
}

// NewLogBuffer creates a log buffer that flushes to the main DB's logs table.
func NewLogBuffer(db *DB, batchSize int, interval time.Duration) *LogBuffer {
	hardCap := batchSize * 2
	lb := &LogBuffer{
		db:        db,
		entries:   make([]LogEntry, 0, batchSize),
		batchSize: batchSize,
		slots:     make(chan struct{}, hardCap),
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
	lb.cond = sync.NewCond(&lb.mu)
	for i := 0; i < hardCap; i++ {
		lb.slots <- struct{}{}
	}
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
	lb.entries = append(lb.entries[:0], lb.entries[n:]...)
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

// drainAll writes the entire buffer in batches until empty. Call when at backpressure.
func (lb *LogBuffer) drainAll() {
	for {
		batch, hadWork := lb.takeBatch()
		if !hadWork || len(batch) == 0 {
			return
		}
		n := len(batch)
		if err := lb.writeBatch(batch); err != nil {
			fmt.Println("error running write", err)
		}
		for i := 0; i < n; i++ {
			lb.slots <- struct{}{}
		}
		lb.setFlushingDone()
	}
}

// Add adds an entry to the buffer. At 2*batchSize (backpressure threshold), blocks and drains the entire buffer before adding.
func (lb *LogBuffer) Add(e LogEntry) {
	hardCap := lb.batchSize * 2
	<-lb.slots
	lb.mu.Lock()
	for len(lb.entries) >= hardCap {
		lb.mu.Unlock()
		lb.drainAll()
		lb.mu.Lock()
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

// Flush writes one batch if the buffer has at least batchSize entries. Does not block on backpressure.
func (lb *LogBuffer) Flush() {
	batch, hadWork := lb.takeBatch()
	if !hadWork || len(batch) == 0 {
		return
	}
	n := len(batch)
	defer lb.setFlushingDone()
	if err := lb.writeBatch(batch); err != nil {
		fmt.Println("error running write", err)
		return
	}
	for i := 0; i < n; i++ {
		lb.slots <- struct{}{}
	}
}

// Stop stops the flush loop and drains the entire buffer. Does not close the DB.
func (lb *LogBuffer) Stop() {
	if atomic.CompareAndSwapInt32(&lb.stopped, 0, 1) {
		close(lb.stopCh)
	}
	lb.drainAll()
}
