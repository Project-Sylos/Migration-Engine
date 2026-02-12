// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"sync"
	"sync/atomic"
	"time"
)

// LogEntry is a single log line for persistence.
type LogEntry struct {
	ID        int64
	Timestamp string
	Level     string
	Entity    string
	EntityID  string
	Message   string
	Queue     string
}

// GenerateLogID returns a unique log id (monotonic in practice).
func GenerateLogID() int64 {
	return time.Now().UnixNano()
}

// LogBuffer buffers log entries and flushes them to the DB logs table.
// Back-pressure: at 2x batch size, Add blocks until a flush completes.
// Flushing guard: only one in-flight flush; getAndClearIfReady returns nil if already flushing.
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

func (lb *LogBuffer) getAndClearIfReady() []LogEntry {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.flushing || len(lb.entries) < lb.batchSize {
		return nil
	}
	batch := lb.entries
	lb.entries = make([]LogEntry, 0, cap(lb.entries))
	lb.flushing = true
	lb.cond.Broadcast()
	return batch
}

func (lb *LogBuffer) setFlushingDone() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.flushing = false
	lb.cond.Broadcast()
}

// Add adds an entry to the buffer.
func (lb *LogBuffer) Add(e LogEntry) {
	hardCap := lb.batchSize * 2
	<-lb.slots
	lb.mu.Lock()
	for len(lb.entries) >= hardCap {
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

// Flush writes buffered entries to the logs table.
func (lb *LogBuffer) Flush() {
	batch := lb.getAndClearIfReady()
	if len(batch) == 0 {
		return
	}
	defer lb.setFlushingDone()
	n := len(batch)
	_ = lb.db.RunUpdateWriterTx(func(w *Writer) error {
		for _, e := range batch {
			if err := w.InsertLog(e.ID, e.Level, e.Message, e.Entity, e.Entity, e.EntityID, e.Queue); err != nil {
				return err
			}
		}
		return nil
	})
	for i := 0; i < n; i++ {
		lb.slots <- struct{}{}
	}
}

// Stop stops the flush loop. Does not close the DB.
func (lb *LogBuffer) Stop() {
	if atomic.CompareAndSwapInt32(&lb.stopped, 0, 1) {
		close(lb.stopCh)
	}
	lb.Flush()
}
