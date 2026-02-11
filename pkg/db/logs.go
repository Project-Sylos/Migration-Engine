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

// DefaultLogShardCap is the default capacity per shard in the log buffer.
const DefaultLogShardCap = 256

// LogBuffer buffers log entries and flushes them to the DB logs table.
type LogBuffer struct {
	db       *DB
	entries  []LogEntry
	mu       sync.Mutex
	interval time.Duration
	stopCh   chan struct{}
	stopped  int32
}

// NewLogBuffer creates a log buffer that flushes to the main DB's logs table.
func NewLogBuffer(db *DB, batchSize int, interval time.Duration, _ int) *LogBuffer {
	lb := &LogBuffer{
		db:       db,
		entries:  make([]LogEntry, 0, batchSize),
		interval: interval,
		stopCh:   make(chan struct{}),
	}
	go lb.flushLoop()
	return lb
}

// Add adds an entry to the buffer.
func (lb *LogBuffer) Add(e LogEntry) {
	lb.mu.Lock()
	lb.entries = append(lb.entries, e)
	lb.mu.Unlock()
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
	lb.mu.Lock()
	batch := lb.entries
	lb.entries = make([]LogEntry, 0, cap(lb.entries))
	lb.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	_ = lb.db.RunUpdateWriterTx(func(w *Writer) error {
		for _, e := range batch {
			if err := w.InsertLog(e.ID, e.Level, e.Message, e.Entity, e.Entity, e.EntityID, e.Queue); err != nil {
				return err
			}
		}
		return nil
	})
}

// Stop stops the flush loop. Does not close the DB.
func (lb *LogBuffer) Stop() {
	if atomic.CompareAndSwapInt32(&lb.stopped, 0, 1) {
		close(lb.stopCh)
	}
	lb.Flush()
}
