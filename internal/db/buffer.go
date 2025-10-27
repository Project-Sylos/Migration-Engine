// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// LogEntry represents a single log record to be buffered.
type LogEntry struct {
	ID        string
	Timestamp time.Time
	Level     string
	Entity    string
	EntityID  string
	Details   interface{}
	Message   string
	Queue     interface{}
}

// LogBuffer is a lightweight buffer specifically for log entries.
// It batches log inserts for improved performance while keeping the implementation simple.
type LogBuffer struct {
	db          *sql.DB
	mu          sync.Mutex
	entries     []LogEntry
	batchSize   int
	flushTicker *time.Ticker
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// NewLogBuffer creates a new log buffer that will flush every N entries or every interval.
func NewLogBuffer(db *sql.DB, batchSize int, flushInterval time.Duration) *LogBuffer {
	lb := &LogBuffer{
		db:          db,
		entries:     make([]LogEntry, 0, batchSize),
		batchSize:   batchSize,
		flushTicker: time.NewTicker(flushInterval),
		stopChan:    make(chan struct{}),
	}

	lb.wg.Add(1)
	go lb.flushLoop()

	return lb
}

// Add adds a log entry to the buffer. If batch size is reached, it triggers a flush.
func (lb *LogBuffer) Add(entry LogEntry) {
	lb.mu.Lock()
	lb.entries = append(lb.entries, entry)
	shouldFlush := len(lb.entries) >= lb.batchSize
	lb.mu.Unlock()

	if shouldFlush {
		lb.Flush()
	}
}

// Flush writes all buffered entries to the database.
func (lb *LogBuffer) Flush() {
	lb.mu.Lock()
	if len(lb.entries) == 0 {
		lb.mu.Unlock()
		return
	}

	// Take snapshot and clear buffer
	batch := lb.entries
	lb.entries = make([]LogEntry, 0, lb.batchSize)
	lb.mu.Unlock()

	// Execute batch insert
	ctx := context.Background()
	tx, err := lb.db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Println("Error beginning transaction:", err)
		return // Silently fail - logs are not critical
	}

	stmt, err := tx.PrepareContext(ctx,
		"INSERT INTO logs (id, timestamp, level, entity, entity_id, details, message, queue) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		_ = tx.Rollback()
		return
	}
	defer stmt.Close()

	for _, entry := range batch {
		_, err := stmt.ExecContext(ctx,
			entry.ID,
			entry.Timestamp,
			entry.Level,
			entry.Entity,
			entry.EntityID,
			entry.Details,
			entry.Message,
			entry.Queue)
		if err != nil {
			_ = tx.Rollback()
			return
		}
	}

	_ = tx.Commit()
}

// flushLoop runs in a goroutine and periodically flushes the buffer.
func (lb *LogBuffer) flushLoop() {
	defer lb.wg.Done()

	for {
		select {
		case <-lb.flushTicker.C:
			lb.Flush()
		case <-lb.stopChan:
			lb.flushTicker.Stop()
			lb.Flush() // Final flush before stopping
			return
		}
	}
}

// Stop gracefully stops the log buffer and flushes remaining entries.
func (lb *LogBuffer) Stop() {
	close(lb.stopChan)
	lb.wg.Wait()
}
