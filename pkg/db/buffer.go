// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// LogBuffer is a lightweight buffer specifically for log entries.
// It batches log inserts for improved performance while keeping the implementation simple.
type LogBuffer struct {
	db          *DB
	mu          sync.Mutex
	entries     []LogEntry
	batchSize   int
	flushTicker *time.Ticker
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// NewLogBuffer creates a new log buffer that will flush every N entries or every interval.
func NewLogBuffer(db *DB, batchSize int, flushInterval time.Duration) *LogBuffer {
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

// Flush writes all buffered entries to BoltDB in a single transaction.
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

	// Execute batch insert to BoltDB in a single transaction
	err := lb.db.Update(func(tx *bolt.Tx) error {
		for _, entry := range batch {
			// Get or create the level-specific bucket
			levelBucket, err := GetOrCreateLogLevelBucket(tx, entry.Level)
			if err != nil {
				return fmt.Errorf("failed to get log level bucket: %w", err)
			}

			// Serialize entry
			value, err := SerializeLogEntry(entry)
			if err != nil {
				return fmt.Errorf("failed to serialize log entry: %w", err)
			}

			// Write to BoltDB using entry ID as key
			if err := levelBucket.Put([]byte(entry.ID), value); err != nil {
				return fmt.Errorf("failed to set log entry: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		// Silently fail - logs are not critical
		fmt.Printf("Error flushing log buffer: %v\n", err)
	}
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
// Uses a timeout to prevent indefinite blocking if the flush loop is stuck.
func (lb *LogBuffer) Stop() {
	// Note: We can't use logservice here as it might cause a circular dependency
	// LogBuffer is part of the db package, and logservice depends on db
	close(lb.stopChan)

	// Wait for flush loop to finish, but with a timeout to prevent hanging
	done := make(chan struct{}, 1)
	go func() {
		lb.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		// Flush loop completed successfully
	case <-time.After(2 * time.Second):
		// Timeout - flush loop may be stuck or slow
		// Continue anyway to prevent blocking the entire shutdown
	}
}
