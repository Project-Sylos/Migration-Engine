// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// LogBuffer is a shard-aware buffer for log entries (plan: count-based sharding, e.g. 1M per shard).
type LogBuffer struct {
	db                 *DB
	mu                 sync.Mutex
	entries            []LogEntry
	batchSize          int
	flushTicker        *time.Ticker
	stopChan           chan struct{}
	wg                 sync.WaitGroup
	shardCap           int64   // Max entries per shard (e.g. 1_000_000)
	currentShardID     int     // In-memory; restored from _meta on first flush
	countInCurrentShard int64  // In-memory; restored from _meta on first flush
	stateLoaded        bool   // True after first load from DB
}

// NewLogBuffer creates a new log buffer (shard-capable). Shards are created when count reaches shardCap.
func NewLogBuffer(db *DB, batchSize int, flushInterval time.Duration, shardCap int64) *LogBuffer {
	if shardCap <= 0 {
		shardCap = DefaultLogShardCap
	}
	lb := &LogBuffer{
		db:          db,
		entries:     make([]LogEntry, 0, batchSize),
		batchSize:   batchSize,
		flushTicker: time.NewTicker(flushInterval),
		stopChan:    make(chan struct{}),
		shardCap:    shardCap,
		currentShardID: 0,
		countInCurrentShard: 0,
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

// Flush writes all buffered entries to the current log shard; advances shard when count >= shardCap.
func (lb *LogBuffer) Flush() {
	lb.mu.Lock()
	if len(lb.entries) == 0 {
		lb.mu.Unlock()
		return
	}
	batch := lb.entries
	lb.entries = make([]LogEntry, 0, lb.batchSize)
	lb.mu.Unlock()

	var nextShardID int
	var nextCount int64
	err := lb.db.Update(func(tx *bolt.Tx) error {
		shardID := lb.currentShardID
		count := lb.countInCurrentShard
		if !lb.stateLoaded {
			sid, cnt, _ := GetCurrentLogShardAndCount(tx)
			shardID = sid
			count = cnt
		}

		logsBucket, err := GetOrCreateLogShardBucket(tx, shardID, LogShardLogsBucket)
		if err != nil {
			return err
		}

		for _, entry := range batch {
			value, err := SerializeLogEntry(entry)
			if err != nil {
				return fmt.Errorf("serialize log entry: %w", err)
			}
			if err := logsBucket.Put([]byte(entry.ID), value); err != nil {
				return fmt.Errorf("put log entry: %w", err)
			}
			levelBucket, err := GetOrCreateLogShardBucket(tx, shardID, entry.Level)
			if err != nil {
				return err
			}
			if err := levelBucket.Put([]byte(entry.ID), []byte{}); err != nil {
				return fmt.Errorf("put log level membership: %w", err)
			}
		}

		newCount := count + int64(len(batch))
		if err := WriteLogShardStats(tx, shardID, newCount); err != nil {
			return err
		}

		if newCount >= lb.shardCap {
			nextShardID = shardID + 1
			nextCount = 0
			if err := SetCurrentLogShardAndCount(tx, nextShardID, 0); err != nil {
				return err
			}
		} else {
			nextShardID = shardID
			nextCount = newCount
			if err := SetCurrentLogShardAndCount(tx, shardID, newCount); err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		lb.mu.Lock()
		lb.stateLoaded = true
		lb.currentShardID = nextShardID
		lb.countInCurrentShard = nextCount
		lb.mu.Unlock()
	}

	if err != nil {
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
