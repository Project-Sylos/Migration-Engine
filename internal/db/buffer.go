// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// FlushFunc is called when a sub-buffer needs to flush its items.
// It receives the accumulated items and should perform the batch operation.
type FlushFunc func(items []interface{}) error

// SubBuffer represents a single buffered channel within a Buffer.
// Each SubBuffer accumulates items of a specific type and flushes them together.
type SubBuffer struct {
	name      string
	items     []interface{}
	mu        sync.Mutex
	flushFunc FlushFunc
}

// NewSubBuffer creates a new sub-buffer with the given flush function.
func NewSubBuffer(name string, flushFunc FlushFunc) *SubBuffer {
	return &SubBuffer{
		name:      name,
		items:     make([]interface{}, 0),
		flushFunc: flushFunc,
	}
}

// Add adds an item to the sub-buffer.
func (sb *SubBuffer) Add(item interface{}) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.items = append(sb.items, item)
}

// Len returns the current number of items in the sub-buffer.
func (sb *SubBuffer) Len() int {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return len(sb.items)
}

// Flush processes all accumulated items and clears the buffer.
func (sb *SubBuffer) Flush() error {
	sb.mu.Lock()
	if len(sb.items) == 0 {
		sb.mu.Unlock()
		return nil
	}

	// Take snapshot and clear
	batch := sb.items
	sb.items = make([]interface{}, 0)
	sb.mu.Unlock()

	// Execute flush function
	if sb.flushFunc != nil {
		return sb.flushFunc(batch)
	}
	return nil
}

// Buffer manages multiple sub-buffers with shared timer and threshold behavior.
// It provides automatic flushing based on time intervals and item thresholds.
type Buffer struct {
	mu             sync.RWMutex
	subBuffers     map[string]*SubBuffer
	ticker         *time.Ticker
	stopChan       chan struct{}
	flushInterval  time.Duration
	flushThreshold int
	wg             sync.WaitGroup
}

// NewBuffer creates a new buffer manager.
// flushInterval: how often to auto-flush all sub-buffers
// flushThreshold: total items across all sub-buffers before forcing a flush
func NewBuffer(flushInterval time.Duration, flushThreshold int) *Buffer {
	return &Buffer{
		subBuffers:     make(map[string]*SubBuffer),
		stopChan:       make(chan struct{}),
		flushInterval:  flushInterval,
		flushThreshold: flushThreshold,
	}
}

// RegisterSubBuffer adds a new sub-buffer to this buffer manager.
func (b *Buffer) RegisterSubBuffer(name string, flushFunc FlushFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subBuffers[name] = NewSubBuffer(name, flushFunc)
}

// Add adds an item to a specific sub-buffer and checks if threshold is reached.
func (b *Buffer) Add(subBufferName string, item interface{}) {
	b.mu.RLock()
	sb, exists := b.subBuffers[subBufferName]
	b.mu.RUnlock()

	if !exists {
		return // Sub-buffer not registered
	}

	sb.Add(item)

	// Check if total items exceed threshold (calculate inline to avoid nested locks)
	totalItems := 0
	b.mu.RLock()
	for _, subBuffer := range b.subBuffers {
		totalItems += subBuffer.Len()
	}
	shouldFlush := totalItems >= b.flushThreshold
	b.mu.RUnlock()

	if shouldFlush {
		go b.FlushAll()
	}
}

// TotalItems returns the total number of items across all sub-buffers.
func (b *Buffer) TotalItems() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0
	for _, sb := range b.subBuffers {
		total += sb.Len()
	}
	return total
}

// FlushAll flushes all sub-buffers.
func (b *Buffer) FlushAll() {
	b.mu.RLock()
	buffers := make([]*SubBuffer, 0, len(b.subBuffers))
	for _, sb := range b.subBuffers {
		buffers = append(buffers, sb)
	}
	b.mu.RUnlock()

	// Flush each sub-buffer
	for _, sb := range buffers {
		_ = sb.Flush() // Errors are handled by flush functions
	}
}

// FlushSubBuffer flushes a specific sub-buffer by name.
func (b *Buffer) FlushSubBuffer(name string) error {
	b.mu.RLock()
	sb, exists := b.subBuffers[name]
	b.mu.RUnlock()

	if !exists {
		return nil
	}

	return sb.Flush()
}

// Start begins the automatic flush timer goroutine.
func (b *Buffer) Start() {
	b.ticker = time.NewTicker(b.flushInterval)
	b.wg.Add(1)

	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-b.ticker.C:
				b.FlushAll()
			case <-b.stopChan:
				b.ticker.Stop()
				b.FlushAll() // Final flush
				return
			}
		}
	}()
}

// Stop gracefully stops the buffer, flushing all remaining items.
func (b *Buffer) Stop() {
	close(b.stopChan)
	b.wg.Wait()
}

// ---------------------------------------------------------------------------
// InsertBuffer (Legacy - for backwards compatibility with db.go)
// ---------------------------------------------------------------------------

// InsertBuffer batches incoming INSERT operations.
// This is kept for backwards compatibility with existing db.Write() calls.
type InsertBuffer struct {
	db       *sql.DB
	mu       sync.Mutex
	ticker   *time.Ticker
	stop     chan struct{}
	table    string
	rows     [][]any
	maxBatch int
	interval time.Duration
}

// NewInsertBuffer constructs a new InsertBuffer.
func NewInsertBuffer(db *sql.DB, table string, batchSize int, flushInt time.Duration) *InsertBuffer {
	b := &InsertBuffer{
		db:       db,
		table:    table,
		maxBatch: batchSize,
		interval: flushInt,
		ticker:   time.NewTicker(flushInt),
		stop:     make(chan struct{}),
	}
	go b.start()
	return b
}

func (b *InsertBuffer) start() {
	for {
		select {
		case <-b.ticker.C:
			b.Flush()
		case <-b.stop:
			b.ticker.Stop()
			b.Flush() // final flush
			return
		}
	}
}

// Add queues a row and flushes when threshold reached.
func (b *InsertBuffer) Add(values []any) {
	b.mu.Lock()
	b.rows = append(b.rows, values)
	shouldFlush := len(b.rows) >= b.maxBatch
	b.mu.Unlock()

	if shouldFlush {
		b.Flush()
	}
}

// Flush writes all buffered rows to the DB in one transaction.
func (b *InsertBuffer) Flush() {
	b.mu.Lock()
	batch := b.rows
	b.rows = nil
	b.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	colCount := len(batch[0])
	query := "INSERT INTO " + b.table + " VALUES " + buildPlaceholderGroup(colCount)

	ctx := context.Background()
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		return
	}
	defer stmt.Close()

	for _, row := range batch {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			_ = tx.Rollback()
			return
		}
	}
	_ = tx.Commit()
}

// Stop gracefully stops the buffer.
func (b *InsertBuffer) Stop() {
	close(b.stop)
}

// buildPlaceholderGroup creates "(?, ?, ?)" etc.
func buildPlaceholderGroup(n int) string {
	if n <= 0 {
		return "()"
	}
	s := "("
	for i := 0; i < n; i++ {
		if i > 0 {
			s += ", "
		}
		s += "?"
	}
	s += ")"
	return s
}
