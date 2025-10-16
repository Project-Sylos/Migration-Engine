// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// BufferBase
// ---------------------------------------------------------------------------

// BufferBase defines the lifecycle and timing behavior shared by all buffers.
type BufferBase struct {
	mu       sync.Mutex
	ticker   *time.Ticker
	stop     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
}

func NewBufferBase(interval time.Duration) *BufferBase {
	ctx, cancel := context.WithCancel(context.Background())
	return &BufferBase{
		ticker:   time.NewTicker(interval),
		stop:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
		interval: interval,
	}
}

func (b *BufferBase) Start(flushFn func()) {
	go func() {
		for {
			select {
			case <-b.ticker.C:
				flushFn()
			case <-b.stop:
				b.ticker.Stop()
				flushFn() // final flush
				b.cancel()
				return
			case <-b.ctx.Done():
				b.ticker.Stop()
				return
			}
		}
	}()
}

func (b *BufferBase) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.stop:
		return
	default:
		close(b.stop)
	}
}

func (b *BufferBase) ResetTicker() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ticker != nil {
		b.ticker.Stop()
	}
	b.ticker = time.NewTicker(b.interval)
}

// ---------------------------------------------------------------------------
// InsertBuffer
// ---------------------------------------------------------------------------

// InsertBuffer batches incoming INSERT operations.
type InsertBuffer struct {
	*BufferBase
	db        *sql.DB
	mu        sync.Mutex
	table     string
	rows      [][]any
	maxBatch  int
	colCount  int // inferred or passed manually
}

// NewInsertBuffer constructs a new InsertBuffer.
func NewInsertBuffer(db *sql.DB, table string, batchSize int, flushInt time.Duration) *InsertBuffer {
	b := &InsertBuffer{
		BufferBase: NewBufferBase(flushInt),
		db:         db,
		table:      table,
		maxBatch:   batchSize,
	}
	b.Start(b.Flush)
	return b
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
	b.ResetTicker()
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
