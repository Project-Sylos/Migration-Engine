// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package etl

import (
	"sort"
	"sync"
)

// nodeBuffer holds node rows with mutex protection
type nodeBuffer struct {
	mu         sync.Mutex
	rows       []NodeRow
	table      string
	flushing   bool
	maxSize    int
	resumeSize int
	cond       *sync.Cond
}

// NewNodeBuffer creates a new node buffer
func NewNodeBuffer(table string) *nodeBuffer {
	nb := &nodeBuffer{
		rows:       make([]NodeRow, 0, defaultBatchSize),
		table:      table,
		maxSize:    2 * defaultBatchSize,
		resumeSize: defaultBatchSize,
	}
	nb.cond = sync.NewCond(&nb.mu)
	return nb
}

// Add appends a row to the buffer (thread-safe)
// Blocks if buffer size >= maxSize until buffer is drained below resumeSize
func (nb *nodeBuffer) Add(row NodeRow) {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	for len(nb.rows) >= nb.maxSize {
		nb.cond.Wait()
	}
	nb.rows = append(nb.rows, row)
}

// Len returns the number of rows in the buffer (thread-safe)
func (nb *nodeBuffer) Len() int {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	return len(nb.rows)
}

// IsFlushing returns whether the buffer is currently flushing (thread-safe)
func (nb *nodeBuffer) IsFlushing() bool {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	return nb.flushing
}

// SetFlushing sets the flushing flag (thread-safe)
func (nb *nodeBuffer) SetFlushing(flushing bool) {
	nb.mu.Lock()
	nb.flushing = flushing
	nb.mu.Unlock()
}

// ShouldFlush checks if buffer has reached flush threshold (thread-safe)
func (nb *nodeBuffer) ShouldFlush() bool {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	return len(nb.rows) >= defaultBatchSize
}

// GetAndClear takes a snapshot of the buffer and clears it (thread-safe)
// Returns nil if buffer is empty
func (nb *nodeBuffer) GetAndClear() []NodeRow {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	if len(nb.rows) == 0 {
		return nil
	}
	// Take snapshot and clear buffer
	batch := make([]NodeRow, len(nb.rows))
	copy(batch, nb.rows)
	nb.rows = make([]NodeRow, 0, defaultBatchSize)
	// Wake workers if buffer is now at or below resumeSize
	if len(nb.rows) <= nb.resumeSize {
		nb.cond.Broadcast()
	}
	return batch
}

// GetAndClearIfReady takes a snapshot and clears if ready to flush (thread-safe)
// Returns nil if not ready (flushing or below threshold)
func (nb *nodeBuffer) GetAndClearIfReady() []NodeRow {
	nb.mu.Lock()
	defer nb.mu.Unlock()
	if nb.flushing || len(nb.rows) < defaultBatchSize {
		return nil
	}
	// Take snapshot and clear buffer
	batch := make([]NodeRow, len(nb.rows))
	copy(batch, nb.rows)
	nb.rows = make([]NodeRow, 0, defaultBatchSize)
	nb.flushing = true
	// Wake workers if buffer is now at or below resumeSize
	if len(nb.rows) <= nb.resumeSize {
		nb.cond.Broadcast()
	}
	return batch
}

// logBuffer holds log entries with mutex protection and row numbers for ordering
type logBuffer struct {
	mu         sync.Mutex
	entries    []LogRow
	flushing   bool
	maxSize    int
	resumeSize int
	cond       *sync.Cond
}

// NewLogBuffer creates a new log buffer
func NewLogBuffer() *logBuffer {
	lb := &logBuffer{
		entries:    make([]LogRow, 0, defaultBatchSize),
		maxSize:    2 * defaultBatchSize,
		resumeSize: defaultBatchSize,
	}
	lb.cond = sync.NewCond(&lb.mu)
	return lb
}

// Add appends a log entry to the buffer (thread-safe)
// Blocks if buffer size >= maxSize until buffer is drained below resumeSize
func (lb *logBuffer) Add(entry LogRow) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for len(lb.entries) >= lb.maxSize {
		lb.cond.Wait()
	}
	lb.entries = append(lb.entries, entry)
}

// Len returns the number of entries in the buffer (thread-safe)
func (lb *logBuffer) Len() int {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return len(lb.entries)
}

// IsFlushing returns whether the buffer is currently flushing (thread-safe)
func (lb *logBuffer) IsFlushing() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.flushing
}

// SetFlushing sets the flushing flag (thread-safe)
func (lb *logBuffer) SetFlushing(flushing bool) {
	lb.mu.Lock()
	lb.flushing = flushing
	lb.mu.Unlock()
}

// ShouldFlush checks if buffer has reached flush threshold (thread-safe)
func (lb *logBuffer) ShouldFlush() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return len(lb.entries) >= defaultBatchSize
}

// GetAndClearSorted takes a snapshot of the buffer, sorts by row number, and clears it (thread-safe)
// Returns nil if buffer is empty
func (lb *logBuffer) GetAndClearSorted() []LogRow {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if len(lb.entries) == 0 {
		return nil
	}
	// Take snapshot and clear buffer
	batch := make([]LogRow, len(lb.entries))
	copy(batch, lb.entries)
	lb.entries = make([]LogRow, 0, defaultBatchSize)

	// Sort by row number to preserve chronological order
	sort.Slice(batch, func(i, j int) bool {
		return batch[i].RowNum < batch[j].RowNum
	})

	// Wake workers if buffer is now at or below resumeSize
	if len(lb.entries) <= lb.resumeSize {
		lb.cond.Broadcast()
	}

	return batch
}

// GetAndClearSortedIfReady takes a snapshot, sorts, and clears if ready to flush (thread-safe)
// Returns nil if not ready (flushing or below threshold)
func (lb *logBuffer) GetAndClearSortedIfReady() []LogRow {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.flushing || len(lb.entries) < defaultBatchSize {
		return nil
	}
	// Take snapshot and clear buffer
	batch := make([]LogRow, len(lb.entries))
	copy(batch, lb.entries)
	lb.entries = make([]LogRow, 0, defaultBatchSize)
	lb.flushing = true

	// Sort by row number to preserve chronological order
	sort.Slice(batch, func(i, j int) bool {
		return batch[i].RowNum < batch[j].RowNum
	})

	// Wake workers if buffer is now at or below resumeSize
	if len(lb.entries) <= lb.resumeSize {
		lb.cond.Broadcast()
	}

	return batch
}
