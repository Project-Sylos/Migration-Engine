// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	duckdb "github.com/marcboeker/go-duckdb"
)

const (
	defaultSealFlushInterval     = 60 * time.Second
	defaultSealFlushRowThreshold = 100_000
	defaultSealBufferHardCap     = 100_000
)

// Arr Arr Arr! (Seal noises lol)
// SealJob is one sealed level's payload: table, depth, nodes, and stats.
type SealJob struct {
	Table      string
	Depth      int
	Nodes      []*NodeState
	Pending    int64
	Successful int64
	Failed     int64
	Completed  int64
	CopyP      int64
	CopyS      int64
	CopyF      int64
}

// SealBuffer buffers seal jobs and flushes them to the DB asynchronously.
// Flush triggers: interval timer, row count threshold, and Stop/ForceFlush (or backpressure sync).
type SealBuffer struct {
	db                *DB
	interval          time.Duration
	rowThreshold      int
	hardCap           int
	mu                sync.Mutex
	cond              *sync.Cond
	queue             []SealJob
	rowsSinceFlush    int
	lastFlushedDepth  int // max depth written by completed Flush(); -1 until first flush
	stopCh            chan struct{}
	stopped           int32
}

// SealBufferOptions configures the seal buffer. Zero value uses defaults.
type SealBufferOptions struct {
	FlushInterval time.Duration
	RowThreshold  int
	HardCap       int
}

// NewSealBuffer creates a seal buffer and starts its flush loop.
func NewSealBuffer(db *DB, opts SealBufferOptions) *SealBuffer {
	interval := opts.FlushInterval
	if interval == 0 {
		interval = defaultSealFlushInterval
	}
	rowThreshold := opts.RowThreshold
	if rowThreshold <= 0 {
		rowThreshold = defaultSealFlushRowThreshold
	}
	hardCap := opts.HardCap
	if hardCap <= 0 {
		hardCap = defaultSealBufferHardCap
	}
	sb := &SealBuffer{
		db:           db,
		interval:     interval,
		rowThreshold: rowThreshold,
		hardCap:      hardCap,
		queue:        make([]SealJob, 0, 64),
		stopCh:       make(chan struct{}),
	}
	sb.cond = sync.NewCond(&sb.mu)
	go sb.flushLoop()
	return sb
}

// Add enqueues a seal job. Blocks if queue size (total node count) would exceed hardCap until a flush completes.
func (sb *SealBuffer) Add(table string, depth int, nodes []*NodeState, pending, successful, failed, completed, copyP, copyS, copyF int64) {
	if table != "SRC" && table != "DST" {
		return
	}
	n := len(nodes)
	nodeCopy := make([]*NodeState, n)
	copy(nodeCopy, nodes)
	sb.mu.Lock()
	for sb.rowsSinceFlush+n > sb.hardCap {
		sb.cond.Wait()
	}
	sb.queue = append(sb.queue, SealJob{
		Table:      table,
		Depth:      depth,
		Nodes:      nodeCopy,
		Pending:    pending,
		Successful: successful,
		Failed:     failed,
		Completed:  completed,
		CopyP:      copyP,
		CopyS:      copyS,
		CopyF:      copyF,
	})
	sb.rowsSinceFlush += n
	overThreshold := sb.rowsSinceFlush >= sb.rowThreshold
	sb.cond.Broadcast()
	sb.mu.Unlock()
	if overThreshold {
		err := sb.Flush()
		if err != nil {
			fmt.Println("Error flushing jobs:", err)
			return
		}
	}
}

func (sb *SealBuffer) drain() []SealJob {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if len(sb.queue) == 0 {
		return nil
	}
	out := sb.queue
	sb.queue = make([]SealJob, 0, cap(sb.queue))
	sb.rowsSinceFlush = 0
	sb.cond.Broadcast()
	return out
}

// Flush drains queued jobs and writes them to the DB using DuckDB's Appender API (nodes) then a transaction (stats), then checkpoint.
func (sb *SealBuffer) Flush() error {
	jobs := sb.drain()
	if len(jobs) == 0 {
		return nil
	}
	maxDepth := -1
	for _, j := range jobs {
		if j.Depth > maxDepth {
			maxDepth = j.Depth
		}
	}
	ctx := context.Background()
	if err := sb.db.RunWithConn(ctx, func(conn *sql.Conn) error {
		if err := conn.Raw(func(driverConn any) error {
			dc, ok := driverConn.(driver.Conn)
			if !ok {
				return fmt.Errorf("seal flush: conn is not driver.Conn")
			}
			appSrc, err := duckdb.NewAppenderFromConn(dc, "", tableSrcNodes)
			if err != nil {
				return err
			}
			defer appSrc.Close()
			appDst, err := duckdb.NewAppenderFromConn(dc, "", tableDstNodes)
			if err != nil {
				return err
			}
			defer appDst.Close()
			for _, j := range jobs {
				app := appSrc
				if j.Table == "DST" {
					app = appDst
				}
				for _, n := range j.Nodes {
					args := NodeStateAppendRowArgs(n)
					dvals := make([]driver.Value, len(args))
					for i := range args {
						dvals[i] = args[i]
					}
					if err := app.AppendRow(dvals...); err != nil {
						return err
					}
				}
			}
			if err := appSrc.Flush(); err != nil {
				return err
			}
			if err := appDst.Flush(); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		w := &Writer{tx: tx}
		for _, j := range jobs {
			if err := w.WriteLevelStatsSnapshot(j.Table, j.Depth, j.Pending, j.Successful, j.Failed, j.Completed, j.CopyP, j.CopyS, j.CopyF); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	}); err != nil {
		return err
	}
	if err := sb.db.Checkpoint(); err != nil {
		return err
	}
	sb.mu.Lock()
	if maxDepth > sb.lastFlushedDepth {
		sb.lastFlushedDepth = maxDepth
	}
	sb.cond.Broadcast()
	sb.mu.Unlock()
	return nil
}

// LastFlushedDepth returns the maximum depth that has been written to the DB by a completed Flush. -1 until first flush.
func (sb *SealBuffer) LastFlushedDepth() int {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.lastFlushedDepth
}

// WaitUntilFlushedThrough blocks until at least the given depth has been written by a completed Flush.
func (sb *SealBuffer) WaitUntilFlushedThrough(depth int) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	for sb.lastFlushedDepth < depth {
		sb.cond.Wait()
	}
}

func (sb *SealBuffer) flushLoop() {
	ticker := time.NewTicker(sb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-sb.stopCh:
			return
		case <-ticker.C:
			err := sb.Flush()
			if err != nil {
				fmt.Println("Error flushing jobs:", err)
				return
			}
		}
	}
}

// Stop stops the flush loop and flushes any remaining jobs.
func (sb *SealBuffer) Stop() {
	if atomic.CompareAndSwapInt32(&sb.stopped, 0, 1) {
		close(sb.stopCh)
	}
	err := sb.Flush()
	if err != nil {
		fmt.Println("Error flushing jobs:", err)
		return
	}
}
