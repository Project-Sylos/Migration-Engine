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
	defaultSealFlushInterval     = 10 * time.Second
	defaultSealFlushRowThreshold = 10_000
	defaultSealBufferHardCap     = 20_000
	checkpointRowThreshold       = 50_000 // run CHECKPOINT only after this many appender rows since last checkpoint
)

type sealStatsSnapshot struct {
	table     string
	depth     int
	pending   int64
	success   int64
	failed    int64
	completed int64
	copyP     int64
	copyS     int64
	copyF     int64
}

// SealJob is one sealed level's payload: table, depth, node metadata, status events, and stats.
type SealJob struct {
	Table      string
	Depth      int
	Nodes      []*NodeState
	Events     []StatusEvent
	Pending    int64
	Successful int64
	Failed     int64
	Completed  int64
	CopyP      int64
	CopyS      int64
	CopyF      int64
}

// phaseAppenders holds persistent appenders for the duration of a phase (traversal or copy).
// Created once at phase start, closed at phase end. Used by Flush() for one tx per flush.
type phaseAppenders struct {
	conn    *sql.Conn
	appSrc  *duckdb.Appender
	appDst  *duckdb.Appender
	appSrcEv *duckdb.Appender
	appDstEv *duckdb.Appender
}

// discoveryJobStatsSentinel marks a SealJob as discovery-only (nodes + events, no stats write). Used for traversal discovery and status events.
const discoveryJobStatsSentinel int64 = -1

// SealBuffer buffers seal jobs and discovery jobs (nodes + events only), flushes them to the DB asynchronously.
// Flush triggers: interval timer, row count threshold, and Stop/ForceFlush.
// When a phase is active (StartPhase called), Flush uses persistent appenders and one tx per flush.
// Discovery jobs use Pending == discoveryJobStatsSentinel so stats are not written.
type SealBuffer struct {
	db                *DB
	interval          time.Duration
	rowThreshold      int
	hardCap           int
	mu                sync.Mutex
	cond              *sync.Cond
	queue             []SealJob
	discoveryQueue    []SealJob // nodes + events only (no stats); flushed with queue, stats skipped when Pending < 0
	rowsSinceFlush    int
	lastFlushedDepth   int   // max depth written by completed Flush(); -1 until first flush
	rowsSinceCheckpoint  int64 // appender rows written since last CHECKPOINT
	pendingCheckpoint   bool  // set when threshold hit from legacyFlush; run Checkpoint after RunWrite returns
	stopCh              chan struct{}
	stopped             int32
	phase              *phaseAppenders // non-nil when phase is active (persistent appenders)
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
		db:             db,
		interval:       interval,
		rowThreshold:   rowThreshold,
		hardCap:        hardCap,
		queue:          make([]SealJob, 0, 64),
		discoveryQueue: make([]SealJob, 0, 64),
		stopCh:         make(chan struct{}),
	}
	sb.cond = sync.NewCond(&sb.mu)
	go sb.flushLoop()
	return sb
}

// Add enqueues a seal job. Status events are derived from nodes (one event per node with current traversal/copy status).
// Returns the error from Flush so callers can avoid removing from cache until data is confirmed in the buffer.
func (sb *SealBuffer) Add(table string, depth int, nodes []*NodeState, pending, successful, failed, completed, copyP, copyS, copyF int64) error {
	if table != "SRC" && table != "DST" {
		return nil
	}
	n := len(nodes)
	nodeCopy := make([]*NodeState, n)
	copy(nodeCopy, nodes)
	eventTime := time.Now().UnixNano()
	events := make([]StatusEvent, 0, n)
	for _, nd := range nodeCopy {
		trav := nd.TraversalStatus
		if trav == "" {
			trav = nd.Status
		}
		e := StatusEvent{ID: nd.ID, TraversalStatus: trav, EventTime: eventTime, Depth: depth}
		if table == "SRC" {
			e.CopyStatus = nd.CopyStatus
		}
		events = append(events, e)
	}
	sb.mu.Lock()
	sb.queue = append(sb.queue, SealJob{
		Table:      table,
		Depth:      depth,
		Nodes:      nodeCopy,
		Events:     events,
		Pending:    pending,
		Successful: successful,
		Failed:     failed,
		Completed:  completed,
		CopyP:      copyP,
		CopyS:      copyS,
		CopyF:      copyF,
	})
	sb.rowsSinceFlush += n
	sb.cond.Broadcast()
	sb.mu.Unlock()
	// Flush immediately after each seal so the queue holds at most one round (max 1 job).
	if err := sb.Flush(); err != nil {
		fmt.Println("Error flushing jobs:", err)
		return err
	}
	return nil
}

// AddDiscoveryNodes enqueues discovered nodes (and their initial status events) for async flush. No stats written (discovery job).
// Call from traversal completion; flush is async until Flush (e.g. before round advance).
func (sb *SealBuffer) AddDiscoveryNodes(ops []InsertOperation) {
	if len(ops) == 0 {
		return
	}
	eventTime := time.Now().UnixNano()
	// Group by (table, depth) so we emit one SealJob per group.
	type key struct{ table string; depth int }
	groups := make(map[key][]*NodeState)
	for _, op := range ops {
		if op.State == nil {
			continue
		}
		s := op.State
		if s.TraversalStatus == "" {
			s.TraversalStatus = op.Status
		}
		if s.Status == "" {
			s.Status = s.TraversalStatus
		}
		k := key{op.QueueType, op.Level}
		groups[k] = append(groups[k], s)
	}
	sb.mu.Lock()
	for k, nodes := range groups {
		events := make([]StatusEvent, 0, len(nodes))
		for _, n := range nodes {
			e := StatusEvent{ID: n.ID, TraversalStatus: n.TraversalStatus, EventTime: eventTime, Depth: k.depth}
			if k.table == "SRC" {
				e.CopyStatus = n.CopyStatus
			}
			events = append(events, e)
		}
		sb.discoveryQueue = append(sb.discoveryQueue, SealJob{
			Table: k.table, Depth: k.depth, Nodes: nodes, Events: events,
			Pending: discoveryJobStatsSentinel, Successful: discoveryJobStatsSentinel, Failed: discoveryJobStatsSentinel,
			Completed: discoveryJobStatsSentinel, CopyP: discoveryJobStatsSentinel, CopyS: discoveryJobStatsSentinel, CopyF: discoveryJobStatsSentinel,
		})
		sb.rowsSinceFlush += len(nodes)
	}
	exceedsThreshold := sb.rowsSinceFlush >= sb.rowThreshold
	sb.cond.Broadcast()
	sb.mu.Unlock()
	if exceedsThreshold {
		if err := sb.Flush(); err != nil {
			fmt.Println("Error flushing discovery (row threshold):", err)
		}
	}
}

// AddDiscoveryStatusEvent enqueues one status event (e.g. completed/failed) for async flush. No stats written.
func (sb *SealBuffer) AddDiscoveryStatusEvent(table string, e StatusEvent) {
	if table != "SRC" && table != "DST" {
		return
	}
	sb.mu.Lock()
	sb.discoveryQueue = append(sb.discoveryQueue, SealJob{
		Table: table, Depth: e.Depth, Nodes: nil, Events: []StatusEvent{e},
		Pending: discoveryJobStatsSentinel, Successful: discoveryJobStatsSentinel, Failed: discoveryJobStatsSentinel,
		Completed: discoveryJobStatsSentinel, CopyP: discoveryJobStatsSentinel, CopyS: discoveryJobStatsSentinel, CopyF: discoveryJobStatsSentinel,
	})
	sb.rowsSinceFlush++
	sb.cond.Broadcast()
	sb.mu.Unlock()
}

func (sb *SealBuffer) drain() []SealJob {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if len(sb.queue) == 0 && len(sb.discoveryQueue) == 0 {
		return nil
	}
	out := append(sb.queue, sb.discoveryQueue...)
	sb.queue = make([]SealJob, 0, cap(sb.queue))
	sb.discoveryQueue = make([]SealJob, 0, cap(sb.discoveryQueue))
	sb.rowsSinceFlush = 0
	sb.cond.Broadcast()
	return out
}

// StartPhase starts a phase (traversal or copy): conn is held for the phase; 4 appenders are created and reused until StopPhase.
// Call from DB.BeginTraversalPhase / BeginCopyPhase. Must not be called when a phase is already active.
func (sb *SealBuffer) StartPhase(conn *sql.Conn) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if sb.phase != nil {
		return fmt.Errorf("seal buffer: phase already active")
	}
	var pa phaseAppenders
	pa.conn = conn
	err := conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("seal buffer: conn is not driver.Conn")
		}
		var err error
		pa.appSrc, err = duckdb.NewAppenderFromConn(dc, "", tableSrcNodes)
		if err != nil {
			return err
		}
		pa.appDst, err = duckdb.NewAppenderFromConn(dc, "", tableDstNodes)
		if err != nil {
			pa.appSrc.Close()
			return err
		}
		pa.appSrcEv, err = duckdb.NewAppenderFromConn(dc, "", tableSrcStatusEvents)
		if err != nil {
			pa.appDst.Close()
			pa.appSrc.Close()
			return err
		}
		pa.appDstEv, err = duckdb.NewAppenderFromConn(dc, "", tableDstStatusEvents)
		if err != nil {
			pa.appSrcEv.Close()
			pa.appDst.Close()
			pa.appSrc.Close()
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	sb.phase = &pa
	return nil
}

// StopPhase flushes any remaining jobs (one tx), closes appenders and conn, and clears phase. Call from DB.EndTraversalPhase / EndCopyPhase.
func (sb *SealBuffer) StopPhase() error {
	sb.mu.Lock()
	pa := sb.phase
	sb.mu.Unlock()
	if pa == nil {
		return nil
	}
	if err := sb.Flush(); err != nil {
		return err
	}
	sb.mu.Lock()
	sb.phase = nil
	sb.mu.Unlock()
	_ = pa.appSrc.Close()
	_ = pa.appDst.Close()
	_ = pa.appSrcEv.Close()
	_ = pa.appDstEv.Close()
	return pa.conn.Close()
}

// phaseFlush runs one transaction per flush when phase is active: drain, BEGIN, append via persistent appenders, app.Flush(), stats in same tx, COMMIT.
func (sb *SealBuffer) phaseFlush(jobs []SealJob) error {
	if len(jobs) == 0 {
		return nil
	}
	sb.db.writeMu.Lock()
	defer sb.db.writeMu.Unlock()
	pa := sb.phase
	if pa == nil {
		return nil
	}
	ctx := context.Background()
	tx, err := pa.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	maxDepth := -1
	statsRows := make([]sealStatsSnapshot, 0, len(jobs))
	nodeDvals := make([]driver.Value, 0, 10)
	evDvals := make([]driver.Value, 0, 5)
	for _, j := range jobs {
		if j.Depth > maxDepth {
			maxDepth = j.Depth
		}
		if j.Pending >= 0 {
			statsRows = append(statsRows, sealStatsSnapshot{
				table:     j.Table,
				depth:     j.Depth,
				pending:   j.Pending,
				success:   j.Successful,
				failed:    j.Failed,
				completed: j.Completed,
				copyP:     j.CopyP,
				copyS:     j.CopyS,
				copyF:     j.CopyF,
			})
		}
		nodeApp := pa.appSrc
		evApp := pa.appSrcEv
		if j.Table == "DST" {
			nodeApp = pa.appDst
			evApp = pa.appDstEv
		}
		for _, n := range j.Nodes {
			args := NodeStateAppendRowArgs(n)
			nodeDvals = nodeDvals[:0]
			for _, v := range args {
				nodeDvals = append(nodeDvals, v)
			}
			if err := nodeApp.AppendRow(nodeDvals...); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		for _, e := range j.Events {
			var args []interface{}
			if j.Table == "SRC" {
				args = SrcStatusEventAppendRowArgs(&e)
			} else {
				args = DstStatusEventAppendRowArgs(&e)
			}
			evDvals = evDvals[:0]
			for _, v := range args {
				evDvals = append(evDvals, v)
			}
			if err := evApp.AppendRow(evDvals...); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	for _, app := range []*duckdb.Appender{pa.appSrc, pa.appDst, pa.appSrcEv, pa.appDstEv} {
		if err := app.Flush(); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if len(statsRows) > 0 {
		w := &Writer{tx: tx}
		if err := w.WriteLevelStatsSnapshotsBatch(statsRows); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	var totalRows int64
	for _, j := range jobs {
		totalRows += int64(len(j.Nodes) + len(j.Events))
	}
	sb.mu.Lock()
	if maxDepth > sb.lastFlushedDepth {
		sb.lastFlushedDepth = maxDepth
	}
	sb.rowsSinceCheckpoint += totalRows
	doCheckpoint := sb.rowsSinceCheckpoint >= checkpointRowThreshold
	if doCheckpoint {
		sb.rowsSinceCheckpoint = 0
	}
	sb.cond.Broadcast()
	sb.mu.Unlock()
	if doCheckpoint {
		if err := sb.db.Checkpoint(); err != nil {
			fmt.Println("error checkpointing after seal flush", err)
		}
	}
	return nil
}

// legacyFlush is used when no phase is active: RunWrite with per-flush appenders and two tx (append block + stats).
func (sb *SealBuffer) legacyFlush(jobs []SealJob) error {
	if len(jobs) == 0 {
		return nil
	}
	var totalRows int64
	for _, j := range jobs {
		totalRows += int64(len(j.Nodes) + len(j.Events))
	}
	maxDepth := -1
	ctx := context.Background()
	if err := sb.db.RunWrite(ctx, func(s *WriteSession) error {
		conn := s.Conn()
		statsRows := make([]sealStatsSnapshot, 0, len(jobs))
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
			appSrcEv, err := duckdb.NewAppenderFromConn(dc, "", tableSrcStatusEvents)
			if err != nil {
				return err
			}
			defer appSrcEv.Close()
			appDstEv, err := duckdb.NewAppenderFromConn(dc, "", tableDstStatusEvents)
			if err != nil {
				return err
			}
			defer appDstEv.Close()
			nodeDvals := make([]driver.Value, 0, 10)
			evDvals := make([]driver.Value, 0, 5)
			for _, j := range jobs {
				if j.Depth > maxDepth {
					maxDepth = j.Depth
				}
				if j.Pending >= 0 {
					statsRows = append(statsRows, sealStatsSnapshot{
						table:     j.Table,
						depth:     j.Depth,
						pending:   j.Pending,
						success:   j.Successful,
						failed:    j.Failed,
						completed: j.Completed,
						copyP:     j.CopyP,
						copyS:     j.CopyS,
						copyF:     j.CopyF,
					})
				}
				nodeApp := appSrc
				evApp := appSrcEv
				if j.Table == "DST" {
					nodeApp = appDst
					evApp = appDstEv
				}
				for _, n := range j.Nodes {
					args := NodeStateAppendRowArgs(n)
					nodeDvals = nodeDvals[:0]
					for _, v := range args {
						nodeDvals = append(nodeDvals, v)
					}
					if err := nodeApp.AppendRow(nodeDvals...); err != nil {
						return err
					}
				}
				for _, e := range j.Events {
					var args []interface{}
					if j.Table == "SRC" {
						args = SrcStatusEventAppendRowArgs(&e)
					} else {
						args = DstStatusEventAppendRowArgs(&e)
					}
					evDvals = evDvals[:0]
					for _, v := range args {
						evDvals = append(evDvals, v)
					}
					if err := evApp.AppendRow(evDvals...); err != nil {
						return err
					}
				}
			}
			for _, app := range []*duckdb.Appender{appSrc, appDst, appSrcEv, appDstEv} {
				if err := app.Flush(); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		sb.mu.Lock()
		if maxDepth > sb.lastFlushedDepth {
			sb.lastFlushedDepth = maxDepth
		}
		sb.rowsSinceCheckpoint += totalRows
		if sb.rowsSinceCheckpoint >= checkpointRowThreshold {
			sb.rowsSinceCheckpoint = 0
			sb.pendingCheckpoint = true
		}
		sb.cond.Broadcast()
		sb.mu.Unlock()
		if len(statsRows) > 0 {
			return s.WithTx(func(w *Writer) error {
				return w.WriteLevelStatsSnapshotsBatch(statsRows)
			})
		}
		return nil
	}); err != nil {
		return err
	}
	sb.mu.Lock()
	p := sb.pendingCheckpoint
	sb.pendingCheckpoint = false
	sb.mu.Unlock()
	if p {
		if err := sb.db.Checkpoint(); err != nil {
			fmt.Println("error checkpointing after seal flush", err)
		}
	}
	return nil
}

// Flush drains queued jobs and writes them to the DB. When a phase is active, uses persistent appenders and one tx per flush (append + stats). Otherwise uses legacy per-flush appenders.
// On write failure, jobs are re-queued so waiters in WaitUntilFlushedThrough do not block forever.
func (sb *SealBuffer) Flush() error {
	jobs := sb.drain()
	if len(jobs) == 0 {
		return nil
	}
	sb.mu.Lock()
	pa := sb.phase
	sb.mu.Unlock()
	var err error
	if pa != nil {
		err = sb.phaseFlush(jobs)
	} else {
		err = sb.legacyFlush(jobs)
	}
	if err != nil {
		sb.requeue(jobs)
		return err
	}
	return nil
}

// requeue puts jobs back on the queue and restores rowsSinceFlush. Call when a flush fails so data is not lost.
func (sb *SealBuffer) requeue(jobs []SealJob) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	rows := int64(0)
	for _, j := range jobs {
		rows += int64(len(j.Nodes))
	}
	sb.queue = append(sb.queue, jobs...)
	sb.rowsSinceFlush += int(rows)
	sb.cond.Broadcast()
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
	if sb.lastFlushedDepth >= depth {
		sb.mu.Unlock()
		return
	}
	for sb.lastFlushedDepth < depth {
		sb.cond.Wait()
	}
	sb.mu.Unlock()
}

func (sb *SealBuffer) flushLoop() {
	ticker := time.NewTicker(sb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-sb.stopCh:
			return
		case <-ticker.C:
			sb.mu.Lock()
			hasWork := sb.rowsSinceFlush > 0
			sb.mu.Unlock()
			if hasWork {
				err := sb.Flush()
				if err != nil {
					fmt.Println("Error flushing jobs:", err)
					return
				}
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
