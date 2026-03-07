// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
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

// SealJob is one sealed level's payload: table, depth, node metadata, status events, and stats.
type SealJob struct {
	Table       string
	Depth       int
	Nodes       []*NodeState
	Events      []StatusEvent
	Pending     int64
	Successful  int64
	Failed      int64
	Completed   int64
	CopyP       int64
	CopyS       int64
	CopyF       int64
	FromRetry   bool // event-only: this completion is from retry mode; subtract from PendingRetry when path zeros
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

// levelStatsKey identifies one (table, depth, key) for level stats.
type levelStatsKey struct {
	table string
	depth int
	key   string
}

// buildMergedLevelStats processes jobs in order: seal jobs set counts for that (table, depth); discovery jobs with nodes add +1 per event.
// Returns one tuple list per table for a single batch upsert (no duplicate keys). Event-only jobs are handled by RecomputeStatsForDepth.
func buildMergedLevelStats(jobs []SealJob) (srcTuples, dstTuples []struct {
	depth int
	key   string
	count int64
}) {
	counts := make(map[levelStatsKey]int64)
	for _, j := range jobs {
		if j.Pending >= 0 {
			// Seal: set totals for this (table, depth).
			t := j.Table
			d := j.Depth
			counts[levelStatsKey{t, d, StatsKeyTraversalStatus(StatusPending)}] = j.Pending
			counts[levelStatsKey{t, d, StatsKeyTraversalStatus(StatusSuccessful)}] = j.Successful
			counts[levelStatsKey{t, d, StatsKeyTraversalStatus(StatusFailed)}] = j.Failed
			counts[levelStatsKey{t, d, StatsKeyCompleted}] = j.Completed
			if t == "SRC" && j.CopyP >= 0 {
				counts[levelStatsKey{t, d, StatsKeyCopyStatus(CopyStatusPending)}] = j.CopyP
				counts[levelStatsKey{t, d, StatsKeyCopyStatus(CopyStatusSuccessful)}] = j.CopyS
				counts[levelStatsKey{t, d, StatsKeyCopyStatus(CopyStatusFailed)}] = j.CopyF
			}
			continue
		}
		if j.Pending != discoveryJobStatsSentinel || len(j.Nodes) == 0 {
			continue
		}
		// Discovery with nodes: add +1 per event.
		for _, e := range j.Events {
			trav := e.TraversalStatus
			if trav == "" {
				trav = StatusPending
			}
			k := levelStatsKey{j.Table, j.Depth, StatsKeyTraversalStatus(trav)}
			counts[k]++
			if j.Table == "SRC" {
				copySt := e.CopyStatus
				if copySt == "" {
					copySt = CopyStatusPending
				}
				if copySt != CopyStatusInProgress {
					counts[levelStatsKey{"SRC", j.Depth, StatsKeyCopyStatus(copySt)}]++
				}
			}
		}
	}
	for k, count := range counts {
		tup := struct {
			depth int
			key   string
			count int64
		}{k.depth, k.key, count}
		if k.table == "SRC" {
			srcTuples = append(srcTuples, tup)
		} else {
			dstTuples = append(dstTuples, tup)
		}
	}
	return srcTuples, dstTuples
}

func hasDiscoveryJobs(jobs []SealJob) bool {
	for _, j := range jobs {
		if j.Pending == discoveryJobStatsSentinel {
			return true
		}
	}
	return false
}

// dedupeJobsByID returns nodes and events from jobs deduped by (table, id); last occurrence wins. Prevents duplicate key on append.
func dedupeJobsByID(jobs []SealJob) (srcNodes, dstNodes []*NodeState, srcEvents, dstEvents []StatusEvent) {
	type key struct{ table, id string }
	nodesByKey := make(map[key]*NodeState)
	eventsByKey := make(map[key]StatusEvent)
	for _, j := range jobs {
		for _, n := range j.Nodes {
			nodesByKey[key{j.Table, n.ID}] = n
		}
		for _, e := range j.Events {
			eventsByKey[key{j.Table, e.ID}] = e
		}
	}
	for k, n := range nodesByKey {
		if k.table == "SRC" {
			srcNodes = append(srcNodes, n)
		} else {
			dstNodes = append(dstNodes, n)
		}
	}
	for k, e := range eventsByKey {
		if k.table == "SRC" {
			srcEvents = append(srcEvents, e)
		} else {
			dstEvents = append(dstEvents, e)
		}
	}
	return srcNodes, dstNodes, srcEvents, dstEvents
}

// discoveryRecomputeDepths returns (table, depth) pairs for discovery jobs that are event-only (status updates); stats for those depths must be recomputed.
func discoveryRecomputeDepths(jobs []SealJob) []struct{ Table string; Depth int } {
	seen := make(map[string]bool)
	var out []struct{ Table string; Depth int }
	for _, j := range jobs {
		if j.Pending != discoveryJobStatsSentinel || len(j.Nodes) > 0 || len(j.Events) == 0 {
			continue
		}
		k := j.Table + "\x00" + strconv.Itoa(j.Depth)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, struct{ Table string; Depth int }{j.Table, j.Depth})
	}
	return out
}

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

// AddDiscoveryStatusEvent enqueues one status event (e.g. completed/failed) for async flush. No stats written. fromRetry should be true when the completion is from retry mode so we subtract from PendingRetry when the path zeros.
func (sb *SealBuffer) AddDiscoveryStatusEvent(table string, e StatusEvent, fromRetry bool) {
	if table != "SRC" && table != "DST" {
		return
	}
	sb.mu.Lock()
	sb.discoveryQueue = append(sb.discoveryQueue, SealJob{
		Table: table, Depth: e.Depth, Nodes: nil, Events: []StatusEvent{e},
		Pending: discoveryJobStatsSentinel, Successful: discoveryJobStatsSentinel, Failed: discoveryJobStatsSentinel,
		Completed: discoveryJobStatsSentinel, CopyP: discoveryJobStatsSentinel, CopyS: discoveryJobStatsSentinel, CopyF: discoveryJobStatsSentinel,
		FromRetry: fromRetry,
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
	for _, j := range jobs {
		if j.Depth > maxDepth {
			maxDepth = j.Depth
		}
	}
	srcNodes, dstNodes, srcEvents, dstEvents := dedupeJobsByID(jobs)
	evDvals := make([]driver.Value, 0, 5)
	w := &Writer{tx: tx}
	if err := w.UpsertNodes(tableSrcNodes, srcNodes); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := w.UpsertNodes(tableDstNodes, dstNodes); err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, e := range srcEvents {
		for _, v := range SrcStatusEventAppendRowArgs(&e) {
			evDvals = append(evDvals, v)
		}
		if err := pa.appSrcEv.AppendRow(evDvals...); err != nil {
			_ = tx.Rollback()
			return err
		}
		evDvals = evDvals[:0]
	}
	for _, e := range dstEvents {
		for _, v := range DstStatusEventAppendRowArgs(&e) {
			evDvals = append(evDvals, v)
		}
		if err := pa.appDstEv.AppendRow(evDvals...); err != nil {
			_ = tx.Rollback()
			return err
		}
		evDvals = evDvals[:0]
	}
	for _, app := range []*duckdb.Appender{pa.appSrc, pa.appDst, pa.appSrcEv, pa.appDstEv} {
		if err := app.Flush(); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	srcTuples, dstTuples := buildMergedLevelStats(jobs)
	if len(srcTuples) > 0 {
		if err := w.UpsertStatsCounts("SRC", srcTuples); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if len(dstTuples) > 0 {
		if err := w.UpsertStatsCounts("DST", dstTuples); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	for _, d := range discoveryRecomputeDepths(jobs) {
		if err := w.RecomputeStatsForDepth(d.Table, d.Depth); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	totalRows := int64(len(srcNodes) + len(dstNodes) + len(srcEvents) + len(dstEvents))
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
	maxDepth := -1
	for _, j := range jobs {
		if j.Depth > maxDepth {
			maxDepth = j.Depth
		}
	}
	srcNodes, dstNodes, srcEvents, dstEvents := dedupeJobsByID(jobs)
	totalRows := int64(len(srcNodes) + len(dstNodes) + len(srcEvents) + len(dstEvents))
	srcTuples, dstTuples := buildMergedLevelStats(jobs)
	ctx := context.Background()
	if err := sb.db.RunWrite(ctx, func(s *WriteSession) error {
		conn := s.Conn()
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
			evDvals := make([]driver.Value, 0, 5)
			for _, e := range srcEvents {
				evDvals = evDvals[:0]
				for _, v := range SrcStatusEventAppendRowArgs(&e) {
					evDvals = append(evDvals, v)
				}
				if err := appSrcEv.AppendRow(evDvals...); err != nil {
					return err
				}
			}
			for _, e := range dstEvents {
				evDvals = evDvals[:0]
				for _, v := range DstStatusEventAppendRowArgs(&e) {
					evDvals = append(evDvals, v)
				}
				if err := appDstEv.AppendRow(evDvals...); err != nil {
					return err
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
		recomputeDepths := discoveryRecomputeDepths(jobs)
		needTx := len(srcTuples) > 0 || len(dstTuples) > 0 || len(recomputeDepths) > 0 || hasDiscoveryJobs(jobs)
		if needTx {
			return s.WithTx(func(w *Writer) error {
				if err := w.UpsertNodes(tableSrcNodes, srcNodes); err != nil {
					return err
				}
				if err := w.UpsertNodes(tableDstNodes, dstNodes); err != nil {
					return err
				}
				if len(srcTuples) > 0 {
					if err := w.UpsertStatsCounts("SRC", srcTuples); err != nil {
						return err
					}
				}
				if len(dstTuples) > 0 {
					if err := w.UpsertStatsCounts("DST", dstTuples); err != nil {
						return err
					}
				}
				for _, d := range recomputeDepths {
					if err := w.RecomputeStatsForDepth(d.Table, d.Depth); err != nil {
						return err
					}
				}
				return nil
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
