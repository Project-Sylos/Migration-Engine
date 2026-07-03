// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	duckdb "github.com/marcboeker/go-duckdb"
)

const (
	defaultSealFlushInterval        = 10 * time.Second
	defaultSealFlushRowThreshold    = 20_000
	defaultSealBufferHardCap        = 40_000
	defaultCheckpointEveryRows      = 100_000
	defaultCheckpointMaxInterval    = 5 * time.Minute
	existingNodeIDChunkSize         = 2_000
)

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
	FromRetry  bool // event-only: this completion is from retry mode; subtract from PendingRetry when path zeros
}

// phaseAppenders holds persistent appenders for the duration of a phase (traversal or copy).
// Created once at phase start, closed at phase end. Used by Flush() for one tx per flush.
type phaseAppenders struct {
	conn     *sql.Conn
	appSrc   *duckdb.Appender
	appDst   *duckdb.Appender
	appSrcEv *duckdb.Appender
	appDstEv *duckdb.Appender
}

// discoveryJobStatsSentinel marks a SealJob as discovery-only (nodes + events, no stats write). Used for traversal discovery and status events.
const discoveryJobStatsSentinel int64 = -1

// anyToDriverValues converts []any to []driver.Value for DuckDB appender AppendRow.
func anyToDriverValues(a []any) []driver.Value {
	out := make([]driver.Value, len(a))
	for i, v := range a {
		out[i] = driver.Value(v)
	}
	return out
}

func appendNodesWithAppender(app *duckdb.Appender, table string, nodes []*NodeState) error {
	for _, n := range nodes {
		if err := app.AppendRow(anyToDriverValues(NodeStateAppendRowArgs(n))...); err != nil {
			return fmt.Errorf("append %s node %s: %w", table, n.ID, err)
		}
	}
	return nil
}

// missingNodesForAppender filters out node IDs that already exist in the target table so appender writes
// preserve the prior "ON CONFLICT DO NOTHING" behavior without staging tables.
func missingNodesForAppender(ctx context.Context, tx *sql.Tx, table string, nodes []*NodeState) ([]*NodeState, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n == nil || n.ID == "" {
			continue
		}
		if _, ok := seen[n.ID]; ok {
			continue
		}
		seen[n.ID] = struct{}{}
		ids = append(ids, n.ID)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	existing := make(map[string]struct{})
	for start := 0; start < len(ids); start += existingNodeIDChunkSize {
		end := start + existingNodeIDChunkSize
		if end > len(ids) {
			end = len(ids)
		}
		args := make([]any, 0, end-start)
		placeholders := make([]string, 0, end-start)
		for i, id := range ids[start:end] {
			args = append(args, id)
			placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
		}
		rows, err := tx.QueryContext(ctx,
			`SELECT id FROM `+table+` WHERE id IN (`+strings.Join(placeholders, ",")+`)`,
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("query existing %s ids: %w", table, err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan existing %s id: %w", table, err)
			}
			existing[id] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate existing %s ids: %w", table, err)
		}
		rows.Close()
	}
	out := make([]*NodeState, 0, len(nodes))
	for _, n := range nodes {
		if n == nil || n.ID == "" {
			continue
		}
		if _, ok := existing[n.ID]; ok {
			continue
		}
		out = append(out, n)
	}
	return out, nil
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

func addCanonicalReviewDelta(deltas map[string]int64, key string, delta int64) {
	if key == "" || delta == 0 {
		return
	}
	deltas[key] += delta
}

// buildCanonicalReviewStatsDeltas computes canonical stats-table deltas from buffered jobs.
// Discovery jobs add new node counts; event-only jobs shift counts from previous -> current status.
func buildCanonicalReviewStatsDeltas(jobs []SealJob) []ReviewStatsDelta {
	deltas := make(map[string]int64)
	for _, j := range jobs {
		if j.Pending == discoveryJobStatsSentinel && len(j.Nodes) > 0 {
			for _, e := range j.Events {
				trav := e.TraversalStatus
				if trav == "" {
					trav = StatusPending
				}
				addCanonicalReviewDelta(deltas, reviewKeyForTraversalStatus(trav), 1)
				if j.Table == "SRC" {
					copySt := e.CopyStatus
					if copySt == "" {
						copySt = CopyStatusPending
					}
					addCanonicalReviewDelta(deltas, reviewKeyForCopyStatus(copySt), 1)
				}
			}
			continue
		}
		if j.Pending != discoveryJobStatsSentinel || len(j.Nodes) > 0 || len(j.Events) == 0 {
			continue
		}
		for _, e := range j.Events {
			if e.PrevTraversalStatus != e.TraversalStatus {
				addCanonicalReviewDelta(deltas, reviewKeyForTraversalStatus(e.PrevTraversalStatus), -1)
				addCanonicalReviewDelta(deltas, reviewKeyForTraversalStatus(e.TraversalStatus), 1)
			}
			if j.Table == "SRC" && e.PrevCopyStatus != e.CopyStatus {
				addCanonicalReviewDelta(deltas, reviewKeyForCopyStatus(e.PrevCopyStatus), -1)
				addCanonicalReviewDelta(deltas, reviewKeyForCopyStatus(e.CopyStatus), 1)
			}
			if j.FromRetry && e.PrevTraversalStatus == StatusPending &&
				(e.TraversalStatus == StatusSuccessful || e.TraversalStatus == StatusFailed) {
				addCanonicalReviewDelta(deltas, ReviewKeyTraversalPendingRetry, -1)
			}
		}
	}
	out := make([]ReviewStatsDelta, 0, len(deltas))
	for key, delta := range deltas {
		if delta == 0 {
			continue
		}
		out = append(out, ReviewStatsDelta{Key: key, Delta: delta})
	}
	return out
}

// sealFlushContext returns a context for one seal-buffer DB transaction. If timeout <= 0, there is no deadline
// (flush runs until commit or a real error). If timeout > 0, enforces a wall-time cap for operators who want it.
func sealFlushContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

// SealBuffer buffers seal jobs and discovery jobs (nodes + events only), flushes them to the DB asynchronously.
// Flush triggers: interval timer, row count threshold, and Stop/ForceFlush.
// When a phase is active (StartPhase called), Flush uses persistent appenders and one tx per flush.
// Discovery jobs use Pending == discoveryJobStatsSentinel so stats are not written.
type SealBuffer struct {
	db                  *DB
	interval            time.Duration
	rowThreshold        int
	hardCap             int
	flushTimeout        time.Duration
	mu                  sync.Mutex
	cond                *sync.Cond
	queue               []SealJob
	discoveryQueue      []SealJob         // nodes + events only (no stats); flushed with queue, stats skipped when Pending < 0
	taskErrorsQueue     []TaskErrorRecord // buffered task errors; flushed with jobs
	failedSubtreePaths  []string          // SRC folder paths whose pending descendants should be marked failed; flushed with jobs
	rowsSinceFlush   int
	lastFlushedDepth int // max depth written by completed Flush(); -1 until first flush
	// Periodic CHECKPOINT: after successful flush, run if rowsSinceCheckpoint >= checkpointEveryRows or
	// time since lastCheckpointTime >= checkpointMaxInterval (whichever condition is met first).
	checkpointEveryRows    int
	checkpointMaxInterval  time.Duration
	// checkpointPending: thresholds met; actual CHECKPOINT runs at the start of a subsequent flush while writeMu
	// is held and before any flush transaction, avoiding DuckDB "other write transactions active" races.
	checkpointPending int32
	rowsSinceCheckpoint    int64
	lastCheckpointTime     time.Time
	stopCh                 chan struct{}
	stopped                int32
	phase                  *phaseAppenders // non-nil when phase is active (persistent appenders)
	// I/O wait instrumentation (atomic): suppress queue stall / progress timeouts while flushing or blocked on seal backpressure.
	flushActive    int32
	depthWaiters   int32
	hardCapWaiters int32
	// Autoscaler telemetry (atomic; HWM/hits/flushes reset on TelemetrySnapshot read).
	telemetryHWM          int64
	telemetryHardCapHits  int64
	telemetryFlushCount   int64
}

// SealBufferOptions configures the seal buffer. Zero value uses defaults.
type SealBufferOptions struct {
	FlushInterval time.Duration
	RowThreshold  int
	HardCap       int
	FlushTimeout  time.Duration // max wall time for one flush; 0 or negative = no deadline (default). Positive = optional cap.
	// CheckpointEveryRows: after each successful flush, CHECKPOINT when this many node+event rows have been written since the last checkpoint (default 500_000). Set <= 0 for default.
	CheckpointEveryRows int
	// CheckpointMaxInterval: also CHECKPOINT when this much wall time has passed since the last checkpoint (default 5m). Set <= 0 for default.
	CheckpointMaxInterval time.Duration
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
	flushTimeout := opts.FlushTimeout
	cpRows := opts.CheckpointEveryRows
	if cpRows <= 0 {
		cpRows = defaultCheckpointEveryRows
	}
	cpInterval := opts.CheckpointMaxInterval
	if cpInterval <= 0 {
		cpInterval = defaultCheckpointMaxInterval
	}
	sb := &SealBuffer{
		db:                    db,
		interval:              interval,
		rowThreshold:          rowThreshold,
		hardCap:               hardCap,
		flushTimeout:          flushTimeout,
		checkpointEveryRows:   cpRows,
		checkpointMaxInterval: cpInterval,
		lastCheckpointTime:    time.Now(),
		queue:                 make([]SealJob, 0, 64),
		discoveryQueue:        make([]SealJob, 0, 64),
		taskErrorsQueue:       make([]TaskErrorRecord, 0, 64),
		stopCh:                make(chan struct{}),
	}
	sb.cond = sync.NewCond(&sb.mu)
	go sb.flushLoop()
	return sb
}

// waitBelowHardCapLocked blocks until rowsSinceFlush < hardCap. sb.mu must be held.
func (sb *SealBuffer) waitBelowHardCapLocked() {
	for sb.rowsSinceFlush >= sb.hardCap {
		sb.noteHardCapHit()
		atomic.AddInt32(&sb.hardCapWaiters, 1)
		sb.cond.Wait()
		atomic.AddInt32(&sb.hardCapWaiters, -1)
	}
}

// IOWaitActive is true while a flush is running, a goroutine waits in WaitUntilFlushedThrough, or producers wait on hard-cap backpressure.
func (sb *SealBuffer) IOWaitActive() bool {
	return atomic.LoadInt32(&sb.flushActive) != 0 ||
		atomic.LoadInt32(&sb.depthWaiters) != 0 ||
		atomic.LoadInt32(&sb.hardCapWaiters) != 0
}

// onCheckpointOK resets periodic checkpoint counters after a successful CHECKPOINT (including calls from DB.Checkpoint).
func (sb *SealBuffer) onCheckpointOK() {
	sb.mu.Lock()
	sb.rowsSinceCheckpoint = 0
	sb.lastCheckpointTime = time.Now()
	sb.mu.Unlock()
}

// considerPeriodicCheckpointAfterSuccess records row volume and, when policy says a checkpoint is due,
// sets checkpointPending. The flush itself does not run CHECKPOINT here (avoids contention with other conns / txs).
func (sb *SealBuffer) considerPeriodicCheckpointAfterSuccess(totalWrittenRows int64) {
	if sb.db.path == ":memory:" {
		return
	}
	sb.mu.Lock()
	sb.rowsSinceCheckpoint += totalWrittenRows
	now := time.Now()
	should := sb.rowsSinceCheckpoint >= int64(sb.checkpointEveryRows) ||
		now.Sub(sb.lastCheckpointTime) >= sb.checkpointMaxInterval
	sb.mu.Unlock()
	if should {
		atomic.StoreInt32(&sb.checkpointPending, 1)
	}
}

// runDeferredCheckpointWithRetry runs CHECKPOINT if checkpointPending is set. Caller must hold db.writeMu.
// A few quick attempts avoid log spam; on failure leaves pending set so the next flush tries again (no long backoff under writeMu).
func (sb *SealBuffer) runDeferredCheckpointWithRetry() {
	if atomic.LoadInt32(&sb.checkpointPending) == 0 || sb.db.path == ":memory:" {
		return
	}
	const maxAttempts = 3
	const pause = 25 * time.Millisecond
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := sb.db.Checkpoint(); err == nil {
			atomic.StoreInt32(&sb.checkpointPending, 0)
			return
		}
		if attempt+1 < maxAttempts {
			time.Sleep(pause)
		}
	}
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
	sb.waitBelowHardCapLocked()
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
	sb.noteRowsLocked(int64(sb.rowsSinceFlush))
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
	type key struct {
		table string
		depth int
	}
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
	sb.waitBelowHardCapLocked()
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
	sb.waitBelowHardCapLocked()
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

// AddFailedSubtreePath enqueues an SRC folder path for subtree failure propagation.
// All pending descendants of this path will be marked as copy_status='failed' at the next flush
// (row threshold, flush ticker, or explicit FlushAppenderBuffer e.g. before round advance).
// Batched with other seal work; do not flush synchronously here.
func (sb *SealBuffer) AddFailedSubtreePath(parentPath string) {
	sb.mu.Lock()
	sb.waitBelowHardCapLocked()
	sb.failedSubtreePaths = append(sb.failedSubtreePaths, parentPath)
	sb.rowsSinceFlush++
	sb.cond.Broadcast()
	sb.mu.Unlock()
}

// AddTaskError enqueues one task error for async flush via the seal buffer.
func (sb *SealBuffer) AddTaskError(rec TaskErrorRecord) {
	sb.mu.Lock()
	sb.waitBelowHardCapLocked()
	sb.taskErrorsQueue = append(sb.taskErrorsQueue, rec)
	sb.rowsSinceFlush++
	sb.cond.Broadcast()
	sb.mu.Unlock()
}

func (sb *SealBuffer) drain() ([]SealJob, []TaskErrorRecord, []string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if len(sb.queue) == 0 && len(sb.discoveryQueue) == 0 && len(sb.taskErrorsQueue) == 0 && len(sb.failedSubtreePaths) == 0 {
		return nil, nil, nil
	}
	out := append(sb.queue, sb.discoveryQueue...)
	taskErrors := append([]TaskErrorRecord(nil), sb.taskErrorsQueue...)
	subtreePaths := append([]string(nil), sb.failedSubtreePaths...)
	sb.queue = make([]SealJob, 0, cap(sb.queue))
	sb.discoveryQueue = make([]SealJob, 0, cap(sb.discoveryQueue))
	sb.taskErrorsQueue = make([]TaskErrorRecord, 0, cap(sb.taskErrorsQueue))
	sb.failedSubtreePaths = sb.failedSubtreePaths[:0]
	sb.rowsSinceFlush = 0
	sb.noteFlushComplete()
	sb.cond.Broadcast()
	return out, taskErrors, subtreePaths
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

// phaseFlush runs one transaction per flush when phase is active: drain, BEGIN, append missing nodes/events via persistent appenders, task errors, subtree failure propagation, stats, COMMIT.
func (sb *SealBuffer) phaseFlush(jobs []SealJob, taskErrors []TaskErrorRecord, subtreePaths []string) error {
	if len(jobs) == 0 && len(taskErrors) == 0 {
		return nil
	}
	var totalRows int64
	err := func() error {
		sb.db.writeMu.Lock()
		defer sb.db.writeMu.Unlock()
		sb.runDeferredCheckpointWithRetry()
		pa := sb.phase
		if pa == nil {
			return nil
		}
		ctx, cancel := sealFlushContext(sb.flushTimeout)
		defer cancel()
		tx, err := pa.conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("phaseFlush begin tx: %w", err)
		}
		maxDepth := -1
		for _, j := range jobs {
			if j.Depth > maxDepth {
				maxDepth = j.Depth
			}
		}
		srcNodes, dstNodes, srcEvents, dstEvents := dedupeJobsByID(jobs)
		w := &Writer{tx: tx}
		srcNodes, err = missingNodesForAppender(ctx, tx, tableSrcNodes, srcNodes)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		dstNodes, err = missingNodesForAppender(ctx, tx, tableDstNodes, dstNodes)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		totalRows = int64(len(srcNodes) + len(dstNodes) + len(srcEvents) + len(dstEvents))
		if err := appendNodesWithAppender(pa.appSrc, tableSrcNodes, srcNodes); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := appendNodesWithAppender(pa.appDst, tableDstNodes, dstNodes); err != nil {
			_ = tx.Rollback()
			return err
		}
		// Batch nodes + events via appenders; same conn as tx so appender flush is part of this transaction.
		for _, e := range srcEvents {
			if err := pa.appSrcEv.AppendRow(anyToDriverValues(SrcStatusEventAppendRowArgs(&e))...); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("append src_status_event: %w", err)
			}
		}
		for _, e := range dstEvents {
			if err := pa.appDstEv.AppendRow(anyToDriverValues(DstStatusEventAppendRowArgs(&e))...); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("append dst_status_event: %w", err)
			}
		}
		for _, app := range []*duckdb.Appender{pa.appSrc, pa.appDst, pa.appSrcEv, pa.appDstEv} {
			if err := app.Flush(); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := flushFailureLogsFromEvents(w, srcEvents, "src"); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := flushFailureLogsFromEvents(w, dstEvents, "dst"); err != nil {
			_ = tx.Rollback()
			return err
		}
		for _, te := range taskErrors {
			if err := w.RecordTaskError(te.QueueType, te.Phase, te.NodeID, te.Message, te.Attempts, te.Path); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("record task error: %w", err)
			}
		}
		reviewDeltas := buildCanonicalReviewStatsDeltas(jobs)
		if len(reviewDeltas) > 0 {
			if err := w.ApplyReviewStatsDeltas(reviewDeltas); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		// Propagate subtree failures: mark all pending descendants of failed folders as failed.
		// Must run after appender flush so the CTE sees the folder's own events.
		for _, path := range subtreePaths {
			if _, err := w.PropagateSubtreeFailure(path); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("propagate subtree failure for %s: %w", path, err)
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		sb.mu.Lock()
		if maxDepth > sb.lastFlushedDepth {
			sb.lastFlushedDepth = maxDepth
		}
		sb.cond.Broadcast()
		sb.mu.Unlock()
		return nil
	}()
	if err != nil {
		return err
	}
	sb.considerPeriodicCheckpointAfterSuccess(totalRows)
	return nil
}

// legacyFlush is used when no phase is active: one tx with temporary appenders for missing nodes/events, task errors, stats, and subtree failure propagation.
func (sb *SealBuffer) legacyFlush(jobs []SealJob, taskErrors []TaskErrorRecord, subtreePaths []string) error {
	if len(jobs) == 0 && len(taskErrors) == 0 && len(subtreePaths) == 0 {
		return nil
	}
	maxDepth := -1
	for _, j := range jobs {
		if j.Depth > maxDepth {
			maxDepth = j.Depth
		}
	}
	srcNodes, dstNodes, srcEvents, dstEvents := dedupeJobsByID(jobs)
	var totalRows int64
	reviewDeltas := buildCanonicalReviewStatsDeltas(jobs)
	ctx, cancel := sealFlushContext(sb.flushTimeout)
	defer cancel()
	if err := sb.db.RunWrite(ctx, func(s *WriteSession) error {
		sb.runDeferredCheckpointWithRetry()
		conn := s.Conn()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("legacyFlush begin tx: %w", err)
		}
		srcNodes, err = missingNodesForAppender(ctx, tx, tableSrcNodes, srcNodes)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		dstNodes, err = missingNodesForAppender(ctx, tx, tableDstNodes, dstNodes)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		totalRows = int64(len(srcNodes) + len(dstNodes) + len(srcEvents) + len(dstEvents))
		var appSrc, appDst, appSrcEv, appDstEv *duckdb.Appender
		if err := conn.Raw(func(driverConn any) error {
			dc, ok := driverConn.(driver.Conn)
			if !ok {
				return fmt.Errorf("seal flush: conn is not driver.Conn")
			}
			appSrc, err = duckdb.NewAppenderFromConn(dc, "", tableSrcNodes)
			if err != nil {
				return err
			}
			appDst, err = duckdb.NewAppenderFromConn(dc, "", tableDstNodes)
			if err != nil {
				if appSrc != nil {
					_ = appSrc.Close()
				}
				return err
			}
			appSrcEv, err = duckdb.NewAppenderFromConn(dc, "", tableSrcStatusEvents)
			if err != nil {
				if appDst != nil {
					_ = appDst.Close()
				}
				if appSrc != nil {
					_ = appSrc.Close()
				}
				return err
			}
			appDstEv, err = duckdb.NewAppenderFromConn(dc, "", tableDstStatusEvents)
			if err != nil {
				if appSrcEv != nil {
					_ = appSrcEv.Close()
				}
				if appDst != nil {
					_ = appDst.Close()
				}
				if appSrc != nil {
					_ = appSrc.Close()
				}
				return err
			}
			return nil
		}); err != nil {
			_ = tx.Rollback()
			return err
		}
		defer func() {
			if appDstEv != nil {
				_ = appDstEv.Close()
			}
			if appSrcEv != nil {
				_ = appSrcEv.Close()
			}
			if appDst != nil {
				_ = appDst.Close()
			}
			if appSrc != nil {
				_ = appSrc.Close()
			}
		}()
		if err := appendNodesWithAppender(appSrc, tableSrcNodes, srcNodes); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := appendNodesWithAppender(appDst, tableDstNodes, dstNodes); err != nil {
			_ = tx.Rollback()
			return err
		}
		for _, e := range srcEvents {
			if err := appSrcEv.AppendRow(anyToDriverValues(SrcStatusEventAppendRowArgs(&e))...); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("append src_status_event: %w", err)
			}
		}
		for _, e := range dstEvents {
			if err := appDstEv.AppendRow(anyToDriverValues(DstStatusEventAppendRowArgs(&e))...); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("append dst_status_event: %w", err)
			}
		}
		for _, app := range []*duckdb.Appender{appSrc, appDst, appSrcEv, appDstEv} {
			if err := app.Flush(); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		w := &Writer{tx: tx}
		if err := flushFailureLogsFromEvents(w, srcEvents, "src"); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := flushFailureLogsFromEvents(w, dstEvents, "dst"); err != nil {
			_ = tx.Rollback()
			return err
		}
		for _, te := range taskErrors {
			if err := w.RecordTaskError(te.QueueType, te.Phase, te.NodeID, te.Message, te.Attempts, te.Path); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if len(reviewDeltas) > 0 {
			if err := w.ApplyReviewStatsDeltas(reviewDeltas); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		for _, path := range subtreePaths {
			if _, err := w.PropagateSubtreeFailure(path); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("propagate subtree failure for %s: %w", path, err)
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	sb.mu.Lock()
	if maxDepth > sb.lastFlushedDepth {
		sb.lastFlushedDepth = maxDepth
	}
	sb.cond.Broadcast()
	sb.mu.Unlock()
	sb.considerPeriodicCheckpointAfterSuccess(totalRows)
	return nil
}

// Flush drains queued jobs and task errors and writes them to the DB. When a phase is active, uses persistent appenders and one tx per flush (append + stats). Otherwise uses legacy per-flush appenders.
// On write failure, jobs and task errors are re-queued so waiters in WaitUntilFlushedThrough do not block forever.
func (sb *SealBuffer) Flush() error {
	atomic.StoreInt32(&sb.flushActive, 1)
	defer atomic.StoreInt32(&sb.flushActive, 0)

	jobs, taskErrors, subtreePaths := sb.drain()
	if len(jobs) == 0 && len(taskErrors) == 0 && len(subtreePaths) == 0 {
		sb.db.writeMu.Lock()
		sb.runDeferredCheckpointWithRetry()
		sb.db.writeMu.Unlock()
		return nil
	}
	sb.mu.Lock()
	pa := sb.phase
	sb.mu.Unlock()
	var err error
	if pa != nil {
		err = sb.phaseFlush(jobs, taskErrors, subtreePaths)
	} else {
		err = sb.legacyFlush(jobs, taskErrors, subtreePaths)
	}
	if err != nil {
		sb.requeue(jobs, taskErrors, subtreePaths)
		return err
	}
	return nil
}

// requeue puts jobs, task errors, and subtree paths back on the queue and restores rowsSinceFlush. Call when a flush fails so data is not lost.
func (sb *SealBuffer) requeue(jobs []SealJob, taskErrors []TaskErrorRecord, subtreePaths []string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	rows := int64(0)
	for _, j := range jobs {
		rows += int64(len(j.Nodes))
	}
	sb.queue = append(sb.queue, jobs...)
	sb.taskErrorsQueue = append(sb.taskErrorsQueue, taskErrors...)
	sb.failedSubtreePaths = append(sb.failedSubtreePaths, subtreePaths...)
	sb.rowsSinceFlush += int(rows) + len(taskErrors) + len(subtreePaths)
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
	atomic.AddInt32(&sb.depthWaiters, 1)
	defer atomic.AddInt32(&sb.depthWaiters, -1)
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
	consecutiveErrors := 0
	const maxConsecutiveErrors = 5
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
					consecutiveErrors++
					fmt.Printf("Error flushing jobs (attempt %d/%d): %v\n", consecutiveErrors, maxConsecutiveErrors, err)
					if consecutiveErrors >= maxConsecutiveErrors {
						fmt.Println("seal buffer: too many consecutive flush errors, stopping flush loop")
						return
					}
					// Backoff before next attempt (exponential: 1s, 2s, 4s, 8s, 16s)
					backoff := time.Duration(1<<(consecutiveErrors-1)) * time.Second
					if backoff > 16*time.Second {
						backoff = 16 * time.Second
					}
					time.Sleep(backoff)
				} else {
					consecutiveErrors = 0 // Reset on success
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
