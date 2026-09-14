// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

const (
	defaultBadgerSealRowThreshold  = 50_000
	defaultBadgerSealHardCap       = 100_000
	defaultBadgerSealFlushInterval = 10 * time.Second
)

type sealNodeJob struct {
	table          string
	depth          int
	node           *NodeState
	discoveryStats bool
}

type sealEventJob struct {
	table     string
	event     StatusEvent
	fromRetry bool
}

type sealMapJob struct {
	event IDMapEvent
}

// badgerSeal implements SealController by buffering worker results and flushing
// unique Badger keys plus SUM'd counters on a serialized flush path.
type badgerSeal struct {
	db  *DB
	ops *opsdb.Store

	mu               sync.Mutex
	cond             *sync.Cond
	flushMu          sync.Mutex
	lastFlushedDepth int
	hardAbort        int32
	rowsSinceFlush   int64
	rowThreshold     int
	hardCap          int
	interval         time.Duration
	lastFlushRows    int64
	lastFlushDurNs   int64

	telemetryHardCapHits int64
	telemetryHWM         int64
	telemetryFlushCount  int64
	hardCapWaiters       int32
	flushActive          int32

	nodes  []sealNodeJob
	events []sealEventJob
	maps   []sealMapJob
	errors []TaskErrorRecord
	gpl    []GPLIssue
	kidsReplace []opsdb.SealKidsReplace
	kidTickets  []opsdb.KidTicket
	failedSubtreePaths []string

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newBadgerSeal(db *DB, ops *opsdb.Store, opts SealBufferOptions) *badgerSeal {
	rowThreshold := opts.RowThreshold
	if rowThreshold <= 0 {
		rowThreshold = defaultBadgerSealRowThreshold
	}
	hardCap := opts.HardCap
	if hardCap <= 0 {
		hardCap = defaultBadgerSealHardCap
	}
	interval := opts.FlushInterval
	if interval <= 0 {
		interval = defaultBadgerSealFlushInterval
	}
	b := &badgerSeal{
		db:           db,
		ops:          ops,
		rowThreshold: rowThreshold,
		hardCap:      hardCap,
		interval:     interval,
		stopCh:       make(chan struct{}),
	}
	b.cond = sync.NewCond(&b.mu)
	b.wg.Add(1)
	go b.flushLoop()
	return b
}

func (b *badgerSeal) flushLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			_ = b.Flush()
		}
	}
}

func (b *badgerSeal) Add(table string, depth int, nodes []*NodeState, pending, successful, failed, completed, copyP, copyS, copyF int64) error {
	_ = pending
	_ = successful
	_ = failed
	_ = completed
	_ = copyP
	_ = copyS
	_ = copyF
	if len(nodes) == 0 {
		return b.Flush()
	}
	b.enqueueNodes(table, depth, nodes, false)
	return b.Flush()
}

func (b *badgerSeal) AddDiscoveryNodes(ops []InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}
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
	for k, nodes := range groups {
		b.enqueueNodes(k.table, k.depth, nodes, true)
	}
	if b.bufferedRows() >= int64(b.rowThreshold) {
		return b.Flush()
	}
	return nil
}

func (b *badgerSeal) AddDiscoveryStatusEvent(table string, e StatusEvent, fromRetry bool) {
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.events = append(b.events, sealEventJob{table: table, event: e, fromRetry: fromRetry})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	if exceeds {
		_ = b.Flush()
	}
}

func (b *badgerSeal) AddTaskError(rec TaskErrorRecord) {
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.errors = append(b.errors, rec)
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	if exceeds {
		_ = b.Flush()
	}
}

func (b *badgerSeal) AddFailedSubtreePath(parentPath string) {
	if parentPath == "" {
		return
	}
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.failedSubtreePaths = append(b.failedSubtreePaths, parentPath)
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *badgerSeal) AddGPLIssue(e GPLIssue) {
	if GPLDisabled || e.SrcID == "" {
		return
	}
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.gpl = append(b.gpl, e)
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	if exceeds {
		_ = b.Flush()
	}
}

func (b *badgerSeal) AddIDMapEvent(e IDMapEvent) {
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.maps = append(b.maps, sealMapJob{event: e})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	if exceeds {
		_ = b.Flush()
	}
}

func (b *badgerSeal) enqueueNodes(table string, depth int, nodes []*NodeState, discoveryStats bool) {
	if len(nodes) == 0 {
		return
	}
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	for _, n := range nodes {
		if n == nil {
			continue
		}
		b.nodes = append(b.nodes, sealNodeJob{table: table, depth: depth, node: n, discoveryStats: discoveryStats})
		b.rowsSinceFlush++
	}
	b.noteRowsLocked(b.rowsSinceFlush)
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *badgerSeal) bufferedRows() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rowsSinceFlush
}

func nodeFrontierDeltas(side string, n *NodeState, st opsdb.StatusRecord, discoveryStats bool) []opsdb.PendingDelta {
	if n == nil || n.ID == "" {
		return nil
	}
	nt := NormalizeQueueNodeType(n.Type)
	var deltas []opsdb.PendingDelta
	if discoveryStats && nt == NodeTypeFolder {
		trav := st.TraversalStatus
		if trav == "" {
			trav = StatusPending
		}
		if trav == StatusPending {
			deltas = append(deltas, opsdb.PendingDelta{Phase: opsdb.PhaseTrav, NodeType: NodeTypeFolder, Add: true, PendWasSet: false})
		}
	}
	if side != opsdb.SideSRC {
		return deltas
	}
	deltas = append(deltas, opsdb.PendingDelta{
		Phase: opsdb.PhaseCopy, NodeType: nt, Add: CopyStatusIsPending(st.CopyStatus), PendWasSet: false,
	})
	deltas = append(deltas, opsdb.PendingDelta{
		Phase: opsdb.PhaseDel, NodeType: nt, Add: DeleteStatusOnFrontier(st.DeleteStatus), PendWasSet: false,
	})
	return deltas
}

func statusEventFrontierDeltas(side string, e StatusEvent, nodeType string) []opsdb.PendingDelta {
	nt := NormalizeQueueNodeType(nodeType)
	var deltas []opsdb.PendingDelta
	if e.TraversalStatus != "" {
		if e.TraversalStatus == StatusPending {
			if nt == NodeTypeFolder {
				deltas = append(deltas, opsdb.PendingDelta{
					Phase: opsdb.PhaseTrav, NodeType: NodeTypeFolder, Add: true,
					PendWasSet: travPendingWasSet(e.PrevTraversalStatus),
				})
			}
		} else {
			deltas = append(deltas, opsdb.PendingDelta{
				Phase: opsdb.PhaseTrav, NodeType: nt, Add: false,
				PendWasSet: travPendingWasSet(e.PrevTraversalStatus),
				// Successful folders stay on pend:trav until the size fold drops the depth.
				RetainKey: nt == NodeTypeFolder && e.TraversalStatus == StatusSuccessful,
			})
		}
	}
	if side != opsdb.SideSRC {
		return deltas
	}
	// Empty copy/delete on a partial event means "unchanged", not "pending".
	// pend:copy is a covering index of copy_status=pending; leaving pending always
	// deletes the key (see applyPendingBatchTrusted). PendWasSet drives schedcnt only.
	if e.CopyStatus != "" {
		deltas = append(deltas, opsdb.PendingDelta{
			Phase: opsdb.PhaseCopy, NodeType: nt, Add: CopyStatusIsPending(e.CopyStatus),
			PendWasSet: copyPendingWasSet(e.PrevCopyStatus),
		})
	}
	if e.DeleteStatus != "" {
		deltas = append(deltas, opsdb.PendingDelta{
			Phase: opsdb.PhaseDel, NodeType: nt, Add: DeleteStatusOnFrontier(e.DeleteStatus),
			PendWasSet: deletePendingWasSet(e.PrevDeleteStatus),
		})
	}
	return deltas
}

func (b *badgerSeal) waitBelowHardCapLocked() {
	for b.rowsSinceFlush >= int64(b.hardCap) {
		atomic.AddInt64(&b.telemetryHardCapHits, 1)
		atomic.AddInt32(&b.hardCapWaiters, 1)
		b.cond.Wait()
		atomic.AddInt32(&b.hardCapWaiters, -1)
	}
}

func (b *badgerSeal) noteRowsLocked(cur int64) {
	for {
		prev := atomic.LoadInt64(&b.telemetryHWM)
		if cur <= prev || atomic.CompareAndSwapInt64(&b.telemetryHWM, prev, cur) {
			return
		}
	}
}

func (b *badgerSeal) Flush() error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	nodes := b.nodes
	events := b.events
	maps := b.maps
	errs := b.errors
	gpl := b.gpl
	subtreePaths := b.failedSubtreePaths
	kidsReplace := b.kidsReplace
	kidTickets := b.kidTickets
	rows := b.rowsSinceFlush
	b.nodes = nil
	b.events = nil
	b.maps = nil
	b.errors = nil
	b.gpl = nil
	b.failedSubtreePaths = nil
	b.kidsReplace = nil
	b.kidTickets = nil
	b.rowsSinceFlush = 0
	b.cond.Broadcast()
	b.mu.Unlock()

	if rows == 0 && len(nodes) == 0 && len(events) == 0 && len(maps) == 0 && len(errs) == 0 && len(gpl) == 0 && len(subtreePaths) == 0 && len(kidsReplace) == 0 && len(kidTickets) == 0 {
		return nil
	}

	atomic.AddInt32(&b.flushActive, 1)
	defer atomic.AddInt32(&b.flushActive, 0)

	start := time.Now()
	if err := b.flushBatch(nodes, events, maps, errs, gpl, kidsReplace, kidTickets); err != nil {
		b.requeue(nodes, events, maps, errs, gpl, subtreePaths, kidsReplace, kidTickets, rows)
		return err
	}
	for _, path := range subtreePaths {
		mut, err := b.db.Ops().PropagateCopyFailureUnderPath(path)
		if err != nil {
			return fmt.Errorf("propagate subtree failure for %s: %w", path, err)
		}
		if mut.Affected > 0 {
			deltas := []ReviewStatsDelta{
				{Key: ReviewKeyCopyPending, Delta: -mut.Affected},
				{Key: ReviewKeyCopyFailed, Delta: mut.Affected},
			}
			if mut.SelectedBytes != 0 {
				deltas = append(deltas, ReviewStatsDelta{Key: ReviewKeySizeSelected, Delta: -mut.SelectedBytes})
			}
			_ = b.db.ApplyReviewStatsDeltas(deltas)
		}
	}
	d := time.Since(start)
	b.lastFlushRows = rows
	b.lastFlushDurNs = d.Nanoseconds()
	atomic.AddInt64(&b.telemetryFlushCount, 1)
	b.db.RecordOp(OpBadgerSync, "", rows, d, nil)
	return nil
}

func (b *badgerSeal) AddKidTicket(side, parentID string, parentDepth int, kid opsdb.KidRecord) {
	if parentID == "" || kid.ID == "" {
		return
	}
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.kidTickets = append(b.kidTickets, opsdb.KidTicket{
		Side: side, ParentID: parentID, ParentDepth: parentDepth, Kid: kid,
	})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	if exceeds {
		_ = b.Flush()
	}
}

func (b *badgerSeal) AddKidsPackReplace(side, parentID string, kids []opsdb.KidRecord) {
	if parentID == "" {
		return
	}
	b.mu.Lock()
	b.waitBelowHardCapLocked()
	b.kidsReplace = append(b.kidsReplace, opsdb.SealKidsReplace{Side: side, ParentID: parentID, Kids: kids})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	if exceeds {
		_ = b.Flush()
	}
}

func (b *badgerSeal) flushBatch(nodes []sealNodeJob, events []sealEventJob, maps []sealMapJob, errs []TaskErrorRecord, gpl []GPLIssue, kidsReplace []opsdb.SealKidsReplace, kidTickets []opsdb.KidTicket) error {
	var review []ReviewStatsDelta
	var depth []DepthStatsDelta

	existing := map[string]bool{}
	if b.ops != nil && len(nodes) > 0 {
		bySide := map[string][]string{}
		for _, job := range nodes {
			if !job.discoveryStats || job.node == nil || job.node.ID == "" {
				continue
			}
			side := sideFromQueue(job.table)
			bySide[side] = append(bySide[side], job.node.ID)
		}
		for side, ids := range bySide {
			got, err := b.ops.BatchGetNode(side, ids)
			if err != nil {
				return fmt.Errorf("flush seal existence probe: %w", err)
			}
			for id := range got {
				existing[side+":"+id] = true
			}
		}
	}

	nodeWrites := make([]opsdb.SealNodeWrite, 0, len(nodes))
	// Existence probe only sees Badger. The same flush may enqueue the same id twice
	// (duplicate ListChildren rows, or two parents sealing before either commits).
	// Track ids accepted in this batch so rediscovery cannot double discovery stats.
	seenNew := map[string]bool{}
	for _, job := range nodes {
		n := job.node
		if n == nil || n.ID == "" {
			continue
		}
		side := sideFromQueue(job.table)
		key := side + ":" + n.ID
		// Retry rediscovery re-lists known children. Do not overwrite sealed status or
		// re-apply discovery review/frontier deltas for nodes that already exist.
		if job.discoveryStats && (existing[key] || seenNew[key]) {
			continue
		}
		st := statusFromNode(n)
		if side == opsdb.SideSRC && CopyStatusIsPending(st.CopyStatus) && st.CopyStatus == "" {
			st.CopyStatus = CopyStatusPending
		}
		nodeWrites = append(nodeWrites, opsdb.SealNodeWrite{
			Side:       side,
			Node:       nodeToOps(n),
			Status:     st,
			Depth:      job.depth,
			Deltas:     nodeFrontierDeltas(side, n, st, job.discoveryStats),
			InsertOnly: true,
		})
		if job.discoveryStats {
			seenNew[key] = true
			review = append(review, discoveryReviewDeltas(job.table, n)...)
			depth = append(depth, discoveryDepthStatsDeltas(job.table, []*NodeState{n})...)
		}
	}

	statusWrites := make([]opsdb.SealStatusWrite, 0, len(events))
	var logs []opsdb.LogRecord
	for _, job := range events {
		side := sideFromQueue(job.table)
		e := job.event
		if e.NodeType == "" {
			return fmt.Errorf("flush status event: missing node_type for id %s", e.ID)
		}
		statusWrites = append(statusWrites, opsdb.SealStatusWrite{
			Side:       side,
			ID:         e.ID,
			Status:     statusFromEvent(e),
			Depth:      e.Depth,
			Deltas:     statusEventFrontierDeltas(side, e, e.NodeType),
			PrevStatus: prevStatusFromEvent(e),
		})
		if rec, ok := taskFailureLog(e, side); ok {
			logs = append(logs, rec)
		}
		review = append(review, statusEventReviewDeltas(job.table, e, job.fromRetry)...)
		depth = append(depth, eventDepthStatsDeltas(job.table, e)...)
	}

	mapWrites := make([]opsdb.SealMapWrite, 0, len(maps))
	for _, job := range maps {
		mapWrites = append(mapWrites, opsdb.SealMapWrite{Map: idMapToOps(job.event), Depth: job.event.Depth})
	}

	taskErrs := make([]opsdb.TaskErrorRecord, 0, len(errs))
	now := time.Now()
	for _, rec := range errs {
		taskErrs = append(taskErrs, opsdb.TaskErrorRecord{
			QueueType: rec.QueueType,
			Phase:     rec.Phase,
			NodeID:    rec.NodeID,
			Message:   rec.Message,
			Attempts:  rec.Attempts,
			Path:      rec.Path,
			At:        now,
		})
	}

	merged, err := opsdb.MergeKidTickets(kidsReplace, kidTickets, func(side, parentID string) ([]opsdb.KidRecord, error) {
		packs, err := b.ops.BatchGetKids(side, []string{parentID})
		if err != nil {
			return nil, err
		}
		return packs[parentID], nil
	})
	if err != nil {
		return fmt.Errorf("merge kid tickets: %w", err)
	}
	sched, err := b.ops.WriteSealBatchTrusted(nodeWrites, statusWrites, merged, mapWrites, logs, taskErrs)
	if err != nil {
		return fmt.Errorf("flush seal batch: %w", err)
	}
	if len(gpl) > 0 {
		recs := make([]opsdb.GPLRecord, 0, len(gpl))
		for _, e := range gpl {
			recs = append(recs, opsdb.GPLRecord{
				SrcID: e.SrcID, Status: e.Status, ProposedName: e.ProposedName,
				IssuesJSON: e.IssuesJSON, UpdatedAt: e.UpdatedAt, DstAction: e.DstAction,
			})
		}
		if err := b.ops.BatchPutGPL(recs); err != nil {
			return fmt.Errorf("flush gpl: %w", err)
		}
	}
	if err := b.ops.ApplySchedCountDeltas(sched); err != nil {
		return fmt.Errorf("flush schedcnt: %w", err)
	}
	if err := b.db.applyOpsHotpathStats(review, depth); err != nil {
		return fmt.Errorf("flush review/depth stats: %w", err)
	}
	return nil
}

func (b *badgerSeal) requeue(nodes []sealNodeJob, events []sealEventJob, maps []sealMapJob, errs []TaskErrorRecord, gpl []GPLIssue, subtreePaths []string, kidsReplace []opsdb.SealKidsReplace, kidTickets []opsdb.KidTicket, rows int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes = append(nodes, b.nodes...)
	b.events = append(events, b.events...)
	b.maps = append(maps, b.maps...)
	b.errors = append(errs, b.errors...)
	b.gpl = append(gpl, b.gpl...)
	b.failedSubtreePaths = append(subtreePaths, b.failedSubtreePaths...)
	b.kidsReplace = append(kidsReplace, b.kidsReplace...)
	b.kidTickets = append(kidTickets, b.kidTickets...)
	b.rowsSinceFlush += rows
	b.noteRowsLocked(b.rowsSinceFlush)
	b.cond.Broadcast()
}

func (b *badgerSeal) WaitUntilFlushedThrough(depth int) {
	_ = b.Flush()
	b.mu.Lock()
	if depth > b.lastFlushedDepth {
		b.lastFlushedDepth = depth
	}
	b.mu.Unlock()
}

func (b *badgerSeal) IOWaitActive() bool {
	return atomic.LoadInt32(&b.flushActive) != 0 ||
		atomic.LoadInt32(&b.hardCapWaiters) != 0
}

func (b *badgerSeal) TelemetrySnapshot() SealBufferTelemetry {
	b.mu.Lock()
	current := b.rowsSinceFlush
	b.mu.Unlock()

	hwm := atomic.SwapInt64(&b.telemetryHWM, 0)
	if current > hwm {
		hwm = current
	}
	return SealBufferTelemetry{
		CurrentRows:              current,
		HWMSinceLastPoll:         hwm,
		HardCapHitsSinceLastPoll: atomic.SwapInt64(&b.telemetryHardCapHits, 0),
		FlushCountSinceLastPoll:  atomic.SwapInt64(&b.telemetryFlushCount, 0),
		LastFlushRows:            b.lastFlushRows,
		LastFlushDurationNs:      b.lastFlushDurNs,
	}
}

func (b *badgerSeal) LastFlushStats() SealFlushStats {
	return SealFlushStats{Rows: b.lastFlushRows, DurationNs: b.lastFlushDurNs}
}

func (b *badgerSeal) UpdateOptions(opts SealBufferOptions) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if opts.RowThreshold > 0 {
		b.rowThreshold = opts.RowThreshold
	}
	if opts.HardCap > 0 {
		b.hardCap = opts.HardCap
	}
	if opts.FlushInterval > 0 {
		b.interval = opts.FlushInterval
	}
}

func (b *badgerSeal) StartPhase() error {
	atomic.StoreInt32(&b.hardAbort, 0)
	return nil
}

func (b *badgerSeal) StopPhase() error { return b.Flush() }

func (b *badgerSeal) AbortPhase() { atomic.StoreInt32(&b.hardAbort, 1) }

func (b *badgerSeal) HardAborted() bool { return atomic.LoadInt32(&b.hardAbort) != 0 }

func (b *badgerSeal) OnCheckpointOK() {}

func (b *badgerSeal) Stop() {
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.wg.Wait()
	_ = b.Flush()
	_ = b.ops.Sync()
}
