// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/memweight"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

const (
	defaultBadgerSealRowThreshold  = 50_000
	defaultBadgerSealHardCap       = 250_000
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

	mu                    sync.Mutex
	cond                  *sync.Cond
	flushMu               sync.Mutex
	lastFlushedDepth      int
	hardAbort             int32
	rowsSinceFlush        int64
	rowThreshold          int
	hardCap               int
	interval              time.Duration
	lastFlushRows         int64
	lastFlushDurNs        int64
	lastOrganicFlushRows  int64
	lastOrganicFlushDurNs int64

	telemetryHardCapHits            int64
	telemetryHardCapWaitNs          int64
	telemetryHWM                    int64
	telemetryFlushCount             int64
	telemetryFlushDurationNs        int64
	telemetryOrganicFlushCount      int64
	telemetryOrganicFlushDurationNs int64
	telemetryForcedFlushCount       int64
	telemetryForcedFlushDurationNs  int64
	flushActive                     int32

	nodes       []sealNodeJob
	events      []sealEventJob
	maps        []sealMapJob
	errors      []TaskErrorRecord
	gpl         []GPLIssue
	kidsReplace []opsdb.SealKidsReplace
	kidTickets  []opsdb.KidTicket

	// overflow accepts in-flight publishes while the main buffer is at the hard cap.
	// Producers return immediately and must not lease more work until flush drains it.
	overflowNodes       []sealNodeJob
	overflowEvents      []sealEventJob
	overflowMaps        []sealMapJob
	overflowErrors      []TaskErrorRecord
	overflowGPL         []GPLIssue
	overflowKidsReplace []opsdb.SealKidsReplace
	overflowKidTickets  []opsdb.KidTicket
	overflowRows        int64
	overflowFlushArmed  int32

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	memWeight *memweight.Ledger
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
			_ = b.flush(true)
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
	overflowed := false
	for k, nodes := range groups {
		if b.enqueueNodes(k.table, k.depth, nodes, true) {
			overflowed = true
		}
	}
	if overflowed || b.bufferedRows() < int64(b.rowThreshold) {
		return nil
	}
	return b.flush(true)
}

func (b *badgerSeal) AddDiscoveryStatusEvent(table string, e StatusEvent, fromRetry bool) {
	b.mu.Lock()
	if b.useOverflowLocked() {
		b.overflowEvents = append(b.overflowEvents, sealEventJob{table: table, event: e, fromRetry: fromRetry})
		b.overflowRows++
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(1)
		b.kickOverflowFlush()
		return
	}
	b.events = append(b.events, sealEventJob{table: table, event: e, fromRetry: fromRetry})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(1)
	if exceeds {
		_ = b.flush(true)
	}
}

func (b *badgerSeal) AddTaskError(rec TaskErrorRecord) {
	b.mu.Lock()
	if b.useOverflowLocked() {
		b.overflowErrors = append(b.overflowErrors, rec)
		b.overflowRows++
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(1)
		b.kickOverflowFlush()
		return
	}
	b.errors = append(b.errors, rec)
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(1)
	if exceeds {
		_ = b.flush(true)
	}
}

func (b *badgerSeal) AddGPLIssue(e GPLIssue) {
	if GPLDisabled || e.SrcID == "" {
		return
	}
	b.mu.Lock()
	if b.useOverflowLocked() {
		b.overflowGPL = append(b.overflowGPL, e)
		b.overflowRows++
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(1)
		b.kickOverflowFlush()
		return
	}
	b.gpl = append(b.gpl, e)
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(1)
	if exceeds {
		_ = b.flush(true)
	}
}

func (b *badgerSeal) AddIDMapEvent(e IDMapEvent) {
	b.mu.Lock()
	if b.useOverflowLocked() {
		b.overflowMaps = append(b.overflowMaps, sealMapJob{event: e})
		b.overflowRows++
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(1)
		b.kickOverflowFlush()
		return
	}
	b.maps = append(b.maps, sealMapJob{event: e})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(1)
	if exceeds {
		_ = b.flush(true)
	}
}

func (b *badgerSeal) enqueueNodes(table string, depth int, nodes []*NodeState, discoveryStats bool) (overflowed bool) {
	if len(nodes) == 0 {
		return
	}
	added := int64(0)
	b.mu.Lock()
	if b.useOverflowLocked() {
		for _, n := range nodes {
			if n == nil {
				continue
			}
			b.overflowNodes = append(b.overflowNodes, sealNodeJob{table: table, depth: depth, node: n, discoveryStats: discoveryStats})
			b.overflowRows++
			added++
		}
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(added)
		b.kickOverflowFlush()
		return true
	}
	for _, n := range nodes {
		if n == nil {
			continue
		}
		b.nodes = append(b.nodes, sealNodeJob{table: table, depth: depth, node: n, discoveryStats: discoveryStats})
		b.rowsSinceFlush++
		added++
	}
	b.noteRowsLocked(b.rowsSinceFlush)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(added)
	return false
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
			PendWasSet: CopyStatusIsPending(e.PrevCopyStatus),
		})
	}
	if e.DeleteStatus != "" {
		deltas = append(deltas, opsdb.PendingDelta{
			Phase: opsdb.PhaseDel, NodeType: nt, Add: DeleteStatusOnFrontier(e.DeleteStatus),
			PendWasSet: DeleteStatusOnFrontier(e.PrevDeleteStatus),
		})
	}
	return deltas
}

// useOverflowLocked reports that the main buffer is already at the hard cap (or an
// overflow publish is waiting). Caller must hold b.mu. The publish goes to overflow
// and must not flush on this goroutine.
func (b *badgerSeal) useOverflowLocked() bool {
	if b.rowsSinceFlush >= int64(b.hardCap) || b.overflowRows > 0 {
		atomic.AddInt64(&b.telemetryHardCapHits, 1)
		return true
	}
	return false
}

// kickOverflowFlush drains overflow off the producer goroutine.
func (b *badgerSeal) kickOverflowFlush() {
	if !atomic.CompareAndSwapInt32(&b.overflowFlushArmed, 0, 1) {
		return
	}
	go func() {
		_ = b.flush(true)
		atomic.StoreInt32(&b.overflowFlushArmed, 0)
	}()
}

// AtHardCap reports whether buffered rows are at or above the hard cap, or an overflow publish is waiting.
func (b *badgerSeal) AtHardCap() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rowsSinceFlush >= int64(b.hardCap) || b.overflowRows > 0
}

func (b *badgerSeal) noteRowsLocked(cur int64) {
	for {
		prev := atomic.LoadInt64(&b.telemetryHWM)
		if cur <= prev || atomic.CompareAndSwapInt64(&b.telemetryHWM, prev, cur) {
			return
		}
	}
}

func (b *badgerSeal) noteSealWeight(n int64) {
	if b == nil || b.memWeight == nil || n <= 0 {
		return
	}
	b.memWeight.Add(memweight.SealBuffer, n)
}

// Flush forces a drain (round advance, phase stop, queue complete, explicit save).
// Does not count toward organic seal-size efficiency probe stats.
func (b *badgerSeal) Flush() error {
	return b.flush(false)
}

func (b *badgerSeal) flush(organic bool) error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	nodes := append(b.nodes, b.overflowNodes...)
	events := append(b.events, b.overflowEvents...)
	maps := append(b.maps, b.overflowMaps...)
	errs := append(b.errors, b.overflowErrors...)
	gpl := append(b.gpl, b.overflowGPL...)
	kidsReplace := append(b.kidsReplace, b.overflowKidsReplace...)
	kidTickets := append(b.kidTickets, b.overflowKidTickets...)
	rows := b.rowsSinceFlush + b.overflowRows
	b.nodes = nil
	b.events = nil
	b.maps = nil
	b.errors = nil
	b.gpl = nil
	b.kidsReplace = nil
	b.kidTickets = nil
	b.overflowNodes = nil
	b.overflowEvents = nil
	b.overflowMaps = nil
	b.overflowErrors = nil
	b.overflowGPL = nil
	b.overflowKidsReplace = nil
	b.overflowKidTickets = nil
	b.rowsSinceFlush = 0
	b.overflowRows = 0
	b.cond.Broadcast()
	b.mu.Unlock()

	if rows == 0 && len(nodes) == 0 && len(events) == 0 && len(maps) == 0 && len(errs) == 0 && len(gpl) == 0 && len(kidsReplace) == 0 && len(kidTickets) == 0 {
		return nil
	}

	atomic.AddInt32(&b.flushActive, 1)
	defer atomic.AddInt32(&b.flushActive, 0)
	flushClock := b.db.Busy(BusyBackpressure)
	if organic {
		flushClock = b.db.Busy(BusyDBWork)
	}
	flushClock.Enter()
	defer flushClock.Leave()

	if b.memWeight != nil && rows > 0 {
		b.memWeight.Move(memweight.SealBuffer, memweight.SealFlush, rows)
	}

	start := time.Now()
	if err := b.flushBatch(nodes, events, maps, errs, gpl, kidsReplace, kidTickets); err != nil {
		if b.memWeight != nil && rows > 0 {
			b.memWeight.Move(memweight.SealFlush, memweight.SealBuffer, rows)
		}
		b.requeue(nodes, events, maps, errs, gpl, kidsReplace, kidTickets, rows)
		return err
	}
	if b.memWeight != nil && rows > 0 {
		b.memWeight.Release(memweight.SealFlush, rows)
	}
	d := time.Since(start)
	ns := d.Nanoseconds()
	b.lastFlushRows = rows
	b.lastFlushDurNs = ns
	atomic.AddInt64(&b.telemetryFlushCount, 1)
	atomic.AddInt64(&b.telemetryFlushDurationNs, ns)
	if organic {
		b.lastOrganicFlushRows = rows
		b.lastOrganicFlushDurNs = ns
		atomic.AddInt64(&b.telemetryOrganicFlushCount, 1)
		atomic.AddInt64(&b.telemetryOrganicFlushDurationNs, ns)
	} else {
		atomic.AddInt64(&b.telemetryForcedFlushCount, 1)
		atomic.AddInt64(&b.telemetryForcedFlushDurationNs, ns)
	}
	b.db.RecordOp(OpBadgerSync, "", rows, d, nil)
	return nil
}

func (b *badgerSeal) AddKidTicket(side, parentID string, parentDepth int, kid opsdb.KidRecord) {
	if parentID == "" || kid.ID == "" {
		return
	}
	b.mu.Lock()
	if b.useOverflowLocked() {
		b.overflowKidTickets = append(b.overflowKidTickets, opsdb.KidTicket{
			Side: side, ParentID: parentID, ParentDepth: parentDepth, Kid: kid,
		})
		b.overflowRows++
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(1)
		b.kickOverflowFlush()
		return
	}
	b.kidTickets = append(b.kidTickets, opsdb.KidTicket{
		Side: side, ParentID: parentID, ParentDepth: parentDepth, Kid: kid,
	})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(1)
	if exceeds {
		_ = b.flush(true)
	}
}

func (b *badgerSeal) AddKidsPackReplace(side, parentID string, kids []opsdb.KidRecord) {
	if parentID == "" {
		return
	}
	b.mu.Lock()
	if b.useOverflowLocked() {
		b.overflowKidsReplace = append(b.overflowKidsReplace, opsdb.SealKidsReplace{Side: side, ParentID: parentID, Kids: kids})
		b.overflowRows++
		b.cond.Broadcast()
		b.mu.Unlock()
		b.noteSealWeight(1)
		b.kickOverflowFlush()
		return
	}
	b.kidsReplace = append(b.kidsReplace, opsdb.SealKidsReplace{Side: side, ParentID: parentID, Kids: kids})
	b.rowsSinceFlush++
	b.noteRowsLocked(b.rowsSinceFlush)
	exceeds := b.rowsSinceFlush >= int64(b.rowThreshold)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.noteSealWeight(1)
	if exceeds {
		_ = b.flush(true)
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
	// Status/frontier seal stays in ops; log rows belong only in the logs sidecar.
	sched, err := b.ops.WriteSealBatchTrusted(nodeWrites, statusWrites, merged, mapWrites, taskErrs)
	if err != nil {
		return fmt.Errorf("flush seal batch: %w", err)
	}
	if len(logs) > 0 {
		if b.db == nil || b.db.Logs() == nil {
			return fmt.Errorf("flush seal logs: logs sidecar not open")
		}
		if err := b.db.Logs().AppendLogs(logs); err != nil {
			return fmt.Errorf("flush seal logs: %w", err)
		}
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

func (b *badgerSeal) requeue(nodes []sealNodeJob, events []sealEventJob, maps []sealMapJob, errs []TaskErrorRecord, gpl []GPLIssue, kidsReplace []opsdb.SealKidsReplace, kidTickets []opsdb.KidTicket, rows int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes = append(nodes, b.nodes...)
	b.events = append(events, b.events...)
	b.maps = append(maps, b.maps...)
	b.errors = append(errs, b.errors...)
	b.gpl = append(gpl, b.gpl...)
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
	return atomic.LoadInt32(&b.flushActive) != 0
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
		CurrentRows:                         current,
		HWMSinceLastPoll:                    hwm,
		HardCapHitsSinceLastPoll:            atomic.SwapInt64(&b.telemetryHardCapHits, 0),
		HardCapWaitNsSinceLastPoll:          atomic.SwapInt64(&b.telemetryHardCapWaitNs, 0),
		FlushCountSinceLastPoll:             atomic.SwapInt64(&b.telemetryFlushCount, 0),
		FlushDurationNsSinceLastPoll:        atomic.SwapInt64(&b.telemetryFlushDurationNs, 0),
		LastFlushRows:                       b.lastFlushRows,
		LastFlushDurationNs:                 b.lastFlushDurNs,
		OrganicFlushCountSinceLastPoll:      atomic.SwapInt64(&b.telemetryOrganicFlushCount, 0),
		OrganicFlushDurationNsSinceLastPoll: atomic.SwapInt64(&b.telemetryOrganicFlushDurationNs, 0),
		LastOrganicFlushRows:                b.lastOrganicFlushRows,
		LastOrganicFlushDurationNs:          b.lastOrganicFlushDurNs,
		ForcedFlushCountSinceLastPoll:       atomic.SwapInt64(&b.telemetryForcedFlushCount, 0),
		ForcedFlushDurationNsSinceLastPoll:  atomic.SwapInt64(&b.telemetryForcedFlushDurationNs, 0),
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

func (b *badgerSeal) AbortPhase() { atomic.StoreInt32(&b.hardAbort, 1) }

func (b *badgerSeal) HardAborted() bool { return atomic.LoadInt32(&b.hardAbort) != 0 }

func (b *badgerSeal) ClearHardAbort() { atomic.StoreInt32(&b.hardAbort, 0) }

func (b *badgerSeal) OnCheckpointOK() {}

func (b *badgerSeal) Stop() {
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.wg.Wait()
	_ = b.Flush()
	_ = b.ops.Sync()
}

func prevStatusFromEvent(e StatusEvent) opsdb.StatusRecord {
	return opsdb.StatusRecord{
		TraversalStatus: e.PrevTraversalStatus,
		CopyStatus:      e.PrevCopyStatus,
		DeleteStatus:    e.PrevDeleteStatus,
		GPLStatus:       e.PrevGPLStatus,
	}
}

func travPendingWasSet(s string) bool {
	return s == "" || s == StatusPending
}
