// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// Options configures Badger-only per-migration open (no Duck catalog).
type Options struct {
	Path          string             // Logical migration path (historically *.db); OpsDir defaults from this
	OpsDir        string             // Badger ops dir; default MigrationOpsPath(Path)
	EncryptionKey []byte             // unused for Badger Open (kept for API compatibility)
	SealBuffer    *SealBufferOptions // seal buffer knobs when attached
	// MemoryLimitGB is a configured memory hint (GB). 0 = auto from host free RAM.
	MemoryLimitGB int
}

// DefaultOptions returns default options.
func DefaultOptions() Options {
	return Options{Path: ":memory:"}
}

// CatalogSyncStats is a legacy progress snapshot (always empty on Badger-only stores).
type CatalogSyncStats struct{}

// DB is the per-migration Badger-only handle (ops store under OpsDir).
type DB struct {
	path          string
	writeMu       sync.Mutex
	sealBuffer    SealController
	pullGate      *PullGate
	memoryLimitGB int
	opsStore      *opsdb.Store
	ops           *dbOpRecorder

	disableQueryTimeout int32
	activity            atomic.Value // string
}

// Open opens the per-migration Badger ops store. Path is the logical migration
// path (historically *.db); OpsDir defaults to MigrationOpsPath(Path). No DuckDB.
func Open(opts Options) (*DB, error) {
	path := opts.Path
	if path == "" {
		path = ":memory:"
	}
	opsDir := opts.OpsDir
	if opsDir == "" {
		opsDir = MigrationOpsPath(path)
	}
	if opsDir == "" {
		return nil, fmt.Errorf("Badger ops dir required (set Options.OpsDir for :memory:)")
	}
	opsStore, err := opsdb.Open(opsdb.Options{Dir: opsDir})
	if err != nil {
		return nil, err
	}
	db := &DB{path: path, pullGate: newPullGate(), opsStore: opsStore, memoryLimitGB: ResolveMemoryLimitGB(opts.MemoryLimitGB)}
	sealOpts := SealBufferOptions{}
	if opts.SealBuffer != nil {
		sealOpts = *opts.SealBuffer
	}
	db.AttachSeal(newBadgerSeal(db, opsStore, sealOpts))
	db.startOpRecorder()
	return db, nil
}

// LogsDBForWrite returns the DB handle used for log persistence (Badger ops store).
func (db *DB) LogsDBForWrite() *DB {
	return db
}

// Close closes the ops store after flushing the seal buffer and recorded ops.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	if db.sealBuffer != nil {
		db.sealBuffer.Stop()
	}
	var errs []error
	db.stopOpRecorder(db.HardAborted())
	if db.opsStore != nil {
		if err := db.opsStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Path returns the database file path (or ":memory:").
func (db *DB) Path() string {
	return db.path
}

// Ops returns the Badger operational store.
func (db *DB) Ops() *opsdb.Store {
	if db == nil {
		return nil
	}
	return db.opsStore
}

// WaitCatalogDrained is a legacy Duck catalog-ingest barrier; Badger seal is synchronous.
func (db *DB) WaitCatalogDrained(ctx context.Context) error {
	_ = ctx
	return nil
}

// NotifyCatalogRoundSealed is a legacy Duck catalog-ingest hook; Badger indexes are written on node insert.
func (db *DB) NotifyCatalogRoundSealed(string, string, int) {}

// CatalogSyncStats is a legacy Duck catalog progress snapshot (always empty on Badger-only stores).
func (db *DB) CatalogSyncStats() CatalogSyncStats {
	return CatalogSyncStats{}
}

// DropPendingPrefix removes a round frontier prefix in Badger.
func (db *DB) DropPendingPrefix(side, phase string, depth int, nodeType string) error {
	if db == nil || db.opsStore == nil {
		return nil
	}
	return db.opsStore.DropPendingPrefix(side, phase, depth, nodeType)
}

// GetSchedCountAtDepth returns the delta-maintained pending count at side/phase/depth/type (O(1)).
func (db *DB) GetSchedCountAtDepth(side, phase string, depth int, nodeType string) (int64, error) {
	if db == nil || db.opsStore == nil {
		return 0, nil
	}
	return db.opsStore.GetSchedCountAtDepth(side, phase, depth, nodeType)
}

// Checkpoint is a no-op on Badger-only stores.
func (db *DB) Checkpoint() error {
	return nil
}

// CheckpointWithRetry is a no-op on Badger-only stores.
func (db *DB) CheckpointWithRetry(ctx context.Context, maxAttempts int) error {
	return nil
}

// AddNodeDeletions deletes multiple nodes from the Badger ops store (retry DST cleanup).
func (db *DB) AddNodeDeletions(deletions []NodeDeletion) error {
	if len(deletions) == 0 {
		return nil
	}
	if db == nil || db.opsStore == nil {
		return fmt.Errorf("ops store required")
	}
	for _, d := range deletions {
		side := opsdb.SideSRC
		if d.Table == "DST" {
			side = opsdb.SideDST
		}
		if err := db.Ops().DeleteNode(side, d.NodeID); err != nil {
			return err
		}
	}
	return nil
}

// NodeDeletion represents a node delete (retry DST cleanup).
type NodeDeletion struct {
	Table  string
	NodeID string
}

// AcquirePull waits for the shared frontier-pull ticket.
func (db *DB) AcquirePull(onWaitBeat func()) {
	if db == nil {
		return
	}
	db.pullGate.Acquire(onWaitBeat)
}

// ReleasePull returns the shared frontier-pull ticket.
func (db *DB) ReleasePull() {
	if db == nil {
		return
	}
	db.pullGate.Release()
}

// SealLevel persists a sealed level from memory cache to the DB (bulk append + stats snapshot). copyP/copyS/copyF are used for SRC copy stats when >= 0.
// Payload is enqueued to the seal buffer, which flushes asynchronously. CHECKPOINT policy: periodic (rows + interval, see SealBufferOptions), phase end (migration runners), soft suspend, seeding, and Close.
func (db *DB) SealLevel(table string, depth int, nodes []*NodeState, pending, successful, failed, completed int64, copyP, copyS, copyF int64) error {
	if table != "SRC" && table != "DST" {
		return nil
	}
	return db.sealBuffer.Add(table, depth, nodes, pending, successful, failed, completed, copyP, copyS, copyF)
}

// AddSealNodes writes a subset of nodes and their status events to the seal buffer (no level stats).
// Use when removing nodes from cache after DST task completion or SRC early completion; only remove from cache if this returns nil.
func (db *DB) AddSealNodes(table string, depth int, nodes []*NodeState) error {
	if table != "SRC" && table != "DST" || len(nodes) == 0 {
		return nil
	}
	return db.sealBuffer.Add(table, depth, nodes, 0, 0, 0, 0, 0, 0, 0)
}

// SealLevelDepth0 emits status events for depth-0 nodes through the Badger seal path.
func (db *DB) SealLevelDepth0(table string, nodes []*NodeState) error {
	if table != "SRC" && table != "DST" || len(nodes) == 0 {
		return nil
	}
	eventTime := time.Now().UnixNano()
	for _, nd := range nodes {
		if nd == nil {
			continue
		}
		trav := nd.TraversalStatus
		if trav == "" {
			trav = nd.Status
		}
		ev := StatusEvent{ID: nd.ID, TraversalStatus: trav, EventTime: eventTime, Depth: 0, NodeType: nd.Type}
		if table == "SRC" {
			ev.CopyStatus = nd.CopyStatus
		}
		db.AppendStatusEvent(table, ev, false)
	}
	return db.Flush(context.Background())
}

// Flush drains pending seal jobs to the DB. Honors ctx cancellation before starting.
func (db *DB) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.SetActivity("Saving: writing buffered progress to database")
	defer db.SetActivity("")
	return db.sealBuffer.Flush()
}

// WaitUntilFlushedThrough blocks until the seal buffer has written at least the given depth (for backpressure: don't run more than one round ahead of flushed state).
func (db *DB) WaitUntilFlushedThrough(depth int) {
	if db == nil || db.sealBuffer == nil {
		return
	}
	db.sealBuffer.WaitUntilFlushedThrough(depth)
}

// SealIOWaitActive reports whether the seal buffer is flushing, waiting on flush depth, or producers are blocked on hard-cap backpressure. Queue stall and progress watchdogs use this to avoid false stalls during long seal I/O.
func (db *DB) SealIOWaitActive() bool {
	return db != nil && db.sealBuffer != nil && db.sealBuffer.IOWaitActive()
}

// SealBufferTelemetry returns seal buffer gauges and read-and-reset event counters.
func (db *DB) SealBufferTelemetry() SealBufferTelemetry {
	if db == nil || db.sealBuffer == nil {
		return SealBufferTelemetry{}
	}
	return db.sealBuffer.TelemetrySnapshot()
}

// LastSealFlushStats returns the last successful seal flush gauges (not reset on read).
func (db *DB) LastSealFlushStats() SealFlushStats {
	var out SealFlushStats
	db.withSealBuffer(func(sb SealController) {
		out = sb.LastFlushStats()
	})
	return out
}

// UpdateSealBufferOptions hot-updates seal buffer tuning knobs.
func (db *DB) UpdateSealBufferOptions(opts SealBufferOptions) {
	if db != nil && db.sealBuffer != nil {
		db.sealBuffer.UpdateOptions(opts)
	}
}

// AppendDiscoveredNodes writes discovered nodes (and their initial status) through the seal controller.
// Node, status, and pending frontier keys are one unit: any write failure is returned.
func (db *DB) AppendDiscoveredNodes(ops []InsertOperation) error {
	if db.sealBuffer == nil || len(ops) == 0 {
		return nil
	}
	return db.sealBuffer.AddDiscoveryNodes(ops)
}

// AppendStatusEvent adds a status event (e.g. completed/failed) to the seal buffer discovery queue.
// Returns true when the event was accepted into the seal write buffer. Callers that bump live
// progress counters must only count after a true return. fromRetry should be true when the
// completion is from retry mode so we decrement PendingRetry (not Pending) when the path zeros.
func (db *DB) AppendStatusEvent(table string, e StatusEvent, fromRetry bool) bool {
	if db.sealBuffer != nil {
		db.sealBuffer.AddDiscoveryStatusEvent(table, e, fromRetry)
		return true
	}
	return false
}

// AppendTaskError adds a task error to the seal buffer for async flush. Call from workers on failure (replaces immediate RecordTaskError).
func (db *DB) AppendTaskError(queueType, phase, nodeID, message string, attempts int, path string) {
	if db.sealBuffer != nil {
		db.sealBuffer.AddTaskError(TaskErrorRecord{
			QueueType: queueType,
			Phase:     phase,
			NodeID:    nodeID,
			Message:   message,
			Attempts:  attempts,
			Path:      path,
		})
	}
}

func (db *DB) withSealBuffer(fn func(SealController)) {
	if db.sealBuffer != nil {
		fn(db.sealBuffer)
	}
}

// AppendFailedSubtree enqueues an SRC folder path for subtree failure propagation.
// At the next flush, all pending descendants will be marked as copy_status='failed'.
func (db *DB) AppendFailedSubtree(parentPath string) {
	if db == nil || db.sealBuffer == nil {
		return
	}
	db.sealBuffer.AddFailedSubtreePath(parentPath)
}

// AppendGPLIssue enqueues a sparse GPL review row for async seal flush.
func (db *DB) AppendGPLIssue(e GPLIssue) {
	if db == nil || db.sealBuffer == nil {
		return
	}
	db.sealBuffer.AddGPLIssue(e)
}

// AppendKidTicket appends one child to a parent's kids pack and tickets that parent for the size fold.
func (db *DB) AppendKidTicket(side, parentID string, parentDepth int, kid opsdb.KidRecord) error {
	if db == nil {
		return nil
	}
	if db.sealBuffer != nil {
		db.sealBuffer.AddKidTicket(side, parentID, parentDepth, kid)
		return nil
	}
	if db.Ops() == nil {
		return nil
	}
	return db.Ops().AppendKidTicket(side, parentID, parentDepth, kid)
}

// AppendKidsPackReplace enqueues an authoritative kids:{side}:{parent} snapshot for async flush.
func (db *DB) AppendKidsPackReplace(side, parentID string, kids []opsdb.KidRecord) {
	if db == nil || db.sealBuffer == nil || parentID == "" {
		return
	}
	db.sealBuffer.AddKidsPackReplace(side, parentID, kids)
}

// AppendIDMapEvent enqueues an id_map row for async seal flush.
func (db *DB) AppendIDMapEvent(e IDMapEvent) {
	if db == nil || db.sealBuffer == nil {
		return
	}
	db.sealBuffer.AddIDMapEvent(e)
}

// InsertRuleEvaluationEvents is unused on the Badger store.
func (db *DB) InsertRuleEvaluationEvents(events []RuleEvaluationEvent) error {
	if len(events) == 0 {
		return nil
	}
	return fmt.Errorf("ops store required")
}

// RunWrite holds writeMu and runs fn. Serializes writers that share the migration DB handle.
func (db *DB) RunWrite(ctx context.Context, fn func() error) error {
	if db == nil {
		return fmt.Errorf("nil database")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	err := fn()
	db.RecordOp(OpRunWrite, "", 0, time.Since(start), err)
	return err
}

// BeginTraversalPhase starts the seal buffer phase for traversal.
func (db *DB) BeginTraversalPhase(ctx context.Context) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	return db.sealBuffer.StartPhase()
}

// EnsureBulkPhaseSecondaryIndexes is a legacy Duck CREATE INDEX hook.
// Badger secondary indexes (path/name/size/mtime/seg/tri) are written on node insert, not at phase end.
func (db *DB) EnsureBulkPhaseSecondaryIndexes() error {
	return nil
}

// IndexBuildOptions is unused on Badger (kept for call-site compatibility).
type IndexBuildOptions struct {
	Threads       int
	MemoryLimitGB int
}

// ClampIndexBuildThreads returns threads in [1, 4].
func ClampIndexBuildThreads(n int) int {
	if n <= 0 {
		return 1
	}
	if n > 4 {
		return 4
	}
	return n
}

// EnsureBulkPhaseSecondaryIndexesWithOptions is a legacy Duck index-build hook (no-op on Badger).
func (db *DB) EnsureBulkPhaseSecondaryIndexesWithOptions(opts IndexBuildOptions) error {
	_ = opts
	return nil
}

// SetThreads is a no-op on Badger-only stores.
func (db *DB) SetThreads(n int) error {
	_ = n
	if db == nil {
		return fmt.Errorf("database not open")
	}
	return nil
}

// MemoryLimitGB returns the configured memory limit hint (GB).
func (db *DB) MemoryLimitGB() int {
	if db == nil || db.memoryLimitGB <= 0 {
		return MinMemoryLimitGB
	}
	return db.memoryLimitGB
}

// SetMemoryLimitGB records a memory limit hint for newly opened stores.
func (db *DB) SetMemoryLimitGB(gb int) error {
	if db == nil {
		return fmt.Errorf("database not open")
	}
	db.memoryLimitGB = ResolveMemoryLimitGB(gb)
	return nil
}

// EndTraversalPhase flushes remaining seal-buffer writes (catalog inserts already indexed inline).
// CHECKPOINT is not run here: callers run it once after both traversal/retry/copy queues finish (see migration run/copy/sweeps), plus periodic seal policy, suspend, and Close.
// No-ops after AbortTraversalPhase.
// Prefer StopBulkPhaseSeal for soft suspend so Stop does not block on a full durable flush.
func (db *DB) EndTraversalPhase() error {
	return db.EndTraversalPhaseWithIndexOptions(IndexBuildOptions{})
}

// EndTraversalPhaseWithIndexOptions flushes the seal buffer. IndexBuildOptions are ignored on Badger.
func (db *DB) EndTraversalPhaseWithIndexOptions(opts IndexBuildOptions) error {
	_ = opts
	if db.HardAborted() {
		return nil
	}
	db.SetActivity("Preparing a clean resume…")
	defer db.SetActivity("")
	return db.StopBulkPhaseSeal()
}

// StopBulkPhaseSeal flushes remaining seal-buffer writes and closes phase appenders.
// Soft suspend uses this so Stop does not wait on a longer durable teardown path.
func (db *DB) StopBulkPhaseSeal() error {
	if db == nil || db.sealBuffer == nil {
		return nil
	}
	if db.HardAborted() {
		return nil
	}
	return db.sealBuffer.StopPhase()
}

// AbortTraversalPhase drops in-memory seal phase state without Flush or CHECKPOINT.
// Use on hard kill / Abort so the run loop does not block on durable teardown.
func (db *DB) AbortTraversalPhase() {
	if db == nil || db.sealBuffer == nil {
		return
	}
	db.SetActivity("")
	db.sealBuffer.AbortPhase()
}

// HardAborted is true after AbortTraversalPhase until the next BeginTraversalPhase.
func (db *DB) HardAborted() bool {
	return db != nil && db.sealBuffer != nil && db.sealBuffer.HardAborted()
}
