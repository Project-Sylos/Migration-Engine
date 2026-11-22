// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/memweight"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// Options configures Badger-only per-migration open (no Duck catalog).
type Options struct {
	Path          string             // Logical migration path (historically *.db); OpsDir defaults from this
	OpsDir        string             // Badger ops dir; default MigrationOpsPath(Path)
	LogsDir       string             // Badger logs sidecar; default MigrationLogsPath(Path)
	EncryptionKey []byte             // unused for Badger Open (kept for API compatibility)
	SealBuffer    *SealBufferOptions // seal buffer knobs when attached
	// MemoryLimitGB is an unused legacy host-memory hint (GB). Badger does not apply it.
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
	logsStore     *opsdb.Store
	ops           *dbOpRecorder

	disableQueryTimeout int32
	activity            atomic.Value // string
	memWeight           *memweight.Ledger

	// Bottleneck wall clocks (overlap-aware; parallel waiters count once).
	busy [busyKindCount]OverlapClock
}

// Open opens the per-migration Badger ops store. Path is the logical migration
// path (historically *.db); OpsDir defaults to MigrationOpsPath(Path). No DB.
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
	logsDir := opts.LogsDir
	if logsDir == "" {
		logsDir = MigrationLogsPath(path)
	}
	if logsDir == "" {
		_ = opsStore.Close()
		return nil, fmt.Errorf("Badger logs sidecar dir required (set Options.LogsDir for :memory:)")
	}
	logsStore, err := opsdb.Open(opsdb.Options{Dir: logsDir})
	if err != nil {
		_ = opsStore.Close()
		return nil, err
	}
	db := &DB{path: path, pullGate: newPullGate(), opsStore: opsStore, logsStore: logsStore, memoryLimitGB: ResolveMemoryLimitGB(opts.MemoryLimitGB)}
	sealOpts := SealBufferOptions{}
	if opts.SealBuffer != nil {
		sealOpts = *opts.SealBuffer
	}
	db.sealBuffer = newBadgerSeal(db, opsStore, sealOpts)
	db.startOpRecorder()
	return db, nil
}

// LogsDBForWrite returns the DB handle used for log persistence (same handle; logs go to Logs()).
func (db *DB) LogsDBForWrite() *DB {
	return db
}

// Logs returns the Badger logs sidecar store (may be nil).
func (db *DB) Logs() *opsdb.Store {
	if db == nil {
		return nil
	}
	return db.logsStore
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
	if db.logsStore != nil {
		if err := db.logsStore.Close(); err != nil {
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
	return db.opsStore.DropPendingPrefixAborted(side, phase, depth, nodeType, nil)
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
	db.pullGate.Acquire(onWaitBeat, db.Busy(BusyDBWork), db.Busy(BusyPullGate))
}

// ReleasePull returns the shared frontier-pull ticket.
func (db *DB) ReleasePull() {
	if db == nil {
		return
	}
	db.pullGate.Release()
}

// BusyKind selects one of the bottleneck wall clocks on DB.
type BusyKind int

const (
	BusyFS           BusyKind = iota // any FS adapter await
	BusyBackpressure                 // hard-cap waiters and/or forced flush
	BusyHardCap                      // hard-cap only (breakdown)
	BusyDBWork                       // organic flush and/or frontier pull (incl. gate)
	BusyPullGate                     // pull-ticket wait only (breakdown)
	busyKindCount
)

// Busy returns the overlap clock for kind. A nil DB returns a nil clock, whose Enter/Leave are no-ops.
func (db *DB) Busy(kind BusyKind) *OverlapClock {
	if db == nil {
		return nil
	}
	return &db.busy[kind]
}

// TakeBottleneckWallNs returns and clears overlap wall clocks for the autoscaler panel.
func (db *DB) TakeBottleneckWallNs() (fs, backpressure, hardCap, dbWork, pullGate int64) {
	if db == nil {
		return 0, 0, 0, 0, 0
	}
	return db.busy[BusyFS].TakeNs(),
		db.busy[BusyBackpressure].TakeNs(),
		db.busy[BusyHardCap].TakeNs(),
		db.busy[BusyDBWork].TakeNs(),
		db.busy[BusyPullGate].TakeNs()
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

// MemoryLimitGB returns the configured memory limit hint (GB).
func (db *DB) MemoryLimitGB() int {
	if db == nil || db.memoryLimitGB <= 0 {
		return MinMemoryLimitGB
	}
	return db.memoryLimitGB
}

// EndTraversalPhase flushes remaining seal-buffer writes (catalog inserts already indexed inline).
// CHECKPOINT is not run here: callers run it once after both traversal/retry/copy queues finish (see migration run/copy/sweeps), plus periodic seal policy, suspend, and Close.
// No-ops after AbortTraversalPhase.
// Prefer StopBulkPhaseSeal for soft suspend so Stop does not block on a full durable flush.
func (db *DB) EndTraversalPhase() error {
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
	return db.sealBuffer.Flush()
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

// HardAborted is true after AbortTraversalPhase until ClearHardAbort or BeginTraversalPhase.
func (db *DB) HardAborted() bool {
	return db != nil && db.sealBuffer != nil && db.sealBuffer.HardAborted()
}

// ClearHardAbort resets the hard-abort flag so durable finalize / fold can run again after Stop.
func (db *DB) ClearHardAbort() {
	if db == nil || db.sealBuffer == nil {
		return
	}
	db.sealBuffer.ClearHardAbort()
}

// SetActivity records a human-readable label for the current DB wait (soft stop UI / observer).
// Pass empty to clear.
func (db *DB) SetActivity(label string) {
	if db == nil {
		return
	}
	db.activity.Store(label)
}

// ActivityLabel returns the current DB activity label, or empty.
func (db *DB) ActivityLabel() string {
	if db == nil {
		return ""
	}
	v := db.activity.Load()
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

// TestOpen opens a file-backed DB with matching Badger ops and logs dirs for tests.
func TestOpen(t *testing.T, name string) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name+".db")
	ops := filepath.Join(dir, name+".ops")
	logs := filepath.Join(dir, name+".logs")
	database, err := Open(Options{Path: path, OpsDir: ops, LogsDir: logs})
	if err != nil {
		t.Fatalf("TestOpen(%q): %v", name, err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}
