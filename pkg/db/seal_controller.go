// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"database/sql"
	"errors"
	"time"
)

// SealBufferOptions configures the seal buffer. Zero value uses defaults.
// The knobs live here (not in pkg/db/seal) so scaling and migration can tune the buffer
// without importing the implementation.
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

// SealBufferTelemetry is a point-in-time view of seal buffer pressure (event counters reset on read).
type SealBufferTelemetry struct {
	CurrentRows              int64
	HWMSinceLastPoll         int64
	HardCapHitsSinceLastPoll int64
	FlushCountSinceLastPoll  int64
}

// SealController is the write-behind buffer that DB delegates all sealing to.
// The implementation lives in pkg/db/seal so the flush machinery can depend on the
// pull/stats/subtree query packages; DB only knows this contract.
type SealController interface {
	Add(table string, depth int, nodes []*NodeState, pending, successful, failed, completed, copyP, copyS, copyF int64) error
	AddDiscoveryNodes(ops []InsertOperation)
	AddDiscoveryStatusEvent(table string, e StatusEvent, fromRetry bool)
	AddTaskError(rec TaskErrorRecord)
	AddFailedSubtreePath(parentPath string)
	AddGPLIssue(e GPLIssue)
	AddIDMapEvent(e IDMapEvent)
	Flush() error
	WaitUntilFlushedThrough(depth int)
	IOWaitActive() bool
	TelemetrySnapshot() SealBufferTelemetry
	UpdateOptions(opts SealBufferOptions)
	StartPhase(conn *sql.Conn) error
	StopPhase() error
	OnCheckpointOK()
	Stop()
}

// ErrNoSealController is returned by Open when no seal implementation has been registered.
var ErrNoSealController = errors.New(`no seal controller registered: import codeberg.org/Sylos/Migration-Engine/pkg/db/seal`)

// sealAttach is installed by pkg/db/seal's init. Open calls it so callers keep using db.Open
// while the buffer implementation stays out of this package's import graph.
var sealAttach func(*DB, SealBufferOptions) SealController

// RegisterSealAttach installs the seal buffer factory. Called from pkg/db/seal init.
func RegisterSealAttach(fn func(*DB, SealBufferOptions) SealController) {
	sealAttach = fn
}

// AttachSeal sets the controller DB delegates sealing to. Open does this via the registered
// factory; call it directly only when constructing a DB with a bespoke controller.
func (db *DB) AttachSeal(c SealController) {
	db.sealBuffer = c
}

// LockWrites acquires the global write mutex. The seal buffer holds it across phase flushes
// (deferred CHECKPOINT plus one appender transaction) instead of taking a pooled connection.
func (db *DB) LockWrites() {
	db.writeMu.Lock()
}

// UnlockWrites releases the global write mutex taken by LockWrites.
func (db *DB) UnlockWrites() {
	db.writeMu.Unlock()
}
