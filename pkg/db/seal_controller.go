// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// SealBufferOptions configures the seal buffer. Zero value uses defaults.
type SealBufferOptions struct {
	FlushInterval time.Duration
	RowThreshold  int
	HardCap       int
	FlushTimeout  time.Duration
	CheckpointEveryRows int
	CheckpointMaxInterval time.Duration
}

// SealBufferTelemetry is a point-in-time view of seal buffer pressure (event counters reset on read).
type SealBufferTelemetry struct {
	CurrentRows              int64
	HWMSinceLastPoll         int64
	HardCapHitsSinceLastPoll int64
	FlushCountSinceLastPoll  int64
	LastFlushRows       int64
	LastFlushDurationNs int64
}

// SealFlushStats is a non-resetting snapshot of the last successful seal flush.
type SealFlushStats struct {
	Rows       int64
	DurationNs int64
}

// SealController is the write-behind buffer that DB delegates all sealing to.
type SealController interface {
	Add(table string, depth int, nodes []*NodeState, pending, successful, failed, completed, copyP, copyS, copyF int64) error
	AddDiscoveryNodes(ops []InsertOperation) error
	AddDiscoveryStatusEvent(table string, e StatusEvent, fromRetry bool)
	AddTaskError(rec TaskErrorRecord)
	AddFailedSubtreePath(parentPath string)
	AddGPLIssue(e GPLIssue)
	AddIDMapEvent(e IDMapEvent)
	AddKidsPackReplace(side, parentID string, kids []opsdb.KidRecord)
	AddKidTicket(side, parentID string, parentDepth int, kid opsdb.KidRecord)
	Flush() error
	WaitUntilFlushedThrough(depth int)
	IOWaitActive() bool
	TelemetrySnapshot() SealBufferTelemetry
	LastFlushStats() SealFlushStats
	UpdateOptions(opts SealBufferOptions)
	StartPhase() error
	StopPhase() error
	AbortPhase()
	HardAborted() bool
	OnCheckpointOK()
	Stop()
}

func (db *DB) AttachSeal(c SealController) {
	db.sealBuffer = c
}

func (db *DB) LockWrites() {
	db.writeMu.Lock()
}

func (db *DB) UnlockWrites() {
	db.writeMu.Unlock()
}
