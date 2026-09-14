// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

const (
	OpSealFlush           = "seal_flush"
	OpFrontierPull        = "frontier_pull"
	OpRebuildCurrent      = "rebuild_current"
	OpRebuildCurrentSkip  = "rebuild_current_skip"
	OpRebuildCurrentIDs   = "rebuild_current_ids"
	OpRebuildCurrentSince = "rebuild_current_since"
	OpRebuildCurrentFull  = "rebuild_current_full"
	OpCurrentMerge        = "current_merge"
	OpBadgerSync          = "badger_sync"
	OpCatalogSync         = "catalog_sync"
	OpBadgerPull          = "badger_pull"
	OpDstPullHydrate      = "dst_pull_hydrate"
	OpBulkHeap            = "bulk_heap"
	OpCheckpoint          = "checkpoint"
	OpCreateIndex         = "create_index"
	OpDropIndex           = "drop_index"
	OpRunWrite            = "run_write"
	OpReviewQuery         = "review_query"
	OpReviewFolderPage    = "review_folder_page"
	OpReviewSearchPage    = "review_search_page"
	OpReviewSearchStats   = "review_search_stats"
	OpReviewFolderStats   = "review_folder_stats"
	OpReviewStats         = "review_stats"

	dbOpsSQLMaxLen     = 2048
	dbOpsRingCap       = 4096
	dbOpsFlushBatch    = 32
	dbOpsFlushInterval = 1500 * time.Millisecond
)

type dbOpSample struct {
	Op         string
	SQL        string
	Rows       int64
	DurationNs int64
	Err        string
	At         time.Time
}

type dbOpRecorder struct {
	mu   sync.Mutex
	ring []dbOpSample
	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

func (db *DB) startOpRecorder() {
	if db == nil || db.ops != nil {
		return
	}
	db.ops = &dbOpRecorder{
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go db.opFlushLoop()
}

func (db *DB) stopOpRecorder(skipFlush bool) {
	if db == nil || db.ops == nil {
		return
	}
	select {
	case <-db.ops.stop:
	default:
		close(db.ops.stop)
	}
	<-db.ops.done
	if !skipFlush {
		db.FlushRecordedOps()
	}
}

func (db *DB) opFlushLoop() {
	defer close(db.ops.done)
	tick := time.NewTicker(dbOpsFlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-db.ops.stop:
			return
		case <-tick.C:
			db.FlushRecordedOps()
		case <-db.ops.wake:
			db.FlushRecordedOps()
		}
	}
}

// RecordOp copies one named operation into the in-memory ring. Never waits on storage.
func (db *DB) RecordOp(op, sqlText string, rows int64, d time.Duration, err error) {
	if db == nil || db.ops == nil || op == "" {
		return
	}
	if d < 0 {
		d = 0
	}
	sample := dbOpSample{
		Op:         op,
		SQL:        truncateSQLText(sqlText),
		Rows:       rows,
		DurationNs: d.Nanoseconds(),
		At:         time.Now(),
	}
	if err != nil {
		sample.Err = err.Error()
	}
	db.ops.mu.Lock()
	if len(db.ops.ring) >= dbOpsRingCap {
		db.ops.ring = db.ops.ring[1:]
	}
	db.ops.ring = append(db.ops.ring, sample)
	n := len(db.ops.ring)
	db.ops.mu.Unlock()
	if n >= dbOpsFlushBatch {
		select {
		case db.ops.wake <- struct{}{}:
		default:
		}
	}
}

// StartOp returns a callback that records elapsed time since this call.
func (db *DB) StartOp(op, sqlText string) func(rows int64, err error) {
	if db == nil || db.ops == nil {
		return func(int64, error) {}
	}
	start := time.Now()
	return func(rows int64, err error) {
		db.RecordOp(op, sqlText, rows, time.Since(start), err)
	}
}

// FlushRecordedOps writes buffered samples to Badger ops store.
func (db *DB) FlushRecordedOps() {
	if db == nil || db.HardAborted() {
		return
	}
	samples := db.drainOps()
	if len(samples) == 0 {
		return
	}
	if db.opsStore == nil {
		return
	}
	for i := range samples {
		rec := opsdb.DBOpRecord{
			Op:         samples[i].Op,
			SQL:        samples[i].SQL,
			Rows:       samples[i].Rows,
			DurationNs: samples[i].DurationNs,
			Err:        samples[i].Err,
			At:         samples[i].At,
		}
		if err := db.opsStore.AppendDBOp(rec); err != nil {
			db.requeueOps(samples[i:])
			return
		}
	}
}

func (db *DB) drainOps() []dbOpSample {
	if db.ops == nil {
		return nil
	}
	db.ops.mu.Lock()
	defer db.ops.mu.Unlock()
	if len(db.ops.ring) == 0 {
		return nil
	}
	out := db.ops.ring
	db.ops.ring = nil
	return out
}

func (db *DB) requeueOps(samples []dbOpSample) {
	if db.ops == nil || len(samples) == 0 {
		return
	}
	db.ops.mu.Lock()
	defer db.ops.mu.Unlock()
	combined := make([]dbOpSample, 0, len(samples)+len(db.ops.ring))
	combined = append(combined, samples...)
	combined = append(combined, db.ops.ring...)
	if len(combined) > dbOpsRingCap {
		combined = combined[len(combined)-dbOpsRingCap:]
	}
	db.ops.ring = combined
}

func truncateSQLText(s string) string {
	if len(s) <= dbOpsSQLMaxLen {
		return s
	}
	return s[:dbOpsSQLMaxLen]
}

// CompactionSQLPrefix is the parseable side/depth header stored on rebuild_current samples.
func CompactionSQLPrefix(side string, depth int) string {
	if depth < 0 {
		return "side=" + side + "\n"
	}
	return fmt.Sprintf("side=%s depth=%d\n", side, depth)
}
