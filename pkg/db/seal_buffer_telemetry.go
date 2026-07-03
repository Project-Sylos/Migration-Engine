// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "sync/atomic"

// SealBufferTelemetry is a point-in-time view of seal buffer pressure (event counters reset on read).
type SealBufferTelemetry struct {
	CurrentRows              int64
	HWMSinceLastPoll         int64
	HardCapHitsSinceLastPoll int64
	FlushCountSinceLastPoll  int64
}

// TelemetrySnapshot returns seal buffer gauges and read-and-reset event counters.
func (sb *SealBuffer) TelemetrySnapshot() SealBufferTelemetry {
	if sb == nil {
		return SealBufferTelemetry{}
	}
	sb.mu.Lock()
	current := int64(sb.rowsSinceFlush)
	sb.mu.Unlock()

	hwm := atomic.SwapInt64(&sb.telemetryHWM, 0)
	if current > hwm {
		hwm = current
	}

	return SealBufferTelemetry{
		CurrentRows:              current,
		HWMSinceLastPoll:         hwm,
		HardCapHitsSinceLastPoll: atomic.SwapInt64(&sb.telemetryHardCapHits, 0),
		FlushCountSinceLastPoll:  atomic.SwapInt64(&sb.telemetryFlushCount, 0),
	}
}

// UpdateOptions hot-updates flush thresholds under sb.mu.
func (sb *SealBuffer) UpdateOptions(opts SealBufferOptions) {
	if sb == nil {
		return
	}
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if opts.FlushInterval > 0 {
		sb.interval = opts.FlushInterval
	}
	if opts.RowThreshold > 0 {
		sb.rowThreshold = opts.RowThreshold
	}
	if opts.HardCap > 0 {
		sb.hardCap = opts.HardCap
	}
	if opts.FlushTimeout != 0 {
		sb.flushTimeout = opts.FlushTimeout
	}
	if opts.CheckpointEveryRows > 0 {
		sb.checkpointEveryRows = opts.CheckpointEveryRows
	}
	if opts.CheckpointMaxInterval > 0 {
		sb.checkpointMaxInterval = opts.CheckpointMaxInterval
	}
}

func (sb *SealBuffer) noteRowsLocked(cur int64) {
	if cur <= 0 {
		return
	}
	for {
		hwm := atomic.LoadInt64(&sb.telemetryHWM)
		if cur <= hwm || atomic.CompareAndSwapInt64(&sb.telemetryHWM, hwm, cur) {
			break
		}
	}
}

func (sb *SealBuffer) noteHardCapHit() {
	atomic.AddInt64(&sb.telemetryHardCapHits, 1)
}

func (sb *SealBuffer) noteFlushComplete() {
	atomic.AddInt64(&sb.telemetryFlushCount, 1)
}
