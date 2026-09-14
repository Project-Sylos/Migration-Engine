// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"sync/atomic"
	"time"
)

// DefaultQueryTimeout caps interactive path-review DB reads (search/diffs/stats/exclude).
// Override per DB via SetDisableQueryTimeout (advanced settings / developer prefs).
const DefaultQueryTimeout = 2 * time.Minute

// SetDisableQueryTimeout when true makes ReviewQueryContext skip the default 2m deadline.
func (db *DB) SetDisableQueryTimeout(disable bool) {
	if db == nil {
		return
	}
	var v int32
	if disable {
		v = 1
	}
	atomic.StoreInt32(&db.disableQueryTimeout, v)
}

// DisableQueryTimeout reports whether the default review query deadline is off.
func (db *DB) DisableQueryTimeout() bool {
	return db != nil && atomic.LoadInt32(&db.disableQueryTimeout) != 0
}

// ReviewQueryContext returns ctx with DefaultQueryTimeout unless disabled or ctx already has a deadline.
func (db *DB) ReviewQueryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db != nil && db.DisableQueryTimeout() {
		return context.WithCancel(ctx)
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, DefaultQueryTimeout)
}
