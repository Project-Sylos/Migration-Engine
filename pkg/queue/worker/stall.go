// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/optimeout"
)

const (
	// copyIOReadTimeout / copyIOWriteTimeout: per-Read / per-Write await budgets (match
	// Sylos-FS ctxstream + chunk deadlines). Fresh deadline each call.
	copyIOReadTimeout  = optimeout.IORead
	copyIOWriteTimeout = optimeout.IOWrite
	// copyCommitStallTimeout bounds dstWriter.Close (session finish / commit).
	copyCommitStallTimeout = 10 * time.Minute
	// gplStallTimeout bounds path-rule revalidation (CPU/DB; no FS) so a wedged GPL
	// task cannot hold a traversal worker forever.
	gplStallTimeout = 60 * time.Second
)

// Package defaults mirror queue.Default*StallTimeout; workers prefer queue overrides.
const (
	copyStallTimeout      = queue.DefaultCopyStallTimeout
	deleteStallTimeout    = queue.DefaultDeleteStallTimeout
	traversalStallTimeout = queue.DefaultTraversalStallTimeout
)

func copyStallBudget(q *queue.Queue) time.Duration {
	if q != nil {
		return q.CopyStallTimeout()
	}
	return copyStallTimeout
}

func deleteStallBudget(q *queue.Queue) time.Duration {
	if q != nil {
		return q.DeleteStallTimeout()
	}
	return deleteStallTimeout
}

func traversalStallBudget(q *queue.Queue) time.Duration {
	if q != nil {
		return q.TraversalStallTimeout()
	}
	return traversalStallTimeout
}

// stallOnFail returns onStall only when timeout is positive (0 = unlimited).
func stallOnFail(q *queue.Queue, nodeID string, timeout time.Duration, op string) func() {
	if timeout <= 0 {
		return nil
	}
	return stallForceFail(q, nodeID, timeout, op)
}

// progressStallSuppress freezes the ProgressWatchdog while an FS rate-limit window is
// open. Seal publish no longer blocks the FS await path, so it is not part of suppress.
func progressStallSuppress(q *queue.Queue) func() bool {
	return func() bool {
		return q != nil && q.IsRateLimitActive()
	}
}

// stallForceFail returns an onStall hook that hard-drops the lease when an FS wait
// exceeds the ProgressWatchdog budget (syscall may still be blocked).
func stallForceFail(q *queue.Queue, nodeID string, timeout time.Duration, op string) func() {
	return func() {
		if q == nil || nodeID == "" {
			return
		}
		msg := fmt.Sprintf("%s stalled after %s", op, timeout)
		q.ForceAbortInFlightLease(nodeID, msg)
	}
}

// awaitCancellable runs fn on a goroutine. ok is false when ctx finishes first; fn may
// still be running (adapters that ignore cancel). Callers must free the worker either way.
// When q is non-nil, the await is bracketed on the migration FS overlap wall clock.
// When wd is non-nil, Open/Close arm the per-task FS stall clock for this await only.
func awaitCancellable[T any](q *queue.Queue, ctx context.Context, wd *observe.ProgressWatchdog, fn func() T) (v T, ok bool) {
	if wd != nil {
		wd.Open()
		defer wd.Close()
	}
	if q != nil {
		fsBusy := q.Database().Busy(db.BusyFS)
		fsBusy.Enter()
		defer fsBusy.Leave()
	}
	done := make(chan T, 1)
	go func() { done <- fn() }()
	select {
	case v = <-done:
		return v, true
	case <-ctx.Done():
		return v, false
	}
}

// awaitCancellableBeating is awaitCancellable plus periodic Beats while fn runs.
// Callers should pass a queue-only beater here so ProgressWatchdog still cancels hung
// RPCs; real progress (bytes written) should Beat the ProgressWatchdog separately.
// Adapter/HTTP per-chunk and per-RPC deadlines are the primary hang protection.
func awaitCancellableBeating[T any](q *queue.Queue, ctx context.Context, wd *observe.ProgressWatchdog, beat func(), interval time.Duration, fn func() T) (v T, ok bool) {
	if beat == nil || interval <= 0 {
		return awaitCancellable(q, ctx, wd, fn)
	}
	if wd != nil {
		wd.Open()
		defer wd.Close()
	}
	if q != nil {
		fsBusy := q.Database().Busy(db.BusyFS)
		fsBusy.Enter()
		defer fsBusy.Leave()
	}
	done := make(chan T, 1)
	go func() { done <- fn() }()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case v = <-done:
			return v, true
		case <-ctx.Done():
			return v, false
		case <-ticker.C:
			beat()
		}
	}
}

func copyStallBeatInterval(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 15 * time.Second
	}
	d := timeout / 4
	if d < time.Second {
		return time.Second
	}
	return d
}

// newProgressBeater beats ProgressWatchdog and the queue watchdog (real progress only).
func newProgressBeater(q *queue.Queue, wd *observe.ProgressWatchdog) func() {
	return func() {
		if wd != nil {
			wd.Beat()
		}
		if q != nil && q.HasWatchdog() {
			q.BeatWatchdog()
		}
	}
}

// newQueueOnlyBeater keeps STALL DETECTED quiet during long FS RPCs without resetting
// ProgressWatchdog (so hung ops still cancel after copyStallTimeout).
func newQueueOnlyBeater(q *queue.Queue) func() {
	return func() {
		if q != nil && q.HasWatchdog() {
			q.BeatWatchdog()
		}
	}
}

func stallOrAbandoned(ctx, parent context.Context, op, path string, timeout time.Duration) error {
	if parent != nil && parent.Err() != nil {
		return errTransferAbandoned
	}
	return fmt.Errorf("%s stalled after %s for %s: %w", op, timeout, path, ctx.Err())
}

// awaitErr runs an error-returning FS (or worker) call under ctx cancel.
// Pass q=nil for non-FS work (e.g. GPL revalidate) so it is not counted as provider wait.
func awaitErr(q *queue.Queue, ctx, parent context.Context, timeout time.Duration, op, path string, wd *observe.ProgressWatchdog, fn func() error) error {
	err, ok := awaitCancellable(q, ctx, wd, fn)
	if !ok {
		return stallOrAbandoned(ctx, parent, op, path, timeout)
	}
	return err
}

// awaitErrBeating is awaitErr with periodic Beats for long adapter RPCs (all FS backends).
func awaitErrBeating(q *queue.Queue, ctx, parent context.Context, timeout time.Duration, beat func(), op, path string, wd *observe.ProgressWatchdog, fn func() error) error {
	err, ok := awaitCancellableBeating(q, ctx, wd, beat, copyStallBeatInterval(timeout), fn)
	if !ok {
		return stallOrAbandoned(ctx, parent, op, path, timeout)
	}
	return err
}

type fsOut[T any] struct {
	Val T
	Err error
}

// awaitResult runs a (T, error) FS call under ctx cancel.
func awaitResult[T any](q *queue.Queue, ctx, parent context.Context, timeout time.Duration, op, path string, wd *observe.ProgressWatchdog, fn func() (T, error)) (T, error) {
	out, ok := awaitCancellable(q, ctx, wd, func() fsOut[T] {
		v, err := fn()
		return fsOut[T]{Val: v, Err: err}
	})
	if !ok {
		var zero T
		return zero, stallOrAbandoned(ctx, parent, op, path, timeout)
	}
	return out.Val, out.Err
}

// awaitResultBeating is awaitResult with periodic Beats for long adapter RPCs (all FS backends).
func awaitResultBeating[T any](q *queue.Queue, ctx, parent context.Context, timeout time.Duration, beat func(), op, path string, wd *observe.ProgressWatchdog, fn func() (T, error)) (T, error) {
	out, ok := awaitCancellableBeating(q, ctx, wd, beat, copyStallBeatInterval(timeout), func() fsOut[T] {
		v, err := fn()
		return fsOut[T]{Val: v, Err: err}
	})
	if !ok {
		var zero T
		return zero, stallOrAbandoned(ctx, parent, op, path, timeout)
	}
	return out.Val, out.Err
}
