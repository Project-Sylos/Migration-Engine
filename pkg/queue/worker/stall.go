// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
)

const (
	// copyStallTimeout / deleteStallTimeout: cancel a copy or delete FS op after this
	// long with no progress Beat while not rate-limited and not blocked on seal I/O.
	// Queue STALL DETECTED dumps are separate (30s diagnostic).
	copyStallTimeout   = 60 * time.Second
	deleteStallTimeout = 60 * time.Second
	// gplStallTimeout bounds path-rule revalidation (CPU/DB; no FS) so a wedged GPL
	// task cannot hold a traversal worker forever.
	gplStallTimeout = 60 * time.Second
	// traversalStallTimeout is intentionally shorter: hung ListChildren (e.g. local
	// /proc Lstat) must fail fast so the BFS round can advance.
	traversalStallTimeout = 15 * time.Second
)

// progressStallSuppress freezes the ProgressWatchdog while seal flush blocks or an
// FS rate-limit window is open. Idle hung ops still cancel after the op timeout.
func progressStallSuppress(q *queue.Queue) func() bool {
	return func() bool {
		if q == nil {
			return false
		}
		return q.SealIOWaitActive() || q.IsRateLimitActive()
	}
}

// awaitCancellable runs fn on a goroutine. ok is false when ctx finishes first; fn may
// still be running (adapters that ignore cancel). Callers must free the worker either way.
func awaitCancellable[T any](ctx context.Context, fn func() T) (v T, ok bool) {
	done := make(chan T, 1)
	go func() { done <- fn() }()
	select {
	case v = <-done:
		return v, true
	case <-ctx.Done():
		return v, false
	}
}

// awaitCancellableBeating is awaitCancellable plus periodic progress Beats so long
// adapter ops (Graph fragment PUT, Dropbox/GDrive session finish on Close, slow
// SFTP/Write) are not treated as idle stalls. Applies to every FSAdapter: heartbeats
// live in ME's shared copy loop, not per provider. Rate-limit/seal still freeze the
// watchdog via stallSuppress. Truly hung ops need adapter/HTTP deadlines.
func awaitCancellableBeating[T any](ctx context.Context, beat func(), interval time.Duration, fn func() T) (v T, ok bool) {
	if beat == nil || interval <= 0 {
		return awaitCancellable(ctx, fn)
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

// newProgressBeater returns a func that beats the per-task ProgressWatchdog and,
// when present, the queue watchdog. Shared by every FSAdapter on the copy path.
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

func stallOrAbandoned(ctx, parent context.Context, op, path string, timeout time.Duration) error {
	if parent != nil && parent.Err() != nil {
		return errTransferAbandoned
	}
	return fmt.Errorf("%s stalled after %s for %s: %w", op, timeout, path, ctx.Err())
}

// awaitErr runs an error-returning FS (or worker) call under ctx cancel.
func awaitErr(ctx, parent context.Context, timeout time.Duration, op, path string, fn func() error) error {
	err, ok := awaitCancellable(ctx, fn)
	if !ok {
		return stallOrAbandoned(ctx, parent, op, path, timeout)
	}
	return err
}

// awaitErrBeating is awaitErr with periodic Beats for long adapter RPCs (all FS backends).
func awaitErrBeating(ctx, parent context.Context, timeout time.Duration, beat func(), op, path string, fn func() error) error {
	err, ok := awaitCancellableBeating(ctx, beat, copyStallBeatInterval(timeout), fn)
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
func awaitResult[T any](ctx, parent context.Context, timeout time.Duration, op, path string, fn func() (T, error)) (T, error) {
	out, ok := awaitCancellable(ctx, func() fsOut[T] {
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
func awaitResultBeating[T any](ctx, parent context.Context, timeout time.Duration, beat func(), op, path string, fn func() (T, error)) (T, error) {
	out, ok := awaitCancellableBeating(ctx, beat, copyStallBeatInterval(timeout), func() fsOut[T] {
		v, err := fn()
		return fsOut[T]{Val: v, Err: err}
	})
	if !ok {
		var zero T
		return zero, stallOrAbandoned(ctx, parent, op, path, timeout)
	}
	return out.Val, out.Err
}
