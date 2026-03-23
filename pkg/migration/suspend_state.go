// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"errors"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// Sentinel errors returned when a live phase ends via soft suspend (not failure).
var (
	ErrTraversalSoftSuspended = errors.New("traversal soft suspended")
	ErrCopySoftSuspended      = errors.New("copy phase soft suspended")
)

type traversalSuspendedError struct {
	stats   RuntimeStats
	suspend RuntimeSuspendV1
}

func (e *traversalSuspendedError) Error() string { return ErrTraversalSoftSuspended.Error() }

func (e *traversalSuspendedError) Unwrap() error { return ErrTraversalSoftSuspended }

func newTraversalSuspendedError(stats RuntimeStats, s RuntimeSuspendV1) error {
	return &traversalSuspendedError{stats: stats, suspend: s}
}

// AsTraversalSuspended unwraps stats and suspend payload from RunMigration / RunRetrySweep soft suspend.
func AsTraversalSuspended(err error) (RuntimeStats, RuntimeSuspendV1, bool) {
	var te *traversalSuspendedError
	if errors.As(err, &te) {
		return te.stats, te.suspend, true
	}
	return RuntimeStats{}, RuntimeSuspendV1{}, false
}

type copySuspendedError struct {
	stats   queue.QueueStats
	suspend RuntimeSuspendV1
}

func (e *copySuspendedError) Error() string { return ErrCopySoftSuspended.Error() }

func (e *copySuspendedError) Unwrap() error { return ErrCopySoftSuspended }

func newCopySuspendedError(stats queue.QueueStats, s RuntimeSuspendV1) error {
	return &copySuspendedError{stats: stats, suspend: s}
}

// AsCopySuspended unwraps stats and suspend payload from RunCopyPhase soft suspend.
func AsCopySuspended(err error) (queue.QueueStats, RuntimeSuspendV1, bool) {
	var ce *copySuspendedError
	if errors.As(err, &ce) {
		return ce.stats, ce.suspend, true
	}
	return queue.QueueStats{}, RuntimeSuspendV1{}, false
}

// RuntimeSuspendV1 is merged into migrations.runtime_state_json under key "suspend_v1".
// It holds only what resume needs: tuning knobs and last-known position for UI / max-depth.
type RuntimeSuspendV1 struct {
	Version int `json:"version"` // 1

	SuspendedAtUnix int64  `json:"suspended_at_unix"`
	Kind            string `json:"kind"` // "traversal" | "copy"

	WorkerCount int `json:"worker_count"`
	MaxRetries  int `json:"max_retries"`

	// Queue pull sizing (0 in JSON means "use engine default" on resume).
	LeaseBatchSize  int `json:"lease_batch_size,omitempty"`
	RefillBatchSize int `json:"refill_batch_size,omitempty"`
	ObserverPollMs  int `json:"observer_poll_ms,omitempty"`
	ProgressTickMs  int `json:"progress_tick_ms,omitempty"`

	// Traversal / coordinator
	LastRoundSrc  int `json:"last_round_src,omitempty"`
	LastRoundDst  int `json:"last_round_dst,omitempty"`
	MaxKnownDepth int `json:"max_known_depth,omitempty"`

	// Copy
	CopyPass           int `json:"copy_pass,omitempty"`
	LastKnownCopyRound int `json:"last_known_copy_round,omitempty"`
}

const runtimeSuspendJSONKey = "suspend_v1"

// mergeRuntimeSuspendV1 merges s into the existing runtime_state_json map (decoded object).
func mergeRuntimeSuspendV1(existing map[string]any, s RuntimeSuspendV1) (map[string]any, error) {
	if existing == nil {
		existing = make(map[string]any)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		return nil, err
	}
	existing[runtimeSuspendJSONKey] = asMap
	return existing, nil
}

// parseRuntimeSuspendV1 extracts suspend_v1 from merged runtime JSON blob bytes.
func parseRuntimeSuspendV1(runtimeStateJSON string) (RuntimeSuspendV1, bool) {
	if runtimeStateJSON == "" || runtimeStateJSON == "{}" {
		return RuntimeSuspendV1{}, false
	}
	var top map[string]any
	if err := json.Unmarshal([]byte(runtimeStateJSON), &top); err != nil {
		return RuntimeSuspendV1{}, false
	}
	raw, ok := top[runtimeSuspendJSONKey]
	if !ok || raw == nil {
		return RuntimeSuspendV1{}, false
	}
	inner, err := json.Marshal(raw)
	if err != nil {
		return RuntimeSuspendV1{}, false
	}
	var s RuntimeSuspendV1
	if err := json.Unmarshal(inner, &s); err != nil {
		return RuntimeSuspendV1{}, false
	}
	if s.Version != 1 || s.Kind == "" {
		return RuntimeSuspendV1{}, false
	}
	return s, true
}

func newTraversalSuspendState(
	workerCount, maxRetries, leaseBatch, refillBatch, observerPollMs, progressTickMs int,
	lastRoundSrc, lastRoundDst, maxKnownDepth int,
) RuntimeSuspendV1 {
	return RuntimeSuspendV1{
		Version:         1,
		SuspendedAtUnix: time.Now().Unix(),
		Kind:            "traversal",
		WorkerCount:     workerCount,
		MaxRetries:      maxRetries,
		LeaseBatchSize:  leaseBatch,
		RefillBatchSize: refillBatch,
		ObserverPollMs:  observerPollMs,
		ProgressTickMs:  progressTickMs,
		LastRoundSrc:    lastRoundSrc,
		LastRoundDst:    lastRoundDst,
		MaxKnownDepth:   maxKnownDepth,
	}
}

func newCopySuspendState(
	workerCount, maxRetries, leaseBatch, refillBatch, observerPollMs, progressTickMs int,
	copyPass, lastRound, maxKnownDepth int,
) RuntimeSuspendV1 {
	return RuntimeSuspendV1{
		Version:            1,
		SuspendedAtUnix:    time.Now().Unix(),
		Kind:               "copy",
		WorkerCount:        workerCount,
		MaxRetries:         maxRetries,
		LeaseBatchSize:     leaseBatch,
		RefillBatchSize:    refillBatch,
		ObserverPollMs:     observerPollMs,
		ProgressTickMs:     progressTickMs,
		CopyPass:           copyPass,
		LastKnownCopyRound: lastRound,
		MaxKnownDepth:      maxKnownDepth,
	}
}

// traversalSuspendRuntimeMergePatch returns JSON for updateRuntimeState (merges suspend_v1 and legacy keys).
func traversalSuspendRuntimeMergePatch(s RuntimeSuspendV1) (string, error) {
	merged, err := mergeRuntimeSuspendV1(make(map[string]any), s)
	if err != nil {
		return "", err
	}
	merged["last_round_src"] = s.LastRoundSrc
	merged["last_round_dst"] = s.LastRoundDst
	merged["max_known_depth"] = s.MaxKnownDepth
	raw, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// copySuspendRuntimeMergePatch returns JSON for updateRuntimeState after copy soft suspend.
func copySuspendRuntimeMergePatch(s RuntimeSuspendV1) (string, error) {
	merged, err := mergeRuntimeSuspendV1(make(map[string]any), s)
	if err != nil {
		return "", err
	}
	merged["last_copy_round"] = s.LastKnownCopyRound
	merged["max_known_depth"] = s.MaxKnownDepth
	raw, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
