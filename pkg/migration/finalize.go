// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// FinalizeOptions controls durable finalize / retry behavior.
type FinalizeOptions struct {
	// Refold clears the fold cursor and recomputes child_size for all folders
	// (no re-traversal). Ignored for delete finalize (no size fold).
	Refold bool
}

// finishBulkPhaseDurable flushes the seal buffer, folds folder sizes, and checkpoints Badger.
func finishBulkPhaseDurable(database *db.DB, foldKind string, opts FinalizeOptions) error {
	if database == nil {
		return nil
	}
	database.ClearHardAbort()
	database.SetActivity("Preparing a clean resume…")
	defer database.SetActivity("")
	if err := database.EndTraversalPhase(); err != nil {
		return fmt.Errorf("end bulk phase: %w", err)
	}
	if database.HardAborted() {
		return fmt.Errorf("durable teardown aborted")
	}
	if foldKind != "" {
		database.SetActivity("Calculating folder sizes…")
		var err error
		if opts.Refold {
			err = foldChildSizesRefold(database, foldKind)
		} else {
			err = foldChildSizes(database, foldKind)
		}
		if err != nil {
			return fmt.Errorf("folder size fold: %w", err)
		}
	}
	if database.HardAborted() {
		return fmt.Errorf("durable teardown aborted")
	}
	database.SetActivity("Making sure everything is safely stored…")
	if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	return nil
}

func foldKindForPhase(finalizing string) string {
	switch finalizing {
	case PhaseTraversalFinalizing:
		return opsdb.FoldKindTrav
	case PhaseCopyFinalizing:
		return opsdb.FoldKindCopy
	default:
		return ""
	}
}

func foldChildSizes(database *db.DB, kind string) error {
	if database == nil || database.Ops() == nil || kind == "" {
		return nil
	}
	hooks := opsdb.FoldHooks{
		Activity: database.SetActivity,
		Aborted:  database.HardAborted,
	}
	ops := database.Ops()
	switch kind {
	case opsdb.FoldKindTrav:
		if err := ops.FoldChildSizes(opsdb.SideSRC, kind, hooks); err != nil {
			return err
		}
		return ops.FoldChildSizes(opsdb.SideDST, kind, hooks)
	case opsdb.FoldKindCopy:
		return ops.FoldChildSizes(opsdb.SideDST, kind, hooks)
	default:
		return nil
	}
}

// foldChildSizesRefold recomputes folder sizes from the catalog (no pend tickets).
// Trav: SRC then DST. Copy: DST only.
func foldChildSizesRefold(database *db.DB, originalKind string) error {
	if database == nil || database.Ops() == nil || originalKind == "" {
		return nil
	}
	hooks := opsdb.FoldHooks{
		Activity:   database.SetActivity,
		Aborted:    database.HardAborted,
		QStatPhase: originalKind, // API polls size-fold/{trav|copy}, not "refold"
	}
	ops := database.Ops()
	switch originalKind {
	case opsdb.FoldKindTrav:
		if err := ops.FoldChildSizes(opsdb.SideSRC, opsdb.FoldKindRefold, hooks); err != nil {
			return err
		}
		return ops.FoldChildSizes(opsdb.SideDST, opsdb.FoldKindRefold, hooks)
	case opsdb.FoldKindCopy:
		return ops.FoldChildSizes(opsdb.SideDST, opsdb.FoldKindRefold, hooks)
	default:
		return nil
	}
}

type finalizeErrorRuntime struct {
	FinalizeError string `json:"finalize_error,omitempty"`
}

func (m *Migration) setFinalizeError(err error) {
	if m == nil {
		return
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	m.mu.Lock()
	m.finalizeError = msg
	m.mu.Unlock()
	if m.store == nil || m.ID == "" {
		return
	}
	patch, marshalErr := json.Marshal(finalizeErrorRuntime{FinalizeError: msg})
	if marshalErr != nil {
		return
	}
	_ = m.store.updateRuntimeState(m.ID, string(patch))
}

// FinalizeError returns the last durable-teardown failure message, if any.
func (m *Migration) FinalizeError() string {
	if m == nil {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.finalizeError
}

func (m *Migration) completeDurableFinalize(finalizing, failed, review string, opts FinalizeOptions) error {
	if err := m.transitionTo(finalizing); err != nil {
		return err
	}
	m.setFinalizeError(nil)
	if err := finishBulkPhaseDurable(m.DB, foldKindForPhase(finalizing), opts); err != nil {
		fmt.Printf("\n[migration] durable finalize failed: %v\n", err)
		m.setFinalizeError(err)
		if e2 := m.transitionTo(failed); e2 != nil {
			return fmt.Errorf("%w (also failed to set finalize-failed phase: %v)", err, e2)
		}
		return err
	}
	m.setFinalizeError(nil)
	if err := m.transitionTo(review); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

// RetryFinalize retries seal flush / folder-size fold / checkpoint after *-finalize-failed.
// Does not re-run traversal, copy, or delete workers.
func (m *Migration) RetryFinalize(opts FinalizeOptions) error {
	if m == nil {
		return fmt.Errorf("nil migration")
	}
	phase := m.Phase()
	var finalizing, failed, review string
	switch phase {
	case PhaseTraversalFinalizeFailed:
		finalizing, failed, review = PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed, PhaseTraversalReview
	case PhaseCopyFinalizeFailed:
		finalizing, failed, review = PhaseCopyFinalizing, PhaseCopyFinalizeFailed, PhaseCopyReview
	case PhaseDeleteFinalizeFailed:
		finalizing, failed, review = PhaseDeleteFinalizing, PhaseDeleteFinalizeFailed, PhaseDeleteReview
	default:
		return fmt.Errorf("retry finalize requires *-finalize-failed phase, got %s", phase)
	}
	runCtx, cancelRun := m.beginRun(context.Background())
	defer func() {
		cancelRun()
		m.endRun()
	}()
	_ = runCtx
	return m.completeDurableFinalize(finalizing, failed, review, opts)
}
