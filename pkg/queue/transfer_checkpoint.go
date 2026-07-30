// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"io"

	"codeberg.org/Sylos/Migration-Engine/pkg/db/checkpoint"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// TransferAbandonMode selects what happens after persisting a checkpoint on cooperative abandon.
type TransferAbandonMode int

const (
	// TransferAbandonRequeue persists checkpoint and returns the task to pendingBuff (scale-down handoff).
	TransferAbandonRequeue TransferAbandonMode = iota
	// TransferAbandonDBOnly persists checkpoint and leaves the task out of pending (stop / soft-suspend).
	TransferAbandonDBOnly
)

// PersistTransferCheckpoint writes mid-transfer progress. copy_status stays pending.
func (q *Queue) PersistTransferCheckpoint(ctx context.Context, task *TaskBase, offset int64, dstRef string) error {
	if q == nil || task == nil || !task.IsFile() || offset <= 0 {
		return nil
	}
	database := q.Database()
	if database == nil {
		return fmt.Errorf("PersistTransferCheckpoint: no database")
	}
	ckpt := checkpoint.TransferCheckpoint{
		Offset:   offset,
		SrcSize:  task.File.Size,
		SrcMTime: task.File.LastUpdated,
		DstRef:   dstRef,
	}
	if err := checkpoint.UpsertTransferCheckpoint(database, ctx, task.ID, ckpt); err != nil {
		return err
	}
	task.XferOffset = offset
	task.XferSrcSize = ckpt.SrcSize
	task.XferSrcMTime = ckpt.SrcMTime
	task.XferDstRef = dstRef
	return nil
}

// ClearTransferCheckpoint clears durable resume state after success or forced full restart.
func (q *Queue) ClearTransferCheckpoint(ctx context.Context, task *TaskBase) error {
	if q == nil || task == nil || task.ID == "" {
		return nil
	}
	database := q.Database()
	if database == nil {
		return nil
	}
	if err := checkpoint.ClearTransferCheckpoint(database, ctx, task.ID); err != nil {
		return err
	}
	task.XferOffset = 0
	task.XferSrcSize = 0
	task.XferSrcMTime = ""
	task.XferDstRef = ""
	return nil
}

// LoadTransferCheckpointOntoTask fills task Xfer* fields from DB (post-lease inspect).
func (q *Queue) LoadTransferCheckpointOntoTask(ctx context.Context, task *TaskBase) error {
	if q == nil || task == nil || !task.IsFile() {
		return nil
	}
	database := q.Database()
	if database == nil {
		return nil
	}
	ckpt, err := checkpoint.GetTransferCheckpoint(database, ctx, task.ID)
	if err != nil || ckpt == nil {
		return err
	}
	task.XferOffset = ckpt.Offset
	task.XferSrcSize = ckpt.SrcSize
	task.XferSrcMTime = ckpt.SrcMTime
	task.XferDstRef = ckpt.DstRef
	return nil
}

// fingerprintMatches reports whether the checkpoint fingerprint still matches the task's SRC file.
func fingerprintMatches(task *TaskBase) bool {
	if task == nil || !task.IsFile() || task.XferOffset <= 0 {
		return false
	}
	return task.XferSrcSize == task.File.Size && task.XferSrcMTime == task.File.LastUpdated
}

// PrepareFileTransferResume applies FS restart policy after lease.
// Returns the byte offset to seek SRC to (0 = full start/restart).
// dst may be an FSAdapter or any value that implements FSTransferRestartPolicy.
func PrepareFileTransferResume(ctx context.Context, q *Queue, dst any, task *TaskBase) (resumeOffset int64, err error) {
	if err := q.LoadTransferCheckpointOntoTask(ctx, task); err != nil {
		return 0, err
	}
	if task.XferOffset <= 0 {
		return 0, nil
	}
	policy := types.ResolveTransferRestartPolicy(dst)
	if fingerprintMatches(task) && policy.SupportsResumableTransfer() {
		return task.XferOffset, nil
	}
	// Mismatch or non-resumable: clear checkpoint; optionally delete dst before full restart.
	dstRef := task.XferDstRef
	if err := q.ClearTransferCheckpoint(ctx, task); err != nil {
		return 0, err
	}
	if policy.RequiresDeleteBeforeRestart() && dstRef != "" {
		if adapter, ok := dst.(types.FSAdapter); ok {
			_ = adapter.DeleteNode(ctx, dstRef, types.NodeTypeFile)
		}
	}
	return 0, nil
}

// SeekReaderTo discards or seeks src to offset. Prefer io.Seeker when available.
func SeekReaderTo(r io.Reader, offset int64) error {
	if offset <= 0 {
		return nil
	}
	if s, ok := r.(io.Seeker); ok {
		_, err := s.Seek(offset, io.SeekStart)
		return err
	}
	_, err := io.CopyN(io.Discard, r, offset)
	return err
}

// AbandonTransferCheckpoint closes the live transfer path (caller closes FS handles),
// persists checkpoint, unlocks the task, and either requeues or leaves DB-only.
// Does not bump attempts.
func (q *Queue) AbandonTransferCheckpoint(ctx context.Context, task *TaskBase, offset int64, dstRef string, mode TransferAbandonMode) error {
	if q == nil || task == nil {
		return nil
	}
	if offset > 0 && task.IsFile() {
		// ProgressWatchdog may have cancelled ctx; persist must still succeed.
		persistCtx := context.WithoutCancel(ctx)
		if err := q.PersistTransferCheckpoint(persistCtx, task, offset, dstRef); err != nil {
			return err
		}
	}
	nodeID := task.ID
	task.Locked = false
	q.RemoveInProgress(nodeID)
	if mode == TransferAbandonRequeue {
		q.Add(task)
	}
	return nil
}
