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

// FileTransferPlan is the result of PrepareFileTransferResume.
type FileTransferPlan struct {
	ResumeOffset int64  // seek SRC to this offset (0 = start/restart)
	ResumeToken  string // provider resume token when resuming
	DstRef       string // existing dst attempt ref (empty = create fresh)
	Restart      bool   // true when an attempt exists but must delete+restart
}

// PersistTransferCheckpointToken writes mid-transfer progress including an opaque provider resume token.
// copy_status stays pending. offset may be 0 when recording an attempt marker (dstRef) at OpenWrite time.
func (q *Queue) PersistTransferCheckpointToken(ctx context.Context, task *TaskBase, offset int64, dstRef, resumeToken string) error {
	if q == nil || task == nil || !task.IsFile() {
		return nil
	}
	if offset <= 0 && dstRef == "" && resumeToken == "" {
		return nil
	}
	database := q.Database()
	if database == nil {
		return fmt.Errorf("PersistTransferCheckpoint: no database")
	}
	if dstRef == "" {
		dstRef = task.XferDstRef
	}
	if resumeToken == "" {
		resumeToken = task.XferResumeToken
	}
	ckpt := checkpoint.TransferCheckpoint{
		Offset:      offset,
		SrcSize:     task.File.Size,
		SrcMTime:    task.File.LastUpdated,
		DstRef:      dstRef,
		ResumeToken: resumeToken,
	}
	if err := checkpoint.UpsertTransferCheckpoint(database, ctx, task.ID, ckpt); err != nil {
		return err
	}
	task.XferOffset = offset
	task.XferSrcSize = ckpt.SrcSize
	task.XferSrcMTime = ckpt.SrcMTime
	task.XferDstRef = dstRef
	task.XferResumeToken = resumeToken
	return nil
}

// ClearTransferCheckpoint clears all durable resume/attempt state after success.
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
	task.XferResumeToken = ""
	return nil
}

// ClearResumeState clears offset/fingerprint/token but keeps the attempt marker (DstRef).
func (q *Queue) ClearResumeState(ctx context.Context, task *TaskBase) error {
	if q == nil || task == nil || task.ID == "" {
		return nil
	}
	database := q.Database()
	if database == nil {
		return nil
	}
	if err := checkpoint.ClearResumeState(database, ctx, task.ID); err != nil {
		return err
	}
	task.XferOffset = 0
	task.XferSrcSize = 0
	task.XferSrcMTime = ""
	task.XferResumeToken = ""
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
	task.XferResumeToken = ckpt.ResumeToken
	return nil
}

// HasCopyAttempt reports whether this task has a durable DST attempt marker from this migration.
func HasCopyAttempt(task *TaskBase) bool {
	return task != nil && task.IsFile() && task.XferDstRef != ""
}

// fingerprintMatches reports whether the checkpoint fingerprint still matches the task's SRC file.
func fingerprintMatches(task *TaskBase) bool {
	if task == nil || !task.IsFile() || task.XferOffset <= 0 {
		return false
	}
	return task.XferSrcSize == task.File.Size && task.XferSrcMTime == task.File.LastUpdated
}

// PrepareFileTransferResume applies FS restart policy after lease.
// Returns a plan: resume from offset, or restart (optionally deleting the prior attempt).
// dst may be an FSAdapter or any value that implements FSTransferRestartPolicy.
func PrepareFileTransferResume(ctx context.Context, q *Queue, dst any, task *TaskBase) (FileTransferPlan, error) {
	if err := q.LoadTransferCheckpointOntoTask(ctx, task); err != nil {
		return FileTransferPlan{}, err
	}
	policy := types.ResolveTransferRestartPolicy(dst)
	if task.XferOffset <= 0 && task.XferDstRef == "" {
		return FileTransferPlan{}, nil
	}
	if fingerprintMatches(task) && policy.SupportsResumableTransfer() {
		return FileTransferPlan{
			ResumeOffset: task.XferOffset,
			ResumeToken:  task.XferResumeToken,
			DstRef:       task.XferDstRef,
		}, nil
	}
	// Mismatch or non-resumable: clear resume state; keep attempt marker for delete-before-restart.
	dstRef := task.XferDstRef
	if err := q.ClearResumeState(ctx, task); err != nil {
		return FileTransferPlan{}, err
	}
	task.XferDstRef = dstRef
	if dstRef == "" {
		return FileTransferPlan{}, nil
	}
	if policy.RequiresDeleteBeforeRestart() || !policy.SupportsResumableTransfer() {
		if adapter, ok := dst.(types.FSAdapter); ok {
			_ = adapter.DeleteNode(ctx, dstRef, types.NodeTypeFile)
		}
		if err := q.ClearTransferCheckpoint(ctx, task); err != nil {
			return FileTransferPlan{}, err
		}
		return FileTransferPlan{Restart: true}, nil
	}
	return FileTransferPlan{DstRef: dstRef, Restart: true}, nil
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

// AbandonTransferCheckpointToken closes the live transfer path (caller closes FS handles),
// persists checkpoint (with optional provider resume token), unlocks the task, and either
// requeues or leaves DB-only. Does not bump attempts.
func (q *Queue) AbandonTransferCheckpointToken(ctx context.Context, task *TaskBase, offset int64, dstRef, resumeToken string, mode TransferAbandonMode) error {
	if q == nil || task == nil {
		return nil
	}
	if task.IsFile() && (offset > 0 || dstRef != "" || resumeToken != "") {
		persistCtx := context.WithoutCancel(ctx)
		if err := q.PersistTransferCheckpointToken(persistCtx, task, offset, dstRef, resumeToken); err != nil {
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
