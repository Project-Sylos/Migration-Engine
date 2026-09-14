// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package checkpoint

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// TransferCheckpoint is durable mid-transfer progress on a SRC file node.
// copy_status stays pending while a checkpoint or attempt marker is present.
// DstRef is the attempt marker (set when OpenWrite starts); ResumeToken is the
// provider resume handle (e.g. Graph uploadUrl). Offset is byte progress.
type TransferCheckpoint struct {
	Offset      int64 // last successfully transferred byte offset (0 = attempt only)
	SrcSize     int64 // fingerprint: size at checkpoint time
	SrcMTime    string
	DstRef      string // dst id/path attempt marker; cleared only on successful copy
	ResumeToken string // opaque provider resume token
}

// HasTransferCheckpoint reports whether ckpt represents a stored resume point or attempt.
func HasTransferCheckpoint(ckpt *TransferCheckpoint) bool {
	return ckpt != nil && (ckpt.Offset > 0 || ckpt.DstRef != "")
}

func loadXferStatus(d *db.DB, nodeID string) (opsdb.StatusRecord, bool, error) {
	if d == nil || d.Ops() == nil {
		return opsdb.StatusRecord{}, false, fmt.Errorf("ops store not open")
	}
	return d.Ops().GetStatus(opsdb.SideSRC, nodeID)
}

func putXferStatus(d *db.DB, nodeID string, st opsdb.StatusRecord) error {
	if d == nil || d.Ops() == nil {
		return fmt.Errorf("ops store not open")
	}
	return d.Ops().PutStatus(opsdb.SideSRC, nodeID, st)
}

// UpsertTransferCheckpoint writes checkpoint fields on the SRC status overlay.
// Does not change copy_status.
func UpsertTransferCheckpoint(d *db.DB, ctx context.Context, nodeID string, ckpt TransferCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nodeID == "" {
		return fmt.Errorf("UpsertTransferCheckpoint: empty node id")
	}
	st, ok, err := loadXferStatus(d, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		st = opsdb.StatusRecord{}
	}
	st.XferOffset = ckpt.Offset
	st.XferSrcSize = ckpt.SrcSize
	st.XferSrcMTime = ckpt.SrcMTime
	st.XferDstRef = ckpt.DstRef
	st.XferResumeToken = ckpt.ResumeToken
	return putXferStatus(d, nodeID, st)
}

// ClearTransferCheckpoint nulls all checkpoint and attempt fields (success path).
func ClearTransferCheckpoint(d *db.DB, ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nodeID == "" {
		return fmt.Errorf("ClearTransferCheckpoint: empty node id")
	}
	st, ok, err := loadXferStatus(d, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	st.XferOffset = 0
	st.XferSrcSize = 0
	st.XferSrcMTime = ""
	st.XferDstRef = ""
	st.XferResumeToken = ""
	return putXferStatus(d, nodeID, st)
}

// ClearResumeState nulls offset/fingerprint/token but keeps DstRef (attempt marker).
func ClearResumeState(d *db.DB, ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if nodeID == "" {
		return fmt.Errorf("ClearResumeState: empty node id")
	}
	st, ok, err := loadXferStatus(d, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	st.XferOffset = 0
	st.XferSrcSize = 0
	st.XferSrcMTime = ""
	st.XferResumeToken = ""
	return putXferStatus(d, nodeID, st)
}

// GetTransferCheckpoint loads checkpoint fields for a SRC node. Returns nil if none.
func GetTransferCheckpoint(d *db.DB, ctx context.Context, nodeID string) (*TransferCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, fmt.Errorf("GetTransferCheckpoint: empty node id")
	}
	st, ok, err := d.Ops().GetStatus(opsdb.SideSRC, nodeID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	ckpt := &TransferCheckpoint{
		Offset:      st.XferOffset,
		SrcSize:     st.XferSrcSize,
		SrcMTime:    st.XferSrcMTime,
		DstRef:      st.XferDstRef,
		ResumeToken: st.XferResumeToken,
	}
	if !HasTransferCheckpoint(ckpt) {
		return nil, nil
	}
	return ckpt, nil
}
