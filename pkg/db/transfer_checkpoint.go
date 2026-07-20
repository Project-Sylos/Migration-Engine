// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
)

// TransferCheckpoint is durable mid-transfer progress on a SRC file node.
// copy_status stays pending while a checkpoint is present; workers branch on Offset > 0.
type TransferCheckpoint struct {
	Offset   int64  // last successfully transferred byte offset
	SrcSize  int64  // fingerprint: size at checkpoint time
	SrcMTime string
	DstRef   string // dst id/path for delete-on-mismatch if needed
}

// HasTransferCheckpoint reports whether ckpt represents a stored resume point.
func HasTransferCheckpoint(ckpt *TransferCheckpoint) bool {
	return ckpt != nil && ckpt.Offset > 0
}

// UpsertTransferCheckpoint writes checkpoint columns on src_nodes. Does not change copy_status.
func (d *DB) UpsertTransferCheckpoint(ctx context.Context, nodeID string, ckpt TransferCheckpoint) error {
	if nodeID == "" {
		return fmt.Errorf("UpsertTransferCheckpoint: empty node id")
	}
	return d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			_, err := w.tx.ExecContext(ctx, `
UPDATE `+tableSrcNodes+` SET
	`+colXferOffset+` = $1,
	`+colXferSrcSize+` = $2,
	`+colXferSrcMTime+` = $3,
	`+colXferDstRef+` = $4
WHERE id = $5`,
				ckpt.Offset, ckpt.SrcSize, ckpt.SrcMTime, ckpt.DstRef, nodeID)
			return err
		})
	})
}

// ClearTransferCheckpoint nulls checkpoint columns on src_nodes.
func (d *DB) ClearTransferCheckpoint(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("ClearTransferCheckpoint: empty node id")
	}
	return d.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			_, err := w.tx.ExecContext(ctx, `
UPDATE `+tableSrcNodes+` SET
	`+colXferOffset+` = NULL,
	`+colXferSrcSize+` = NULL,
	`+colXferSrcMTime+` = NULL,
	`+colXferDstRef+` = NULL
WHERE id = $1`, nodeID)
			return err
		})
	})
}

// GetTransferCheckpoint loads checkpoint columns for a SRC node. Returns nil if none.
func (d *DB) GetTransferCheckpoint(ctx context.Context, nodeID string) (*TransferCheckpoint, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("GetTransferCheckpoint: empty node id")
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	var offset, size sql.NullInt64
	var mtime, dstRef sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT `+colXferOffset+`, `+colXferSrcSize+`, `+colXferSrcMTime+`, `+colXferDstRef+`
FROM `+tableSrcNodes+` WHERE id = $1`, nodeID).Scan(&offset, &size, &mtime, &dstRef)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !offset.Valid || offset.Int64 <= 0 {
		return nil, nil
	}
	ckpt := &TransferCheckpoint{Offset: offset.Int64}
	if size.Valid {
		ckpt.SrcSize = size.Int64
	}
	if mtime.Valid {
		ckpt.SrcMTime = mtime.String
	}
	if dstRef.Valid {
		ckpt.DstRef = dstRef.String
	}
	return ckpt, nil
}
