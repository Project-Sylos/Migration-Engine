// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
)

// WriteReviewStatsSnapshot writes the full canonical review stats snapshot to the universal stats table (replaces counts for review keys).
func (w *Writer) WriteReviewStatsSnapshot(s ReviewStatsSnapshot) error {
	ctx := context.Background()
	pairs := []struct {
		key   string
		count int64
	}{
		{ReviewKeyTraversalPending, s.TraversalPending},
		{ReviewKeyTraversalPendingRetry, s.TraversalPendingRetry},
		{ReviewKeyTraversalSuccessful, s.TraversalSuccessful},
		{ReviewKeyTraversalFailed, s.TraversalFailed},
		{ReviewKeyCopyPending, s.CopyPending},
		{ReviewKeyCopySuccessful, s.CopySuccessful},
		{ReviewKeyCopyFailed, s.CopyFailed},
		{ReviewKeyDeletePending, s.DeletePending},
		{ReviewKeyDeleteDeleted, s.DeleteDeleted},
		{ReviewKeyDeleteFailed, s.DeleteFailed},
		{ReviewKeyExcluded, s.Excluded},
		{ReviewKeyFolders, s.Folders},
		{ReviewKeyFiles, s.Files},
		{ReviewKeySizeSrc, s.SizeSrc},
		{ReviewKeySizeDst, s.SizeDst},
		{ReviewKeySizeSelected, s.SizeSelected},
		{ReviewKeySizeDeleteSelected, s.SizeDeleteSelected},
	}
	for _, p := range pairs {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableStats+` (key, count) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET count = excluded.count`,
			p.key, p.count,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
