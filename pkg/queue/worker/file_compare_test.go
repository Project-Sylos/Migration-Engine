// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestResolveFileOverwritePolicies(t *testing.T) {
	older := time.Now().Add(-time.Hour).Format(time.RFC3339)
	newer := time.Now().Format(time.RFC3339)
	srcNewer := types.File{LastUpdated: newer, Size: 10}
	dstOlder := types.File{LastUpdated: older, Size: 10}
	srcOlder := types.File{LastUpdated: older, Size: 10}
	dstNewer := types.File{LastUpdated: newer, Size: 10}

	out := resolveFileOverwrite(queue.OverwriteNewestMTime, srcNewer, dstOlder)
	if out.SrcCopyStatus != db.CopyStatusPending || out.DstCopyStatus != db.CopyStatusNeedsOverwrite {
		t.Fatalf("newest_mtime src newer: %+v", out)
	}
	out = resolveFileOverwrite(queue.OverwriteNewestMTime, srcOlder, dstNewer)
	if out.SrcCopyStatus != db.CopyStatusAlreadyExisted || out.DstCopyStatus != db.CopyStatusAlreadyExisted {
		t.Fatalf("newest_mtime dst newer: %+v", out)
	}

	out = resolveFileOverwrite(queue.OverwriteDecide, srcOlder, dstNewer)
	if out.SrcCopyStatus != db.CopyStatusPending || out.DstCopyStatus != db.CopyStatusNeedsOverwrite || out.ExclusionSource != "" {
		t.Fatalf("decide: %+v", out)
	}

	out = resolveFileOverwrite(queue.OverwriteSourceWins, srcOlder, dstNewer)
	if out.SrcCopyStatus != db.CopyStatusPending ||
		out.DstCopyStatus != db.CopyStatusNeedsOverwrite ||
		out.ExclusionSource != opsdb.ExclusionForceReplaceCopy {
		t.Fatalf("source_wins: %+v", out)
	}
}
