// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

type fileCompareOutcome struct {
	SrcCopyStatus   string
	DstCopyStatus   string
	ExclusionSource string
}

func resolveFileOverwrite(policy string, src, dst types.File) fileCompareOutcome {
	out := fileCompareOutcome{
		SrcCopyStatus: db.CopyStatusPending,
		DstCopyStatus: db.CopyStatusNeedsOverwrite,
	}
	switch queue.NormalizeOverwritePolicy(policy) {
	case queue.OverwriteDecide:
		return out
	case queue.OverwriteSourceWins:
		out.ExclusionSource = opsdb.ExclusionForceReplaceCopy
		return out
	default:
		switch compareTimestamps(src.LastUpdated, dst.LastUpdated) {
		case "Successful":
			out.SrcCopyStatus = db.CopyStatusAlreadyExisted
			out.DstCopyStatus = db.CopyStatusAlreadyExisted
			out.ExclusionSource = ""
		case "Unparseable":
			if src.Size > 0 && src.Size == dst.Size {
				out.SrcCopyStatus = db.CopyStatusAlreadyExisted
				out.DstCopyStatus = db.CopyStatusAlreadyExisted
				out.ExclusionSource = ""
			}
		}
		return out
	}
}
