// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func reviewDeltaMap(deltas []ReviewStatsDelta) map[string]int64 {
	out := make(map[string]int64, len(deltas))
	for _, d := range deltas {
		out[d.Key] += d.Delta
	}
	return out
}

func TestDiscoveryReviewDeltasSRCFilePending(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(discoveryReviewDeltas("SRC", &NodeState{
		Type: NodeTypeFile, Size: 42, CopyStatus: CopyStatusPending,
	}))
	if got[ReviewKeyCopyPending] != 1 {
		t.Fatalf("copy/pending=%d want 1", got[ReviewKeyCopyPending])
	}
	if got[ReviewKeyFiles] != 1 {
		t.Fatalf("files=%d want 1", got[ReviewKeyFiles])
	}
	if got[ReviewKeySizeSrc] != 42 {
		t.Fatalf("size_src=%d want 42", got[ReviewKeySizeSrc])
	}
	if got[ReviewKeySizeSelected] != 42 {
		t.Fatalf("size_selected=%d want 42", got[ReviewKeySizeSelected])
	}
	if got[ReviewKeyFolders] != 0 {
		t.Fatalf("folders=%d want 0", got[ReviewKeyFolders])
	}
}

func TestDiscoveryReviewDeltasSRCFolderPending(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(discoveryReviewDeltas("SRC", &NodeState{
		Type: NodeTypeFolder, CopyStatus: "",
	}))
	if got[ReviewKeyFolders] != 1 {
		t.Fatalf("folders=%d want 1", got[ReviewKeyFolders])
	}
	if got[ReviewKeyCopyPending] != 1 {
		t.Fatalf("copy/pending=%d want 1", got[ReviewKeyCopyPending])
	}
}

func TestDiscoveryReviewDeltasDSTFileSizeOnly(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(discoveryReviewDeltas("DST", &NodeState{
		Type: NodeTypeFile, Size: 7,
	}))
	if got[ReviewKeySizeDst] != 7 {
		t.Fatalf("size_dst=%d want 7", got[ReviewKeySizeDst])
	}
	if got[ReviewKeyFiles] != 0 || got[ReviewKeyCopyPending] != 0 {
		t.Fatalf("dst must not increment copy selected counters: %#v", got)
	}
	if got[ReviewKeyTraversalPending] != 0 || got[ReviewKeyTraversalFailed] != 0 {
		t.Fatalf("dst must not increment SRC traversal footer counters: %#v", got)
	}
}

func TestStatusEventReviewDeltasTraversalFailDoesNotTouchCopyPending(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		TraversalStatus:     StatusFailed,
		PrevTraversalStatus: StatusPending,
		NodeType:            NodeTypeFolder,
	}, false))
	if got[ReviewKeyCopyPending] != 0 {
		t.Fatalf("st:trav fail must not move st:copy counters: %#v", got)
	}
	if got[ReviewKeyTraversalFailed] != 1 {
		t.Fatalf("failed=%d want 1", got[ReviewKeyTraversalFailed])
	}
}

func TestStatusEventReviewDeltasDSTFailDoesNotMoveFooterFailed(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("DST", StatusEvent{
		TraversalStatus:     StatusFailed,
		PrevTraversalStatus: StatusPending,
		NodeType:            NodeTypeFolder,
	}, false))
	if got[ReviewKeyTraversalPending] != 0 || got[ReviewKeyTraversalFailed] != 0 {
		t.Fatalf("DST fail must not move SRC footer counters: %#v", got)
	}
}

func TestStatusEventReviewDeltasCopyPendingToAlreadyExisted(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		CopyStatus:          CopyStatusAlreadyExisted,
		PrevCopyStatus:      CopyStatusPending,
		NodeType:            NodeTypeFile,
		Size:                10,
		TraversalStatus:     StatusSuccessful,
		PrevTraversalStatus: StatusSuccessful,
	}, false))
	if got[ReviewKeyCopyPending] != -1 {
		t.Fatalf("copy/pending=%d want -1", got[ReviewKeyCopyPending])
	}
	if got[ReviewKeyCopySuccessful] != 1 {
		t.Fatalf("copy/successful=%d want 1", got[ReviewKeyCopySuccessful])
	}
	if got[ReviewKeyFiles] != -1 {
		t.Fatalf("files=%d want -1", got[ReviewKeyFiles])
	}
	if got[ReviewKeySizeSelected] != -10 {
		t.Fatalf("size_selected=%d want -10", got[ReviewKeySizeSelected])
	}
}

func TestStatusEventReviewDeltasEmptyCopyIsUnchanged(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		TraversalStatus:     StatusFailed,
		PrevTraversalStatus: StatusPending,
		CopyStatus:          "",
		PrevCopyStatus:      CopyStatusSuccessful,
		NodeType:            NodeTypeFolder,
	}, false))
	if got[ReviewKeyCopyPending] != 0 || got[ReviewKeyCopySuccessful] != 0 {
		t.Fatalf("empty copy must not move copy counters: %#v", got)
	}
	if got[ReviewKeyTraversalPending] != -1 || got[ReviewKeyTraversalFailed] != 1 {
		t.Fatalf("traversal deltas = %#v", got)
	}
	if got[ReviewKeyFolders] != 0 {
		t.Fatalf("folders=%d want 0", got[ReviewKeyFolders])
	}
}

func TestStatusEventReviewDeltasCopyOmitsTraversal(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		CopyStatus:     CopyStatusAlreadyExisted,
		PrevCopyStatus: CopyStatusPending,
		NodeType:       NodeTypeFile,
		Size:           8,
	}, false))
	if got[ReviewKeyTraversalPending] != 0 || got[ReviewKeyTraversalSuccessful] != 0 {
		t.Fatalf("omitted traversal must not move traversal counters: %#v", got)
	}
	if got[ReviewKeyCopyPending] != -1 || got[ReviewKeyCopySuccessful] != 1 {
		t.Fatalf("copy deltas = %#v", got)
	}
}

func TestStatusEventReviewDeltasFromRetryPending(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		TraversalStatus:     StatusSuccessful,
		PrevTraversalStatus: StatusPending,
		NodeType:            NodeTypeFolder,
		ExclusionSource:     opsdb.ExclusionRetryParkCopy,
	}, true))
	if got[ReviewKeyTraversalPending] != 0 {
		t.Fatalf("fromRetry must not debit traversal/pending: %#v", got)
	}
	if got[ReviewKeyTraversalPendingRetry] != -1 {
		t.Fatalf("pending_retry=%d want -1", got[ReviewKeyTraversalPendingRetry])
	}
	if got[ReviewKeyTraversalSuccessful] != 1 {
		t.Fatalf("successful=%d want 1", got[ReviewKeyTraversalSuccessful])
	}
	if got[ReviewKeyCopyPending] != 0 {
		t.Fatalf("discovery retry must not touch copy/pending: %#v", got)
	}
}

func TestStatusEventReviewDeltasRetryModeUnmarkedDoesNotInflateCopyPending(t *testing.T) {
	t.Parallel()
	// Still-pending children completed during a retry sweep are not mark-parked.
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		TraversalStatus:     StatusSuccessful,
		PrevTraversalStatus: StatusPending,
		NodeType:            NodeTypeFolder,
	}, false))
	if got[ReviewKeyCopyPending] != 0 {
		t.Fatalf("unmarked completion must not touch copy/pending: %#v", got)
	}
	if got[ReviewKeyTraversalPending] != -1 || got[ReviewKeyTraversalSuccessful] != 1 {
		t.Fatalf("unmarked completion traversal deltas = %#v", got)
	}
	if got[ReviewKeyTraversalPendingRetry] != 0 {
		t.Fatalf("unmarked must not debit pending_retry: %#v", got)
	}
}

func TestStatusEventReviewDeltasDeleteOmitsCopy(t *testing.T) {
	t.Parallel()
	got := reviewDeltaMap(statusEventReviewDeltas("SRC", StatusEvent{
		DeleteStatus:     DeleteStatusDeleted,
		PrevDeleteStatus: DeleteStatusPendingExplicit,
		NodeType:         NodeTypeFile,
		Size:             20,
	}, false))
	if got[ReviewKeyCopyPending] != 0 || got[ReviewKeyCopySuccessful] != 0 || got[ReviewKeySizeSelected] != 0 {
		t.Fatalf("delete-only must not move copy pending/selected: %#v", got)
	}
	if got[ReviewKeyDeletePending] != -1 || got[ReviewKeyDeleteDeleted] != 1 {
		t.Fatalf("delete deltas = %#v", got)
	}
	if got[ReviewKeySizeDeleteSelected] != -20 {
		t.Fatalf("size_delete_selected=%d want -20", got[ReviewKeySizeDeleteSelected])
	}
	if got[ReviewKeySizeSrc] != -20 {
		t.Fatalf("size_src=%d want -20", got[ReviewKeySizeSrc])
	}
}

func TestEventDepthStatsDeltasEmptyCopyIsUnchanged(t *testing.T) {
	t.Parallel()
	got := eventDepthStatsDeltas("SRC", StatusEvent{
		TraversalStatus:     StatusFailed,
		PrevTraversalStatus: StatusPending,
		CopyStatus:          "",
		PrevCopyStatus:      CopyStatusPending,
		NodeType:            NodeTypeFile,
		Depth:               2,
		Size:                5,
	})
	for _, d := range got {
		if d.Key == StatsKeyTyped(StatsKindCopy, CopyStatusPending, NodeTypeFile) ||
			d.Key == StatsKeyCopyFileBytes(CopyStatusPending) {
			t.Fatalf("empty copy must not move depth copy counters: %#v", got)
		}
	}
}
