// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// ActiveOperations returns FS operations performed by workers for this context.
func ActiveOperations(ctx queue.ScalingContext) []FSOperation {
	switch ctx.Mode {
	case queue.ScalingModeCopy, queue.ScalingModeCopyRetry:
		if ctx.CopyPass == 1 {
			return []FSOperation{OpCreateFolder}
		}
		return []FSOperation{OpDownload, OpUpload}
	case queue.ScalingModeTraversal, queue.ScalingModeRetry:
		return []FSOperation{OpListChildren}
	case queue.ScalingModeDelete, queue.ScalingModeDeleteRetry:
		return []FSOperation{OpDelete}
	default:
		return nil
	}
}

// ResolveOperationProfile returns the composed OperationProfile for the context (before adapter pagination).
func ResolveOperationProfile(ctx queue.ScalingContext) OperationProfile {
	switch ctx.Mode {
	case queue.ScalingModeTraversal, queue.ScalingModeRetry:
		switch ctx.QueueName {
		case "src":
			return LookupOperationProfile(ctx.SrcProvider, "", OpListChildren)
		case "dst":
			return LookupOperationProfile(ctx.DstProvider, "", OpListChildren)
		default:
			if ctx.SrcProvider != "" {
				return LookupOperationProfile(ctx.SrcProvider, "", OpListChildren)
			}
			return LookupOperationProfile(ctx.DstProvider, "", OpListChildren)
		}
	case queue.ScalingModeCopy, queue.ScalingModeCopyRetry:
		if ctx.CopyPass == 1 {
			return LookupOperationProfile(ctx.DstProvider, "", OpCreateFolder)
		}
		srcDL := LookupOperationProfile(ctx.SrcProvider, "", OpDownload)
		dstUL := LookupOperationProfile(ctx.DstProvider, "", OpUpload)
		return ComposePipelineMin(srcDL, dstUL)
	case queue.ScalingModeDelete, queue.ScalingModeDeleteRetry:
		return LookupOperationProfile(ctx.DstProvider, "", OpDelete)
	default:
		return LookupOperationProfile("generic", "", OpListChildren)
	}
}

// ResolveEffectiveProfile returns actuator-ready profile for the context.
func ResolveEffectiveProfile(ctx queue.ScalingContext, srcAdapter, dstAdapter fstypes.FSAdapter) FSPerformanceProfile {
	op := ResolveOperationProfile(ctx)
	prof := ToActuatorProfile(op)
	if !usesListKnobs(ctx) {
		return prof
	}
	var adapter fstypes.FSAdapter
	switch ctx.QueueName {
	case "src":
		adapter = srcAdapter
	case "dst":
		adapter = dstAdapter
	}
	if adapter != nil {
		prof = ApplyAdapterListPagination(prof, adapter)
	}
	return prof
}

func usesListKnobs(ctx queue.ScalingContext) bool {
	for _, op := range ActiveOperations(ctx) {
		if op == OpListChildren {
			return true
		}
	}
	return false
}

// ResolveInitialWorkers returns startup worker count for a scaling context.
func ResolveInitialWorkers(ctx queue.ScalingContext, requested int) int {
	op := ResolveOperationProfile(ctx)
	return EffectiveWorkersFromOperation(requested, op)
}
