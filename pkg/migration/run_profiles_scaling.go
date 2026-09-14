// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/backend"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

func scalingContextForTraversal(queueName string, srcService, dstService Service, mode queue.QueueMode) queue.ScalingContext {
	scalingMode := string(mode)
	if scalingMode == "" {
		scalingMode = queue.ScalingModeTraversal
	}
	return queue.ScalingContext{
		QueueName:   queueName,
		Mode:        scalingMode,
		SrcProvider: srcService.ProviderID,
		DstProvider: dstService.ProviderID,
	}
}

func scalingContextForCopy(srcService, dstService Service, copyPass int, mode queue.QueueMode) queue.ScalingContext {
	scalingMode := string(mode)
	if scalingMode == "" {
		scalingMode = queue.ScalingModeCopy
	}
	return queue.ScalingContext{
		QueueName:   "copy",
		Mode:        scalingMode,
		CopyPass:    copyPass,
		SrcProvider: srcService.ProviderID,
		DstProvider: dstService.ProviderID,
	}
}

func scalingContextForDelete(srcService, dstService Service, mode queue.QueueMode) queue.ScalingContext {
	scalingMode := string(mode)
	if scalingMode == "" {
		scalingMode = queue.ScalingModeDelete
	}
	return queue.ScalingContext{
		QueueName:   "delete",
		Mode:        scalingMode,
		SrcProvider: srcService.ProviderID,
		DstProvider: dstService.ProviderID,
	}
}

func resolveWorkersForScalingContext(ctx queue.ScalingContext, requested int, suspend *RuntimeSuspendV1) int {
	suspendWorkers := 0
	if suspend != nil {
		suspendWorkers = suspend.WorkerCount
	}
	wc := effectiveInt(suspendWorkers, requested)
	return profile.ResolveInitialWorkers(ctx, wc)
}

func queueSizingForScalingContext(ctx queue.ScalingContext, suspend *RuntimeSuspendV1) *queue.QueueSizing {
	if s := queueSizingFromSuspend(suspend); s != nil {
		return s
	}
	for _, op := range profile.ActiveOperations(ctx) {
		if op == profile.OpListChildren {
			prof := profile.ToActuatorProfile(profile.ResolveOperationProfile(ctx))
			if batch := profile.QueueBatchSizingFromProfile(prof); batch != nil {
				sizing := &queue.QueueSizing{
					LeaseBatchSize:  batch.LeaseBatchSize,
					RefillBatchSize: batch.RefillBatchSize,
				}
				if ctx.QueueName == "dst" && (ctx.Mode == "" || ctx.Mode == queue.ScalingModeTraversal) {
					if sizing.RefillBatchSize <= 0 || sizing.RefillBatchSize >= 10000 {
						sizing.RefillBatchSize = profile.DstTraversalDefaultRefillBatch
					}
				}
				return sizing
			}
		}
	}
	return nil
}

func wireQueueScalingContext(srcQ, dstQ, copyQ *queue.Queue, srcService, dstService Service, sameBackend bool, pathCheckProfile string, windowsCompat bool) {
	srcGroup := backend.ResolveGroupID(srcService.BackendGroupID, "", "src")
	dstGroup := backend.ResolveGroupID(dstService.BackendGroupID, "", "dst")
	if sameBackend {
		dstGroup = srcGroup
	}
	srcProv := srcService.ProviderID
	dstProv := dstService.ProviderID
	if srcQ != nil {
		srcQ.ConfigureScalingContext(srcProv, dstProv, srcGroup, dstGroup, pathCheckProfile, windowsCompat)
	}
	if dstQ != nil {
		dstQ.ConfigureScalingContext(srcProv, dstProv, srcGroup, dstGroup, pathCheckProfile, windowsCompat)
	}
	if copyQ != nil {
		copyQ.ConfigureScalingContext(srcProv, dstProv, "queue:copy", "queue:copy", pathCheckProfile, windowsCompat)
	}
}

func wireDeleteQueueScalingContext(deleteQ *queue.Queue, srcService, dstService Service, pathCheckProfile string, windowsCompat bool) {
	if deleteQ == nil {
		return
	}
	deleteQ.ConfigureScalingContext(srcService.ProviderID, dstService.ProviderID, "queue:delete", "queue:delete", pathCheckProfile, windowsCompat)
}
