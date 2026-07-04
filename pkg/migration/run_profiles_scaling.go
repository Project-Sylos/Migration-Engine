// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
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

func resolveWorkersForScalingContext(ctx queue.ScalingContext, requested int, suspend *RuntimeSuspendV1) int {
	wc := effectiveWorkerCount(requested, suspend)
	return scaling.ResolveInitialWorkers(ctx, wc)
}

func queueSizingForScalingContext(ctx queue.ScalingContext, suspend *RuntimeSuspendV1) *queue.QueueSizing {
	if s := queueSizingFromSuspend(suspend); s != nil {
		return s
	}
	for _, op := range scaling.ActiveOperations(ctx) {
		if op == scaling.OpListChildren {
			prof := scaling.ToActuatorProfile(scaling.ResolveOperationProfile(ctx))
			if batch := scaling.QueueBatchSizingFromProfile(prof); batch != nil {
				return &queue.QueueSizing{
					LeaseBatchSize:  batch.LeaseBatchSize,
					RefillBatchSize: batch.RefillBatchSize,
				}
			}
		}
	}
	return nil
}

func wireQueueScalingContext(srcQ, dstQ, copyQ *queue.Queue, srcService, dstService Service, sameBackend bool) {
	srcGroup := scaling.ResolveGroupID(srcService.BackendGroupID, "", "src")
	dstGroup := scaling.ResolveGroupID(dstService.BackendGroupID, "", "dst")
	if sameBackend {
		dstGroup = srcGroup
	}
	srcProv := srcService.ProviderID
	dstProv := dstService.ProviderID
	if srcQ != nil {
		srcQ.SetScalingMigrationContext(srcProv, dstProv, srcGroup, dstGroup)
	}
	if dstQ != nil {
		dstQ.SetScalingMigrationContext(srcProv, dstProv, srcGroup, dstGroup)
	}
	if copyQ != nil {
		copyQ.SetScalingMigrationContext(srcProv, dstProv, "queue:copy", "queue:copy")
	}
}
