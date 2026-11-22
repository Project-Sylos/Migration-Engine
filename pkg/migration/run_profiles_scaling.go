// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"cmp"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

// newScalingContext builds the queue scaling context; an empty mode uses defaultMode.
func newScalingContext(queueName, defaultMode string, mode queue.QueueMode, copyPass int, srcService, dstService Service) queue.ScalingContext {
	return queue.ScalingContext{
		QueueName:   queueName,
		Mode:        cmp.Or(string(mode), defaultMode),
		CopyPass:    copyPass,
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
