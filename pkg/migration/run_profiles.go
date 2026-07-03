// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
)

func resolveServiceProfiles(cfg MigrationConfig) (src, dst, merged scaling.FSPerformanceProfile) {
	src = scaling.ApplyAdapterListPagination(
		scaling.LookupProfile(cfg.SrcService.ProviderID, cfg.SrcService.Name),
		cfg.SrcAdapter,
	)
	dst = scaling.ApplyAdapterListPagination(
		scaling.LookupProfile(cfg.DstService.ProviderID, cfg.DstService.Name),
		cfg.DstAdapter,
	)
	merged = scaling.MergeRunProfiles(src, dst)
	return src, dst, merged
}

func resolveWorkersForProfile(requested int, suspend *RuntimeSuspendV1, merged scaling.FSPerformanceProfile) int {
	wc := effectiveWorkerCount(requested, suspend)
	if wc <= 0 {
		return scaling.EffectiveWorkers(0, merged)
	}
	return scaling.EffectiveWorkers(wc, merged)
}

func queueSizingFromProfileOrSuspend(suspend *RuntimeSuspendV1, merged scaling.FSPerformanceProfile) *queue.QueueSizing {
	if s := queueSizingFromSuspend(suspend); s != nil {
		return s
	}
	if batch := scaling.QueueBatchSizingFromProfile(merged); batch != nil {
		return &queue.QueueSizing{
			LeaseBatchSize:  batch.LeaseBatchSize,
			RefillBatchSize: batch.RefillBatchSize,
		}
	}
	return nil
}
