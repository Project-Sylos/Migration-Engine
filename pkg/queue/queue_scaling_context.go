// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// SetScalingMigrationContext configures provider and backend group IDs for operation-based scaling.
func (q *Queue) SetScalingMigrationContext(srcProvider, dstProvider, srcGroupID, dstGroupID string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.scalingSrcProvider = srcProvider
	q.scalingDstProvider = dstProvider
	q.scalingSrcGroupID = srcGroupID
	q.scalingDstGroupID = dstGroupID
}

// ScalingContext returns the current scaling context for autoscaler profile resolution.
func (q *Queue) ScalingContext() ScalingContext {
	if q == nil {
		return ScalingContext{}
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	mode := string(q.mode)
	if mode == "" {
		mode = ScalingModeTraversal
	}
	return ScalingContext{
		QueueName:   q.name,
		Mode:        mode,
		CopyPass:    q.copyPass,
		SrcProvider: q.scalingSrcProvider,
		DstProvider: q.scalingDstProvider,
		SrcGroupID:  q.scalingSrcGroupID,
		DstGroupID:  q.scalingDstGroupID,
	}
}

// ScalingSrcAdapter returns the copy source adapter when this queue runs copy workers.
func (q *Queue) ScalingSrcAdapter() types.FSAdapter {
	if q == nil {
		return nil
	}
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	if q.pool.isCopy {
		return q.pool.copySrcAdapter
	}
	if q.name == "src" {
		return q.pool.traversalAdapter
	}
	return nil
}

// ScalingDstAdapter returns the copy destination adapter or dst traversal adapter.
func (q *Queue) ScalingDstAdapter() types.FSAdapter {
	if q == nil {
		return nil
	}
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	if q.pool.isCopy {
		return q.pool.copyDstAdapter
	}
	if q.name == "dst" {
		return q.pool.traversalAdapter
	}
	return nil
}
