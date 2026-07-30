// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"time"

	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// ScalingEvent describes one knob change for tests and logs.
type ScalingEvent struct {
	Queue    string
	Knob     string
	OldValue int
	NewValue int
	Pressure PressureClass
	At       time.Time
}

// QueueActuator applies knob changes to a queue.
type QueueActuator interface {
	GetWorkerCount() int
	SetTargetWorkerCount(n int) error
	GetInterOpDelay() time.Duration
	SetInterOpDelay(d time.Duration)
	EffectiveLeaseBatchSize() int
	SetLeaseBatchSize(n int)
	EffectiveRefillBatchSize() int
	SetRefillBatchSize(n int)
	GetListPageSize() int
	SetListPageSize(n int)
	ListItemsP95() int
	GetPendingCount() int
	InProgressCount() int
	ScalingContext() queue.ScalingContext
	ScalingAdapter(srcSide bool) fstypes.FSAdapter
	// ReleaseInFlightOnThrottle requeues in-progress work after an FS_THROTTLE step-down.
	ReleaseInFlightOnThrottle()
}
