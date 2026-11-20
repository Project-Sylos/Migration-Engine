// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"context"
	"time"

	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// ScalingEvent describes one knob change for tests and logs.
type ScalingEvent struct {
	Queue    string        `json:"queue"`
	Knob     string        `json:"knob"`
	OldValue int           `json:"oldValue"`
	NewValue int           `json:"newValue"`
	Pressure PressureClass `json:"pressure"`
	At       time.Time     `json:"at"`
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
	EffectiveDstPullChildMultiplier() int
	SetDstPullChildMultiplier(n int)
	EffectiveDstPullChildQuota(taskQuota int) int
	GetListPageSize() int
	SetListPageSize(n int)
	ListItemsP95() int
	GetPendingCount() int
	InProgressCount() int
	ScalingContext() queue.ScalingContext
	ScalingAdapter(srcSide bool) fstypes.FSAdapter
}

// MemoryShedding is optional on QueueActuator: pause pulls/leases, drop the
// in-memory pendingBuff, drain in-flight work, then rewind the keyset cursor so
// Badger pend:* rows can be re-pulled after a host MEMORY_PRESSURE shed.
type MemoryShedding interface {
	BeginMemoryShed() (resumeState queue.QueueState)
	FinishMemoryShed(resumeState queue.QueueState)
	ClearPendingBufferForSuspend()
	ResetKeysetCursorForShed()
	WaitInProgressZeroFunc(ctx context.Context, every time.Duration, onTick func(inProgress int)) error
	SetLastPullWasPartial(value bool)
}
