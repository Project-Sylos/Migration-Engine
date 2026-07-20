// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

// Scaling mode strings for operation-based autoscaler profile resolution.
const (
	ScalingModeTraversal   = "traversal"
	ScalingModeRetry       = "retry"
	ScalingModeCopy        = "copy"
	ScalingModeCopyRetry   = "copy-retry"
	ScalingModeDelete      = "delete"
	ScalingModeDeleteRetry = "delete-retry"
)

// ScalingContext describes queue state used to resolve operation-based profiles.
type ScalingContext struct {
	QueueName   string
	Mode        string
	CopyPass    int
	SrcProvider string
	DstProvider string
	SrcGroupID  string
	DstGroupID  string
}
