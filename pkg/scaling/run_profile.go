// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

// MergeRunProfiles combines src/dst profiles for a single worker pool (traversal or copy).
// The tighter cap wins on throughput knobs; inter-op delay uses the longer (more conservative) cap.
func MergeRunProfiles(src, dst FSPerformanceProfile) FSPerformanceProfile {
	out := src
	if dst.ProviderID != "" && (out.ProviderID == "" || out.ProviderID == "generic") {
		out.ProviderID = dst.ProviderID
	}
	out.MinWorkers = maxInt(out.MinWorkers, dst.MinWorkers)
	out.DefaultWorkers = minPositive(out.DefaultWorkers, dst.DefaultWorkers)
	out.MaxWorkers = minPositive(out.MaxWorkers, dst.MaxWorkers)
	out.MinListPageSize = maxInt(out.MinListPageSize, dst.MinListPageSize)
	out.DefaultListPageSize = minPositive(out.DefaultListPageSize, dst.DefaultListPageSize)
	out.MaxListPageSize = minPositive(out.MaxListPageSize, dst.MaxListPageSize)
	out.ListPageStep = minPositive(out.ListPageStep, dst.ListPageStep)
	out.PreferLargePages = out.PreferLargePages && dst.PreferLargePages
	out.DefaultLeaseBatch = minPositive(out.DefaultLeaseBatch, dst.DefaultLeaseBatch)
	out.MaxLeaseBatch = minPositive(out.MaxLeaseBatch, dst.MaxLeaseBatch)
	out.MinLeaseBatch = maxInt(out.MinLeaseBatch, dst.MinLeaseBatch)
	out.DefaultRefillBatch = minPositive(out.DefaultRefillBatch, dst.DefaultRefillBatch)
	out.MaxRefillBatch = minPositive(out.MaxRefillBatch, dst.MaxRefillBatch)
	out.MinRefillBatch = maxInt(out.MinRefillBatch, dst.MinRefillBatch)
	if dst.MaxInterOpDelay > out.MaxInterOpDelay {
		out.MaxInterOpDelay = dst.MaxInterOpDelay
	}
	return out
}

// MergeCopyProfiles is an alias for MergeRunProfiles (copy uses one shared worker pool).
func MergeCopyProfiles(src, dst FSPerformanceProfile) FSPerformanceProfile {
	return MergeRunProfiles(src, dst)
}

func minPositive(a, b int) int {
	if a <= 0 {
		return b
	}
	if b <= 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// QueueBatchSizing holds initial lease/refill batch sizes derived from a profile.
type QueueBatchSizing struct {
	LeaseBatchSize  int
	RefillBatchSize int
}

// QueueBatchSizingFromProfile returns non-nil when the profile specifies batch defaults.
func QueueBatchSizingFromProfile(p FSPerformanceProfile) *QueueBatchSizing {
	if p.DefaultLeaseBatch <= 0 && p.DefaultRefillBatch <= 0 {
		return nil
	}
	return &QueueBatchSizing{
		LeaseBatchSize:  p.DefaultLeaseBatch,
		RefillBatchSize: p.DefaultRefillBatch,
	}
}
