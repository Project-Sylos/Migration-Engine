// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "time"

// FSPerformanceProfile defines starting defaults and bounds for autoscaler knobs.
type FSPerformanceProfile struct {
	ProviderID string

	MinWorkers, DefaultWorkers, MaxWorkers int

	MinListPageSize, DefaultListPageSize, MaxListPageSize int
	ListPageStep                                          int // additive bump when doubling would not advance
	PreferLargePages                                      bool

	DefaultLeaseBatch, MaxLeaseBatch int
	MinLeaseBatch                    int
	DefaultRefillBatch, MaxRefillBatch int
	MinRefillBatch                     int

	MaxInterOpDelay time.Duration // cap for inter-op pacing fallback at worker floor
}

// ClampInt clamps v to [min, max].
func ClampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// EffectiveWorkers returns worker count from cfg or profile default.
func EffectiveWorkers(cfgWorkers int, profile FSPerformanceProfile) int {
	if cfgWorkers > 0 {
		return ClampInt(cfgWorkers, profile.MinWorkers, profile.MaxWorkers)
	}
	return profile.DefaultWorkers
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
