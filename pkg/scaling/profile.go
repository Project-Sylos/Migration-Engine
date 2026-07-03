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

	WorkerStepDownOnThrottle int // deprecated: AIMD multiplicative decrease is used instead

	DefaultLeaseBatch, MaxLeaseBatch     int
	MinLeaseBatch                         int
	DefaultRefillBatch, MaxRefillBatch   int
	MinRefillBatch                        int
	DefaultCopyStreamBuffer, MaxCopyWorkers int

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

var profiles = map[string]FSPerformanceProfile{
	"generic": {
		ProviderID:          "generic",
		MinWorkers:          1,
		DefaultWorkers:      10,
		MaxWorkers:          32,
		MinListPageSize:     20,
		DefaultListPageSize: 100,
		MaxListPageSize:     10000,
		ListPageStep:        20,
		PreferLargePages:    true,
		WorkerStepDownOnThrottle: 2,
		DefaultLeaseBatch:   1000,
		MaxLeaseBatch:       10000,
		MinLeaseBatch:       100,
		DefaultRefillBatch:  10000,
		MaxRefillBatch:      10000,
		MinRefillBatch:      500,
		MaxInterOpDelay:     5 * time.Second,
	},
	"spectra": {
		ProviderID:          "spectra",
		MinWorkers:          1,
		DefaultWorkers:      10,
		MaxWorkers:          32,
		MinListPageSize:     20,
		DefaultListPageSize: 100,
		MaxListPageSize:     10000,
		ListPageStep:        20,
		PreferLargePages:    true,
		WorkerStepDownOnThrottle: 2,
		DefaultLeaseBatch:   1000,
		MaxLeaseBatch:       10000,
		MinLeaseBatch:       100,
		DefaultRefillBatch:  10000,
		MaxRefillBatch:      10000,
		MinRefillBatch:      500,
		MaxInterOpDelay:     5 * time.Second,
	},
	"local": {
		ProviderID:          "local",
		MinWorkers:          1,
		DefaultWorkers:      8,
		MaxWorkers:          64,
		MinListPageSize:     20,
		DefaultListPageSize: 100,
		MaxListPageSize:     1000,
		ListPageStep:        20,
		PreferLargePages:    false,
		WorkerStepDownOnThrottle: 4,
		DefaultLeaseBatch:   1000,
		MaxLeaseBatch:       10000,
		MinLeaseBatch:       100,
		DefaultRefillBatch:  10000,
		MaxRefillBatch:      10000,
		MinRefillBatch:      500,
		MaxInterOpDelay:     2 * time.Second,
	},
}

// LookupProfile returns the profile for providerID or serviceName, else generic.
// List page min/max/default in the returned profile are fallbacks; at FS connect /
// run startup ApplyAdapterListPagination merges authoritative bounds from the adapter.
func LookupProfile(providerID, serviceName string) FSPerformanceProfile {
	if providerID != "" {
		if p, ok := profiles[providerID]; ok {
			return p
		}
	}
	if serviceName != "" {
		if p, ok := profiles[serviceName]; ok {
			return p
		}
	}
	return profiles["generic"]
}

// EffectiveWorkers returns worker count from cfg or profile default.
func EffectiveWorkers(cfgWorkers int, profile FSPerformanceProfile) int {
	if cfgWorkers > 0 {
		return ClampInt(cfgWorkers, profile.MinWorkers, profile.MaxWorkers)
	}
	return profile.DefaultWorkers
}
