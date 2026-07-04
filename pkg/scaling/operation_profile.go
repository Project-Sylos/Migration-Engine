// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "time"

// FSOperation identifies an FS adapter workload class for scaling profiles.
type FSOperation string

const (
	OpListChildren FSOperation = "list_children"
	OpCreateFolder FSOperation = "create_folder"
	OpDownload     FSOperation = "download"
	OpUpload       FSOperation = "upload"
)

// UnboundedMaxWorkers is the autoscaler ceiling when an operation profile sets MaxWorkers=0.
const UnboundedMaxWorkers = 32

// DefaultWorkersForUnbounded is the startup worker count when DefaultWorkers=0 (AIMD probes up).
const DefaultWorkersForUnbounded = 2

// DefaultMaxInterOpDelay is used when MaxInterOpDelay=0 in an operation profile.
const DefaultMaxInterOpDelay = 5 * time.Second

// OperationProfile holds autoscaler bounds for one provider operation.
type OperationProfile struct {
	MinWorkers, DefaultWorkers, MaxWorkers int
	MaxInterOpDelay                        time.Duration
	MinListPageSize, DefaultListPageSize, MaxListPageSize int
	ListPageStep                                          int
	PreferLargePages                                      bool
	DefaultLeaseBatch, MaxLeaseBatch, MinLeaseBatch       int
	DefaultRefillBatch, MaxRefillBatch, MinRefillBatch    int
}

// ProviderOperationProfiles maps operations to profiles for one provider.
type ProviderOperationProfiles struct {
	ProviderID string
	Ops        map[FSOperation]OperationProfile
}

var operationProfiles = map[string]ProviderOperationProfiles{
	"generic":      buildGenericOperationProfiles(),
	"spectra":      buildSpectraOperationProfiles(),
	"local":        buildLocalOperationProfiles(),
	"google_drive": buildGoogleDriveOperationProfiles(),
}

func genericListProfile() OperationProfile {
	return OperationProfile{
		MinWorkers: 1, DefaultWorkers: 10, MaxWorkers: 32,
		MaxInterOpDelay:     5 * time.Second,
		MinListPageSize:     20,
		DefaultListPageSize: 100,
		MaxListPageSize:     10000,
		ListPageStep:        20,
		PreferLargePages:    true,
		DefaultLeaseBatch:   1000,
		MaxLeaseBatch:       10000,
		MinLeaseBatch:       100,
		DefaultRefillBatch:  10000,
		MaxRefillBatch:      10000,
		MinRefillBatch:      500,
	}
}

func localListProfile() OperationProfile {
	return OperationProfile{
		MinWorkers: 1, DefaultWorkers: 8, MaxWorkers: 64,
		MaxInterOpDelay:     2 * time.Second,
		MinListPageSize:     20,
		DefaultListPageSize: 100,
		MaxListPageSize:     1000,
		ListPageStep:        20,
		PreferLargePages:    false,
		DefaultLeaseBatch:   1000,
		MaxLeaseBatch:       10000,
		MinLeaseBatch:       100,
		DefaultRefillBatch:  10000,
		MaxRefillBatch:      10000,
		MinRefillBatch:      500,
	}
}

func buildGenericOperationProfiles() ProviderOperationProfiles {
	transfer := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 8, MaxWorkers: 32,
		MaxInterOpDelay: 5 * time.Second,
	}
	return ProviderOperationProfiles{
		ProviderID: "generic",
		Ops: map[FSOperation]OperationProfile{
			OpListChildren: genericListProfile(),
			OpCreateFolder: transfer,
			OpDownload:     transfer,
			OpUpload:       transfer,
		},
	}
}

func buildSpectraOperationProfiles() ProviderOperationProfiles {
	uncapped := spectraUncappedOp()
	return ProviderOperationProfiles{
		ProviderID: "spectra",
		Ops: map[FSOperation]OperationProfile{
			OpListChildren: uncapped,
			OpCreateFolder: uncapped,
			OpDownload:     uncapped,
			OpUpload:       uncapped,
		},
	}
}

func spectraUncappedOp() OperationProfile {
	return OperationProfile{
		MinWorkers:       1,
		DefaultWorkers:   DefaultWorkersForUnbounded,
		MaxWorkers:       0,
		MaxInterOpDelay:  0,
		MinListPageSize:  20,
		PreferLargePages: true,
	}
}

func buildLocalOperationProfiles() ProviderOperationProfiles {
	transfer := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 8, MaxWorkers: 64,
		MaxInterOpDelay: 2 * time.Second,
	}
	return ProviderOperationProfiles{
		ProviderID: "local",
		Ops: map[FSOperation]OperationProfile{
			OpListChildren: localListProfile(),
			OpCreateFolder: transfer,
			OpDownload:     transfer,
			OpUpload:       transfer,
		},
	}
}

func buildGoogleDriveOperationProfiles() ProviderOperationProfiles {
	list := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 6, MaxWorkers: 16,
		MaxInterOpDelay:     5 * time.Second,
		MinListPageSize:     20,
		DefaultListPageSize: 100,
		MaxListPageSize:     500,
		ListPageStep:        20,
		PreferLargePages:    false,
		DefaultLeaseBatch:   100,
		MaxLeaseBatch:       500,
		MinLeaseBatch:       25,
		DefaultRefillBatch:  500,
		MaxRefillBatch:      1000,
		MinRefillBatch:      100,
	}
	createFolder := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 4, MaxWorkers: 12,
		MaxInterOpDelay: 5 * time.Second,
	}
	transfer := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 8, MaxWorkers: 16,
		MaxInterOpDelay: 5 * time.Second,
	}
	return ProviderOperationProfiles{
		ProviderID: "google_drive",
		Ops: map[FSOperation]OperationProfile{
			OpListChildren: list,
			OpCreateFolder: createFolder,
			OpDownload:     transfer,
			OpUpload:       transfer,
		},
	}
}

// LookupProviderOperations returns operation profiles for providerID or serviceName.
func LookupProviderOperations(providerID, serviceName string) ProviderOperationProfiles {
	if providerID != "" {
		if p, ok := operationProfiles[providerID]; ok {
			return p
		}
	}
	if serviceName != "" {
		if p, ok := operationProfiles[serviceName]; ok {
			return p
		}
	}
	return operationProfiles["generic"]
}

// LookupOperationProfile returns the profile for one operation.
func LookupOperationProfile(providerID, serviceName string, op FSOperation) OperationProfile {
	pop := LookupProviderOperations(providerID, serviceName)
	if prof, ok := pop.Ops[op]; ok {
		return prof
	}
	return operationProfiles["generic"].Ops[op]
}

// ComposePipelineMin merges src and dst legs of a copy pipeline; 0 caps are uncapped (minPositive).
func ComposePipelineMin(a, b OperationProfile) OperationProfile {
	return OperationProfile{
		MinWorkers:          maxInt(a.MinWorkers, b.MinWorkers),
		DefaultWorkers:      minPositive(a.DefaultWorkers, b.DefaultWorkers),
		MaxWorkers:          minPositive(a.MaxWorkers, b.MaxWorkers),
		MaxInterOpDelay:     maxDuration(a.MaxInterOpDelay, b.MaxInterOpDelay),
		MinListPageSize:     maxInt(a.MinListPageSize, b.MinListPageSize),
		DefaultListPageSize: minPositive(a.DefaultListPageSize, b.DefaultListPageSize),
		MaxListPageSize:     minPositive(a.MaxListPageSize, b.MaxListPageSize),
		ListPageStep:        minPositive(a.ListPageStep, b.ListPageStep),
		PreferLargePages:    a.PreferLargePages && b.PreferLargePages,
		DefaultLeaseBatch:   minPositive(a.DefaultLeaseBatch, b.DefaultLeaseBatch),
		MaxLeaseBatch:       minPositive(a.MaxLeaseBatch, b.MaxLeaseBatch),
		MinLeaseBatch:       maxInt(a.MinLeaseBatch, b.MinLeaseBatch),
		DefaultRefillBatch:  minPositive(a.DefaultRefillBatch, b.DefaultRefillBatch),
		MaxRefillBatch:      minPositive(a.MaxRefillBatch, b.MaxRefillBatch),
		MinRefillBatch:      maxInt(a.MinRefillBatch, b.MinRefillBatch),
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// ToActuatorProfile maps an OperationProfile to FSPerformanceProfile with zero-cap defaults applied.
func ToActuatorProfile(op OperationProfile) FSPerformanceProfile {
	out := FSPerformanceProfile{
		MinWorkers:          op.MinWorkers,
		DefaultWorkers:      effectiveDefaultWorkers(op.DefaultWorkers),
		MaxWorkers:          effectiveMaxWorkers(op.MaxWorkers),
		MaxInterOpDelay:     effectiveMaxInterOpDelay(op.MaxInterOpDelay),
		MinListPageSize:     op.MinListPageSize,
		DefaultListPageSize: op.DefaultListPageSize,
		MaxListPageSize:     op.MaxListPageSize,
		ListPageStep:        op.ListPageStep,
		PreferLargePages:    op.PreferLargePages,
		DefaultLeaseBatch:   op.DefaultLeaseBatch,
		MaxLeaseBatch:       op.MaxLeaseBatch,
		MinLeaseBatch:       op.MinLeaseBatch,
		DefaultRefillBatch:  op.DefaultRefillBatch,
		MaxRefillBatch:      op.MaxRefillBatch,
		MinRefillBatch:      op.MinRefillBatch,
	}
	if out.MinWorkers <= 0 {
		out.MinWorkers = 1
	}
	return out
}

func effectiveDefaultWorkers(v int) int {
	if v <= 0 {
		return DefaultWorkersForUnbounded
	}
	return v
}

func effectiveMaxWorkers(v int) int {
	if v <= 0 {
		return UnboundedMaxWorkers
	}
	return v
}

func effectiveMaxInterOpDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultMaxInterOpDelay
	}
	return d
}

// EffectiveWorkersFromOperation returns worker count from cfg or operation profile defaults.
func EffectiveWorkersFromOperation(cfgWorkers int, op OperationProfile) int {
	prof := ToActuatorProfile(op)
	if cfgWorkers > 0 {
		return ClampInt(cfgWorkers, prof.MinWorkers, prof.MaxWorkers)
	}
	return prof.DefaultWorkers
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
