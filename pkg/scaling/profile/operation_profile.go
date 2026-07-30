// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	"time"

	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

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

// ApplyAdapterListPagination merges adapter-reported API bounds into a provider profile.
// Profile values are the operational defaults; adapter values cap or floor when tighter.
func ApplyAdapterListPagination(base FSPerformanceProfile, adapter fstypes.FSAdapter) FSPerformanceProfile {
	lim, ok := fstypes.ListChildrenPaginationFrom(adapter)
	if !ok {
		return base
	}
	if lim.MaxPageSize > 0 {
		base.MaxListPageSize = minPositive(base.MaxListPageSize, lim.MaxPageSize)
	}
	if lim.MinPageSize > 0 {
		base.MinListPageSize = maxInt(base.MinListPageSize, lim.MinPageSize)
	}
	if lim.DefaultPageSize > 0 {
		if base.DefaultListPageSize <= 0 {
			base.DefaultListPageSize = lim.DefaultPageSize
		} else {
			base.DefaultListPageSize = minPositive(base.DefaultListPageSize, lim.DefaultPageSize)
		}
	}
	if !lim.PreferLargePagesUnderThrottle {
		base.PreferLargePages = false
	}
	return base
}

// ListPageSizeSetter is the queue surface needed to apply list pagination bounds.
type ListPageSizeSetter interface {
	SetListPageSize(n int)
}

// ApplyQueueListPagination sets initial list page size from a provider profile.
func ApplyQueueListPagination(q ListPageSizeSetter, profile FSPerformanceProfile) {
	if q == nil {
		return
	}
	size := profile.DefaultListPageSize
	if size <= 0 {
		size = 100
	}
	if min := profile.MinListPageSize; min > 0 && size < min {
		size = min
	}
	if max := profile.MaxListPageSize; max > 0 && size > max {
		size = max
	}
	q.SetListPageSize(size)
}

// p95ListItems must come from recent ListChildren result sizes; zero means insufficient data.
func ListPageIncreaseAllowed(curPageSize, p95ListItems int) bool {
	if curPageSize <= 0 || p95ListItems <= 0 {
		return false
	}
	return p95ListItems > curPageSize
}

// IncreaseListPageSize grows pagination under FS throttle (fewer list API calls per folder).
func IncreaseListPageSize(cur, min, max, step int) (int, bool) {
	if cur <= 0 {
		cur = min
	}
	if cur >= max {
		return cur, false
	}
	if step <= 0 {
		step = 20
	}
	next := cur * 2
	if next > max {
		next = max
	}
	if next <= cur {
		next = cur + step
		if next > max {
			next = max
		}
	}
	next = ClampInt(next, min, max)
	return next, next > cur
}

// DecreaseListPageSize reduces pagination toward default during calm scale-up.
func DecreaseListPageSize(cur, min, defaultSize int) (int, bool) {
	if defaultSize <= 0 {
		defaultSize = min
	}
	if cur <= defaultSize {
		return cur, false
	}
	next := cur / 2
	if next < defaultSize {
		next = defaultSize
	}
	next = ClampInt(next, min, next)
	return next, next < cur
}

// FSOperation identifies an FS adapter workload class for scaling profiles.
type FSOperation string

const (
	OpListChildren FSOperation = "list_children"
	OpCreateFolder FSOperation = "create_folder"
	OpDelete       FSOperation = "delete"
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
// Default is used for any operation not present in Ops (single worker count for everything).
type ProviderOperationProfiles struct {
	ProviderID string
	Default    OperationProfile
	Ops        map[FSOperation]OperationProfile
}

var operationProfiles = map[string]ProviderOperationProfiles{
	"generic": buildGenericOperationProfiles(),
	"spectra": buildSpectraOperationProfiles(),
	"local":   buildLocalOperationProfiles(),
	"google_drive": buildCloudProviderProfiles("google_drive", 8, 16,
		cloudListProfile(6, 16, 100, 500, false),
		cloudDefaultProfile(4, 12),
		cloudDefaultProfile(4, 8),
	),
	// Dropbox list quotas are harsh; keep list below create/delete (4/8).
	"dropbox": buildCloudProviderProfilesMutate("dropbox", 8, 16,
		cloudListProfile(4, 5, 100, 500, false),
		cloudDefaultProfile(4, 8),
	),
	"onedrive": buildCloudProviderProfiles("onedrive", 8, 16,
		cloudListProfile(6, 12, 200, 200, false),
		cloudDefaultProfile(4, 12),
		cloudDefaultProfile(4, 8),
	),
	// SharePoint Online RU is shared tenant-wide; stay more conservative than OneDrive.
	"sharepoint": buildCloudProviderProfilesMutate("sharepoint", 6, 12,
		cloudListProfile(4, 8, 200, 200, false),
		cloudDefaultProfile(4, 6),
	),
	"box": buildBoxOperationProfiles(),
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

// buildOps returns op overrides for a provider. Lookup falls back to Default for missing keys.
func buildOps(overrides map[FSOperation]OperationProfile) map[FSOperation]OperationProfile {
	if len(overrides) == 0 {
		return nil
	}
	out := make(map[FSOperation]OperationProfile, len(overrides))
	for op, prof := range overrides {
		out[op] = prof
	}
	return out
}

func buildGenericOperationProfiles() ProviderOperationProfiles {
	def := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 8, MaxWorkers: 32,
		MaxInterOpDelay: 5 * time.Second,
	}
	return ProviderOperationProfiles{
		ProviderID: "generic",
		Default:    def,
		Ops: buildOps(map[FSOperation]OperationProfile{
			OpListChildren: genericListProfile(),
		}),
	}
}

func buildSpectraOperationProfiles() ProviderOperationProfiles {
	return ProviderOperationProfiles{
		ProviderID: "spectra",
		Default:    spectraUncappedOp(),
		Ops:        nil,
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
	def := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 8, MaxWorkers: 64,
		MaxInterOpDelay: 2 * time.Second,
	}
	return ProviderOperationProfiles{
		ProviderID: "local",
		Default:    def,
		Ops: buildOps(map[FSOperation]OperationProfile{
			OpListChildren: localListProfile(),
		}),
	}
}

func cloudDefaultProfile(defaultWorkers, maxWorkers int) OperationProfile {
	return OperationProfile{
		MinWorkers: 1, DefaultWorkers: defaultWorkers, MaxWorkers: maxWorkers,
		MaxInterOpDelay: 5 * time.Second,
	}
}

func cloudListProfile(defaultWorkers, maxWorkers, defaultPageSize, maxPageSize int, preferLargePages bool) OperationProfile {
	return OperationProfile{
		MinWorkers: 1, DefaultWorkers: defaultWorkers, MaxWorkers: maxWorkers,
		MaxInterOpDelay:     5 * time.Second,
		MinListPageSize:     20,
		DefaultListPageSize: defaultPageSize,
		MaxListPageSize:     maxPageSize,
		ListPageStep:        20,
		PreferLargePages:    preferLargePages,
		DefaultLeaseBatch:   100,
		MaxLeaseBatch:       500,
		MinLeaseBatch:       25,
		DefaultRefillBatch:  500,
		MaxRefillBatch:      1000,
		MinRefillBatch:      100,
	}
}

func buildCloudProviderProfiles(providerID string, defDefault, defMax int, list, create, delete OperationProfile) ProviderOperationProfiles {
	return ProviderOperationProfiles{
		ProviderID: providerID,
		Default:    cloudDefaultProfile(defDefault, defMax),
		Ops: buildOps(map[FSOperation]OperationProfile{
			OpListChildren: list,
			OpCreateFolder: create,
			OpDelete:       delete,
		}),
	}
}

func buildCloudProviderProfilesMutate(providerID string, defDefault, defMax int, list, mutate OperationProfile) ProviderOperationProfiles {
	return ProviderOperationProfiles{
		ProviderID: providerID,
		Default:    cloudDefaultProfile(defDefault, defMax),
		Ops: buildOps(map[FSOperation]OperationProfile{
			OpListChildren: list,
			OpCreateFolder: mutate,
			OpDelete:       mutate,
		}),
	}
}

func buildBoxOperationProfiles() ProviderOperationProfiles {
	// Box: ~1000 req/min/user general, ~240 upload/min/user.
	def := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 6, MaxWorkers: 12,
		MaxInterOpDelay: 5 * time.Second,
	}
	list := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 6, MaxWorkers: 12,
		MaxInterOpDelay:     5 * time.Second,
		MinListPageSize:     100,
		DefaultListPageSize: 1000,
		MaxListPageSize:     1000,
		ListPageStep:        100,
		PreferLargePages:    true,
		DefaultLeaseBatch:   100,
		MaxLeaseBatch:       500,
		MinLeaseBatch:       25,
		DefaultRefillBatch:  500,
		MaxRefillBatch:      1000,
		MinRefillBatch:      100,
	}
	upload := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 16, MaxWorkers: 32,
		MaxInterOpDelay: 5 * time.Second,
	}
	createFolder := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 6, MaxWorkers: 12,
		MaxInterOpDelay: 5 * time.Second,
	}
	deleteOp := OperationProfile{
		MinWorkers: 1, DefaultWorkers: 4, MaxWorkers: 8,
		MaxInterOpDelay: 5 * time.Second,
	}
	return ProviderOperationProfiles{
		ProviderID: "box",
		Default:    def,
		Ops: buildOps(map[FSOperation]OperationProfile{
			OpListChildren: list,
			OpUpload:       upload,
			OpDownload:     upload,
			OpCreateFolder: createFolder,
			OpDelete:       deleteOp,
		}),
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
// Order: provider Ops override → provider Default → generic Ops override → generic Default.
func LookupOperationProfile(providerID, serviceName string, op FSOperation) OperationProfile {
	pop := LookupProviderOperations(providerID, serviceName)
	if prof, ok := pop.Ops[op]; ok {
		return prof
	}
	if profileHasWorkerBounds(pop.Default) {
		return pop.Default
	}
	generic := operationProfiles["generic"]
	if prof, ok := generic.Ops[op]; ok {
		return prof
	}
	return generic.Default
}

func profileHasWorkerBounds(p OperationProfile) bool {
	return p.MinWorkers != 0 || p.DefaultWorkers != 0 || p.MaxWorkers != 0 || p.MaxInterOpDelay != 0 ||
		p.MinListPageSize != 0 || p.DefaultListPageSize != 0 || p.DefaultLeaseBatch != 0
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

func effectiveOrDefaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func effectiveOrDefaultDuration(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// ToActuatorProfile maps an OperationProfile to FSPerformanceProfile with zero-cap defaults applied.
func ToActuatorProfile(op OperationProfile) FSPerformanceProfile {
	out := FSPerformanceProfile{
		MinWorkers:          op.MinWorkers,
		DefaultWorkers:      effectiveOrDefaultInt(op.DefaultWorkers, DefaultWorkersForUnbounded),
		MaxWorkers:          effectiveOrDefaultInt(op.MaxWorkers, UnboundedMaxWorkers),
		MaxInterOpDelay:     effectiveOrDefaultDuration(op.MaxInterOpDelay, DefaultMaxInterOpDelay),
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
