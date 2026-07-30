// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// AbsoluteMaxWorkers is the hard safety rail for user/API MaxWorkers overrides.
const AbsoluteMaxWorkers = 256

// WorkerCapMode identifies which autoscaler phase a MaxWorkers override applies to.
type WorkerCapMode string

const (
	WorkerCapTraversal   WorkerCapMode = "traversal"
	WorkerCapCopyFolders WorkerCapMode = "copy_folders"
	WorkerCapCopyFiles   WorkerCapMode = "copy_files"
	WorkerCapDelete      WorkerCapMode = "delete"
)

// AllWorkerCapModes is the stable UI/API mode list.
var AllWorkerCapModes = []WorkerCapMode{
	WorkerCapTraversal,
	WorkerCapCopyFolders,
	WorkerCapCopyFiles,
	WorkerCapDelete,
}

// ValidWorkerCapMode reports whether mode is a known override key.
func ValidWorkerCapMode(mode string) bool {
	switch WorkerCapMode(mode) {
	case WorkerCapTraversal, WorkerCapCopyFolders, WorkerCapCopyFiles, WorkerCapDelete:
		return true
	default:
		return false
	}
}

// WorkerCapOverrides are optional MaxWorkers hard-cap overlays keyed by mode.
// Missing or non-positive entries leave the shipped operation-profile MaxWorkers unchanged.
type WorkerCapOverrides struct {
	Caps map[WorkerCapMode]int
}

// Clone returns a deep copy safe for concurrent readers after Store.
func (o WorkerCapOverrides) Clone() WorkerCapOverrides {
	if len(o.Caps) == 0 {
		return WorkerCapOverrides{}
	}
	out := WorkerCapOverrides{Caps: make(map[WorkerCapMode]int, len(o.Caps))}
	for k, v := range o.Caps {
		out.Caps[k] = v
	}
	return out
}

// MaxFor returns the override for mode, or 0 if unset.
func (o WorkerCapOverrides) MaxFor(mode WorkerCapMode) int {
	if o.Caps == nil {
		return 0
	}
	return o.Caps[mode]
}

// WorkerCapModeFromContext maps a queue scaling context to a WorkerCapMode.
func WorkerCapModeFromContext(ctx queue.ScalingContext) WorkerCapMode {
	switch ctx.Mode {
	case queue.ScalingModeCopy, queue.ScalingModeCopyRetry:
		if ctx.CopyPass == 1 {
			return WorkerCapCopyFolders
		}
		return WorkerCapCopyFiles
	case queue.ScalingModeDelete, queue.ScalingModeDeleteRetry:
		return WorkerCapDelete
	default:
		return WorkerCapTraversal
	}
}

// ApplyWorkerCapOverride raises or lowers MaxWorkers when an override is set.
// Soft-cap / AIMD still operate inside the resulting hard max.
func ApplyWorkerCapOverride(prof FSPerformanceProfile, overrides WorkerCapOverrides, mode WorkerCapMode) FSPerformanceProfile {
	max := overrides.MaxFor(mode)
	if max <= 0 {
		return prof
	}
	minW := prof.MinWorkers
	if minW <= 0 {
		minW = 1
	}
	prof.MaxWorkers = ClampInt(max, minW, AbsoluteMaxWorkers)
	return prof
}

// ModeWorkerCaps holds shipped MinWorkers / MaxWorkers for one mode.
type ModeWorkerCaps struct {
	MinWorkers     int `json:"min_workers"`
	MaxWorkers     int `json:"max_workers"`
	DefaultWorkers int `json:"default_workers"`
}

// ProviderWorkerCaps is the shipped default caps for all modes of one provider.
type ProviderWorkerCaps struct {
	ProviderID string                     `json:"provider_id"`
	Modes      map[WorkerCapMode]ModeWorkerCaps `json:"modes"`
}

// DefaultWorkerCaps returns shipped profile caps for providerID (generic fallback).
func DefaultWorkerCaps(providerID string) ProviderWorkerCaps {
	if providerID == "" {
		providerID = "generic"
	}
	modes := make(map[WorkerCapMode]ModeWorkerCaps, len(AllWorkerCapModes))
	for _, mode := range AllWorkerCapModes {
		modes[mode] = defaultModeCaps(providerID, mode)
	}
	return ProviderWorkerCaps{ProviderID: providerID, Modes: modes}
}

// DefaultMaxWorkers returns shipped MaxWorkers for providerID + mode.
func DefaultMaxWorkers(providerID string, mode WorkerCapMode) int {
	return defaultModeCaps(providerID, mode).MaxWorkers
}

func defaultModeCaps(providerID string, mode WorkerCapMode) ModeWorkerCaps {
	var op OperationProfile
	switch mode {
	case WorkerCapTraversal:
		op = LookupOperationProfile(providerID, "", OpListChildren)
	case WorkerCapCopyFolders:
		op = LookupOperationProfile(providerID, "", OpCreateFolder)
	case WorkerCapCopyFiles:
		// Single-provider view for Settings: use upload leg (dst-bound in real pipelines).
		op = LookupOperationProfile(providerID, "", OpUpload)
	case WorkerCapDelete:
		op = LookupOperationProfile(providerID, "", OpListChildren)
	default:
		op = LookupOperationProfile(providerID, "", OpListChildren)
	}
	prof := ToActuatorProfile(op)
	return ModeWorkerCaps{
		MinWorkers:     prof.MinWorkers,
		MaxWorkers:     prof.MaxWorkers,
		DefaultWorkers: prof.DefaultWorkers,
	}
}

// KnownProviderIDs returns provider keys that have dedicated operation profiles.
func KnownProviderIDs() []string {
	ids := make([]string, 0, len(operationProfiles))
	for id := range operationProfiles {
		ids = append(ids, id)
	}
	return ids
}
