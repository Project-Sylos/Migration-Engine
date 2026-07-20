// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// MapFSOperation maps Sylos-FS adapter operation strings to FSOperation.
func MapFSOperation(operation string) FSOperation {
	switch operation {
	case "ListChildren":
		return OpListChildren
	case "CreateFolder":
		return OpCreateFolder
	case "DeleteNode", "DeleteBatch", "DeleteFile", "DeleteFolder":
		return OpDelete
	case "OpenRead":
		return OpDownload
	case "CreateFileUpload", "UploadFile", "OpenWrite":
		return OpUpload
	default:
		return ""
	}
}

// ClassifyFSOperation returns download vs upload for copy-related operation names.
func ClassifyFSOperation(operation string) FSOperation {
	switch operation {
	case "OpenRead":
		return OpDownload
	case "CreateFileUpload", "UploadFile", "OpenWrite":
		return OpUpload
	case "CreateFolder":
		return OpCreateFolder
	case "ListChildren":
		return OpListChildren
	case "DeleteNode", "DeleteBatch", "DeleteFile", "DeleteFolder":
		return OpDelete
	default:
		return MapFSOperation(operation)
	}
}

// DegradationAppliesToOperation reports whether a degradation signal matches active ops.
func DegradationAppliesToOperation(signalOp string, active []FSOperation) bool {
	mapped := ClassifyFSOperation(signalOp)
	if mapped == "" {
		return true
	}
	for _, op := range active {
		if op == mapped {
			return true
		}
	}
	return false
}

// AdaptersForScaling holds fallback FS adapters when queue-local adapters are unset.
type AdaptersForScaling struct {
	Src fstypes.FSAdapter
	Dst fstypes.FSAdapter
}

func (a *Autoscaler) refreshProfiles() {
	if a == nil {
		return
	}
	resolved := make(map[string]FSPerformanceProfile, len(a.queues))
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		ctx := q.ScalingContext()
		srcAd := q.ScalingSrcAdapter()
		dstAd := q.ScalingDstAdapter()
		if srcAd == nil {
			srcAd = a.adapters.Src
		}
		if dstAd == nil {
			dstAd = a.adapters.Dst
		}
		resolved[name] = ResolveEffectiveProfile(ctx, srcAd, dstAd)
	}
	a.mu.Lock()
	a.profiles = resolved
	a.mu.Unlock()
	a.syncRegistryProfiles(resolved)
}

func (a *Autoscaler) syncRegistryProfiles(resolved map[string]FSPerformanceProfile) {
	if a.registry == nil {
		return
	}
	for name, prof := range resolved {
		g := a.registry.GroupForQueue(name)
		if g == nil {
			continue
		}
		g.mu.Lock()
		g.Profile = prof
		g.MaxWorkers = prof.MaxWorkers
		g.mu.Unlock()
	}
}

func (a *Autoscaler) reconcileProfileBounds() {
	if a == nil {
		return
	}
	a.mu.Lock()
	profiles := make(map[string]FSPerformanceProfile, len(a.profiles))
	for name, prof := range a.profiles {
		profiles[name] = prof
	}
	a.mu.Unlock()
	for name, q := range a.queues {
		if q == nil {
			continue
		}
		prof, ok := profiles[name]
		if !ok {
			continue
		}
		maxW := prof.MaxWorkers
		if maxW <= 0 {
			maxW = UnboundedMaxWorkers
		}
		if q.GetWorkerCount() > maxW {
			_ = q.SetTargetWorkerCount(maxW)
		}
	}
}
