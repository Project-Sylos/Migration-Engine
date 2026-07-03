// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"time"

	spectrafs "codeberg.org/Sylos/Sylos-FS/pkg/fs/spectra"
	localfs "codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
)

const defaultAutoscalerInterval = 3 * time.Second

// AutoscalerConfig controls in-engine autoscaler behavior.
// Autoscaler is enabled by default for all migration runs; set DisableAutoscaler to opt out.
type AutoscalerConfig struct {
	// DisableAutoscaler turns off the control loop (explicit opt-out).
	DisableAutoscaler bool
	Enabled           bool // deprecated: use DisableAutoscaler; kept for callers that set Enabled: true explicitly
	Interval          time.Duration
	OnEvent           func(scaling.ScalingEvent)
	DebugAIMD         bool // probe cooldown + scale-up diagnostics on stdout
}

// DefaultAutoscalerConfig returns enabled autoscaler settings used when none are supplied.
func DefaultAutoscalerConfig() AutoscalerConfig {
	return AutoscalerConfig{
		Enabled:  true,
		Interval: defaultAutoscalerInterval,
	}
}

// Resolve applies migration-engine defaults. Autoscaler runs unless DisableAutoscaler is set.
func (c AutoscalerConfig) Resolve() AutoscalerConfig {
	out := c
	if out.DisableAutoscaler {
		out.Enabled = false
		return out
	}
	out.Enabled = true
	if out.Interval <= 0 {
		out.Interval = defaultAutoscalerInterval
	}
	return out
}

// autoscalerRunContext holds autoscaler lifecycle for a migration run.
type autoscalerRunContext struct {
	cancel context.CancelFunc
	scaler *scaling.Autoscaler
}

func (a *autoscalerRunContext) stop() {
	if a == nil || a.cancel == nil {
		return
	}
	a.cancel()
}

func (a *autoscalerRunContext) Autoscaler() *scaling.Autoscaler {
	if a == nil {
		return nil
	}
	return a.scaler
}

func startAutoscaler(
	ctx context.Context,
	cfg AutoscalerConfig,
	observer *queue.QueueObserver,
	database *db.DB,
	srcQueue, dstQueue *queue.Queue,
	srcService, dstService Service,
) *autoscalerRunContext {
	return startAutoscalerActuators(ctx, cfg, observer, database, map[string]scaling.QueueActuator{
		"src": srcQueue,
		"dst": dstQueue,
	}, []autoscalerQueueSpec{
		{Name: "src", Service: srcService, InitialWorkers: srcQueue.GetWorkerCount()},
		{Name: "dst", Service: dstService, InitialWorkers: dstQueue.GetWorkerCount()},
	}, srcService.Adapter, dstService.Adapter)
}

func startCopyAutoscaler(
	ctx context.Context,
	cfg AutoscalerConfig,
	observer *queue.QueueObserver,
	database *db.DB,
	copyQueue *queue.Queue,
	srcService, dstService Service,
) *autoscalerRunContext {
	srcProfile := scaling.ApplyAdapterListPagination(
		scaling.LookupProfile(srcService.ProviderID, srcService.Name),
		srcService.Adapter,
	)
	dstProfile := scaling.ApplyAdapterListPagination(
		scaling.LookupProfile(dstService.ProviderID, dstService.Name),
		dstService.Adapter,
	)
	copyProfile := scaling.MergeCopyProfiles(srcProfile, dstProfile)
	return startAutoscalerActuators(ctx, cfg, observer, database, map[string]scaling.QueueActuator{
		"copy": copyQueue,
	}, []autoscalerQueueSpec{
		{Name: "copy", Service: srcService, InitialWorkers: copyQueue.GetWorkerCount(), ProfileOverride: copyProfile},
	}, srcService.Adapter, dstService.Adapter)
}

type autoscalerQueueSpec struct {
	Name            string
	Service         Service
	InitialWorkers  int
	ProfileOverride scaling.FSPerformanceProfile // optional (copy merged profile)
}

func startAutoscalerActuators(
	ctx context.Context,
	cfg AutoscalerConfig,
	observer *queue.QueueObserver,
	database *db.DB,
	actuators map[string]scaling.QueueActuator,
	specs []autoscalerQueueSpec,
	adapters ...fstypes.FSAdapter,
) *autoscalerRunContext {
	cfg = cfg.Resolve()
	if !cfg.Enabled || observer == nil {
		return nil
	}

	bridge := combinedRateLimitBridge(adapters...)
	if bridge.TakeHits != nil {
		for _, spec := range specs {
			observer.RegisterRateLimitTelemetry(spec.Name, bridge)
		}
	}

	registry := scaling.NewBackendRegistry()
	profiles := make(map[string]scaling.FSPerformanceProfile, len(specs))

	var srcGroup, dstGroup string
	for _, spec := range specs {
		if spec.Name == "src" {
			srcGroup = scaling.ResolveGroupID(spec.Service.BackendGroupID, "", spec.Name)
		}
		if spec.Name == "dst" {
			dstGroup = scaling.ResolveGroupID(spec.Service.BackendGroupID, "", spec.Name)
		}
	}
	if len(adapters) >= 2 && sameBackend(adapters[0], adapters[1]) {
		dstGroup = srcGroup
	}

	for _, spec := range specs {
		profile := spec.ProfileOverride
		if profile.ProviderID == "" && profile.MaxWorkers == 0 {
			profile = scaling.ApplyAdapterListPagination(
				scaling.LookupProfile(spec.Service.ProviderID, spec.Service.Name),
				spec.Service.Adapter,
			)
		}
		groupID := scaling.ResolveGroupID(spec.Service.BackendGroupID, "", spec.Name)
		if spec.Name == "dst" && dstGroup != "" {
			groupID = dstGroup
		}
		if spec.Name == "src" && srcGroup != "" {
			groupID = srcGroup
		}
		if spec.Name == "copy" {
			groupID = "queue:copy"
		}
		registry.RegisterQueue(spec.Name, groupID, profile, spec.InitialWorkers)
		profiles[spec.Name] = profile
	}

	scaler := scaling.NewAutoscaler(database, observer, registry, profiles, actuators, scaling.Config{
		Enabled:   true,
		Interval:  cfg.Interval,
		OnEvent:   cfg.OnEvent,
		DebugAIMD: cfg.DebugAIMD,
	})

	runCtx, cancel := context.WithCancel(ctx)
	go scaler.Run(runCtx)
	return &autoscalerRunContext{cancel: cancel, scaler: scaler}
}

func combinedRateLimitBridge(adapters ...fstypes.FSAdapter) scaling.RateLimitBridge {
	seen := make(map[*fstypes.FSDegradationState]struct{})
	var states []*fstypes.FSDegradationState
	for _, adapter := range adapters {
		if adapter == nil {
			continue
		}
		if st := degradationStateFrom(adapter); st != nil {
			if _, dup := seen[st]; dup {
				continue
			}
			seen[st] = struct{}{}
			states = append(states, st)
		}
	}
	if len(states) == 0 {
		return scaling.RateLimitBridge{}
	}
	return scaling.RateLimitBridge{
		TakeHits: func() int64 {
			var n int64
			for _, st := range states {
				n += st.TakeRecentHits()
			}
			return n
		},
		Until: func() time.Time {
			var latest time.Time
			for _, st := range states {
				u := st.DegradationState().RateLimitedUntil
				if u.After(latest) {
					latest = u
				}
			}
			return latest
		},
	}
}

func degradationStateFrom(adapter fstypes.FSAdapter) *fstypes.FSDegradationState {
	if s, ok := adapter.(*spectrafs.SpectraFS); ok {
		return s.GetDegradationState()
	}
	if s, ok := adapter.(*localfs.LocalFS); ok {
		return s.GetDegradationState()
	}
	if r, ok := adapter.(interface{ GetDegradationState() *fstypes.FSDegradationState }); ok {
		return r.GetDegradationState()
	}
	return nil
}

func sharedDegradationState(src, dst fstypes.FSAdapter) *fstypes.FSDegradationState {
	if s := degradationStateFrom(src); s != nil {
		return s
	}
	return degradationStateFrom(dst)
}

func sameBackend(src, dst fstypes.FSAdapter) bool {
	s, okS := src.(*spectrafs.SpectraFS)
	d, okD := dst.(*spectrafs.SpectraFS)
	if !okS || !okD {
		return false
	}
	return s.GetSDKInstance() == d.GetSDKInstance()
}
