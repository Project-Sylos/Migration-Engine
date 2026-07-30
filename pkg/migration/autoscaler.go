// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"time"

	spectrafs "codeberg.org/Sylos/Sylos-FS/pkg/fs/spectra"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/backend"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/loop"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

const defaultAutoscalerInterval = 3 * time.Second

// AutoscalerConfig controls in-engine autoscaler behavior.
// Autoscaler is enabled by default for all migration runs; set DisableAutoscaler to opt out.
type AutoscalerConfig struct {
	DisableAutoscaler bool
	Interval          time.Duration
	OnEvent           func(scaling.ScalingEvent)
	DebugAIMD         bool // probe cooldown + scale-up diagnostics on stdout
	// WorkerCapOverrides overlay MaxWorkers after profile resolve (from API/DB or live UI).
	WorkerCapOverrides profile.WorkerCapOverrides
}

// DefaultAutoscalerConfig returns enabled autoscaler settings used when none are supplied.
func DefaultAutoscalerConfig() AutoscalerConfig {
	return AutoscalerConfig{
		Interval: defaultAutoscalerInterval,
	}
}

// Enabled reports whether the autoscaler control loop should run.
func (c AutoscalerConfig) Enabled() bool {
	return !c.DisableAutoscaler
}

// Resolve applies migration-engine defaults. Autoscaler runs unless DisableAutoscaler is set.
func (c AutoscalerConfig) Resolve() AutoscalerConfig {
	out := c
	if out.Interval <= 0 {
		out.Interval = defaultAutoscalerInterval
	}
	return out
}

// autoscalerRunContext holds autoscaler lifecycle for a migration run.
type autoscalerRunContext struct {
	cancel context.CancelFunc
	scaler *loop.Autoscaler
}

func (a *autoscalerRunContext) stop() {
	if a == nil || a.cancel == nil {
		return
	}
	a.cancel()
}

func (a *autoscalerRunContext) Autoscaler() *loop.Autoscaler {
	if a == nil {
		return nil
	}
	return a.scaler
}

func startAutoscaler(
	ctx context.Context,
	cfg AutoscalerConfig,
	observer *observe.QueueObserver,
	database *db.DB,
	srcQueue, dstQueue *queue.Queue,
	srcService, dstService Service,
	pathCheckProfile string,
	windowsCompat bool,
) *autoscalerRunContext {
	same := sameBackend(srcService.Adapter, dstService.Adapter)
	wireQueueScalingContext(srcQueue, dstQueue, nil, srcService, dstService, same, pathCheckProfile, windowsCompat)
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
	observer *observe.QueueObserver,
	database *db.DB,
	copyQueue *queue.Queue,
	srcService, dstService Service,
	pathCheckProfile string,
	windowsCompat bool,
) *autoscalerRunContext {
	wireQueueScalingContext(nil, nil, copyQueue, srcService, dstService, false, pathCheckProfile, windowsCompat)
	return startAutoscalerActuators(ctx, cfg, observer, database, map[string]scaling.QueueActuator{
		"copy": copyQueue,
	}, []autoscalerQueueSpec{
		{Name: "copy", Service: srcService, InitialWorkers: copyQueue.GetWorkerCount()},
	}, srcService.Adapter, dstService.Adapter)
}

func startDeleteAutoscaler(
	ctx context.Context,
	cfg AutoscalerConfig,
	observer *observe.QueueObserver,
	database *db.DB,
	deleteQueue *queue.Queue,
	srcService, dstService Service,
	pathCheckProfile string,
	windowsCompat bool,
) *autoscalerRunContext {
	wireDeleteQueueScalingContext(deleteQueue, srcService, dstService, pathCheckProfile, windowsCompat)
	return startAutoscalerActuators(ctx, cfg, observer, database, map[string]scaling.QueueActuator{
		"delete": deleteQueue,
	}, []autoscalerQueueSpec{
		{Name: "delete", Service: srcService, InitialWorkers: deleteQueue.GetWorkerCount()},
	}, srcService.Adapter, dstService.Adapter)
}

type autoscalerQueueSpec struct {
	Name           string
	Service        Service
	InitialWorkers int
}

func startAutoscalerActuators(
	ctx context.Context,
	cfg AutoscalerConfig,
	observer *observe.QueueObserver,
	database *db.DB,
	actuators map[string]scaling.QueueActuator,
	specs []autoscalerQueueSpec,
	adapters ...fstypes.FSAdapter,
) *autoscalerRunContext {
	cfg = cfg.Resolve()
	if cfg.DisableAutoscaler || observer == nil {
		return nil
	}

	bridge := combinedRateLimitBridge(adapters...)
	if bridge.TakeHits != nil {
		for _, spec := range specs {
			observer.RegisterRateLimitTelemetry(spec.Name, bridge)
		}
	}

	var srcAdapter, dstAdapter fstypes.FSAdapter
	if len(adapters) >= 1 {
		srcAdapter = adapters[0]
	}
	if len(adapters) >= 2 {
		dstAdapter = adapters[1]
	}

	registry := backend.NewBackendRegistry()
	profiles := make(map[string]profile.FSPerformanceProfile, len(specs))

	var srcGroup, dstGroup string
	for _, spec := range specs {
		if spec.Name == "src" {
			srcGroup = backend.ResolveGroupID(spec.Service.BackendGroupID, "", spec.Name)
		}
		if spec.Name == "dst" {
			dstGroup = backend.ResolveGroupID(spec.Service.BackendGroupID, "", spec.Name)
		}
	}
	if len(adapters) >= 2 && sameBackend(adapters[0], adapters[1]) {
		dstGroup = srcGroup
	}

	for _, spec := range specs {
		q := actuators[spec.Name]
		var prof profile.FSPerformanceProfile
		if q != nil {
			sctx := q.ScalingContext()
			prof = profile.ResolveEffectiveProfile(sctx, srcAdapter, dstAdapter)
		} else {
			prof = profile.ToActuatorProfile(
				profile.LookupOperationProfile(spec.Service.ProviderID, spec.Service.Name, profile.OpListChildren),
			)
		}
		groupID := backend.ResolveGroupID(spec.Service.BackendGroupID, "", spec.Name)
		if spec.Name == "dst" && dstGroup != "" {
			groupID = dstGroup
		}
		if spec.Name == "src" && srcGroup != "" {
			groupID = srcGroup
		}
		if spec.Name == "copy" {
			groupID = "queue:copy"
		}
		if spec.Name == "delete" {
			groupID = "queue:delete"
		}
		registry.RegisterQueue(spec.Name, groupID, prof, spec.InitialWorkers)
		profiles[spec.Name] = prof
	}

	scaler := loop.NewAutoscaler(database, observer, registry, profiles, actuators, loop.Config{
		Enabled:   true,
		Interval:  cfg.Interval,
		OnEvent:   cfg.OnEvent,
		DebugAIMD: cfg.DebugAIMD,
		Adapters: profile.AdaptersForScaling{
			Src: srcAdapter,
			Dst: dstAdapter,
		},
		WorkerCapOverrides: cfg.WorkerCapOverrides,
	})
	// Apply overlays to the initial registry profiles (NewAutoscaler stores them; refresh on first tick too).
	scaler.SetWorkerCapOverrides(cfg.WorkerCapOverrides)

	runCtx, cancel := context.WithCancel(ctx)
	go scaler.Run(runCtx)
	return &autoscalerRunContext{cancel: cancel, scaler: scaler}
}

func combinedRateLimitBridge(adapters ...fstypes.FSAdapter) backend.RateLimitBridge {
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
		return backend.RateLimitBridge{}
	}
	return backend.RateLimitBridge{
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
	if adapter == nil {
		return nil
	}
	if r, ok := adapter.(fstypes.FSDegradationReporter); ok {
		return r.GetDegradationState()
	}
	// Legacy fallback for adapters that expose the bridge method without the full reporter.
	if r, ok := adapter.(interface{ GetDegradationState() *fstypes.FSDegradationState }); ok {
		return r.GetDegradationState()
	}
	return nil
}

func sameBackend(src, dst fstypes.FSAdapter) bool {
	s, okS := src.(*spectrafs.SpectraFS)
	d, okD := dst.(*spectrafs.SpectraFS)
	if !okS || !okD {
		return false
	}
	return s.GetSDKInstance() == d.GetSDKInstance()
}

// rateLimitBridgeForAdapter exposes FS degradation telemetry to queue workers for throttle idle windows.
func rateLimitBridgeForAdapter(adapter fstypes.FSAdapter) queue.RateLimitTelemetry {
	bridge := combinedRateLimitBridge(adapter)
	if bridge.TakeHits == nil {
		return nil
	}
	return bridge
}
