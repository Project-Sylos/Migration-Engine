// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"time"

	spectrafs "codeberg.org/Sylos/Sylos-FS/pkg/fs/spectra"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/memweight"
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
	DebugAutoscaler         bool // probe cooldown + scale-up diagnostics on stdout
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

// startAutoscaler wires each queue's scaling context (keyed by queue name: src, dst, copy, delete)
// and starts the autoscaler loop over them.
func (rt *PhaseRuntime) startAutoscaler(observer *observe.QueueObserver, database *db.DB, queues ...*queue.Queue) *autoscalerRunContext {
	srcGroup := backend.ResolveGroupID(rt.SrcService.BackendGroupID, "", "src")
	dstGroup := backend.ResolveGroupID(rt.DstService.BackendGroupID, "", "dst")
	if sameBackend(rt.SrcService.Adapter, rt.DstService.Adapter) {
		dstGroup = srcGroup
	}
	actuators := make(map[string]scaling.QueueActuator, len(queues))
	specs := make([]autoscalerQueueSpec, 0, len(queues))
	for _, q := range queues {
		name := q.Name()
		spec := autoscalerQueueSpec{Name: name, Service: rt.SrcService, GroupID: srcGroup, InitialWorkers: q.GetWorkerCount()}
		queueSrcGroup, queueDstGroup := srcGroup, dstGroup
		switch name {
		case "dst":
			spec.Service, spec.GroupID = rt.DstService, dstGroup
		case "copy", "delete":
			spec.GroupID = "queue:" + name
			queueSrcGroup, queueDstGroup = spec.GroupID, spec.GroupID
		}
		q.ConfigureScalingContext(rt.SrcService.ProviderID, rt.DstService.ProviderID, queueSrcGroup, queueDstGroup, rt.PathCheckTarget, rt.WindowsCompat)
		q.ConfigureTraversalCompare(rt.OverwritePolicy, rt.HashVerify)
		actuators[name] = q
		specs = append(specs, spec)
	}
	return startAutoscalerActuators(rt.ShutdownOrBackground(), rt.Autoscaler, observer, database, actuators, specs, rt.SrcService.Adapter, rt.DstService.Adapter)
}

type autoscalerQueueSpec struct {
	Name           string
	Service        Service
	GroupID        string
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
	for _, spec := range specs {
		var prof profile.FSPerformanceProfile
		if q := actuators[spec.Name]; q != nil {
			prof = profile.ResolveEffectiveProfile(q.ScalingContext(), srcAdapter, dstAdapter)
		} else {
			prof = profile.ToActuatorProfile(
				profile.LookupOperationProfile(spec.Service.ProviderID, spec.Service.Name, profile.OpListChildren),
			)
		}
		registry.RegisterQueue(spec.Name, spec.GroupID, prof, spec.InitialWorkers)
		profiles[spec.Name] = prof
	}

	scaler := loop.NewAutoscaler(database, observer, registry, profiles, actuators, loop.Config{
		Enabled:   true,
		Interval:  cfg.Interval,
		OnEvent:   cfg.OnEvent,
		DebugAutoscaler: cfg.DebugAutoscaler,
		Adapters: profile.AdaptersForScaling{
			Src: srcAdapter,
			Dst: dstAdapter,
		},
		WorkerCapOverrides: cfg.WorkerCapOverrides,
	})
	// Apply overlays to the initial registry profiles (NewAutoscaler stores them; refresh on first tick too).
	scaler.SetWorkerCapOverrides(cfg.WorkerCapOverrides)

	ledger := memweight.New()
	scaler.SetWeightLedger(ledger)
	if database != nil {
		database.SetMemWeightLedger(ledger)
	}
	for _, act := range actuators {
		if q, ok := act.(*queue.Queue); ok && q != nil {
			q.SetMemWeightLedger(ledger)
		}
	}

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
