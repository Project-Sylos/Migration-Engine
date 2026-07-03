// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
)

func TestAutoscalerConfig_Resolve_defaultsOn(t *testing.T) {
	got := (AutoscalerConfig{}).Resolve()
	if !got.Enabled {
		t.Fatal("expected enabled by default")
	}
	if got.Interval != defaultAutoscalerInterval {
		t.Fatalf("interval = %v, want %v", got.Interval, defaultAutoscalerInterval)
	}
}

func TestAutoscalerConfig_Resolve_disableOptOut(t *testing.T) {
	got := AutoscalerConfig{DisableAutoscaler: true}.Resolve()
	if got.Enabled {
		t.Fatal("expected disabled when DisableAutoscaler is set")
	}
}

func TestAutoscalerConfig_Resolve_preservesCallbacks(t *testing.T) {
	called := false
	got := AutoscalerConfig{
		OnEvent:   func(scaling.ScalingEvent) { called = true },
		DebugAIMD: true,
		Interval:  5 * time.Second,
	}.Resolve()
	if !got.Enabled {
		t.Fatal("expected enabled")
	}
	if got.Interval != 5*time.Second {
		t.Fatalf("interval = %v", got.Interval)
	}
	if got.OnEvent == nil {
		t.Fatal("expected OnEvent preserved")
	}
	got.OnEvent(scaling.ScalingEvent{})
	if !called {
		t.Fatal("expected OnEvent to run")
	}
	if !got.DebugAIMD {
		t.Fatal("expected DebugAIMD preserved")
	}
}
