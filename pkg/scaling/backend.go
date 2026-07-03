// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"fmt"
	"sync"
	"time"
)

// BackendGroup tracks shared FS backend budget and throttle signals.
type BackendGroup struct {
	ID       string
	Profile  FSPerformanceProfile
	MaxWorkers int
	Allocation map[string]int
	mu sync.RWMutex
}

// BackendRegistry resolves queue → backend group budgets.
type BackendRegistry struct {
	mu     sync.RWMutex
	groups map[string]*BackendGroup
	queueToGroup map[string]string
}

// NewBackendRegistry creates an empty registry.
func NewBackendRegistry() *BackendRegistry {
	return &BackendRegistry{
		groups:       make(map[string]*BackendGroup),
		queueToGroup: make(map[string]string),
	}
}

// RegisterQueue adds a queue to a backend group with initial worker allocation.
func (r *BackendRegistry) RegisterQueue(queueName, groupID string, profile FSPerformanceProfile, initialWorkers int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[groupID]
	if !ok {
		g = &BackendGroup{
			ID:         groupID,
			Profile:    profile,
			MaxWorkers: profile.MaxWorkers,
			Allocation: make(map[string]int),
		}
		r.groups[groupID] = g
	}
	g.mu.Lock()
	g.Allocation[queueName] = ClampInt(initialWorkers, profile.MinWorkers, profile.MaxWorkers)
	g.mu.Unlock()
	r.queueToGroup[queueName] = groupID
}

// GroupForQueue returns the backend group for a queue name.
func (r *BackendRegistry) GroupForQueue(queueName string) *BackendGroup {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	id := r.queueToGroup[queueName]
	r.mu.RUnlock()
	if id == "" {
		return nil
	}
	r.mu.RLock()
	g := r.groups[id]
	r.mu.RUnlock()
	return g
}

// ResolveGroupID picks backend group id from explicit id, connection id, or queue name.
func ResolveGroupID(explicit, connectionID, queueName string) string {
	if explicit != "" {
		return explicit
	}
	if connectionID != "" {
		return "conn:" + connectionID
	}
	return "queue:" + queueName
}

// QueuesByGroup returns group ID → queue names registered in that group.
func (r *BackendRegistry) QueuesByGroup() map[string][]string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][]string, len(r.groups))
	for queueName, groupID := range r.queueToGroup {
		out[groupID] = append(out[groupID], queueName)
	}
	return out
}

// SplitWorkersTotal divides total workers across queue names (even split; remainder to first queues).
func SplitWorkersTotal(total int, queueNames []string, minPerQueue int) map[string]int {
	if len(queueNames) == 0 {
		return nil
	}
	if minPerQueue <= 0 {
		minPerQueue = 1
	}
	minTotal := minPerQueue * len(queueNames)
	if total < minTotal {
		total = minTotal
	}
	if len(queueNames) == 2 {
		a, b := SplitWorkersSameBackend(total, queueNames[0], queueNames[1])
		if a < minPerQueue {
			a = minPerQueue
		}
		if b < minPerQueue {
			b = minPerQueue
		}
		return map[string]int{queueNames[0]: a, queueNames[1]: b}
	}
	base := total / len(queueNames)
	rem := total % len(queueNames)
	out := make(map[string]int, len(queueNames))
	for i, name := range queueNames {
		n := base
		if i < rem {
			n++
		}
		if n < minPerQueue {
			n = minPerQueue
		}
		out[name] = n
	}
	return out
}

// GroupMaxWorkers returns the combined worker cap for a backend group.
func (r *BackendRegistry) GroupMaxWorkers(groupID string) int {
	g := r.groupByID(groupID)
	if g == nil {
		return 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.MaxWorkers
}

func (r *BackendRegistry) groupByID(groupID string) *BackendGroup {
	if r == nil || groupID == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.groups[groupID]
}

func groupAIMDKey(groupID string) string {
	return "group:" + groupID
}

// SplitWorkersSameBackend returns 50/50 split of total cap across two active queues.
func SplitWorkersSameBackend(total int, queueA, queueB string) (int, int) {
	if total < 2 {
		return 1, 1
	}
	a := total / 2
	b := total - a
	_ = queueA
	_ = queueB
	return a, b
}

// RateLimitBridge adapts FS degradation state to queue.RateLimitTelemetry.
type RateLimitBridge struct {
	TakeHits func() int64
	Until    func() time.Time
}

func (b RateLimitBridge) TakeRecentHits() int64 {
	if b.TakeHits != nil {
		return b.TakeHits()
	}
	return 0
}

func (b RateLimitBridge) RateLimitedUntil() time.Time {
	if b.Until != nil {
		return b.Until()
	}
	return time.Time{}
}

// FormatScalingEvent builds a log-friendly scaling event string.
func FormatScalingEvent(queue, knob string, oldVal, newVal int, pressure PressureClass) string {
	if knob == "InterOpDelayMs" {
		return fmt.Sprintf("autoscaler queue=%s knob=InterOpDelay %s->%s pressure=%s",
			queue, formatInterOpDelayFromMicros(oldVal), formatInterOpDelayFromMicros(newVal), pressure)
	}
	return fmt.Sprintf("autoscaler queue=%s knob=%s %d->%d pressure=%s", queue, knob, oldVal, newVal, pressure)
}

// formatInterOpDelayFromMicros formats delay values stored as microseconds in ScalingEvent OldValue/NewValue.
func formatInterOpDelayFromMicros(micros int) string {
	if micros <= 0 {
		return "0ms"
	}
	ms := float64(micros) / 1000.0
	if ms >= 100 || ms == float64(int(ms)) {
		return fmt.Sprintf("%.0fms", ms)
	}
	if ms >= 1 {
		return fmt.Sprintf("%.1fms", ms)
	}
	return fmt.Sprintf("%.2fms", ms)
}
