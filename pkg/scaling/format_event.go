// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "fmt"

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
