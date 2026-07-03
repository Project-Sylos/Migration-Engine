// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

// MergeCopyProfiles combines src/dst profiles for a single copy queue (tightest worker cap wins).
func MergeCopyProfiles(src, dst FSPerformanceProfile) FSPerformanceProfile {
	out := src
	if dst.ProviderID != "" && out.ProviderID == "generic" {
		out.ProviderID = dst.ProviderID
	}
	if dst.MaxWorkers > 0 && (out.MaxWorkers == 0 || dst.MaxWorkers < out.MaxWorkers) {
		out.MaxWorkers = dst.MaxWorkers
	}
	if dst.MaxInterOpDelay > out.MaxInterOpDelay {
		out.MaxInterOpDelay = dst.MaxInterOpDelay
	}
	if dst.MinWorkers > out.MinWorkers {
		out.MinWorkers = dst.MinWorkers
	}
	return out
}
