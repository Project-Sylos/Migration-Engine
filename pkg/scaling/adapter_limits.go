// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

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
