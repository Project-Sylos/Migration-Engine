// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"

// ApplyAdapterListPagination merges provider-reported ListChildren bounds from the
// connected FS adapter into base. Profile map defaults (e.g. generic 20–10000) are
// fallbacks only when the adapter does not implement FSListChildrenPagination.
func ApplyAdapterListPagination(base FSPerformanceProfile, adapter fstypes.FSAdapter) FSPerformanceProfile {
	lim, ok := fstypes.ListChildrenPaginationFrom(adapter)
	if !ok {
		return base
	}
	if lim.MinPageSize > 0 {
		base.MinListPageSize = lim.MinPageSize
	}
	if lim.MaxPageSize > 0 {
		base.MaxListPageSize = lim.MaxPageSize
	}
	if lim.DefaultPageSize > 0 {
		base.DefaultListPageSize = lim.DefaultPageSize
	}
	base.PreferLargePages = lim.PreferLargePagesUnderThrottle
	return base
}

// ApplyQueueListPagination sets the queue's initial list page size from a merged profile.
func ApplyQueueListPagination(q QueueActuator, profile FSPerformanceProfile) {
	if q == nil {
		return
	}
	page := profile.DefaultListPageSize
	if page <= 0 {
		page = 100
	}
	min := profile.MinListPageSize
	max := profile.MaxListPageSize
	if min > 0 && max > 0 {
		page = ClampInt(page, min, max)
	} else if min > 0 {
		page = ClampInt(page, min, page)
	} else if max > 0 {
		page = ClampInt(page, page, max)
	}
	if page > 0 {
		q.SetListPageSize(page)
	}
}
