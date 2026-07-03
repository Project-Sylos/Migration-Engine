// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"

	spectrafs "codeberg.org/Sylos/Sylos-FS/pkg/fs/spectra"
	localfs "codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

type stubAdapter struct{ fstypes.FSAdapter }

func (stubAdapter) ListChildrenPagination() fstypes.ListChildrenPagination {
	return fstypes.ListChildrenPagination{
		MinPageSize:                   50,
		MaxPageSize:                   500,
		DefaultPageSize:               200,
		PreferLargePagesUnderThrottle: true,
	}
}

func TestApplyAdapterListPaginationFromAdapter(t *testing.T) {
	base := LookupProfile("generic", "")
	got := ApplyAdapterListPagination(base, stubAdapter{})
	if got.MinListPageSize != 50 || got.MaxListPageSize != 500 || got.DefaultListPageSize != 100 {
		t.Fatalf("expected conservative merge with profile, got %+v", got)
	}
	if !got.PreferLargePages {
		t.Fatal("expected PreferLargePages true when adapter allows")
	}
}

func TestApplyAdapterListPaginationAdapterDisablesPreferLarge(t *testing.T) {
	base := LookupProfile("google_drive", "")
	got := ApplyAdapterListPagination(base, googledrivePaginationAdapter{})
	if got.PreferLargePages {
		t.Fatal("adapter PreferLargePages=false should disable profile prefer large")
	}
	if got.MaxListPageSize != 200 {
		t.Fatalf("MaxListPageSize=%d want 200", got.MaxListPageSize)
	}
}

type googledrivePaginationAdapter struct{ stubAdapter }

func (googledrivePaginationAdapter) ListChildrenPagination() fstypes.ListChildrenPagination {
	return fstypes.ListChildrenPagination{
		MaxPageSize:                   1000,
		PreferLargePagesUnderThrottle: false,
	}
}

func TestApplyAdapterListPaginationFallback(t *testing.T) {
	base := LookupProfile("generic", "")
	got := ApplyAdapterListPagination(base, nil)
	if got.MinListPageSize != base.MinListPageSize || got.MaxListPageSize != base.MaxListPageSize {
		t.Fatalf("nil adapter should keep profile defaults: %+v", got)
	}
}

func TestProviderAdaptersExposePagination(t *testing.T) {
	local := &localfs.LocalFS{}
	lim, ok := fstypes.ListChildrenPaginationFrom(local)
	if !ok {
		t.Fatal("local adapter should expose pagination")
	}
	if lim.MaxPageSize != 1000 {
		t.Fatalf("local max=%d", lim.MaxPageSize)
	}

	// Spectra requires SDK; verify method exists on zero value via interface assertion.
	var _ fstypes.FSListChildrenPagination = (*spectrafs.SpectraFS)(nil)
}
