// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// AdaptersForScaling holds fallback FS adapters when queue-local adapters are unset.
type AdaptersForScaling struct {
	Src fstypes.FSAdapter
	Dst fstypes.FSAdapter
}
