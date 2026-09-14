// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "codeberg.org/Sylos/Migration-Engine/pkg/db"

// FormatCatalogSyncSuffix is a no-op; Duck catalog sync was removed.
func FormatCatalogSyncSuffix(database *db.DB) string {
	_ = database
	return ""
}
