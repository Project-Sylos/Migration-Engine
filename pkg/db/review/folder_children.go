// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// ListFolderChildrenPage lists direct children of parentPath without COUNT(*).
func ListFolderChildrenPage(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, bool, error) {
	return listFolderChildrenPage(d, f, orderBy, limit, offset)
}
