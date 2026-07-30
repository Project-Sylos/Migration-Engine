// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package seal

import "codeberg.org/Sylos/Migration-Engine/pkg/db"

func init() {
	db.RegisterSealAttach(func(database *db.DB, opts db.SealBufferOptions) db.SealController {
		return NewSealBuffer(database, opts)
	})
}
