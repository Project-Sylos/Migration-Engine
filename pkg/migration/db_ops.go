// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// GetDBOpsReport returns seal buffer telemetry and recent DB op timing samples.
func (m *Migration) GetDBOpsReport(opts db.DBOpsReportOptions) (db.DBOpsReport, error) {
	if m == nil || m.DB == nil {
		return db.DBOpsReport{}, fmt.Errorf("migration database not open")
	}
	return m.DB.DBOpsReport(opts)
}
