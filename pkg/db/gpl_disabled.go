// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"path/filepath"
	"strings"
)

// GPLDisabled gates path verifier / GPL queue on the Badger branch until re-enabled.
const GPLDisabled = true

// MigrationOpsPath returns the Badger ops directory for a migration DuckDB file path.
func MigrationOpsPath(dbPath string) string {
	if dbPath == ":memory:" {
		return ""
	}
	base := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	dir := filepath.Dir(dbPath)
	return filepath.Join(dir, base+".ops")
}
