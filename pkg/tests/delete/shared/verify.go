// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"os"
	"path/filepath"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

// VerifySrcTreeEmpty returns an error if any file or subfolder exists under srcDir (the root itself may remain).
func VerifySrcTreeEmpty(srcDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	e := entries[0]
	if e.IsDir() {
		return fmt.Errorf("src still has directory: %s", filepath.Join(srcDir, e.Name()))
	}
	return fmt.Errorf("src still has file: %s", e.Name())
}

// VerifyDeleteCounts checks universal stats for deleted count when duckDB is available.
func VerifyDeleteCounts(duckDB *db.DB, minDeleted int64) error {
	counts, err := stats.GetDeleteStatusCountsFromEvents(duckDB)
	if err != nil {
		return err
	}
	if counts.Deleted < minDeleted {
		return fmt.Errorf("expected at least %d deleted, got %d", minDeleted, counts.Deleted)
	}
	return nil
}
