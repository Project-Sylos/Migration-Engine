// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// VerifySrcTreeEmpty walks srcDir and returns an error if any file or subfolder exists (root may remain).
func VerifySrcTreeEmpty(srcDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			sub := filepath.Join(srcDir, e.Name())
			if err := filepath.WalkDir(sub, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return fmt.Errorf("src still has subtree %s: %w", sub, err)
			}
			return fmt.Errorf("src still has directory: %s", sub)
		}
		return fmt.Errorf("src still has file: %s", e.Name())
	}
	return nil
}

// VerifyDeleteCounts checks universal stats for deleted count when duckDB is available.
func VerifyDeleteCounts(duckDB *db.DB, minDeleted int64) error {
	counts, err := duckDB.GetDeleteStatusCountsFromEvents()
	if err != nil {
		return err
	}
	if counts.Deleted < minDeleted {
		return fmt.Errorf("expected at least %d deleted, got %d", minDeleted, counts.Deleted)
	}
	return nil
}
