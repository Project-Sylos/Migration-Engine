// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// repair-suspend-resume patches runtime_state_json.suspend_v1 so ProgressMonitor Resume
// continues the unfinished traversal round instead of a retry sweep from 0.
// Stop Sylos (or soft-stop the migration) first; DuckDB is single-writer.
//
//	go run ./cmd/repair-suspend-resume -db /path/to/migration.db
//	go run ./cmd/repair-suspend-resume -db /path/to/migration.db -src-fallback 7 -id migration-...
package main

import (
	"flag"
	"fmt"
	"os"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func main() {
	dbPath := flag.String("db", "", "path to migration DuckDB file")
	srcFallback := flag.Int("src-fallback", 7, "SRC last_round when suspend_v1 has no last_round_src")
	migrationID := flag.String("id", "", "migration_id if the db has more than one migrations row")
	memoryGB := flag.Int("memory-gb", 12, "DuckDB PRAGMA memory_limit in GB")
	flag.Parse()
	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "usage: repair-suspend-resume -db /path/to/migration.db [-src-fallback 7] [-id ID]")
		os.Exit(2)
	}
	if _, err := os.Stat(*dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "db: %v\n", err)
		os.Exit(1)
	}

	database, err := db.Open(db.Options{Path: *dbPath, MemoryLimitGB: *memoryGB})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	s, err := migration.RepairTraversalSuspendV1(database, *srcFallback, *migrationID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("suspend_v1 last_round_src=%d last_round_dst=%d src_cursor=%q dst_cursor=%q\n",
		s.LastRoundSrc, s.LastRoundDst, s.SrcKeysetCursor, s.DstKeysetCursor)
	fmt.Println("phase is traversal-suspended; Resume from ProgressMonitor (not Path Review retry discovery)")
}
