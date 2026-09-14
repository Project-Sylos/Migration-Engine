// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// readops prints DB op timing aggregates from a migration Badger ops directory.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func main() {
	limit := flag.Int("limit", 5000, "max samples to read")
	op := flag.String("op", "", "filter by op name")
	samples := flag.Bool("samples", false, "include raw samples in JSON output")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: readops [-limit N] [-op name] [-samples] <migration.ops-dir>...")
		os.Exit(1)
	}
	for _, dir := range flag.Args() {
		if err := printDir(dir, *limit, *op, *samples); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", dir, err)
			os.Exit(1)
		}
	}
}

func printDir(dir string, limit int, op string, includeSamples bool) error {
	s, err := opsdb.Open(opsdb.Options{Dir: dir})
	if err != nil {
		return err
	}
	defer s.Close()

	recs, err := s.ListDBOps(op, limit)
	if err != nil {
		return err
	}
	summary := db.SummarizeDBOpRecords(recs)

	fmt.Printf("\n=== %s (%d samples", dir, len(recs))
	if op != "" {
		fmt.Printf(", filter=%q", op)
	}
	fmt.Println(") ===")
	if len(summary) == 0 {
		fmt.Println("(no op samples)")
		return nil
	}
	fmt.Printf("%-22s %7s %10s %10s %10s %10s\n", "op", "count", "total", "avg", "max", "rows")
	for _, a := range summary {
		fmt.Printf("%-22s %7d %10s %10s %10s %10d\n",
			a.Op, a.Count,
			time.Duration(a.TotalDurationNs).Round(time.Millisecond),
			time.Duration(a.AvgDurationNs).Round(time.Millisecond),
			time.Duration(a.MaxDurationNs).Round(time.Millisecond),
			a.TotalRows,
		)
	}

	if op == "badger_sync" || op == "" {
		sync := recs
		if op == "" {
			sync = nil
			for _, r := range recs {
				if r.Op == db.OpBadgerSync {
					sync = append(sync, r)
				}
			}
		}
		if len(sync) > 0 {
			fmt.Println("\nTop badger_sync flushes:")
			n := 8
			if len(sync) < n {
				n = len(sync)
			}
			for i := 0; i < n; i++ {
				r := sync[i]
				fmt.Printf("  %s rows=%d dur=%s\n", r.At.Format(time.RFC3339), r.Rows, time.Duration(r.DurationNs).Round(time.Millisecond))
			}
		}
	}

	if includeSamples {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"dir":     dir,
			"summary": summary,
			"samples": recs,
		})
	}
	return nil
}
