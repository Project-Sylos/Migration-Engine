// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Command migrate_already_existed rewrites fixture DuckDB copy_status values:
// current successful → already_existed (post-traversal / pre-copy fixtures).
//
// Usage:
//
//	go run ./pkg/tests/cmd/migrate_already_existed PATH [PATH ...]
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <db_path> [db_path...]\n", os.Args[0])
		os.Exit(2)
	}
	for _, path := range os.Args[1:] {
		if err := migrate(path); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
			os.Exit(1)
		}
	}
}

func migrate(path string) error {
	database, err := db.Open(db.Options{Path: path})
	if err != nil {
		return err
	}
	defer database.Close()

	conn, err := database.GetDB()
	if err != nil {
		return err
	}
	ctx := context.Background()

	var hasEvents int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'src_status_events'`).Scan(&hasEvents); err != nil {
		return err
	}
	if hasEvents == 0 {
		fmt.Printf("%s: no src_status_events; skip\n", path)
		return nil
	}

	before, err := countByCopy(conn)
	if err != nil {
		return err
	}
	fmt.Printf("%s: before %v\n", path, before)

	eventTime := time.Now().UnixNano()
	// Append-only rewrite: every current successful → already_existed (fixtures never ran real copy with new semantics).
	res, err := conn.ExecContext(ctx, `
INSERT INTO src_status_events (id, traversal_status, copy_status, delete_status, event_time, depth)
SELECT cur.id,
       COALESCE(cur.traversal_status, ''),
       'already_existed',
       COALESCE(cur.delete_status, ''),
       $1,
       COALESCE(n.depth, 0)
FROM (
  SELECT id,
         arg_max(traversal_status, event_time) AS traversal_status,
         arg_max(copy_status, event_time) AS copy_status,
         arg_max(delete_status, event_time) AS delete_status
  FROM src_status_events
  GROUP BY id
) cur
JOIN src_nodes n ON n.id = cur.id
WHERE COALESCE(cur.copy_status, '') = 'successful'
`, eventTime)
	if err != nil {
		return fmt.Errorf("rewrite successful→already_existed: %w", err)
	}
	n, _ := res.RowsAffected()
	fmt.Printf("%s: rewrote %d rows\n", path, n)

	after, err := countByCopy(conn)
	if err != nil {
		return err
	}
	fmt.Printf("%s: after %v\n", path, after)
	if after["successful"] != 0 {
		return fmt.Errorf("expected 0 successful after migrate, got %d", after["successful"])
	}
	return nil
}

func countByCopy(conn *sql.DB) (map[string]int64, error) {
	out := map[string]int64{}
	rows, err := conn.Query(`
WITH latest AS (
  SELECT id, arg_max(copy_status, event_time) AS copy_status
  FROM src_status_events
  WHERE COALESCE(copy_status, '') <> ''
  GROUP BY id
)
SELECT COALESCE(e.copy_status, ''), count(*)::BIGINT
FROM src_nodes n
LEFT JOIN latest e ON n.id = e.id
GROUP BY 1
ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		if st == "" {
			st = "(empty)"
		}
		out[st] = n
	}
	return out, rows.Err()
}
