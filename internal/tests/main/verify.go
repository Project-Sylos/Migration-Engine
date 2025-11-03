// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/internal/db"
)

// verifyMigration checks that the migration completed successfully
func verifyMigration(database *db.DB) error {
	// Count total nodes discovered
	var srcTotal, dstTotal int
	rows, _ := database.Query("SELECT COUNT(*) FROM src_nodes")
	if rows != nil && rows.Next() {
		rows.Scan(&srcTotal)
		rows.Close()
	}

	rows, _ = database.Query("SELECT COUNT(*) FROM dst_nodes")
	if rows != nil && rows.Next() {
		rows.Scan(&dstTotal)
		rows.Close()
	}

	fmt.Printf("Nodes discovered: src=%d, dst=%d\n", srcTotal, dstTotal)

	// Check 1: Any nodes discovered?
	if srcTotal == 0 && dstTotal == 0 {
		return fmt.Errorf("no nodes discovered - migration did not run")
	}

	// Check 2: Any pending traversals remaining?
	var srcPending, dstPending int
	rows, _ = database.Query("SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Pending'")
	if rows != nil && rows.Next() {
		rows.Scan(&srcPending)
		rows.Close()
	}

	rows, _ = database.Query("SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Pending'")
	if rows != nil && rows.Next() {
		rows.Scan(&dstPending)
		rows.Close()
	}

	if srcPending > 0 || dstPending > 0 {
		return fmt.Errorf("incomplete migration: %d src and %d dst folders still have Pending status", srcPending, dstPending)
	}

	// Check 3: Any failed traversals?
	var srcFailed, dstFailed int
	rows, _ = database.Query("SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Failed'")
	if rows != nil && rows.Next() {
		rows.Scan(&srcFailed)
		rows.Close()
	}

	rows, _ = database.Query("SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Failed'")
	if rows != nil && rows.Next() {
		rows.Scan(&dstFailed)
		rows.Close()
	}

	if srcFailed > 0 || dstFailed > 0 {
		fmt.Printf("⚠ Warning: %d src and %d dst nodes have Failed status\n", srcFailed, dstFailed)
	}

	// Check 4: Traversal status breakdown
	var srcCompleted, dstCompleted int
	rows, _ = database.Query("SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Successful'")
	if rows != nil && rows.Next() {
		rows.Scan(&srcCompleted)
		rows.Close()
	}

	rows, _ = database.Query("SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Successful'")
	if rows != nil && rows.Next() {
		rows.Scan(&dstCompleted)
		rows.Close()
	}

	// Optional: Check if any NotOnSrc nodes are present in dst_nodes table - if so raise error if you are doing this check.
	// This is optional because in a real migration their might be some dst nodes that are not on src.
	// But with SpectraFS we are not expecting any dst nodes that are not on src. Just by its very design.
	// So if we find any, we should raise an error.
	var dstNotOnSrc int
	rows, _ = database.Query("SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'NotOnSrc'")
	if rows != nil && rows.Next() {
		rows.Scan(&dstNotOnSrc)
		rows.Close()
	}
	if dstNotOnSrc > 0 {
		return fmt.Errorf("incomplete migration: %d dst nodes have NotOnSrc status", dstNotOnSrc)
	}

	fmt.Printf("Traversal status:\n")
	fmt.Printf("  Src: %d Successful, %d Pending, %d Failed\n", srcCompleted, srcPending, srcFailed)
	fmt.Printf("  Dst: %d Successful, %d Pending, %d Failed\n", dstCompleted, dstPending, dstFailed)

	// Success!
	fmt.Println()
	fmt.Println("✓ All verification checks passed!")
	fmt.Printf("✓ Successfully migrated %d nodes\n", srcTotal)

	return nil
}
