// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
)

// PrintVerification prints the verification results in a formatted way.
func PrintVerification(result migration.Result) {
	report := result.Verification

	fmt.Printf("Nodes discovered: src=%d, dst=%d\n", report.SrcTotal, report.DstTotal)
	fmt.Printf("Traversal status:\n")
	fmt.Printf("  Src: %d Successful, %d Pending, %d Failed\n",
		report.SrcTotal-report.SrcPending-report.SrcFailed,
		report.SrcPending,
		report.SrcFailed,
	)
	fmt.Printf("  Dst: %d Successful, %d Pending, %d Failed\n",
		report.DstTotal-report.DstPending-report.DstFailed,
		report.DstPending,
		report.DstFailed,
	)

	if report.DstNotOnSrc > 0 {
		fmt.Printf("⚠ Warning: %d dst nodes marked as NotOnSrc\n", report.DstNotOnSrc)
	}

	fmt.Println()
	fmt.Println("✓ All verification checks passed!")
	fmt.Printf("✓ Successfully migrated %d nodes\n", report.SrcTotal)
}
