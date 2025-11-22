// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// HandleShutdownSignals sets up signal handlers for SIGINT (Ctrl+C) and SIGTERM.
// When either signal is received, it cancels the provided context.
// If shutdown doesn't complete within 10 seconds, it hard-kills the process.
// This function should be called in a goroutine at the start of the migration.
// On Windows, SIGINT is supported but SIGTERM may not be available.
func HandleShutdownSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)

	// Register signals based on OS
	// SIGINT (Ctrl+C) works on both Windows and Unix
	// SIGTERM works on Unix systems
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Notify(sigChan, signals...)

	if sig := <-sigChan; sig != nil {
		fmt.Printf("\n⚠️  Shutdown signal received (%v). Initiating graceful shutdown...\n", sig)
		fmt.Println("   (Press Ctrl+C again to force exit if needed)")
		cancel()
	}

	select {
	case sig := <-sigChan:
		if sig != nil {
			fmt.Println("\n⚠️  Second interrupt received. Forcing immediate exit...")
			os.Exit(1)
		}
	case <-time.After(10 * time.Second):
		fmt.Println("\n❌ Shutdown timeout reached (10 seconds). Forcing exit to prevent hang...")
		os.Exit(1)
	}
}
