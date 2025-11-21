// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// HandleShutdownSignals sets up signal handlers for SIGINT (Ctrl+C) and SIGTERM.
// When either signal is received, it cancels the provided context.
// This function should be called in a goroutine at the start of the migration.
// On Windows, SIGINT is supported but SIGTERM may not be available.
func HandleShutdownSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)

	// Register signals based on OS
	// SIGINT (Ctrl+C) works on both Windows and Unix
	// SIGTERM works on Unix systems
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Notify(sigChan, signals...)

	// Wait for signal and cancel context when received
	<-sigChan
	cancel()
}
