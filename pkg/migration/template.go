// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "time"

// NewConfig returns a Config with standard default values.
// Use the builder methods (WithDatabase, WithServices, WithRoots) to customize.
func NewConfig() *Config {
	return &Config{
		Runtime:         ModeStandalone,
		SeedRoots:       true,
		WorkerCount:     10,
		MaxRetries:      3,
		CoordinatorLead: 4,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		SkipListener:    true,
		StartupDelay:    1 * time.Second,
		ProgressTick:    500 * time.Millisecond,
		Verification:    VerifyOptions{},
	}
}
