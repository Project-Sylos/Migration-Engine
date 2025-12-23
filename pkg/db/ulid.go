// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// GenerateNodeID generates a new ULID for use as an internal node identifier.
// This is a public function that can be called by the API before seeding root tasks.
// ULIDs are used as database keys instead of path hashes.
func GenerateNodeID() string {
	entropy := ulid.DefaultEntropy()
	now := time.Now()
	id, err := ulid.New(ulid.Timestamp(now), entropy)
	if err != nil {
		// This should never happen in practice, but handle it gracefully
		// Fallback to timestamp-based ID if ULID generation fails
		return ulid.Make().String()
	}
	return id.String()
}
