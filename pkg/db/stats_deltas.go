// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// ReviewStatsDelta is one incremental update to a review counter key.
type ReviewStatsDelta struct {
	Key   string
	Delta int64
}

// DepthStatsDelta is one incremental update to a per-depth counter key.
type DepthStatsDelta struct {
	Table string
	Depth int
	Key   string
	Delta int64
}
