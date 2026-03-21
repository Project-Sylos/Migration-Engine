// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// Traversal status values (live table and staging). Stats keys use: pending, successful, failed, not_on_src (DST only).
const (
	StatusPending    = "pending"
	StatusSuccessful = "successful"
	StatusFailed     = "failed"
	StatusNotOnSrc   = "not_on_src" // DST only
	StatusExcluded   = "excluded"
)

// Copy status values (src_nodes only). Stats keys use: pending, successful, failed (no in_progress in stats).
const (
	CopyStatusPending    = "pending"
	CopyStatusInProgress = "in_progress" // transient; not stored in stats
	CopyStatusSuccessful = "successful"
	CopyStatusFailed     = "failed"
	CopyStatusSkipped    = "skipped"
)

// Node type values.
const (
	NodeTypeFolder = "folder"
	NodeTypeFile   = "file"
)
