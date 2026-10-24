// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

const (
	TableSrcNodes             = "src_nodes"
	TableDstNodes             = "dst_nodes"
	TableSrcStatusEvents      = "src_status_events"
	TableDstStatusEvents      = "dst_status_events"
	TableGPLIssues            = "gpl_issues"
	TableIDMap                = "id_map"
	TableSrcCurrent           = "src_current"
	TableDstCurrent           = "dst_current"
	TableSrcCurrentDelta      = "src_current_delta"
	TableDstCurrentDelta      = "dst_current_delta"
	TableSrcStats             = "src_stats"
	TableDstStats             = "dst_stats"
	TableStats                = "stats" // universal key/count table for canonical review stats
	TableCopyWorkRoundStats   = "copy_work_round_stats"
	TableDeleteWorkRoundStats = "delete_work_round_stats"
	// TableMigrations is the migrations lifecycle table name (used by schema and migration store).
	TableMigrations = "migrations"
	// TableFSCredentialBinding holds per-side connection id, optional creds file path, and serialized root folder for API rehydration.
	TableFSCredentialBinding = "fs_credential_binding"
	// TableOAuthCredentials stores OAuth refresh token JSON keyed by connection id.
	// Values may be plaintext (tests) or AES-GCM blobs prefixed with enc:v1: (production).
	TableOAuthCredentials     = "oauth_credentials"
	TableRuleEvaluationEvents = "rule_evaluation_events"
	TableFilterApplications   = "filter_applications"
	// TableStatusEventWatermarks stores per-side seal-flush high water (event_time nanos).
	TableStatusEventWatermarks = "status_event_watermarks"
	// TableDBOps is named DuckDB operation timings on the logs sidecar.
	TableDBOps = "db_ops"
)

// Transfer checkpoint columns are nullable; copy_status stays pending while a checkpoint exists.
// ensureSrcTransferCheckpointColumns ALTERs older DBs that were created before these columns existed.
const (
	ColXferOffset      = "xfer_offset"
	ColXferSrcSize     = "xfer_src_size"
	ColXferSrcMTime    = "xfer_src_mtime"
	ColXferDstRef      = "xfer_dst_ref"
	ColXferResumeToken = "xfer_resume_token"
)


