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

// nodeTableDDL returns CREATE TABLE for src_nodes or dst_nodes (metadata only; no status columns).
// src_nodes includes nullable transfer checkpoint columns (xfer_*) and gpl_state; dst_nodes does not.
// path / parent_path store immutable id ancestry chains (id_path), not display name paths.
// name is the mutable basename and is last so ADD COLUMN on an older DB yields the same
// physical layout as a fresh CREATE (the appender writes values positionally).
func nodeTableDDL(table string) string {
	base := `CREATE TABLE IF NOT EXISTS ` + table + ` (
		id VARCHAR PRIMARY KEY,
		service_id VARCHAR,
		parent_id VARCHAR,
		parent_service_id VARCHAR,
		path VARCHAR,
		parent_path VARCHAR,
		type VARCHAR,
		size BIGINT,
		mtime VARCHAR,
		depth INTEGER NOT NULL`
	if table == TableSrcNodes {
		base += `,
		` + colXferOffset + ` BIGINT,
		` + colXferSrcSize + ` BIGINT,
		` + colXferSrcMTime + ` VARCHAR,
		` + colXferDstRef + ` VARCHAR,
		gpl_state VARCHAR`
	}
	base += `,
		` + colName + ` VARCHAR`
	if table == TableSrcNodes {
		// Last so ADD COLUMN on older DBs matches fresh CREATE physical layout.
		base += `,
		` + colXferResumeToken + ` VARCHAR`
	}
	// display_path then SRC include_only (ALTER-compatible: always append).
	base += `,
		` + colDisplayPath + ` VARCHAR`
	if table == TableSrcNodes {
		base += `,
		` + colIncludeOnly + ` VARCHAR`
	}
	return base + `
	)`
}

// colName is the stored display basename (provider-reported), used by review search,
// name sort, and destination child matching instead of re-deriving it from path.
const colName = "name"

// colDisplayPath is the write-once root-relative name path for filter rules.
const colDisplayPath = "display_path"

// colIncludeOnly is JSON []serviceId allowlist for sparse SRC traversal (null/empty = all).
const colIncludeOnly = "include_only"

// Transfer checkpoint columns are nullable; copy_status stays pending while a checkpoint exists.
// ensureSrcTransferCheckpointColumns ALTERs older DBs that were created before these columns existed.
const (
	ColXferOffset      = "xfer_offset"
	ColXferSrcSize     = "xfer_src_size"
	ColXferSrcMTime    = "xfer_src_mtime"
	ColXferDstRef      = "xfer_dst_ref"
	ColXferResumeToken = "xfer_resume_token"
)

// Deprecated aliases kept for any in-package references during the split.
const (
	colXferOffset      = ColXferOffset
	colXferSrcSize     = ColXferSrcSize
	colXferSrcMTime    = ColXferSrcMTime
	colXferDstRef      = ColXferDstRef
	colXferResumeToken = ColXferResumeToken
)

func dbOpsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableDBOps + ` (
		op VARCHAR NOT NULL,
		sql_text VARCHAR,
		rows BIGINT,
		duration_ns BIGINT NOT NULL,
		err VARCHAR,
		event_time TIMESTAMP NOT NULL DEFAULT current_timestamp
	)`
}

// logsTableDDL returns CREATE TABLE for logs.
func logsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS logs (
		id VARCHAR PRIMARY KEY,
		level VARCHAR,
		message VARCHAR,
		detail VARCHAR,
		component VARCHAR,
		entity VARCHAR,
		entity_id VARCHAR,
		queue VARCHAR,
		created_at TIMESTAMP DEFAULT current_timestamp
	)`
}

// gplIssuesTableDDL is the sparse, current GPL review queue. Hot-path writers
// only insert rows; review mutations update them in place.
func gplIssuesTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableGPLIssues + ` (
		src_id VARCHAR PRIMARY KEY,
		status VARCHAR NOT NULL,
		proposed_name VARCHAR,
		issues_json VARCHAR,
		updated_at BIGINT NOT NULL
	)`
}

func srcCurrentTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableSrcCurrent + ` (
		id VARCHAR PRIMARY KEY,
		traversal_status VARCHAR,
		copy_status VARCHAR,
		delete_status VARCHAR,
		error_log_id VARCHAR,
		gpl_status VARCHAR,
		resolved_dst_name VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL,
		type VARCHAR,
		exclusion_source VARCHAR,
		determining_rule_id VARCHAR
	)`
}

func dstCurrentTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableDstCurrent + ` (
		id VARCHAR PRIMARY KEY,
		traversal_status VARCHAR,
		error_log_id VARCHAR,
		gpl_status VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL,
		type VARCHAR
	)`
}

func gplIssuesDstActionAlter() string {
	return `ALTER TABLE ` + TableGPLIssues + ` ADD COLUMN IF NOT EXISTS dst_action VARCHAR`
}

func srcNodesGPLStateAlter() string {
	return `ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS gpl_state VARCHAR`
}

func statusEventsGPLStatusAlters() []string {
	return []string{
		`ALTER TABLE ` + TableSrcStatusEvents + ` ADD COLUMN IF NOT EXISTS gpl_status VARCHAR`,
		`ALTER TABLE ` + TableDstStatusEvents + ` ADD COLUMN IF NOT EXISTS gpl_status VARCHAR`,
	}
}
